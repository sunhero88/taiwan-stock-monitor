# downloader_tw.py
# Predator Data Layer (TW) - Audit-Locked / Tiered Fallback / No Streamlit Dependency
# - TWII: TWSE Index endpoint (no yfinance)
# - TWSE Amount + TopN: STOCK_DAY_ALL audit sum + ranking
# - TPEX Amount: Tiered fallback (Official JSON -> Pricing HTML -> Estimate -> Constant)
# - Institutional: TWSE T86 (+ optional 3D map helper)
# - Market Guard: EOD before 15:30 (TPE) auto-use previous effective trade day
#
# Output:
# - build_snapshot(): full audit snapshot dict
# - build_v203_min_json(): minimal JSON to feed Arbiter (compatible with your V20.x philosophy)

import os
import json
import time
import re
import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Basic settings
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

TZ_TPE = timezone(timedelta(hours=8))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.twse.com.tw/",
}

# Network retry (critical for GitHub Actions / cross-border networks)
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

sess = build_session()

# =========================
# Helpers
# =========================
def today_tpe() -> datetime:
    return datetime.now(TZ_TPE)

def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def yyyy_mm_dd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def roc_yyy_mm_dd(dt: datetime) -> str:
    # TPEX commonly uses ROC date like 115/02/26
    return f"{dt.year - 1911}/{dt.strftime('%m/%d')}"

def safe_int(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).replace(",", "").strip()
        if s in ("", "--", "nan", "None"):
            return default
        return int(float(s))
    except:
        return default

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(",", "").strip()
        if s in ("", "--", "nan", "None"):
            return default
        return float(s)
    except:
        return default

def hash_text(txt: str) -> str:
    return hashlib.sha256((txt or "").encode("utf-8")).hexdigest()[:16]

def clamp(lo: float, hi: float, x: float) -> float:
    return max(lo, min(hi, x))

# =========================
# Market Guard (EOD date selection)
# =========================
def is_before_eod_cutoff(dt: datetime, cutoff_hhmm: Tuple[int, int] = (15, 30)) -> bool:
    hh, mm = cutoff_hhmm
    return (dt.hour, dt.minute) < (hh, mm)

def resolve_effective_trade_date(session_name: str, now_tpe: Optional[datetime] = None) -> Dict[str, Any]:
    """
    EOD guard:
    - If session == "EOD" and now in 00:00~15:30 (TPE), you must not use "today" data.
    - We do not assume "today is a trading day". We confirm by TWII endpoint success.
    """
    now = now_tpe or today_tpe()
    out = {
        "now_ts": now.strftime("%Y-%m-%d %H:%M:%S%z"),
        "session": session_name,
        "is_using_previous_day": False,
        "effective_trade_date": yyyy_mm_dd(now),
        "effective_trade_date_yyyymmdd": yyyymmdd(now),
        "guard_reason": None,
    }

    if str(session_name).upper() == "EOD" and is_before_eod_cutoff(now):
        out["is_using_previous_day"] = True
        out["guard_reason"] = "EOD_BEFORE_1530_USE_PREV_TRADE_DAY"
        # find previous trading day by probing TWII endpoint
        prev = find_prev_trade_date_for_twii(now)
        if prev:
            # prev is yyyymmdd
            out["effective_trade_date_yyyymmdd"] = prev
            out["effective_trade_date"] = f"{prev[:4]}-{prev[4:6]}-{prev[6:8]}"
        else:
            # worst case: fallback to yesterday calendar day (still audit-marked)
            y = now - timedelta(days=1)
            out["effective_trade_date_yyyymmdd"] = yyyymmdd(y)
            out["effective_trade_date"] = yyyy_mm_dd(y)
            out["guard_reason"] += "|PROBE_FAIL_FALLBACK_YESTERDAY"
    return out

# =========================
# 1) TWII via TWSE Index endpoint (NO yfinance)
# =========================
def fetch_twii_from_twse(trade_date_yyyymmdd: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    TWSE index endpoint:
    https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=YYYYMMDD&type=IND
    We parse "發行量加權股價指數" close + change.
    """
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
    params = {"response": "json", "date": trade_date_yyyymmdd, "type": "IND"}

    meta = {
        "source_name": "TWSE_MI_INDEX",
        "url": url,
        "params": params,
        "status_code": None,
        "final_url": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "rows": 0,
        "raw_hash": None,
        "error_code": None,
    }

    t0 = time.time()
    try:
        r = sess.get(url, params=params, timeout=20)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        meta["latency_ms"] = int((time.time() - t0) * 1000)

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            return None, meta

        txt = r.text or ""
        meta["raw_hash"] = hash_text(txt[:8000])

        j = r.json()
        # common schema: "data1" contains index rows
        data1 = j.get("data1", []) or []
        meta["rows"] = len(data1)
        if not data1:
            meta["error_code"] = "EMPTY"
            return None, meta

        # Find row with TWII name. Columns typically:
        # ["指數", "收盤指數", "漲跌(+/-)", "漲跌點數", ...] or variants.
        # We'll use robust matching.
        target = None
        for row in data1:
            if not row:
                continue
            s0 = str(row[0]).strip()
            if "發行量加權股價指數" in s0 or "加權指數" in s0:
                target = row
                break

        if not target:
            meta["error_code"] = "TWII_ROW_NOT_FOUND"
            return None, meta

        # Try parse close and change points from common positions
        close = safe_float(target[1], None) if len(target) > 1 else None

        # Some schemas include +/- and points separated; points often at index 3
        chg_pts = None
        if len(target) > 3:
            chg_pts = safe_float(target[3], None)
            # if has "+/-" sign in index2, apply it when needed
            sign = str(target[2]).strip() if len(target) > 2 else ""
            if sign == "-" and chg_pts is not None:
                chg_pts = -abs(chg_pts)
        # fallback: try scan all cells for a plausible change number (small magnitude)
        if chg_pts is None:
            for cell in target:
                v = safe_float(cell, None)
                if v is not None and abs(v) < 2000:  # TWII daily change rarely beyond this
                    # keep the last plausible
                    chg_pts = v

        if close is None:
            meta["error_code"] = "TWII_CLOSE_PARSE_FAIL"
            return None, meta

        out = {
            "date": f"{trade_date_yyyymmdd[:4]}-{trade_date_yyyymmdd[4:6]}-{trade_date_yyyymmdd[6:8]}",
            "close": float(close),
            "chg": float(chg_pts) if chg_pts is not None else None,
        }
        if out["chg"] is not None:
            prev = out["close"] - out["chg"]
            out["chg_pct"] = (out["chg"] / prev) if prev not in (0, None) else None
        else:
            out["chg_pct"] = None

        return out, meta

    except Exception as e:
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        meta["error_code"] = type(e).__name__
        return None, meta

def find_prev_trade_date_for_twii(effective_dt: datetime, max_lookback_days: int = 14) -> Optional[str]:
    cur = effective_dt - timedelta(days=1)
    for _ in range(max_lookback_days):
        d = yyyymmdd(cur)
        twii, meta = fetch_twii_from_twse(d)
        if twii is not None and meta.get("error_code") is None and twii.get("close") is not None:
            return d
        cur -= timedelta(days=1)
    return None

# =========================
# 2) TWSE STOCK_DAY_ALL (amount + topN)
# =========================
def _twse_stock_day_all(trade_date_yyyymmdd: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date_yyyymmdd}

    meta = {
        "source_name": "TWSE_STOCK_DAY_ALL",
        "url": url,
        "params": params,
        "status_code": None,
        "final_url": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "rows": 0,
        "error_code": None,
        "raw_hash": None,
    }

    t0 = time.time()
    try:
        r = sess.get(url, params=params, timeout=20)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        meta["latency_ms"] = int((time.time() - t0) * 1000)

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            return None, meta

        txt = r.text or ""
        meta["raw_hash"] = hash_text(txt[:8000])

        j = r.json()
        rows = j.get("data", []) or []
        meta["rows"] = len(rows)
        if not rows:
            meta["error_code"] = "EMPTY"
            return None, meta

        return pd.DataFrame(rows), meta

    except Exception as e:
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        meta["error_code"] = type(e).__name__
        return None, meta

def fetch_twse_amount_audit_sum(trade_date_yyyymmdd: str) -> Tuple[Optional[int], Dict[str, Any]]:
    df, meta0 = _twse_stock_day_all(trade_date_yyyymmdd)
    meta = {**meta0}
    meta.update({"amount_sum": 0, "ok_rows": 0, "module_status": "FAIL", "confidence": "LOW"})

    if df is None or df.empty:
        return None, meta

    amount_sum = 0
    ok_rows = 0

    # STOCK_DAY_ALL each row usually: [code, name, volume, amount, open, high, low, close, chg, ...]
    for _, row in df.iterrows():
        # robust: scan reversed, take first positive int
        best = None
        for cell in reversed(list(row.values)):
            v = safe_int(cell, None)
            if v is not None and v > 0:
                best = v
                break
        if best is not None:
            amount_sum += best
            ok_rows += 1

    meta["amount_sum"] = int(amount_sum)
    meta["ok_rows"] = int(ok_rows)

    # sanity floor: 1000 億 (100_000_000_000)
    if amount_sum < 100_000_000_000:
        meta["error_code"] = "AMOUNT_TOO_LOW"
        return None, meta

    meta["module_status"] = "OK"
    meta["confidence"] = "HIGH"
    return int(amount_sum), meta

def fetch_twse_topn_by_amount(trade_date_yyyymmdd: str, top_n: int = 20) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    df, meta0 = _twse_stock_day_all(trade_date_yyyymmdd)
    meta = {**meta0}
    meta.update({"module_status": "FAIL", "confidence": "LOW", "top_n": int(top_n)})

    if df is None or df.empty:
        meta["error_code"] = meta.get("error_code") or "EMPTY"
        return [], meta

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        code = str(r.iloc[0]).strip() if len(r) > 0 else ""
        name = str(r.iloc[1]).strip() if len(r) > 1 else ""

        # typical columns: amount at idx=3, close at idx=7
        amount = safe_int(r.iloc[3], None) if len(r) > 3 else None
        close = safe_float(r.iloc[7], None) if len(r) > 7 else None

        # fallback parse
        if amount is None:
            best = None
            for cell in list(r.values):
                v = safe_int(cell, None)
                if v is not None and v > 0 and (best is None or v > best):
                    best = v
            amount = best

        if close is None:
            bestf = None
            for cell in reversed(list(r.values)):
                v = safe_float(cell, None)
                if v is not None and v > 0:
                    bestf = v
                    break
            close = bestf

        if not code or code.lower() == "nan":
            continue
        if amount is None or amount <= 0:
            continue
        if close is None or close <= 0:
            continue

        rows.append({"code": code, "name": name, "close": float(close), "amount": int(amount)})

    if not rows:
        meta["error_code"] = "PARSE_EMPTY"
        return [], meta

    rows = sorted(rows, key=lambda x: x["amount"], reverse=True)[: max(1, int(top_n))]
    meta["module_status"] = "OK"
    meta["confidence"] = "HIGH"
    return rows, meta

# =========================
# 3) TPEX amount tiered fallback
# =========================
def fetch_tpex_amount_official_json(trade_dt: datetime) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    Tier-1:
    https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=ROC_DATE&se=EW
    returns JSON includes "集合成交金額" (string with commas)
    """
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    params = {"l": "zh-tw", "d": roc_yyy_mm_dd(trade_dt), "se": "EW"}

    meta = {
        "tier": 1,
        "source_name": "TPEX_ST43_JSON",
        "url": url,
        "params": params,
        "status_code": None,
        "final_url": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "raw_hash": None,
        "error_code": None,
    }

    t0 = time.time()
    try:
        r = sess.get(url, params=params, timeout=20, allow_redirects=True)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        meta["latency_ms"] = int((time.time() - t0) * 1000)

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            return None, meta

        txt = r.text or ""
        meta["raw_hash"] = hash_text(txt[:8000])

        j = r.json()
        val = j.get("集合成交金額")
        amt = safe_int(val, None)
        if amt is None or amt <= 0:
            meta["error_code"] = "FIELD_MISSING_OR_ZERO"
            return None, meta

        return int(amt), meta

    except Exception as e:
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        meta["error_code"] = type(e).__name__
        return None, meta

def fetch_tpex_amount_pricing_html() -> Tuple[Optional[int], Dict[str, Any]]:
    """
    Tier-2:
    pricing.html (summary page), parse "成交金額 xxx 億"
    Note: returns in 億, convert to NTD.
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    meta = {
        "tier": 2,
        "source_name": "TPEX_PRICING_HTML",
        "url": url,
        "params": None,
        "status_code": None,
        "final_url": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "raw_hash": None,
        "error_code": None,
    }

    t0 = time.time()
    try:
        r = sess.get(url, timeout=20)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        meta["latency_ms"] = int((time.time() - t0) * 1000)

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            return None, meta

        txt = r.text or ""
        meta["raw_hash"] = hash_text(txt[:8000])

        # very lightweight regex (avoid bs4 dependency here)
        # match: 成交金額 1,234 億
        m = re.search(r"成交金額\s*([\d,]+)\s*億", txt)
        if not m:
            meta["error_code"] = "PATTERN_NOT_FOUND"
            return None, meta

        yi = safe_int(m.group(1), None)
        if yi is None or yi <= 0:
            meta["error_code"] = "PARSE_FAIL"
            return None, meta

        amt = int(yi) * 100_000_000  # 億 -> NTD
        return amt, meta

    except Exception as e:
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        meta["error_code"] = type(e).__name__
        return None, meta

def tpex_amount_estimate_from_twse(twse_amount: Optional[int], ratio: float = 0.22) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    Tier-3 estimate: tpex ~= twse * ratio
    ratio default 0.22 (as used in your older provider)
    """
    meta = {
        "tier": 3,
        "source_name": "TPEX_ESTIMATE_FROM_TWSE",
        "ratio": ratio,
        "error_code": None,
    }
    if twse_amount is None or twse_amount <= 0:
        meta["error_code"] = "TWSE_AMOUNT_MISSING"
        return None, meta
    return int(twse_amount * float(ratio)), meta

def tpex_amount_constant_safe(constant_amt: int = 200_000_000_000) -> Tuple[int, Dict[str, Any]]:
    """
    Tier-4 constant safe mode
    """
    return int(constant_amt), {
        "tier": 4,
        "source_name": "TPEX_SAFE_CONSTANT",
        "constant": int(constant_amt),
        "error_code": None,
    }

def fetch_tpex_amount_tiered(trade_dt: datetime, twse_amount: Optional[int]) -> Tuple[int, Dict[str, Any]]:
    # Tier-1
    amt1, meta1 = fetch_tpex_amount_official_json(trade_dt)
    if amt1 is not None:
        return int(amt1), meta1

    # Tier-2
    amt2, meta2 = fetch_tpex_amount_pricing_html()
    if amt2 is not None:
        return int(amt2), meta2

    # Tier-3
    amt3, meta3 = tpex_amount_estimate_from_twse(twse_amount, ratio=0.22)
    if amt3 is not None:
        return int(amt3), meta3

    # Tier-4
    amt4, meta4 = tpex_amount_constant_safe(200_000_000_000)
    return int(amt4), meta4

# =========================
# 4) TWSE T86 institutional
# =========================
def fetch_twse_t86(trade_date_yyyymmdd: str, select_type: str = "ALL") -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, int]], Dict[str, Any]]:
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    params = {"response": "json", "date": trade_date_yyyymmdd, "selectType": select_type}

    meta = {
        "source_name": "TWSE_T86",
        "url": url,
        "params": params,
        "status_code": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "rows": 0,
        "error_code": None,
        "final_url": None,
        "raw_hash": None,
    }

    t0 = time.time()
    try:
        r = sess.get(url, params=params, timeout=20)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        meta["latency_ms"] = int((time.time() - t0) * 1000)

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            return None, None, meta

        txt = r.text or ""
        meta["raw_hash"] = hash_text(txt[:8000])

        j = r.json()
        data = j.get("data", []) or []
        fields = j.get("fields", []) or []
        meta["rows"] = len(data)
        if not data or not fields:
            meta["error_code"] = "EMPTY"
            return None, None, meta

        df = pd.DataFrame(data, columns=fields)

        col_code = next((c for c in df.columns if "代號" in c), None)
        col_name = next((c for c in df.columns if "名稱" in c), None)
        col_foreign = next((c for c in df.columns if "外" in c and "買賣超" in c and "不含外資自營商" in c), None)
        col_trust = next((c for c in df.columns if "投信" in c and "買賣超" in c), None)
        col_dealer = next((c for c in df.columns if "自營商" in c and "買賣超" in c), None)
        col_total = next((c for c in df.columns if "三大法人" in c and "買賣超" in c), None)

        for c in [col_foreign, col_trust, col_dealer, col_total]:
            if c and c in df.columns:
                df[c] = df[c].astype(str).str.replace(",", "").str.replace("--", "0")
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        summary: Dict[str, int] = {}
        if col_foreign: summary["外資及陸資(不含外資自營商)"] = int(df[col_foreign].sum())
        if col_trust: summary["投信"] = int(df[col_trust].sum())
        if col_dealer: summary["自營商"] = int(df[col_dealer].sum())
        if col_total: summary["合計"] = int(df[col_total].sum())

        keep = [c for c in [col_code, col_name, col_foreign, col_trust, col_dealer, col_total] if c]
        df_view = df[keep].copy()

        rename = {}
        if col_code: rename[col_code] = "代號"
        if col_name: rename[col_name] = "名稱"
        if col_foreign: rename[col_foreign] = "外資淨買賣超"
        if col_trust: rename[col_trust] = "投信淨買賣超"
        if col_dealer: rename[col_dealer] = "自營商淨買賣超"
        if col_total: rename[col_total] = "三大法人合計"
        df_view = df_view.rename(columns=rename)

        return df_view, summary, meta

    except Exception as e:
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        meta["error_code"] = type(e).__name__
        return None, None, meta

# =========================
# 5) Snapshot builder (full audit)
# =========================
def build_snapshot(target_dt: datetime, session_name: str = "EOD", top_n: int = 20) -> Dict[str, Any]:
    """
    Produce a single snapshot:
    - recency: effective date selection
    - twii: TWSE MI_INDEX
    - market_amount: TWSE sum + TPEX tiered + total
    - top: TopN by amount (TWSE)
    - t86: institutional
    - integrity: module ok flags
    """
    rec = resolve_effective_trade_date(session_name=session_name, now_tpe=target_dt)
    trade_yyyymmdd = rec["effective_trade_date_yyyymmdd"]
    trade_iso = rec["effective_trade_date"]

    # TWII
    twii_data, twii_meta = fetch_twii_from_twse(trade_yyyymmdd)
    twii_ok = (twii_data is not None) and (twii_meta.get("error_code") is None)

    # TWSE amount
    twse_amt, twse_amt_meta = fetch_twse_amount_audit_sum(trade_yyyymmdd)
    twse_ok = twse_amt is not None and twse_amt_meta.get("module_status") == "OK"

    # TPEX tiered amount
    tpex_amt, tpex_meta = fetch_tpex_amount_tiered(
        trade_dt=datetime(int(trade_yyyymmdd[:4]), int(trade_yyyymmdd[4:6]), int(trade_yyyymmdd[6:8]), tzinfo=TZ_TPE),
        twse_amount=twse_amt
    )

    # TopN
    top_rows, top_meta = fetch_twse_topn_by_amount(trade_yyyymmdd, top_n=top_n)
    top_ok = (len(top_rows) > 0) and (top_meta.get("module_status") == "OK")

    # T86
    t86_df, t86_sum, t86_meta = fetch_twse_t86(trade_yyyymmdd, "ALL")
    t86_ok = (t86_df is not None) and (t86_sum is not None) and (t86_meta.get("error_code") is None)

    snapshot = {
        "recency": rec,
        "trade_date": trade_yyyymmdd,
        "trade_date_iso": trade_iso,
        "twii": {
            "ok": twii_ok,
            "data": twii_data or {},
            "meta": twii_meta,
        },
        "market_amount": {
            "amount_twse": int(twse_amt) if twse_amt is not None else None,
            "amount_tpex": int(tpex_amt) if tpex_amt is not None else None,
            "amount_total": int((twse_amt or 0) + (tpex_amt or 0)),
            "status_twse": "OK" if twse_ok else "FAIL",
            "confidence_twse": twse_amt_meta.get("confidence", "LOW") if isinstance(twse_amt_meta, dict) else "LOW",
            "twse_amount_meta": twse_amt_meta,
            "tpex_amount_meta": tpex_meta,
        },
        "top": {
            "ok": top_ok,
            "rows": top_rows,
            "meta": top_meta,
        },
        "t86": {
            "ok": t86_ok,
            "summary": t86_sum or {},
            "meta": t86_meta,
            "df": t86_df.to_dict(orient="records") if isinstance(t86_df, pd.DataFrame) else [],
        },
        "integrity": {
            "twii_ok": twii_ok,
            "twse_amount_ok": twse_ok,
            "tpex_tier": tpex_meta.get("tier"),
            "top_ok": top_ok,
            "t86_ok": t86_ok,
        },
    }
    return snapshot

# =========================
# 6) V20.3 minimal JSON builder (Arbiter input)
# =========================
def build_v203_min_json(snapshot: Dict[str, Any],
                        system_params: Dict[str, Any],
                        portfolio: Dict[str, Any],
                        monitoring: Dict[str, Any],
                        session: str = "EOD") -> Dict[str, Any]:
    rec = snapshot.get("recency", {}) or {}
    ma = snapshot.get("market_amount", {}) or {}
    top = snapshot.get("top", {}) or {}
    t86 = snapshot.get("t86", {}) or {}
    twii_pack = snapshot.get("twii", {}) or {}
    twii = (twii_pack.get("data") or {}) if twii_pack.get("ok") else {}

    # modules audit list (for traceability)
    modules = []

    twii_meta = twii_pack.get("meta") or {}
    modules.append({
        "name": "TWSE_TWII_INDEX",
        "status": "OK" if twii_pack.get("ok") else "FAIL",
        "confidence": "HIGH" if twii_pack.get("ok") else "LOW",
        "asof": snapshot.get("trade_date_iso"),
        "error": twii_meta.get("error_code"),
        "latency_ms": twii_meta.get("latency_ms"),
        "status_code": twii_meta.get("status_code"),
        "raw_hash": twii_meta.get("raw_hash"),
        "final_url": twii_meta.get("final_url"),
        "source_name": twii_meta.get("source_name"),
    })

    amt_meta = ma.get("twse_amount_meta") or {}
    modules.append({
        "name": "TWSE_STOCK_DAY_ALL_AUDIT_SUM",
        "status": "OK" if ma.get("status_twse") == "OK" else "FAIL",
        "confidence": ma.get("confidence_twse", "LOW"),
        "asof": snapshot.get("trade_date_iso"),
        "error": amt_meta.get("error_code"),
        "latency_ms": amt_meta.get("latency_ms"),
        "status_code": amt_meta.get("status_code"),
        "raw_hash": amt_meta.get("raw_hash"),
        "final_url": amt_meta.get("final_url"),
        "source_name": amt_meta.get("source_name"),
    })

    tpex_meta = ma.get("tpex_amount_meta") or {}
    modules.append({
        "name": "TPEX_AMOUNT_TIERED",
        "status": "OK" if tpex_meta.get("tier") in (1, 2) else "ESTIMATED",
        "confidence": "HIGH" if tpex_meta.get("tier") in (1, 2) else ("MED" if tpex_meta.get("tier") == 3 else "LOW"),
        "asof": snapshot.get("trade_date_iso"),
        "tier": tpex_meta.get("tier"),
        "error": tpex_meta.get("error_code"),
        "latency_ms": tpex_meta.get("latency_ms"),
        "status_code": tpex_meta.get("status_code"),
        "raw_hash": tpex_meta.get("raw_hash"),
        "final_url": tpex_meta.get("final_url"),
        "source_name": tpex_meta.get("source_name"),
    })

    top_meta = (top.get("meta") or {})
    modules.append({
        "name": "TWSE_TOPN_BY_AMOUNT",
        "status": "OK" if top.get("ok") else "FAIL",
        "confidence": "HIGH" if top.get("ok") else "LOW",
        "asof": snapshot.get("trade_date_iso"),
        "error": top_meta.get("error_code"),
        "latency_ms": top_meta.get("latency_ms"),
        "status_code": top_meta.get("status_code"),
        "raw_hash": top_meta.get("raw_hash"),
        "final_url": top_meta.get("final_url"),
        "source_name": top_meta.get("source_name"),
    })

    t86_meta = (t86.get("meta") or {})
    modules.append({
        "name": "TWSE_T86",
        "status": "OK" if t86.get("ok") else "FAIL",
        "confidence": "HIGH" if t86.get("ok") else "LOW",
        "asof": snapshot.get("trade_date_iso"),
        "error": t86_meta.get("error_code"),
        "latency_ms": t86_meta.get("latency_ms"),
        "status_code": t86_meta.get("status_code"),
        "raw_hash": t86_meta.get("raw_hash"),
        "final_url": t86_meta.get("final_url"),
        "source_name": t86_meta.get("source_name"),
    })

    # Build stocks[] from top rows (keep minimal: symbol + price + inst)
    stocks = []
    for r in (top.get("rows") or []):
        code = r.get("code")
        if not code:
            continue
        sym = f"{code}.TW" if not str(code).endswith(".TW") else str(code)
        stocks.append({
            "symbol": sym,
            "price": float(r.get("close")),
            "institutional": {
                "inst_status": "OK" if t86.get("ok") else "NO_UPDATE_TODAY",
                "inst_net_3d": None,  # keep None unless you compute 3D elsewhere
            },
            "risk": {
                "stop_distance_pct": None  # leave for downstream enrichment
            },
            "signals": {}
        })

    out = {
        "meta": {
            "timestamp": today_tpe().strftime("%Y-%m-%d %H:%M:%S"),
            "session": session,
            "market_status": "NORMAL",  # you can override upstream
            "confidence_level": "HIGH",  # you can override upstream
            "is_using_previous_day": bool(rec.get("is_using_previous_day")),
            "effective_trade_date": snapshot.get("trade_date_iso"),
            "war_time_override": False,
            "audit_modules": modules,
        },
        "macro": {
            "integrity": {"kill": False},
            "overview": {
                "twii_close": twii.get("close"),
                "daily_return_pct": twii.get("chg_pct"),
                "daily_return_pct_prev": None,
                "max_equity_allowed_pct": system_params.get("max_equity_allowed_pct", 0.55),
                "vix": system_params.get("vix", None),
                "smr": system_params.get("smr", None),
            },
            "market_amount": {
                "amount_twse": ma.get("amount_twse"),
                "amount_tpex": ma.get("amount_tpex"),
                "amount_total": ma.get("amount_total"),
                "source_twse": amt_meta.get("source_name"),
                "source_tpex": tpex_meta.get("source_name"),
                "tpex_tier": tpex_meta.get("tier"),
            }
        },
        "portfolio": portfolio,
        "monitoring": monitoring,
        "system_params": system_params,
        "stocks": stocks,
    }
    return out

# =========================
# CLI helper (optional)
# =========================
def _write_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    # Example CLI usage:
    #   python downloader_tw.py
    # Outputs snapshot.json (audit) for today_tpe()
    now = today_tpe()
    snap = build_snapshot(now, session_name="EOD", top_n=20)
    _write_json("snapshot_tw.json", snap)
    logging.info("Wrote snapshot_tw.json")

if __name__ == "__main__":
    main()
