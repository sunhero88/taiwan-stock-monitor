# downloader_tw.py
# Predator Data Layer (TW) - Audit-Locked / Tiered Fallback / No Streamlit Dependency
# - TWII: TWSE Index endpoint + tiered fallback (TWSE -> Yahoo -> FinMind)
# - TWSE Amount + TopN: STOCK_DAY_ALL audit sum + ranking
# - TPEX Amount: Tiered fallback (Official JSON -> Pricing HTML -> Estimate -> Constant)
# - Institutional: TWSE T86 (+ optional 3D map helper)
# - Market Guard: EOD before 15:30 (TPE) auto-use previous effective trade day
#
# Output:
# - build_snapshot(): full audit snapshot dict
# - build_v203_min_json(): minimal JSON to feed Arbiter (compatible with your V20.x philosophy)
# - get_market_snapshot(): backward compatible alias

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

TWSE_BASE = "https://www.twse.com.tw"
TPEX_BASE = "https://www.tpex.org.tw"

# Stable constant fallback for TPEX amount (when all official sources fail)
TPEX_SAFE_CONSTANT = 200_000_000_000  # 2000億

# =========================
# HTTP helpers
# =========================
def _requests_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s


SESSION = _requests_session()


def _now_tpe() -> datetime:
    return datetime.now(TZ_TPE)


def _ymd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _to_twse_yyyymmdd(d: str) -> str:
    # input: YYYY-MM-DD -> YYYYMMDD
    return d.replace("-", "")


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        s = s.replace(",", "")
        if s in ("", "--", "—", "None", "nan"):
            return None
        return float(s)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return x
        s = str(x).strip().replace(",", "")
        if s in ("", "--", "—", "None", "nan"):
            return None
        return int(float(s))
    except Exception:
        return None


def _hash_payload(obj: Any) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


# =========================
# Market Guard (EOD)
# =========================
def resolve_effective_trade_date(session: str, target_date: str) -> Tuple[str, bool]:
    """
    If session == EOD and (TPE time is before 15:30) then use previous trade day.
    Return: (effective_trade_date, is_using_previous_day)
    """
    if session != "EOD":
        return target_date, False

    now = _now_tpe()
    # 00:00 ~ 15:30 -> use previous day
    if now.hour < 15 or (now.hour == 15 and now.minute < 30):
        prev = find_prev_trade_date_for_twii(target_date)
        return prev, True

    return target_date, False


# =========================
# TWII (Index) - Tiered Fallback
# =========================
def fetch_twii_from_twse(trade_date: str) -> Dict[str, Any]:
    """
    Primary: TWSE index endpoint.
    Return: {status, source, asof, close, change, pct, latency_ms, error}
    """
    t0 = time.time()
    url = f"{TWSE_BASE}/exchangeReport/MI_INDEX?response=json&date={_to_twse_yyyymmdd(trade_date)}&type=IND"
    try:
        r = SESSION.get(url, timeout=8)
        latency_ms = int((time.time() - t0) * 1000)
        if r.status_code != 200:
            return {
                "status": "FAIL",
                "source": "TWSE_MI_INDEX",
                "asof": trade_date,
                "close": None,
                "change": None,
                "pct": None,
                "latency_ms": latency_ms,
                "error": f"HTTP_{r.status_code}",
            }
        j = r.json()
        # Expected: data1 list; look for "發行量加權股價指數"
        data1 = j.get("data1", [])
        if not data1:
            return {
                "status": "FAIL",
                "source": "TWSE_MI_INDEX",
                "asof": trade_date,
                "close": None,
                "change": None,
                "pct": None,
                "latency_ms": latency_ms,
                "error": "EMPTY_DATA1",
            }

        close = None
        change = None
        pct = None

        # We scan rows to find the TWII line.
        for row in data1:
            if not row or len(row) < 2:
                continue
            # Some variants use row[0] as name
            name = str(row[0]).strip()
            if "發行量加權股價指數" in name or "加權指數" == name:
                # Common column mapping:
                # [0]=name, [1]=close, [2]=change, [3]=pct
                close = _safe_float(row[1]) if len(row) > 1 else None
                change = _safe_float(row[2]) if len(row) > 2 else None
                pct = _safe_float(row[3].replace("%", "")) if len(row) > 3 and isinstance(row[3], str) else _safe_float(row[3]) if len(row) > 3 else None
                break

        if close is None:
            # fallback: try any numeric in row where name contains 指數
            for row in data1:
                if row and len(row) > 1 and "指數" in str(row[0]):
                    c = _safe_float(row[1])
                    if c is not None:
                        close = c
                        break

        if close is None:
            return {
                "status": "FAIL",
                "source": "TWSE_MI_INDEX",
                "asof": trade_date,
                "close": None,
                "change": None,
                "pct": None,
                "latency_ms": latency_ms,
                "error": "TWII_CLOSE_MISSING",
            }

        return {
            "status": "OK",
            "source": "TWSE_MI_INDEX",
            "asof": trade_date,
            "close": close,
            "change": change,
            "pct": pct,
            "latency_ms": latency_ms,
            "error": None,
        }
    except Exception as e:
        latency_ms = int((time.time() - t0) * 1000)
        return {
            "status": "FAIL",
            "source": "TWSE_MI_INDEX",
            "asof": trade_date,
            "close": None,
            "change": None,
            "pct": None,
            "latency_ms": latency_ms,
            "error": type(e).__name__ if str(e) == "" else str(e),
        }


def fetch_twii_from_yahoo(trade_date: str, timeout: int = 8) -> Dict[str, Any]:
    """Yahoo Finance 備援（^TWII）。回傳格式與 fetch_twii_from_twse 類似。"""
    t0 = time.time()
    try:
        import yfinance as yf  # requirements 已含 yfinance
        dt0 = datetime.strptime(trade_date, "%Y-%m-%d").date()
        dt1 = dt0 + timedelta(days=1)
        df = yf.download("^TWII", start=str(dt0), end=str(dt1), progress=False, threads=False, auto_adjust=False)
        if df is None or df.empty:
            raise ValueError("YAHOO_EMPTY")
        close = float(df["Close"].iloc[-1])
        latency_ms = int((time.time() - t0) * 1000)
        return {
            "status": "OK",
            "source": "YAHOO_^TWII",
            "asof": trade_date,
            "close": close,
            "change": None,
            "pct": None,
            "latency_ms": latency_ms,
            "error": None,
        }
    except Exception as e:
        latency_ms = int((time.time() - t0) * 1000)
        return {
            "status": "FAIL",
            "source": "YAHOO_^TWII",
            "asof": trade_date,
            "close": None,
            "change": None,
            "pct": None,
            "latency_ms": latency_ms,
            "error": type(e).__name__ if str(e) == "" else str(e),
        }


def _get_finmind_token() -> Optional[str]:
    """優先讀環境變數，其次讀 streamlit secrets（若存在）。"""
    tok = os.environ.get("FINMIND_TOKEN") or os.environ.get("FINMIND_API_KEY") or os.environ.get("FINMIND_KEY")
    if tok:
        return tok.strip()
    try:
        import streamlit as st  # type: ignore
        tok = st.secrets.get("FINMIND_TOKEN", None)
        if tok:
            return str(tok).strip()
    except Exception:
        pass
    return None


def fetch_twii_from_finmind(trade_date: str, timeout: int = 8) -> Dict[str, Any]:
    """FinMind 備援：盡量以「台股指數」資料集取 TAIEX 收盤。
    注意：FinMind dataset / data_id 可能因帳號權限或版本差異而異，這裡採「多候選」策略。
    """
    t0 = time.time()
    token = _get_finmind_token()
    if not token:
        return {
            "status": "FAIL",
            "source": "FINMIND",
            "asof": trade_date,
            "close": None,
            "change": None,
            "pct": None,
            "latency_ms": int((time.time() - t0) * 1000),
            "error": "FINMIND_TOKEN_MISSING",
        }

    url = "https://api.finmindtrade.com/api/v4/data"
    candidates = [
        {"dataset": "TaiwanStockIndex", "data_id": "TAIEX"},
        {"dataset": "TaiwanStockIndex", "data_id": "TAIEX Index"},
        {"dataset": "TaiwanStockIndex", "data_id": "TAIEX.TW"},
        {"dataset": "TaiwanStockIndex", "data_id": "TSE"},
        {"dataset": "TaiwanStockTotalReturnIndex", "data_id": "TAIEX"},
    ]
    headers = {"Authorization": f"Bearer {token}"}

    last_err: Optional[str] = None
    for c in candidates:
        try:
            params = {
                "dataset": c["dataset"],
                "data_id": c["data_id"],
                "start_date": trade_date,
                "end_date": trade_date,
            }
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code != 200:
                last_err = f"HTTP_{r.status_code}"
                continue
            j = r.json()
            if not isinstance(j, dict) or j.get("status") != 200:
                last_err = f"API_STATUS_{j.get('status')}"
                continue
            data = j.get("data", [])
            if not data:
                last_err = "FINMIND_EMPTY"
                continue
            row = data[-1]
            close = None
            for k in ("close", "value", "index", "price"):
                if k in row and row[k] not in (None, ""):
                    close = float(row[k])
                    break
            if close is None:
                last_err = "FINMIND_CLOSE_MISSING"
                continue

            latency_ms = int((time.time() - t0) * 1000)
            return {
                "status": "OK",
                "source": f"FINMIND:{c['dataset']}:{c['data_id']}",
                "asof": trade_date,
                "close": close,
                "change": None,
                "pct": None,
                "latency_ms": latency_ms,
                "error": None,
            }
        except Exception as e:
            last_err = type(e).__name__ if str(e) == "" else str(e)
            continue

    return {
        "status": "FAIL",
        "source": "FINMIND",
        "asof": trade_date,
        "close": None,
        "change": None,
        "pct": None,
        "latency_ms": int((time.time() - t0) * 1000),
        "error": last_err or "FINMIND_UNKNOWN",
    }


def fetch_twii_tiered(trade_date: str) -> Dict[str, Any]:
    """TWII Tiered Fallback：TWSE → Yahoo → FinMind。"""
    r1 = fetch_twii_from_twse(trade_date)
    if r1.get("status") == "OK" and r1.get("close") is not None:
        r1["tier"] = 1
        return r1

    r2 = fetch_twii_from_yahoo(trade_date)
    if r2.get("status") == "OK" and r2.get("close") is not None:
        r2["tier"] = 2
        r2["prev_fail"] = {"source": r1.get("source"), "error": r1.get("error")}
        return r2

    r3 = fetch_twii_from_finmind(trade_date)
    if r3.get("status") == "OK" and r3.get("close") is not None:
        r3["tier"] = 3
        r3["prev_fail"] = {"source": r2.get("source"), "error": r2.get("error")}
        return r3

    r1["tier"] = 9
    r1["fallback_errors"] = [
        {"source": r1.get("source"), "error": r1.get("error")},
        {"source": r2.get("source"), "error": r2.get("error")},
        {"source": r3.get("source"), "error": r3.get("error")},
    ]
    return r1


def find_prev_trade_date_for_twii(trade_date: str) -> str:
    """
    Find previous trade date by stepping back (up to 10 days) and checking TWII availability.
    Note: uses TWSE first, but can accept that TWSE is down; so we use tiered fallback
    to decide whether a day is a "valid" trading day.
    """
    d = datetime.strptime(trade_date, "%Y-%m-%d").date()
    for _ in range(1, 11):
        d2 = d - timedelta(days=1)
        d = d2
        cand = d.strftime("%Y-%m-%d")
        r = fetch_twii_tiered(cand)
        if r.get("status") == "OK" and r.get("close") is not None:
            return cand
    # fallback: just return trade_date-1 if cannot confirm
    return (datetime.strptime(trade_date, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")


# =========================
# TWSE Amount + TopN
# =========================
def fetch_twse_stock_day_all(trade_date: str) -> Dict[str, Any]:
    """
    Fetch STOCK_DAY_ALL for TWSE:
    - total amount = sum(成交金額)
    - ranking = by 成交金額 desc
    """
    t0 = time.time()
    url = f"{TWSE_BASE}/exchangeReport/STOCK_DAY_ALL?response=json&date={_to_twse_yyyymmdd(trade_date)}"
    try:
        r = SESSION.get(url, timeout=10)
        latency_ms = int((time.time() - t0) * 1000)
        if r.status_code != 200:
            return {"status": "FAIL", "source": "TWSE_STOCK_DAY_ALL", "asof": trade_date, "latency_ms": latency_ms, "error": f"HTTP_{r.status_code}"}
        j = r.json()
        data = j.get("data", [])
        fields = j.get("fields", [])
        if not data or not fields:
            return {"status": "FAIL", "source": "TWSE_STOCK_DAY_ALL", "asof": trade_date, "latency_ms": latency_ms, "error": "EMPTY_DATA"}
        df = pd.DataFrame(data, columns=fields)
        # 欄位名稱可能變動：成交金額/成交金額(元)
        amt_col = None
        for c in df.columns:
            if "成交金額" in c:
                amt_col = c
                break
        if not amt_col:
            return {"status": "FAIL", "source": "TWSE_STOCK_DAY_ALL", "asof": trade_date, "latency_ms": latency_ms, "error": "AMOUNT_COL_MISSING"}
        df[amt_col] = df[amt_col].apply(_safe_int)
        total_amount = int(df[amt_col].fillna(0).sum())
        # code col likely "證券代號"
        code_col = None
        name_col = None
        for c in df.columns:
            if "證券代號" in c:
                code_col = c
            if "證券名稱" in c:
                name_col = c
        if not code_col:
            code_col = df.columns[0]
        if not name_col:
            name_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        df2 = df[[code_col, name_col, amt_col]].copy()
        df2.columns = ["Symbol", "Name", "Amount"]
        df2 = df2.sort_values("Amount", ascending=False).reset_index(drop=True)
        return {
            "status": "OK",
            "source": "TWSE_STOCK_DAY_ALL",
            "asof": trade_date,
            "latency_ms": latency_ms,
            "total_amount": total_amount,
            "top_df": df2,
            "error": None,
        }
    except Exception as e:
        return {
            "status": "FAIL",
            "source": "TWSE_STOCK_DAY_ALL",
            "asof": trade_date,
            "latency_ms": int((time.time() - t0) * 1000),
            "error": type(e).__name__ if str(e) == "" else str(e),
        }


# =========================
# TPEX Amount (tiered)
# =========================
def fetch_tpex_amount_official_json(trade_date: str) -> Dict[str, Any]:
    """Try official TPEX JSON endpoint (may change)."""
    t0 = time.time()
    # This endpoint is known to change; keep as best-effort.
    url = f"{TPEX_BASE}/web/stock/aftertrading/market_highlight/market_highlight_result.php?l=zh-tw&d={trade_date.replace('-', '/')}"
    try:
        r = SESSION.get(url, timeout=10)
        latency_ms = int((time.time() - t0) * 1000)
        if r.status_code != 200:
            return {"status": "FAIL", "source": "TPEX_MARKET_HIGHLIGHT", "asof": trade_date, "latency_ms": latency_ms, "error": f"HTTP_{r.status_code}"}
        j = r.json()
        # look for "tpex_amount" style fields
        raw = json.dumps(j, ensure_ascii=False)
        m = re.search(r"成交金額[^0-9]*([0-9,]+)", raw)
        if not m:
            return {"status": "FAIL", "source": "TPEX_MARKET_HIGHLIGHT", "asof": trade_date, "latency_ms": latency_ms, "error": "PARSE_FAIL"}
        amt = _safe_int(m.group(1))
        if amt is None:
            return {"status": "FAIL", "source": "TPEX_MARKET_HIGHLIGHT", "asof": trade_date, "latency_ms": latency_ms, "error": "AMOUNT_NONE"}
        return {"status": "OK", "source": "TPEX_MARKET_HIGHLIGHT", "asof": trade_date, "latency_ms": latency_ms, "amount": amt, "error": None}
    except Exception as e:
        return {"status": "FAIL", "source": "TPEX_MARKET_HIGHLIGHT", "asof": trade_date, "latency_ms": int((time.time() - t0) * 1000), "error": type(e).__name__ if str(e) == "" else str(e)}


def fetch_tpex_amount_html_parse(trade_date: str) -> Dict[str, Any]:
    """Parse from TPEX index / highlight page HTML as fallback."""
    t0 = time.time()
    url = f"{TPEX_BASE}/web/stock/index_info/idx_main.php?l=zh-tw"
    try:
        r = SESSION.get(url, timeout=10)
        latency_ms = int((time.time() - t0) * 1000)
        if r.status_code != 200:
            return {"status": "FAIL", "source": "TPEX_IDX_HTML", "asof": trade_date, "latency_ms": latency_ms, "error": f"HTTP_{r.status_code}"}
        text = r.text
        # Very loose: find a big number near "成交金額"
        m = re.search(r"成交金額[^0-9]*([0-9,]{6,})", text)
        if not m:
            return {"status": "FAIL", "source": "TPEX_IDX_HTML", "asof": trade_date, "latency_ms": latency_ms, "error": "PARSE_FAIL"}
        amt = _safe_int(m.group(1))
        if amt is None:
            return {"status": "FAIL", "source": "TPEX_IDX_HTML", "asof": trade_date, "latency_ms": latency_ms, "error": "AMOUNT_NONE"}
        return {"status": "OK", "source": "TPEX_IDX_HTML", "asof": trade_date, "latency_ms": latency_ms, "amount": amt, "error": None}
    except Exception as e:
        return {"status": "FAIL", "source": "TPEX_IDX_HTML", "asof": trade_date, "latency_ms": int((time.time() - t0) * 1000), "error": type(e).__name__ if str(e) == "" else str(e)}


def fetch_tpex_amount_tiered(trade_date: str) -> Dict[str, Any]:
    """
    Tiered: official JSON -> HTML parse -> estimate -> safe constant
    """
    r1 = fetch_tpex_amount_official_json(trade_date)
    if r1.get("status") == "OK" and r1.get("amount") is not None:
        r1["tier"] = 1
        return r1
    r2 = fetch_tpex_amount_html_parse(trade_date)
    if r2.get("status") == "OK" and r2.get("amount") is not None:
        r2["tier"] = 2
        r2["prev_fail"] = {"source": r1.get("source"), "error": r1.get("error")}
        return r2

    # estimate (very conservative): if TWSE available, take 0.15 * TWSE total
    # (you can tune this later; keep audit-trace)
    est = None
    try:
        twse = fetch_twse_stock_day_all(trade_date)
        if twse.get("status") == "OK":
            est = int(twse.get("total_amount", 0) * 0.15)
    except Exception:
        est = None

    if est and est > 0:
        return {
            "status": "OK",
            "source": "TPEX_EST_FROM_TWSE",
            "asof": trade_date,
            "amount": est,
            "tier": 3,
            "latency_ms": 0,
            "error": None,
            "prev_fail": {"source": r2.get("source"), "error": r2.get("error")},
        }

    return {
        "status": "OK",
        "source": "TPEX_SAFE_CONSTANT",
        "asof": trade_date,
        "amount": TPEX_SAFE_CONSTANT,
        "tier": 4,
        "latency_ms": 0,
        "error": None,
        "prev_fail": {"source": r2.get("source"), "error": r2.get("error")},
    }


# =========================
# TWSE T86 (Institutional)
# =========================
def fetch_twse_t86(trade_date: str) -> Dict[str, Any]:
    """
    Fetch 3 big institutions net buy/sell from TWSE T86.
    """
    t0 = time.time()
    url = f"{TWSE_BASE}/fund/T86?response=json&date={_to_twse_yyyymmdd(trade_date)}&selectType=ALLBUT0999"
    try:
        r = SESSION.get(url, timeout=10)
        latency_ms = int((time.time() - t0) * 1000)
        if r.status_code != 200:
            return {"status": "FAIL", "source": "TWSE_T86", "asof": trade_date, "latency_ms": latency_ms, "error": f"HTTP_{r.status_code}"}
        j = r.json()
        data = j.get("data", [])
        fields = j.get("fields", [])
        if not data or not fields:
            return {"status": "FAIL", "source": "TWSE_T86", "asof": trade_date, "latency_ms": latency_ms, "error": "EMPTY_DATA"}
        df = pd.DataFrame(data, columns=fields)

        # locate net columns: 三大法人買賣超股數
        # different language variants exist; we approximate:
        # 外資及陸資買賣超股數, 投信買賣超股數, 自營商買賣超股數, 三大法人買賣超股數
        net_cols = [c for c in df.columns if "買賣超" in c and "股數" in c]
        if not net_cols:
            return {"status": "FAIL", "source": "TWSE_T86", "asof": trade_date, "latency_ms": latency_ms, "error": "NET_COLS_MISSING"}

        # Total for "三大法人買賣超股數" if exists; else sum first 3
        total_net = None
        for c in net_cols:
            if "三大法人" in c:
                total_net = int(df[c].apply(_safe_int).fillna(0).sum())
                break
        if total_net is None:
            # sum first 3 net cols
            total_net = 0
            for c in net_cols[:3]:
                total_net += int(df[c].apply(_safe_int).fillna(0).sum())

        return {"status": "OK", "source": "TWSE_T86", "asof": trade_date, "latency_ms": latency_ms, "net_total": total_net, "error": None}
    except Exception as e:
        return {"status": "FAIL", "source": "TWSE_T86", "asof": trade_date, "latency_ms": int((time.time() - t0) * 1000), "error": type(e).__name__ if str(e) == "" else str(e)}


# =========================
# Snapshot builder
# =========================
def build_snapshot(session: str, target_date: str, topn: int = 20) -> Dict[str, Any]:
    """
    Build full snapshot used by UI and arbiter input builder.
    """
    effective_trade_date, is_using_prev = resolve_effective_trade_date(session, target_date)

    # TWII (tiered)
    twii = fetch_twii_tiered(effective_trade_date)

    # TWSE amount + topn
    twse = fetch_twse_stock_day_all(effective_trade_date)

    # TPEX amount (tiered)
    tpex = fetch_tpex_amount_tiered(effective_trade_date)

    # Institutional (T86)
    inst = fetch_twse_t86(effective_trade_date)

    # Compose audit modules
    audit_modules = []

    audit_modules.append({
        "name": "TWSE_TWII_INDEX",
        "status": "OK" if twii.get("status") == "OK" and twii.get("close") is not None else "FAIL",
        "confidence": "HIGH" if twii.get("status") == "OK" and twii.get("close") is not None else "LOW",
        "asof": effective_trade_date,
        "source": twii.get("source"),
        "tier": twii.get("tier"),
        "latency_ms": twii.get("latency_ms"),
        "error": twii.get("error"),
        "fallback_errors": twii.get("fallback_errors"),
    })

    audit_modules.append({
        "name": "TWSE_AMOUNT_TOPN",
        "status": "OK" if twse.get("status") == "OK" else "FAIL",
        "confidence": "HIGH" if twse.get("status") == "OK" else "LOW",
        "asof": effective_trade_date,
        "source": twse.get("source"),
        "latency_ms": twse.get("latency_ms"),
        "error": twse.get("error"),
    })

    audit_modules.append({
        "name": "TPEX_AMOUNT",
        "status": "OK" if tpex.get("status") == "OK" and tpex.get("amount") is not None else "FAIL",
        "confidence": "MED" if tpex.get("tier", 9) <= 2 else ("LOW" if tpex.get("tier", 9) == 3 else "LOW"),
        "asof": effective_trade_date,
        "source": tpex.get("source"),
        "tier": tpex.get("tier"),
        "latency_ms": tpex.get("latency_ms"),
        "error": tpex.get("error"),
        "prev_fail": tpex.get("prev_fail"),
        "amount": tpex.get("amount"),
    })

    audit_modules.append({
        "name": "TWSE_T86",
        "status": "OK" if inst.get("status") == "OK" else "FAIL",
        "confidence": "HIGH" if inst.get("status") == "OK" else "LOW",
        "asof": effective_trade_date,
        "source": inst.get("source"),
        "latency_ms": inst.get("latency_ms"),
        "error": inst.get("error"),
    })

    # totals
    twse_amount = twse.get("total_amount") if twse.get("status") == "OK" else None
    tpex_amount = tpex.get("amount") if tpex.get("status") == "OK" else None
    total_amount = (twse_amount or 0) + (tpex_amount or 0) if (twse_amount is not None or tpex_amount is not None) else None

    # TopN list
    top_list: List[Dict[str, Any]] = []
    if twse.get("status") == "OK":
        df = twse.get("top_df")
        if isinstance(df, pd.DataFrame) and not df.empty:
            df3 = df.head(int(topn)).copy()
            for _, row in df3.iterrows():
                top_list.append({
                    "Symbol": str(row["Symbol"]),
                    "Name": str(row["Name"]),
                    "Amount": int(row["Amount"]) if row["Amount"] is not None else None,
                })

    snapshot = {
        "meta": {
            "timestamp": _now_tpe().strftime("%Y-%m-%d %H:%M:%S"),
            "session": session,
            "target_date": target_date,
            "effective_trade_date": effective_trade_date,
            "is_using_previous_day": bool(is_using_prev),
            "snapshot_hash": None,  # fill later
        },
        "market": {
            "twii_close": twii.get("close"),
            "twii_change": twii.get("change"),
            "twii_pct": twii.get("pct"),
            "twse_amount": twse_amount,
            "tpex_amount": tpex_amount,
            "amount_total": total_amount,
            "inst_net_total": inst.get("net_total") if inst.get("status") == "OK" else None,
            "inst_asof": inst.get("asof"),
        },
        "topn": {
            "n": int(topn),
            "list": top_list,
        },
        "audit_modules": audit_modules,
    }

    snapshot["meta"]["snapshot_hash"] = _hash_payload(snapshot)
    return snapshot


# =========================
# Minimal JSON for Arbiter
# =========================
def build_v203_min_json(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal JSON payload for arbiter.
    Keep your V20 stable philosophy: only include fields needed.
    """
    meta = snapshot.get("meta", {})
    mkt = snapshot.get("market", {})
    topn = snapshot.get("topn", {}).get("list", [])

    payload = {
        "meta": {
            "timestamp": meta.get("timestamp"),
            "session": meta.get("session"),
            "market_status": "NORMAL",
            "confidence_level": "HIGH",
            "is_using_previous_day": meta.get("is_using_previous_day", False),
            "effective_trade_date": meta.get("effective_trade_date"),
            "war_time_override": False,
            "audit_modules": snapshot.get("audit_modules", []),
        },
        "macro": {
            "overview": {
                "twii_close": mkt.get("twii_close"),
                "max_equity_allowed_pct": 0.05,  # keep small in UI build; arbiter may override
            },
            "market_amount": {
                "amount_twse": mkt.get("twse_amount"),
                "amount_tpex": mkt.get("tpex_amount"),
                "amount_total": mkt.get("amount_total"),
            },
            "institutional": {
                "net_total": mkt.get("inst_net_total"),
                "asof": mkt.get("inst_asof"),
            }
        },
        "stocks": [
            {"Symbol": x.get("Symbol"), "Name": x.get("Name"), "Amount": x.get("Amount")}
            for x in topn
        ],
    }
    return payload


# -------------------------------------------------------------------
# Backward-compat entrypoint (older main.py may import this)
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# Backward-compat entrypoint (main.py may call get_market_snapshot(target_iso, session=..., topn=...))
# -------------------------------------------------------------------
def get_market_snapshot(*args, **kwargs) -> Dict[str, Any]:
    """
    兼容多種舊版呼叫方式：
    1) get_market_snapshot(target_iso, session="EOD", topn=20)
    2) get_market_snapshot(target_date="YYYY-MM-DD", session="EOD", topn=20)
    3) get_market_snapshot(session="EOD", target_date="YYYY-MM-DD", topn=20)
    4) get_market_snapshot(session, target_date, topn)  # 你若有別處這樣呼叫也不會炸
    """

    # ---- default ----
    session = kwargs.pop("session", "EOD")
    topn = int(kwargs.pop("topn", 20))

    # 可能的日期參數名稱（你 main.py 用 target_iso）
    target_date = kwargs.pop("target_date", None)
    target_iso = kwargs.pop("target_iso", None)
    date = None

    # 先吃 kwargs
    if target_date:
        date = target_date
    elif target_iso:
        date = target_iso

    # 再吃 args（位置參數）
    # main.py 目前是 get_market_snapshot(target_iso, session=..., topn=...)
    if date is None and len(args) >= 1:
        date = args[0]

    # 也有人可能寫成 get_market_snapshot(session, date, topn)
    # 若第一個 args 看起來像 "EOD"/"INTRADAY"，就交換解析
    if isinstance(date, str) and date in ("EOD", "INTRADAY") and len(args) >= 2:
        session = date
        date = args[1]
        if len(args) >= 3:
            topn = int(args[2])

    if not isinstance(date, str) or len(date) < 8:
        raise TypeError(f"get_market_snapshot 缺少有效日期，收到 date={date}, args={args}, kwargs={kwargs}")

    return build_snapshot(session=session, target_date=date, topn=topn)

