import os
import json
import time
import logging
import hashlib
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

import streamlit as st

# =========================
# 基本設定
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

TZ_TPE = timezone(timedelta(hours=8))
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

sess = requests.Session()
sess.headers.update(HEADERS)

# =========================
# 工具
# =========================
def today_tpe() -> datetime:
    return datetime.now(TZ_TPE)

def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def yyyy_mm_dd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def safe_int(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).replace(",", "").strip()
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
        return float(s)
    except:
        return default

def is_finite_number(x) -> bool:
    try:
        v = float(x)
        return (v == v) and (v not in (float("inf"), float("-inf")))
    except:
        return False

def hash_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

# =========================
# Market Guard（EOD 00:00~15:30 -> 前一日）
# =========================
def is_market_guard_window(now_tpe: datetime) -> bool:
    h, m = now_tpe.hour, now_tpe.minute
    return (h < 15) or (h == 15 and m < 30)

def effective_trade_date_for_eod(target_dt: datetime, now_tpe: datetime):
    """
    回傳：
      effective_dt, is_using_previous_day, previous_trade_date_iso, recency_reason
    """
    if is_market_guard_window(now_tpe):
        prev = target_dt - timedelta(days=1)
        return prev, True, prev.strftime("%Y-%m-%d"), "MARKET_GUARD_EOD_BEFORE_1530"
    return target_dt, False, None, None

# =========================
# 0) TWSE 指數（TWII close / change / pct）
# =========================
def _fetch_twse_json(url: str, params: dict, timeout: int = 20):
    meta = {
        "source_name": None,
        "url": url,
        "params": params,
        "status_code": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "final_url": None,
        "raw_hash": None,
        "error_code": None,
    }
    t0 = time.time()
    try:
        r = sess.get(url, params=params, timeout=timeout)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        meta["latency_ms"] = int((time.time() - t0) * 1000)

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            return None, meta

        txt = r.text or ""
        meta["raw_hash"] = hash_text(txt[:5000])
        j = r.json()
        return j, meta
    except Exception as e:
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        meta["error_code"] = type(e).__name__
        return None, meta

def _parse_twii_from_data_fields(j: dict, name_keywords=None):
    """
    解析 TWSE 常見 JSON： fields + data
    會嘗試找「加權股價指數/發行量加權股價指數/TWII」相關列
    回傳：close(float|None), change(float|None)
    """
    if name_keywords is None:
        name_keywords = ["加權", "發行量加權", "TWII", "TAIEX", "加權股價指數"]

    fields = j.get("fields", []) or []
    data = j.get("data", []) or []
    if not fields or not data:
        return None, None

    # 找「名稱/指數」欄位
    name_col_idx = None
    for i, f in enumerate(fields):
        fs = str(f)
        if ("指數" in fs) or ("名稱" in fs) or ("項目" in fs):
            name_col_idx = i
            break

    # 找「收盤」與「漲跌」欄位
    close_idx = None
    chg_idx = None
    for i, f in enumerate(fields):
        fs = str(f)
        if close_idx is None and ("收盤" in fs or "收市" in fs or fs.strip() == "收盤價"):
            close_idx = i
        if chg_idx is None and ("漲跌" in fs or "增減" in fs or "漲跌點數" in fs):
            chg_idx = i

    # 若找不到 close 欄位，就先放棄
    if close_idx is None:
        return None, None

    def match_name(x: str) -> bool:
        s = str(x)
        return any(k in s for k in name_keywords)

    # 先挑最像加權指數的列
    best_row = None
    if name_col_idx is not None:
        for row in data:
            if len(row) <= close_idx:
                continue
            if match_name(row[name_col_idx]):
                best_row = row
                break

    # 若沒有名稱欄位或沒命中，就退而求其次：直接取第一列（有些 endpoint 只有加權）
    if best_row is None and len(data) >= 1:
        best_row = data[0]

    if best_row is None or len(best_row) <= close_idx:
        return None, None

    close = safe_float(best_row[close_idx], default=None)
    chg = safe_float(best_row[chg_idx], default=None) if (chg_idx is not None and len(best_row) > chg_idx) else None
    return close, chg

@st.cache_data(ttl=60)
def fetch_twii_from_twse(trade_date_yyyymmdd: str):
    """
    依序嘗試多個 TWSE 指數 endpoint，成功即回傳：
      twii = {last_dt, close, change, pct}
      meta = {source_name, ...}
    """
    candidates = [
        # 1) FMTQIK（常見可拿到指數資訊）
        ("TWSE_FMTQIK", "https://www.twse.com.tw/exchangeReport/FMTQIK", {"response": "json", "date": trade_date_yyyymmdd}),
        # 2) MI_5MINS_HIST（當日 5 分鐘歷史，常可推收盤/變動）
        ("TWSE_MI_5MINS_HIST", "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST", {"response": "json", "date": trade_date_yyyymmdd}),
        # 3) 另外一個常見路徑（若未來你要擴充可再加）
    ]

    for source_name, url, params in candidates:
        j, meta = _fetch_twse_json(url, params, timeout=20)
        meta["source_name"] = source_name
        if j is None:
            continue

        close, chg = _parse_twii_from_data_fields(j)
        if close is None:
            meta["error_code"] = meta.get("error_code") or "PARSE_NO_CLOSE"
            continue

        pct = (chg / close) if (chg is not None and is_finite_number(close) and close != 0) else None
        twii = {
            "last_dt": datetime.strptime(trade_date_yyyymmdd, "%Y%m%d").date().isoformat(),
            "close": float(close),
            "change": float(chg) if chg is not None else None,
            "pct": float(pct) if pct is not None else None,
        }
        meta["error_code"] = None
        return twii, meta

    # 全部失敗
    return None, {
        "source_name": "TWSE_INDEX_ALL_CANDIDATES",
        "url": None,
        "params": {"date": trade_date_yyyymmdd},
        "status_code": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "final_url": None,
        "raw_hash": None,
        "error_code": "ALL_ENDPOINTS_FAILED",
    }

def find_prev_trade_date_for_twii(effective_dt: datetime, max_lookback_days: int = 14):
    """
    往回找一個能成功拿到 TWII close 的日期
    回傳：yyyymmdd 或 None
    """
    cur = effective_dt - timedelta(days=1)
    for _ in range(max_lookback_days):
        d = yyyymmdd(cur)
        twii, meta = fetch_twii_from_twse(d)
        if twii is not None and meta.get("error_code") is None and twii.get("close") is not None:
            return d
        cur = cur - timedelta(days=1)
    return None

# =========================
# 1) TWSE：STOCK_DAY_ALL（成交額 + TopN + close）
# =========================
def _twse_stock_day_all(trade_date_yyyymmdd: str):
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date_yyyymmdd}
    meta = {
        "source_name": "TWSE_STOCK_DAY_ALL",
        "url": url,
        "params": params,
        "status_code": None,
        "asof_ts": today_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
        "latency_ms": None,
        "final_url": None,
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
        meta["raw_hash"] = hash_text(txt[:5000])

        j = r.json()
        rows = j.get("data", []) or []
        meta["rows"] = len(rows)
        if not rows:
            meta["error_code"] = "EMPTY"
            return None, meta

        df = pd.DataFrame(rows)
        return df, meta
    except Exception as e:
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        meta["error_code"] = type(e).__name__
        return None, meta

def fetch_twse_amount_audit_sum(trade_date_yyyymmdd: str):
    df, meta0 = _twse_stock_day_all(trade_date_yyyymmdd)
    meta = {**meta0}
    meta.update({
        "amount_sum": 0,
        "ok_rows": 0,
        "module_status": "FAIL",
        "confidence": "LOW",
    })
    if df is None or df.empty:
        return None, meta

    amount_sum = 0
    ok_rows = 0
    for _, row in df.iterrows():
        best = None
        for cell in reversed(list(row.values)):
            v = safe_int(cell, default=None)
            if v is not None and v > 0:
                best = v
                break
        if best is not None:
            amount_sum += best
            ok_rows += 1

    meta["amount_sum"] = amount_sum
    meta["ok_rows"] = ok_rows

    if amount_sum < 100_000_000_000:
        meta["error_code"] = "AMOUNT_TOO_LOW"
        return None, meta

    meta["module_status"] = "OK"
    meta["confidence"] = "HIGH"
    return amount_sum, meta

def fetch_twse_topn_by_amount(trade_date_yyyymmdd: str, top_n: int = 20):
    df, meta0 = _twse_stock_day_all(trade_date_yyyymmdd)
    meta = {**meta0}
    meta.update({"module_status": "FAIL", "confidence": "LOW", "top_n": top_n})
    if df is None or df.empty:
        return [], meta

    rows = []
    for _, r in df.iterrows():
        code = str(r.iloc[0]).strip()
        name = str(r.iloc[1]).strip()

        amount = safe_int(r.iloc[3], default=None)
        close = safe_float(r.iloc[7], default=None)

        if amount is None:
            best = None
            for cell in list(r.values):
                v = safe_int(cell, default=None)
                if v is not None and (best is None or v > best):
                    best = v
            amount = best

        if close is None:
            bestf = None
            for cell in reversed(list(r.values)):
                v = safe_float(cell, default=None)
                if v is not None and v > 0:
                    bestf = v
                    break
            close = bestf

        if not code or code == "nan":
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
# 2) TPEX 成交額：Safe Mode（暫留）
# =========================
def tpex_safe_mode_amount():
    return 200_000_000_000, "TPEX_SAFE_MODE_200B"

# =========================
# 3) TWSE T86：三大法人（單日）
# =========================
@st.cache_data(ttl=60)
def fetch_twse_t86(trade_date_yyyymmdd: str, select_type: str = "ALL"):
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
    }
    t0 = time.time()
    try:
        r = sess.get(url, params=params, timeout=20)
        meta["status_code"] = r.status_code
        meta["latency_ms"] = int((time.time() - t0) * 1000)

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            return None, None, meta

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

        summary = {}
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

def find_recent_trade_dates_for_t86(effective_dt: datetime, n: int = 3, max_lookback_days: int = 14):
    found = []
    cur = effective_dt
    for _ in range(max_lookback_days):
        d = yyyymmdd(cur)
        _, _, meta = fetch_twse_t86(d, "ALL")
        if meta.get("error_code") is None:
            found.append(d)
            if len(found) >= n:
                break
        cur = cur - timedelta(days=1)
    return found

def build_t86_net_3d_map(effective_dt: datetime, n_days: int = 3):
    dates = find_recent_trade_dates_for_t86(effective_dt, n=n_days)
    meta = {"dates_used": dates, "error_code": None}
    if len(dates) < n_days:
        meta["error_code"] = "INSUFFICIENT_T86_DAYS"

    per_code_sum = {}
    for d in dates:
        df, _, m = fetch_twse_t86(d, "ALL")
        if m.get("error_code") is not None or df is None or df.empty:
            continue
        if "代號" not in df.columns or "三大法人合計" not in df.columns:
            continue
        for _, r in df.iterrows():
            code = str(r["代號"]).strip()
            v = safe_int(r["三大法人合計"], default=0)
            per_code_sum[code] = per_code_sum.get(code, 0) + int(v)

    return per_code_sum, meta

# =========================
# 4) 組合：市場概況 + TopN stocks（TWSE）
# =========================
def build_market_snapshot(target_date: datetime, top_n: int = 20):
    now = today_tpe()
    effective_dt, is_prev, prev_iso, rec_reason = effective_trade_date_for_eod(target_date, now)
    trade_date = yyyymmdd(effective_dt)

    # TWII (TWSE endpoints)
    twii, twii_meta = fetch_twii_from_twse(trade_date)
    twii_ok = (twii is not None) and (twii_meta.get("error_code") is None)

    # 前一有效交易日 TWII（用於 daily_return_pct_prev）
    prev_trade_yyyymmdd = find_prev_trade_date_for_twii(effective_dt)
    prev_twii = None
    prev_twii_meta = None
    if prev_trade_yyyymmdd is not None:
        prev_twii, prev_twii_meta = fetch_twii_from_twse(prev_trade_yyyymmdd)

    # TWSE amount
    twse_amt, twse_amt_meta = fetch_twse_amount_audit_sum(trade_date)
    twse_ok = twse_amt is not None

    # TopN
    top_rows, top_meta = fetch_twse_topn_by_amount(trade_date, top_n=top_n)
    top_ok = (top_meta.get("module_status") == "OK") and (len(top_rows) > 0)

    # TPEX safe
    tpex_amt, tpex_src = tpex_safe_mode_amount()

    # T86 今日（摘要 + UI 明細）
    t86_df, t86_sum, t86_meta = fetch_twse_t86(trade_date, "ALL")
    t86_ok = (t86_df is not None) and (t86_sum is not None) and (t86_meta.get("error_code") is None)

    # T86 最近 3 個交易日合計（inst_net_3d）
    t86_3d_map, t86_3d_meta = build_t86_net_3d_map(effective_dt, n_days=3)

    snapshot = {
        "trade_date": trade_date,
        "trade_date_iso": effective_dt.strftime("%Y-%m-%d"),
        "query_date_iso": target_date.strftime("%Y-%m-%d"),
        "now_tpe": now.strftime("%Y-%m-%d %H:%M:%S%z"),
        "recency": {
            "is_using_previous_day": is_prev,
            "previous_trade_date": prev_iso,
            "recency_reason": rec_reason,
            "effective_trade_date": effective_dt.strftime("%Y-%m-%d"),
        },

        "twii": {
            "ok": twii_ok,
            "data": twii,
            "meta": twii_meta,
            "prev_trade_yyyymmdd": prev_trade_yyyymmdd,
            "prev_data": prev_twii,
            "prev_meta": prev_twii_meta,
        },

        "market_amount": {
            "amount_twse": twse_amt,
            "amount_tpex": tpex_amt,
            "amount_total": (twse_amt or 0) + (tpex_amt or 0),
            "source_twse": "TWSE_STOCK_DAY_ALL_AUDIT_SUM" if twse_ok else f"TWSE_FAIL:{twse_amt_meta.get('error_code')}",
            "source_tpex": tpex_src,
            "status_twse": "OK" if twse_ok else "FAIL",
            "status_tpex": "ESTIMATED",
            "confidence_twse": "HIGH" if twse_ok else "LOW",
            "confidence_tpex": "LOW",
            "twse_amount_meta": twse_amt_meta,
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
            "df": t86_df,
            "net_3d_map": t86_3d_map,
            "net_3d_meta": t86_3d_meta,
        },

        "integrity": {
            "twii_ok": twii_ok,
            "twse_amount_ok": twse_ok,
            "top_ok": top_ok,
            "t86_ok": t86_ok,
            "tpex_mode": "SAFE_MODE",
        },
    }
    return snapshot

# =========================
# 5) V20.3 最小可運行 JSON（可直接餵 Arbiter）
# =========================
def build_v203_min_json(snapshot: dict, system_params: dict, portfolio: dict, monitoring: dict, session: str = "EOD"):
    rec = snapshot.get("recency", {}) or {}
    ma = snapshot.get("market_amount", {}) or {}
    top = snapshot.get("top", {}) or {}
    t86 = snapshot.get("t86", {}) or {}
    twii_pack = snapshot.get("twii", {}) or {}
    twii = (twii_pack.get("data") or {}) if twii_pack.get("ok") else {}

    warnings = []
    modules = []

    # ---- modules: TWII
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

    # ---- modules: TWSE amount
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
    })

    # ---- modules: TopN
    top_meta = top.get("meta") or {}
    modules.append({
        "name": "TWSE_STOCK_DAY_ALL_TOPN",
        "status": "OK" if top.get("ok") else "FAIL",
        "confidence": "HIGH" if top.get("ok") else "LOW",
        "asof": snapshot.get("trade_date_iso"),
        "error": top_meta.get("error_code"),
        "latency_ms": top_meta.get("latency_ms"),
        "status_code": top_meta.get("status_code"),
        "raw_hash": top_meta.get("raw_hash"),
        "final_url": top_meta.get("final_url"),
    })

    # ---- modules: TPEX safe
    modules.append({
        "name": "TPEX_SAFE_MODE",
        "status": "ESTIMATED",
        "confidence": "LOW",
        "asof": snapshot.get("trade_date_iso"),
        "error": "SAFE_MODE_200B",
    })

    # ---- modules: T86
    t86_meta = t86.get("meta") or {}
    modules.append({
        "name": "TWSE_T86",
        "status": "OK" if t86.get("ok") else "FAIL",
        "confidence": "HIGH" if t86.get("ok") else "LOW",
        "asof": snapshot.get("trade_date_iso"),
        "error": t86_meta.get("error_code"),
        "latency_ms": t86_meta.get("latency_ms"),
        "status_code": t86_meta.get("status_code"),
    })

    # ---- modules: T86_3D
    t86_3d_meta = t86.get("net_3d_meta") or {}
    modules.append({
        "name": "TWSE_T86_NET_3D",
        "status": "OK" if (t86_3d_meta.get("error_code") is None) else "DEGRADED",
        "confidence": "HIGH" if (t86_3d_meta.get("error_code") is None) else "LOW",
        "asof": snapshot.get("trade_date_iso"),
        "error": t86_3d_meta.get("error_code"),
        "dates_used": t86_3d_meta.get("dates_used"),
    })
    if t86_3d_meta.get("error_code") is not None:
        warnings.append(f"t86_3d degraded: {t86_3d_meta.get('error_code')} dates_used={t86_3d_meta.get('dates_used')}")

    # ---- macro：vix/smr 暫未提供（保留 null + warning）
    vix = None
    smr = None
    warnings.append("macro.overview.vix is null (not provided by data layer).")
    warnings.append("macro.overview.smr is null (not provided by data layer).")

    # ---- twii_close + daily_return_pct
    twii_close = safe_float(twii.get("close"), default=None)
    daily_return_pct = safe_float(twii.get("pct"), default=None)

    # ---- daily_return_pct_prev：用前一有效交易日 close 推算（若能取到）
    daily_return_pct_prev = None
    prev_trade_yyyymmdd = twii_pack.get("prev_trade_yyyymmdd")
    prev_twii = twii_pack.get("prev_data") or {}
    prev_close = safe_float(prev_twii.get("close"), default=None) if prev_twii else None
    if twii_close is not None and prev_close is not None and prev_close != 0:
        daily_return_pct_prev = (twii_close - prev_close) / prev_close
    else:
        warnings.append("macro.overview.daily_return_pct_prev is null (prev TWII close not available).")

    # ---- kill switch：twii_close 缺失就 kill（符合 L1 F1）
    kill = (twii_close is None)
    if kill:
        warnings.append("macro.integrity.kill TRUE because macro.overview.twii_close is null (L1 F1).")

    meta = {
        "timestamp": snapshot.get("now_tpe"),
        "session": session,
        "market_status": "NORMAL",
        "confidence_level": "HIGH",
        "is_using_previous_day": bool(rec.get("is_using_previous_day", False)),
        "effective_trade_date": rec.get("effective_trade_date"),
        "previous_trade_date": rec.get("previous_trade_date"),
        "recency_reason": rec.get("recency_reason"),
        "war_time_override": False,
        "schema_version": "V20.3",
    }

    if meta["is_using_previous_day"]:
        if not meta.get("previous_trade_date") or not meta.get("recency_reason"):
            warnings.append("meta.is_using_previous_day=true but previous_trade_date or recency_reason missing (violates V20.3).")

    macro = {
        "integrity": {"kill": kill},
        "overview": {
            "twii_close": twii_close,
            "vix": vix,
            "smr": smr,
            "daily_return_pct": daily_return_pct,
            "daily_return_pct_prev": daily_return_pct_prev,
            "max_equity_allowed_pct": safe_float(system_params.get("max_equity_allowed_pct"), default=0.55),
        },
        "market_amount": {
            "amount_twse": ma.get("amount_twse"),
            "amount_tpex": ma.get("amount_tpex"),
            "amount_total": ma.get("amount_total"),
            "source_twse": ma.get("source_twse"),
            "source_tpex": ma.get("source_tpex"),
            "status_twse": ma.get("status_twse"),
            "status_tpex": ma.get("status_tpex"),
            "confidence_twse": ma.get("confidence_twse"),
            "confidence_tpex": ma.get("confidence_tpex"),
        },
        "institutional_market": {
            "t86_ok": bool(t86.get("ok")),
            "t86_summary": (t86.get("summary") or {}),
        }
    }

    portfolio_min = {
        "equity": safe_float((portfolio or {}).get("equity"), default=2_000_000),
        "drawdown_pct": safe_float((portfolio or {}).get("drawdown_pct"), default=-0.06),
        "loss_streak": safe_int((portfolio or {}).get("loss_streak"), default=0),
        "alpha_prev": safe_float((portfolio or {}).get("alpha_prev"), default=0.45),
    }

    monitoring_min = {
        "regime_predictive_score": safe_float((monitoring or {}).get("regime_predictive_score"), default=0.60),
        "regime_outcome_score": safe_float((monitoring or {}).get("regime_outcome_score"), default=0.60),
        "trade_count_20d": safe_int((monitoring or {}).get("trade_count_20d"), default=0),
        "effective_trades": safe_int((monitoring or {}).get("effective_trades"), default=0),
        "effective_trade_lookback": safe_int((monitoring or {}).get("effective_trade_lookback"), default=None),
        "effective_trade_winrate": safe_float((monitoring or {}).get("effective_trade_winrate"), default=None),
        "effective_trade_avg_r": safe_float((monitoring or {}).get("effective_trade_avg_r"), default=None),
    }
    if monitoring_min["effective_trades"] == 0:
        warnings.append("monitoring.effective_trades=0 (ETF gate will cap Trust until provided).")

    # ---- stocks（TopN from TWSE）
    stocks = []
    t86_3d_map = t86.get("net_3d_map") or {}
    default_stop = safe_float(system_params.get("default_stop_distance_pct"), default=0.06)
    if not is_finite_number(default_stop) or default_stop <= 0:
        default_stop = 0.06
        warnings.append("system_params.default_stop_distance_pct invalid -> forced 0.06")

    top_rows = (top.get("rows") or [])
    if not top_rows:
        warnings.append("stocks[] empty (TopN not available).")
    else:
        for it in top_rows:
            code = it["code"]
            name = it["name"]
            price = safe_float(it["close"], default=None)
            if price is None or price <= 0:
                continue

            inst_3d = t86_3d_map.get(code, None)
            stocks.append({
                "symbol": f"{code}.TW",
                "name": name,
                "price": float(price),
                "signals": {"slope5": None, "acceleration": None},
                "institutional": {
                    "inst_status": "OK",
                    "inst_net_3d": int(inst_3d) if inst_3d is not None else None
                },
                "risk": {"stop_distance_pct": float(default_stop)},
                "liquidity": {"amount": int(it.get("amount", 0))}
            })

    out = {
        "meta": meta,
        "macro": macro,
        "portfolio": portfolio_min,
        "monitoring": monitoring_min,
        "system_params": system_params,
        "stocks": stocks,
        "audit": {
            "modules": modules,
            "warnings": warnings,
            "build": {
                "query_date": snapshot.get("query_date_iso"),
                "effective_trade_date": snapshot.get("trade_date_iso"),
                "trade_date_yyyymmdd": snapshot.get("trade_date"),
            }
        }
    }
    return out

# =========================
# UI 格式化
# =========================
def fmt_money(n):
    if n is None:
        return "—"
    return f"{int(n):,}"

def fmt_num(n):
    if n is None:
        return "—"
    return f"{int(n):,}"

# =========================
# Streamlit App
# =========================
def app():
    st.set_page_config(page_title="Sunhero 的股市智能超盤", layout="wide")
    st.title("Sunhero 的股市智能超盤（TWSE-only / V20.3 JSON Export + TWII Endpoint）")

    with st.sidebar:
        st.subheader("更新設定")
        default_date = today_tpe().date()
        d = st.date_input("目標日期（交易日）", value=default_date)

        top_n = st.number_input("TopN（依成交金額）", min_value=5, max_value=50, value=20, step=5)

        if st.button("立即更新", type="primary"):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption("規則：TWSE 指數 endpoint 取 TWII；STOCK_DAY_ALL 取成交額 + TopN；T86 取三大法人；TPEX 成交額暫用 Safe Mode 200B。")

    target_dt = datetime(d.year, d.month, d.day, tzinfo=TZ_TPE)
    snap = build_market_snapshot(target_dt, top_n=int(top_n))

    # ====== 第一列：TWII / 成交額 / 法人 ======
    c1, c2, c3, c4 = st.columns(4)

    twii_pack = snap["twii"]
    twii = twii_pack["data"] or {}
    with c1:
        st.metric(
            "加權指數 TWII（TWSE）",
            f"{twii.get('close'):.2f}" if twii.get("close") is not None else "—",
            f"{twii.get('change'):+.2f}" if twii.get("change") is not None else None,
        )
        st.caption(f"資料日：{twii.get('last_dt','—')}｜來源：{(twii_pack.get('meta') or {}).get('source_name')}")

    ma = snap["market_amount"]
    with c2:
        st.metric("上市成交額（TWSE）", fmt_money(ma.get("amount_twse")))
        st.caption(f"來源：{ma.get('source_twse')}｜信心：{ma.get('confidence_twse')}")

    with c3:
        st.metric("上櫃成交額（TPEX）", fmt_money(ma.get("amount_tpex")))
        st.caption(f"來源：{ma.get('source_tpex')}｜信心：{ma.get('confidence_tpex')}（估算）")

    rec = snap["recency"]
    with c4:
        st.metric("有效交易日", snap.get("trade_date_iso"))
        st.caption(f"is_prev={rec.get('is_using_previous_day')}｜reason={rec.get('recency_reason')}")

    st.divider()

    # ====== 法人摘要 ======
    st.subheader("三大法人（TWSE T86）")
    t86 = snap["t86"]
    if not t86.get("ok"):
        st.error(f"T86 讀取失敗：{(t86.get('meta') or {}).get('error_code')}")
    else:
        s = t86.get("summary", {})
        colA, colB, colC, colD = st.columns(4)
        colA.metric("外資淨買賣超", fmt_num(s.get("外資及陸資(不含外資自營商)")))
        colB.metric("投信淨買賣超", fmt_num(s.get("投信")))
        colC.metric("自營商淨買賣超", fmt_num(s.get("自營商")))
        colD.metric("三大法人合計", fmt_num(s.get("合計")))

        with st.expander("查看 T86 明細（可搜尋/排序）", expanded=False):
            st.dataframe(t86["df"], use_container_width=True, height=520)

    st.divider()

    # ====== TopN ======
    st.subheader(f"Top{int(top_n)}（TWSE 成交金額排序 / STOCK_DAY_ALL）")
    top = snap["top"]
    if not top.get("ok"):
        st.error(f"TopN 讀取失敗：{(top.get('meta') or {}).get('error_code')}")
    else:
        df_top = pd.DataFrame(top["rows"])
        df_top = df_top.rename(columns={"code": "代號", "name": "名稱", "close": "收盤價", "amount": "成交金額"})
        st.dataframe(df_top, use_container_width=True, height=460)

    st.divider()

    # ====== 輸出 V20.3 JSON ======
    st.subheader("輸出：Predator Apex V20.3 最小可運行 JSON（TWSE-only）")

    system_params_v203 = {
        "k_regime": 1.2,
        "lambda_drawdown": 2.0,
        "max_loss_per_trade_pct": 0.02,
        "stress_drawdown_trigger": 0.10,
        "max_equity_allowed_pct": 0.55,

        "min_trades_for_trust": 8,
        "trust_default_when_insufficient": 0.49,
        "trust_attack_scale_low": 0.30,
        "trust_coldstart_ramp_trades": 20,

        "l1_price_min": 1,
        "l1_price_max": 5000,
        "l1_price_median_mult_hi": 50,
        "l1_price_pair_ratio_hi": 200,

        "prev_day_allocation_scale": 0.70,

        "smr_deadzone": 0.05,
        "smr_smoothing_alpha": 0.3,

        "min_effective_trades": 5,
        "effective_trade_winrate_floor": 0.45,
        "effective_trade_lookback": 10,

        "default_stop_distance_pct": 0.06,
    }

    v203_json = build_v203_min_json(
        snapshot=snap,
        system_params=system_params_v203,
        portfolio={},
        monitoring={},
        session="EOD"
    )

    st.json(v203_json)

    v203_str = json.dumps(v203_json, ensure_ascii=False, indent=2)
    st.download_button(
        "下載 V20.3 JSON",
        data=v203_str,
        file_name=f"predator_v203_{snap['trade_date_iso']}.json",
        mime="application/json"
    )

    st.divider()
    st.subheader("稽核狀態（Integrity）")
    st.write({
        "query_date": snap.get("query_date_iso"),
        "effective_trade_date": snap.get("trade_date_iso"),
        "is_using_previous_day": snap["recency"].get("is_using_previous_day"),
        "recency_reason": snap["recency"].get("recency_reason"),
        "twii_ok": snap["integrity"].get("twii_ok"),
        "twse_amount_ok": snap["integrity"].get("twse_amount_ok"),
        "top_ok": snap["integrity"].get("top_ok"),
        "t86_ok": snap["integrity"].get("t86_ok"),
        "kill_switch": v203_json["macro"]["integrity"]["kill"],
    })

    with st.expander("查看 TWII meta", expanded=False):
        st.json(snap["twii"].get("meta"))

    with st.expander("查看 TWSE amount meta", expanded=False):
        st.json(snap["market_amount"].get("twse_amount_meta"))

    with st.expander("查看 TopN meta", expanded=False):
        st.json(snap["top"].get("meta"))

    with st.expander("查看 T86 3D meta", expanded=False):
        st.json(snap["t86"].get("net_3d_meta"))

if __name__ == "__main__":
    app()
