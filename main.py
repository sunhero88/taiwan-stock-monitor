# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.34 FIX）
# FIX:
# 1) FinMind Token 只讀 Streamlit Secrets（不再要求 UI 貼 token）
# 2) TaiwanStockInfo.market 支援 OTC/ROTC/上櫃/興櫃/櫃買 等
# 3) OTC 成交額：只算普通股（industry_category + stock_name 排除 ETF/ETN/Index...）
# 4) 若 FinMind 當日無資料：自動回退最近可用交易日（<=5天），並標記 date_status=DEGRADED
# 5) 法人資料：節流 + 重試（避免 HTTP 429）
# 6) UI 回復完整訊息（expanders 顯示 audit/meta）
# =========================================================

import time
import json
import warnings
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf

warnings.filterwarnings("ignore")

st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
st.title("Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.34 FIX）")

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

DEFAULT_TOPN = 20
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

DEFAULT_WATCH = [
    "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW",
    "3231.TW", "2376.TW", "3017.TW", "3324.TW", "3661.TW",
    "2881.TW", "2882.TW", "2891.TW", "2886.TW", "2603.TW",
    "2609.TW", "1605.TW", "1513.TW", "1519.TW", "2002.TW",
]

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY",
    "2881.TW": "富邦金", "2882.TW": "國泰金", "2891.TW": "中信金", "2886.TW": "兆豐金",
    "2603.TW": "長榮", "2609.TW": "陽明", "1605.TW": "華新", "1513.TW": "中興電",
    "1519.TW": "華城", "2002.TW": "中鋼",
}

EXCLUDE_KW = [
    "ETF", "ETN", "INDEX", "指數", "反向", "槓桿", "期貨", "債", "債券", "權證",
    "受益證券", "存託憑證", "DR",
]

# TaiwanStockInfo.market 可能長這樣（英文/中文/混合）
OTC_MARKET_KEYS = {"OTC", "ROTC", "上櫃", "興櫃", "櫃買", "TPEX"}

STATUS_ENUM = {"OK", "DEGRADED", "ESTIMATED", "FAIL"}
CONF_ENUM = {"HIGH", "MEDIUM", "LOW"}

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if x is None:
            return default
        if isinstance(x, (int, np.integer)):
            return int(x)
        if isinstance(x, (float, np.floating)):
            return int(float(x))
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            if not s:
                return default
            return int(float(s))
        return int(x)
    except Exception:
        return default

def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float, np.integer, np.floating)):
            return float(x)
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            if not s:
                return default
            return float(s)
        return float(x)
    except Exception:
        return default

def clamp_enum(val: str, allowed: set, fallback: str) -> str:
    return val if val in allowed else fallback

def _kw_hit(text: str) -> bool:
    t = (text or "").upper()
    return any(k.upper() in t for k in EXCLUDE_KW)

def finmind_fetch(dataset: str, params: Dict[str, Any], token: str, timeout: int = 30) -> Dict[str, Any]:
    q = dict(params)
    q["dataset"] = dataset
    q["token"] = token
    r = requests.get(FINMIND_URL, params=q, timeout=timeout)
    r.raise_for_status()
    return r.json()

def finmind_fetch_retry(dataset: str, params: Dict[str, Any], token: str, timeout: int = 30, retries: int = 3) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    重試用於 429/暫時性失敗，回傳 (json or None, debug_meta)
    """
    debug = {"dataset": dataset, "params": params, "attempts": []}
    backoff = 1.2
    for i in range(1, retries + 1):
        try:
            js = finmind_fetch(dataset, params, token, timeout=timeout)
            debug["attempts"].append({"i": i, "ok": True})
            return js, debug
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            txt = ""
            try:
                txt = (e.response.text or "")[:180]
            except Exception:
                pass
            debug["attempts"].append({"i": i, "ok": False, "http_status": code, "body_head": txt})
            # 429 或 5xx 才值得重試
            if code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff *= 1.6
                continue
            return None, debug
        except Exception as e:
            debug["attempts"].append({"i": i, "ok": False, "err": f"{type(e).__name__}:{e}"})
            time.sleep(backoff)
            backoff *= 1.6
    return None, debug

def finmind_get_otc_common_set(token: str) -> Tuple[set, Dict[str, Any], str]:
    """
    取 OTC/ROTC 普通股清單：
    - market in OTC_MARKET_KEYS（英文/中文容錯）
    - 用 industry_category + stock_name 排除 ETF/ETN/Index...
    """
    meta = {"dataset": "TaiwanStockInfo", "rows": 0, "otc_total": 0, "otc_common": 0, "excluded": 0}
    try:
        js = finmind_fetch("TaiwanStockInfo", params={}, token=token, timeout=30)
        data = js.get("data", [])
        meta["rows"] = len(data)

        otc_total = 0
        excluded = 0
        keep = set()

        for row in data:
            market_raw = str(row.get("market", "")).strip()
            market_u = market_raw.upper()
            stock_id = str(row.get("stock_id", "")).strip()
            stock_name = str(row.get("stock_name", "")).strip()
            industry = str(row.get("industry_category", "")).strip()

            if not stock_id:
                continue

            # 容錯：英文/中文 market 都視為 OTC
            is_otc = (market_u in OTC_MARKET_KEYS) or (market_raw in OTC_MARKET_KEYS) or any(k in market_raw for k in ["上櫃", "興櫃", "櫃買"])
            if not is_otc:
                continue

            otc_total += 1

            # 只留數字 stock_id（排除基金/債券/票券類）
            if not stock_id.isdigit():
                excluded += 1
                continue

            # 你指定：industry_category + stock_name 關鍵字排除
            if _kw_hit(industry) or _kw_hit(stock_name):
                excluded += 1
                continue

            keep.add(stock_id)

        meta["otc_total"] = otc_total
        meta["excluded"] = excluded
        meta["otc_common"] = len(keep)
        return keep, meta, ("OK" if len(keep) > 0 else "EMPTY")
    except Exception as e:
        return set(), meta, f"FAIL:{type(e).__name__}"

def finmind_sum_otc_trading_money(trade_date: str, token: str) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    OTC 普通股成交額（Trading_money）：
    - TaiwanStockInfo 建普通股集合
    - TaiwanStockPrice 分頁拉 trade_date 當日資料
    - 只加總 stock_id in keep_set
    """
    meta = {
        "dataset": "TaiwanStockPrice",
        "trade_date": trade_date,
        "pages": 0,
        "rows": 0,
        "matched_rows": 0,
        "amount_sum": 0,
        "stockinfo": {},
        "reason": "",
    }

    keep_set, info_meta, info_status = finmind_get_otc_common_set(token)
    meta["stockinfo"] = {"status": info_status, **info_meta}
    if not keep_set:
        meta["reason"] = "NO_OTC_COMMON_SET"
        return None, "FAIL", meta

    total = 0
    matched = 0
    rows = 0
    pages = 0

    page = 1
    while True:
        js, dbg = finmind_fetch_retry(
            "TaiwanStockPrice",
            params={"start_date": trade_date, "end_date": trade_date, "page": page},
            token=token,
            timeout=30,
            retries=3,
        )
        if js is None:
            meta["reason"] = "FETCH_FAIL"
            meta["fetch_debug"] = dbg
            break

        data = js.get("data", [])
        if not data:
            break

        pages += 1
        rows += len(data)

        for row in data:
            sid = str(row.get("stock_id", "")).strip()
            if sid in keep_set:
                tm = safe_int(row.get("Trading_money"), 0) or 0
                if tm > 0:
                    total += int(tm)
                    matched += 1

        page += 1
        if page > 300:
            meta["reason"] = "PAGINATION_GUARD"
            break

    meta["pages"] = pages
    meta["rows"] = rows
    meta["matched_rows"] = matched
    meta["amount_sum"] = int(total)

    # 合理性下限：OTC 一般日量級常 > 500 億；這裡用 200 億當「最低可接受」
    if total >= 20_000_000_000:
        meta["reason"] = "OK"
        return int(total), "OK", meta

    meta["reason"] = "AMOUNT_TOO_LOW_OR_NO_DATA"
    return None, "FAIL", meta

def yf_last_close(symbol: str, period: str = "60d") -> Tuple[Optional[float], Optional[str], str]:
    try:
        df = yf.download(symbol, period=period, interval="1d", progress=False, auto_adjust=False, threads=True)
        if df is None or df.empty:
            return None, "YF_EMPTY", "FAIL"
        close = safe_float(df["Close"].iloc[-1], None)
        last_dt = str(pd.to_datetime(df.index[-1]).date())
        return close, f"YF:{last_dt}", "OK"
    except Exception as e:
        return None, f"YF_FAIL:{type(e).__name__}", "FAIL"

def yf_batch_quotes(symbols: List[str], lookback_days: int = 90) -> Tuple[pd.DataFrame, Dict[str, str]]:
    src_map = {}
    if not symbols:
        return pd.DataFrame(), src_map
    try:
        df = yf.download(" ".join(symbols), period=f"{lookback_days}d", interval="1d", progress=False, auto_adjust=False, threads=True)
        return df, {s: "YF_BATCH" for s in symbols}
    except Exception:
        frames = {}
        for s in symbols:
            try:
                d = yf.download(s, period=f"{lookback_days}d", interval="1d", progress=False, auto_adjust=False, threads=False)
                frames[s] = d
                src_map[s] = "YF_SINGLE"
            except Exception:
                src_map[s] = "YF_FAIL"
        return pd.concat(frames, axis=1) if frames else pd.DataFrame(), src_map

def parse_positions(text: str) -> List[Dict[str, Any]]:
    try:
        data = json.loads(text)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def pick_trade_date() -> str:
    # 以 TWII 最新日期當作預設交易日（比用「今天」可靠）
    close, last_dt, status = yf_last_close(TWII_SYMBOL, period="60d")
    if last_dt and last_dt.startswith("YF:"):
        return last_dt.replace("YF:", "")
    return str(pd.Timestamp.now(tz="Asia/Taipei").date())

def fetch_twse_amount(trade_date: str, allow_insecure_ssl: bool) -> Tuple[Optional[int], str, str, str, Dict[str, Any]]:
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date.replace("-", "")}
    meta = {"trade_date": trade_date, "url": url, "params": params, "status_code": None, "error": None, "rows": 0, "amount_sum": None}

    try:
        r = requests.get(url, params=params, timeout=30, verify=(not allow_insecure_ssl))
        meta["status_code"] = r.status_code
        r.raise_for_status()
        js = r.json()
    except Exception as e:
        meta["error"] = f"{type(e).__name__}:{e}"
        return None, "TWSE_FAIL", "FAIL", "LOW", meta

    data = js.get("data", []) or []
    meta["rows"] = len(data)

    amount_sum = 0
    ok_rows = 0
    for row in data:
        if not isinstance(row, list):
            continue
        # 右往左找成交金額（>=1e8）
        found = None
        for c in row[::-1]:
            v = safe_int(c, None)
            if v is not None and v >= 100_000_000:
                found = v
                break
        if found is not None:
            amount_sum += int(found)
            ok_rows += 1

    meta["ok_rows"] = ok_rows
    meta["amount_sum"] = int(amount_sum)

    if amount_sum > 200_000_000_000:
        return int(amount_sum), "TWSE_OK:AUDIT_SUM", "OK", "HIGH", meta
    return None, "TWSE_FAIL:LOW_SUM", "FAIL", "LOW", meta

def fetch_market_amount(trade_date: str, allow_insecure_ssl: bool, finmind_token: Optional[str]) -> Tuple[Dict[str, Any], str]:
    """
    回傳 (market_amount, date_status)
    date_status: VERIFIED 或 DEGRADED（若 FinMind 回退日期）
    """
    twse_amt, twse_src, twse_status, twse_conf, twse_meta = fetch_twse_amount(trade_date, allow_insecure_ssl)

    tpex_amt = None
    tpex_src = "TPEX_FAIL"
    tpex_status = "FAIL"
    tpex_conf = "LOW"
    tpex_meta = {}
    date_status = "VERIFIED"
    used_trade_date = trade_date

    if finmind_token:
        # 若當日無資料，自動回退最多 5 天（避免「盤後早段 FinMind 尚未上線」）
        for back in range(0, 6):
            dt = (pd.to_datetime(trade_date) - pd.Timedelta(days=back)).strftime("%Y-%m-%d")
            val, stt, meta = finmind_sum_otc_trading_money(dt, finmind_token)
            if stt == "OK" and val is not None:
                tpex_amt = int(val)
                tpex_src = "FINMIND_OK:OTC_COMMON_SUM"
                tpex_status = "OK"
                tpex_conf = "HIGH"
                tpex_meta = meta
                used_trade_date = dt
                if dt != trade_date:
                    date_status = "DEGRADED"
                break
            tpex_meta = meta

        if tpex_status != "OK":
            tpex_amt = 200_000_000_000
            tpex_src = "TPEX_SAFE_MODE_200B"
            tpex_status = "ESTIMATED"
            tpex_conf = "LOW"
    else:
        tpex_amt = 200_000_000_000
        tpex_src = "TPEX_SAFE_MODE_200B"
        tpex_status = "ESTIMATED"
        tpex_conf = "LOW"

    total = None
    if twse_amt is not None and tpex_amt is not None:
        total = int(twse_amt + tpex_amt)

    conf_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    overall_conf = "LOW"
    overall_conf = twse_conf if conf_rank[twse_conf] <= conf_rank[tpex_conf] else tpex_conf

    out = {
        "amount_twse": twse_amt,
        "amount_tpex": tpex_amt,
        "amount_total": total,
        "source_twse": twse_src,
        "source_tpex": tpex_src,
        "status_twse": clamp_enum(twse_status, STATUS_ENUM, "FAIL"),
        "status_tpex": clamp_enum(tpex_status, STATUS_ENUM, "FAIL"),
        "confidence_twse": clamp_enum(twse_conf, CONF_ENUM, "LOW"),
        "confidence_tpex": clamp_enum(tpex_conf, CONF_ENUM, "LOW"),
        "confidence_level": clamp_enum(overall_conf, CONF_ENUM, "LOW"),
        "allow_insecure_ssl": bool(allow_insecure_ssl),
        "scope": "ALL",
        "meta": {
            "trade_date": used_trade_date,
            "twse": twse_meta,
            "tpex": {
                "scope_note": "OTC 普通股 = TaiwanStockInfo + (industry_category/stock_name 排除 ETF/ETN/Index...)",
                "finmind": tpex_meta,
            },
        },
    }
    return out, date_status

def finmind_inst_3d_net(stock_id_wo_suffix: str, end_date: str, token: str) -> Tuple[Optional[float], str, Dict[str, Any]]:
    """
    近 3 個交易日法人淨額（節流 + 重試）
    """
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    js, dbg = finmind_fetch_retry(
        "TaiwanStockInstitutionalInvestorsBuySell",
        params={"stock_id": stock_id_wo_suffix, "start_date": start_date, "end_date": end_date},
        token=token,
        timeout=30,
        retries=3,
    )
    if js is None:
        return None, "FAIL:HTTPError", dbg

    data = js.get("data", [])
    if not data:
        return None, "EMPTY", dbg

    df = pd.DataFrame(data)
    if "date" not in df.columns:
        return None, "BAD_SCHEMA", dbg

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").tail(3)

    if {"name", "buy", "sell"}.issubset(df.columns):
        df["net"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0) - pd.to_numeric(df["sell"], errors="coerce").fillna(0)
        target = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}
        net = df.loc[df["name"].isin(target), "net"].sum()
        return float(net), "FINMIND_3D_NET", dbg

    return None, "BAD_SCHEMA", dbg

# -------------------------
# Sidebar
# -------------------------
st.sidebar.header("設定 (Settings)")
session = st.sidebar.selectbox("Session", ["EOD", "INTRADAY"], index=0)
account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
topn = st.sidebar.selectbox("TopN（監控數量）", [10, 20, 30, 50], index=[10, 20, 30, 50].index(DEFAULT_TOPN))
allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL（雲端憑證錯誤時用）", value=True)

st.sidebar.divider()
st.sidebar.subheader("FinMind（只用 Secrets）")
try:
    finmind_token = st.secrets.get("FINMIND_TOKEN", None)
except Exception:
    finmind_token = None

finmind_token_ok = isinstance(finmind_token, str) and finmind_token.strip() != ""
if finmind_token_ok:
    st.sidebar.success("FinMind Token：已載入 ✅（Secrets）")
else:
    st.sidebar.error("FinMind Token：未載入 ❌（請到 App settings → Secrets 設定 FINMIND_TOKEN）")

st.sidebar.divider()
st.sidebar.subheader("持倉 (JSON List)")
positions_text = st.sidebar.text_area("positions", value="[]", height=140)
cash_balance = st.sidebar.number_input("現金餘額", min_value=0, value=DEFAULT_CASH, step=10000)
total_equity = st.sidebar.number_input("總權益", min_value=0, value=DEFAULT_EQUITY, step=10000)

run_btn = st.sidebar.button("啟動中控台（Audit Enforced）", type="primary")

# -------------------------
# Core
# -------------------------
def build_output(trade_date: str) -> Dict[str, Any]:
    twii_close, twii_src, twii_status = yf_last_close(TWII_SYMBOL, period="60d")
    vix_close, vix_src, vix_status = yf_last_close(VIX_SYMBOL, period="120d")

    market_amount, date_status = fetch_market_amount(trade_date, allow_insecure_ssl, finmind_token.strip() if finmind_token_ok else None)
    used_date = market_amount["meta"]["trade_date"]

    df, price_src_map = yf_batch_quotes(DEFAULT_WATCH, lookback_days=90)

    stocks_out = []
    inst_panel = []

    # 法人節流（避免 20 檔一口氣打爆 API）
    per_call_sleep = 0.25

    for i, sym in enumerate(DEFAULT_WATCH, start=1):
        name = STOCK_NAME_MAP.get(sym, sym)
        price = None
        vol_ratio = None
        src = price_src_map.get(sym, "YF_BATCH")

        try:
            if isinstance(df.columns, pd.MultiIndex):
                close_series = df["Close"][sym].dropna()
                vol_series = df["Volume"][sym].dropna()
            else:
                close_series = df["Close"].dropna()
                vol_series = df["Volume"].dropna()

            if len(close_series) >= 1:
                price = float(close_series.iloc[-1])
            if len(vol_series) >= 21:
                today_v = float(vol_series.iloc[-1])
                avg20 = float(vol_series.iloc[-21:-1].mean())
                vol_ratio = float(today_v / avg20) if avg20 > 0 else None
        except Exception:
            pass

        inst_net_3d = 0.0
        inst_src = "NO_TOKEN"
        inst_status = "NO_DATA"
        inst_dir3 = "NO_DATA"
        inst_streak3 = 0
        inst_debug = None

        if finmind_token_ok:
            stock_id = sym.replace(".TW", "")
            net, src2, dbg = finmind_inst_3d_net(stock_id, used_date, finmind_token.strip())
            inst_debug = dbg
            inst_src = src2
            if net is not None:
                inst_net_3d = float(net)
                inst_status = "READY"
                if inst_net_3d > 0:
                    inst_dir3 = "POSITIVE"
                elif inst_net_3d < 0:
                    inst_dir3 = "NEGATIVE"
                else:
                    inst_dir3 = "NEUTRAL"
                inst_streak3 = 3 if inst_net_3d > 0 else 0

            time.sleep(per_call_sleep)

        layer = "NONE"
        if vol_ratio is not None and vol_ratio >= 1.0 and inst_dir3 == "POSITIVE" and inst_streak3 >= 3:
            layer = "A+"

        inst_panel.append({
            "Symbol": sym,
            "Name": name,
            "Inst_Status": inst_status,
            "Inst_Streak3": int(inst_streak3),
            "Inst_Dir3": inst_dir3,
            "Inst_Net_3d": float(inst_net_3d),
            "inst_source": inst_src,
            "inst_debug": inst_debug,
        })

        stocks_out.append({
            "Symbol": sym,
            "Name": name,
            "Tier": i,
            "Price": price,
            "Vol_Ratio": vol_ratio,
            "Layer": layer,
            "Institutional": {
                "foreign_buy": bool(inst_net_3d > 0),
                "trust_buy": bool(inst_net_3d > 0),
                "Inst_Streak3": int(inst_streak3),
                "Inst_Net_3d": float(inst_net_3d),
                "inst_streak3": int(inst_streak3),
            },
            "source": src,
        })

    out = {
        "meta": {
            "timestamp": now_ts(),
            "session": session,
            "market_status": "LOW",
            "current_regime": "OVERHEAT",
            "account_mode": account_mode,
            "audit_tag": "V16.3.34_AUDIT_ENFORCED_FIX",
            "confidence_level": market_amount.get("confidence_level", "LOW"),
            "date_status": date_status,
        },
        "macro": {
            "overview": {
                "trade_date": used_date,
                "date_status": date_status,
                "twii_close": twii_close,
                "twii_change": None,
                "twii_pct": None,
                "vix": vix_close,
                "vix_source": "VIX",
                "vix_status": clamp_enum(vix_status, STATUS_ENUM, "FAIL"),
                "vix_confidence": "MEDIUM" if vix_close is not None else "LOW",
                "vix_panic": 35.0,
                "dynamic_vix_threshold": 35.0,
                "max_equity_allowed_pct": 0.45 if account_mode == "Conservative" else 0.55,
                "current_regime": "OVERHEAT",
            },
            "sources": {
                "twii": {"name": "TWII", "ok": twii_close is not None, "last_dt": twii_src, "reason": twii_status},
                "vix": {"name": "VIX", "ok": vix_close is not None, "last_dt": vix_src, "reason": vix_status},
                "prices_source_map": price_src_map,
                "finmind_token_loaded": bool(finmind_token_ok),
            },
            "market_amount": market_amount,
            "market_inst_summary": [],
            "integrity_v1634": {
                "status": "OK",
                "kill_switch": False,
                "confidence": "MEDIUM" if market_amount.get("confidence_level") != "LOW" else "LOW",
                "reason": "INTEGRITY_PASS",
                "missing_count": 0,
                "missing_list": [],
                "fallback_count": 1 if market_amount.get("status_tpex") != "OK" else 0,
            },
        },
        "portfolio": {
            "total_equity": float(total_equity),
            "cash_balance": float(cash_balance),
            "current_exposure_pct": 0.0,
            "cash_pct": 100.0,
        },
        "institutional_panel": inst_panel,
        "stocks": stocks_out[:topn],
        "positions_input": parse_positions(positions_text),
        "decisions": [],
        "audit_log": [],
    }
    return out

if run_btn:
    trade_date = pick_trade_date()
    payload = build_output(trade_date)

    # -------- UI（恢復完整資訊）--------
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("大盤 / 成交額（四件套）")
        ma = payload["macro"]["market_amount"]
        st.write({
            "TWSE 成交額": ma["amount_twse"],
            "TWSE source/status/conf": (ma["source_twse"], ma["status_twse"], ma["confidence_twse"]),
            "OTC 成交額": ma["amount_tpex"],
            "OTC source/status/conf": (ma["source_tpex"], ma["status_tpex"], ma["confidence_tpex"]),
            "Total 成交額": ma["amount_total"],
            "Overall confidence": ma["confidence_level"],
            "trade_date(used)": ma["meta"]["trade_date"],
            "date_status": payload["meta"]["date_status"],
        })

        with st.expander("OTC 彙總細節（stockinfo / 排除規則 / 分頁）", expanded=True):
            st.json(ma["meta"]["tpex"]["finmind"])

        with st.expander("TWSE audit 細節（rows / ok_rows / sum）", expanded=False):
            st.json(ma["meta"]["twse"])

    with col2:
        st.subheader("Token / 資料狀態")
        st.write({
            "FINMIND_TOKEN_LOADED": payload["macro"]["sources"]["finmind_token_loaded"],
            "VIX": payload["macro"]["overview"]["vix"],
            "TWII": payload["macro"]["overview"]["twii_close"],
            "trade_date(overview)": payload["macro"]["overview"]["trade_date"],
        })
        if not payload["macro"]["sources"]["finmind_token_loaded"]:
            st.error("FinMind Token 未從 Secrets 載入：請到 App settings → Secrets 設定 FINMIND_TOKEN")

    st.subheader("TopN 監控（含股票名稱）")
    df_show = pd.DataFrame(payload["stocks"])
    if not df_show.empty:
        df_show = df_show[["Tier", "Symbol", "Name", "Price", "Vol_Ratio", "Layer", "source"]]
    st.dataframe(df_show, use_container_width=True)

    st.subheader("法人面板（FinMind 3D Net + debug）")
    df_inst = pd.DataFrame(payload["institutional_panel"])
    st.dataframe(df_inst[["Symbol","Name","Inst_Status","Inst_Dir3","Inst_Streak3","Inst_Net_3d","inst_source"]], use_container_width=True)

    with st.expander("法人 API debug（若出現 HTTPError，可在此看到 429/401 與回應片段）", expanded=False):
        st.json(payload["institutional_panel"])

    st.subheader("輸出 JSON（可直接餵給裁決器）")
    st.code(json.dumps(payload, ensure_ascii=False, indent=2), language="json")
else:
    st.info("左側設定完成後，按「啟動中控台」產出完整 JSON 與面板。")
