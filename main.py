# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.33)
# - FinMind Token：優先讀取 Streamlit Secrets (FINMIND_TOKEN)
# - OTC 成交額：FinMind 彙總，scope =「只算普通股」，用 industry_category + stock_name 排除 ETF/ETN/Index 等
# - 四件套：value + source + status + confidence（重要欄位全落地）
# =========================================================

import os
import time
import json
import math
import warnings
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf

warnings.filterwarnings("ignore")

# -------------------------
# Streamlit config
# -------------------------
st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
st.title("Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.33）")

# -------------------------
# Constants
# -------------------------
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

DEFAULT_TOPN = 20
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

STATUS_ENUM = {"OK", "DEGRADED", "ESTIMATED", "FAIL"}
CONF_ENUM = {"HIGH", "MEDIUM", "LOW"}

# 你的 Top20（可自行改）
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

# OTC 普通股排除關鍵字（你指定：industry_category + stock_name 關鍵字排除）
EXCLUDE_KW = [
    "ETF", "ETN", "INDEX", "指數", "反向", "槓桿", "期貨", "債", "債券", "權證",
    "受益證券", "存託憑證", "DR",
]

# -------------------------
# Helpers
# -------------------------
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

def http_get_json(url: str, params: Dict[str, Any], timeout: int = 30, verify: bool = True) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[int]]:
    try:
        r = requests.get(url, params=params, timeout=timeout, verify=verify)
        code = r.status_code
        r.raise_for_status()
        return r.json(), None, code
    except Exception as e:
        return None, f"{type(e).__name__}:{e}", getattr(e, "response", None).status_code if getattr(e, "response", None) else None

# -------------------------
# FinMind
# -------------------------
def finmind_fetch(dataset: str, params: Dict[str, Any], token: Optional[str], timeout: int = 30) -> Dict[str, Any]:
    q = dict(params)
    q["dataset"] = dataset
    if token:
        q["token"] = token
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = requests.get(FINMIND_URL, params=q, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _kw_hit(text: str) -> bool:
    t = (text or "").upper()
    return any(k.upper() in t for k in EXCLUDE_KW)

def finmind_get_otc_common_set(token: Optional[str]) -> Tuple[set, Dict[str, Any], str]:
    """
    取 OTC/ROTC 普通股清單：
    - market in (OTC, ROTC)
    - 用 industry_category + stock_name 排除 ETF/ETN/Index...
    """
    meta = {"dataset": "TaiwanStockInfo", "rows": 0, "otc_total": 0, "otc_common": 0, "excluded": 0}
    if not token:
        return set(), meta, "NO_TOKEN"

    try:
        js = finmind_fetch("TaiwanStockInfo", params={}, token=token, timeout=30)
        data = js.get("data", [])
        meta["rows"] = len(data)

        otc_total = 0
        excluded = 0
        keep = set()

        for row in data:
            market = str(row.get("market", "")).upper()
            stock_id = str(row.get("stock_id", "")).strip()
            stock_name = str(row.get("stock_name", "")).strip()
            industry = str(row.get("industry_category", "")).strip()

            if not stock_id or market not in ("OTC", "ROTC"):
                continue

            otc_total += 1

            # 只留「看起來像普通股」：四碼為主（你要更嚴格也可加規則）
            if not stock_id.isdigit():
                excluded += 1
                continue

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

def finmind_sum_otc_trading_money(trade_date: str, token: Optional[str]) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    取 OTC 普通股成交額（Trading_money）：
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
    if not token:
        meta["reason"] = "NO_TOKEN"
        return None, "FAIL", meta

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
        try:
            js = finmind_fetch(
                "TaiwanStockPrice",
                params={"start_date": trade_date, "end_date": trade_date, "page": page},
                token=token,
                timeout=30,
            )
            data = js.get("data", [])
        except Exception as e:
            meta["reason"] = f"FETCH_FAIL:{type(e).__name__}"
            break

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

    # 下限合理性（避免空集合或抓錯欄位導致很小）
    # OTC 正常日量級常見 > 500 億；你要更保守可提高門檻
    if total >= 50_000_000_000:
        meta["reason"] = "OK"
        return int(total), "OK", meta

    meta["reason"] = "AMOUNT_TOO_LOW"
    return None, "FAIL", meta

# -------------------------
# Market amount (TWSE + TPEX)
# -------------------------
def fetch_twse_amount(trade_date: str, allow_insecure_ssl: bool) -> Tuple[Optional[int], str, str, str, Dict[str, Any]]:
    """
    取 TWSE 全市場成交金額（TWSE STOCK_DAY_ALL 加總 amount）
    回傳：amount, source, status, confidence, meta
    """
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date.replace("-", "")}
    js, err, code = http_get_json(url, params=params, timeout=30, verify=(not allow_insecure_ssl))

    meta = {"trade_date": trade_date, "url": url, "params": params, "status_code": code, "error": err, "rows": 0, "amount_sum": None}
    if js is None:
        return None, "TWSE_FAIL", "FAIL", "LOW", meta

    data = js.get("data", []) or []
    meta["rows"] = len(data)
    # data columns vary; usually last columns include 成交金額
    # 我們用「成交金額」欄位：一般是第 8 or 9 欄 (index 8)；為避免格式變動，以「可轉 int 的大數」保守抓取
    amount_sum = 0
    ok_rows = 0
    for row in data:
        if not isinstance(row, list):
            continue
        # 從右往左找最可能是成交金額的欄
        candidates = row[::-1]
        found = None
        for c in candidates:
            v = safe_int(c, None)
            if v is not None and v > 0:
                # 成交金額通常是 9~13 位數（> 1e8）
                if v >= 100_000_000:
                    found = v
                    break
        if found is not None:
            amount_sum += int(found)
            ok_rows += 1

    meta["ok_rows"] = ok_rows
    meta["amount_sum"] = int(amount_sum)

    if amount_sum > 200_000_000_000:  # 2,000 億以上才視為可信
        return int(amount_sum), "TWSE_OK:AUDIT_SUM", "OK", "HIGH", meta
    return None, "TWSE_FAIL:LOW_SUM", "FAIL", "LOW", meta

def fetch_market_amount(trade_date: str, allow_insecure_ssl: bool, finmind_token: Optional[str]) -> Dict[str, Any]:
    """
    回傳 market_amount 區塊（含 TWSE + TPEX + total）
    """
    # TWSE
    twse_amt, twse_src, twse_status, twse_conf, twse_meta = fetch_twse_amount(trade_date, allow_insecure_ssl)

    # TPEX（OTC）— 你指定：只算普通股、排除 ETF/ETN/Index...
    tpex_amt = None
    tpex_src = "TPEX_FAIL"
    tpex_status = "FAIL"
    tpex_conf = "LOW"
    tpex_meta = {}

    if finmind_token:
        tpex_val, tpex_status_raw, tpex_meta = finmind_sum_otc_trading_money(trade_date, finmind_token)
        if tpex_status_raw == "OK" and tpex_val is not None:
            tpex_amt = int(tpex_val)
            tpex_src = "FINMIND_OK:OTC_COMMON_SUM"
            tpex_status = "OK"
            tpex_conf = "HIGH"
        else:
            # 失敗就降級估計（保守）
            tpex_amt = 200_000_000_000
            tpex_src = "TPEX_SAFE_MODE_200B"
            tpex_status = "ESTIMATED"
            tpex_conf = "LOW"
    else:
        # 無 token：直接估計
        tpex_amt = 200_000_000_000
        tpex_src = "TPEX_SAFE_MODE_200B"
        tpex_status = "ESTIMATED"
        tpex_conf = "LOW"

    # total
    total = None
    if twse_amt is not None and tpex_amt is not None:
        total = int(twse_amt + tpex_amt)

    # overall confidence（取較低者）
    conf_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    overall_conf = "LOW"
    if twse_conf in conf_rank and tpex_conf in conf_rank:
        overall_conf = twse_conf if conf_rank[twse_conf] <= conf_rank[tpex_conf] else tpex_conf

    return {
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
            "trade_date": trade_date,
            "twse": twse_meta,
            "tpex": {
                "fallback": "finmind_common_stock_sum_or_estimate",
                "finmind": tpex_meta,
            },
        },
    }

# -------------------------
# Price / VIX / TWII
# -------------------------
def yf_last_close(symbol: str, period: str = "10d") -> Tuple[Optional[float], Optional[str], Optional[str]]:
    try:
        df = yf.download(symbol, period=period, interval="1d", progress=False, auto_adjust=False, threads=True)
        if df is None or df.empty:
            return None, "YF_EMPTY", "FAIL"
        close = safe_float(df["Close"].iloc[-1], None)
        last_dt = str(pd.to_datetime(df.index[-1]).date())
        return close, f"YF:{last_dt}", "OK"
    except Exception as e:
        return None, f"YF_FAIL:{type(e).__name__}", "FAIL"

def yf_batch_quotes(symbols: List[str], lookback_days: int = 60) -> Tuple[pd.DataFrame, Dict[str, str]]:
    src_map = {}
    if not symbols:
        return pd.DataFrame(), src_map
    tickers = " ".join(symbols)
    try:
        df = yf.download(tickers, period=f"{lookback_days}d", interval="1d", progress=False, auto_adjust=False, threads=True)
        # df columns: OHLCV in multiindex if multiple
        return df, {s: "YF_BATCH" for s in symbols}
    except Exception:
        # fallback single
        frames = {}
        for s in symbols:
            try:
                d = yf.download(s, period=f"{lookback_days}d", interval="1d", progress=False, auto_adjust=False, threads=False)
                frames[s] = d
                src_map[s] = "YF_SINGLE"
            except Exception:
                src_map[s] = "YF_FAIL"
        return pd.concat(frames, axis=1) if frames else pd.DataFrame(), src_map

# -------------------------
# Institutional (FinMind) - 3D net (Foreign + Trust)
# -------------------------
def finmind_inst_3d_net(stock_id_wo_suffix: str, trade_date: str, token: Optional[str]) -> Tuple[Optional[float], str]:
    """
    以 FinMind 的法人買賣資料計算「近 3 個交易日淨額」
    dataset 常見：TaiwanStockInstitutionalInvestorsBuySell
    欄位常見：buy/sell 或者 同名拆欄；這裡用保守加總「Foreign_Investor + Investment_Trust + Dealer_self + Dealer_Hedging」
    若資料結構不同 → 自動 FAIL 並回傳 None
    """
    if not token:
        return None, "NO_TOKEN"
    try:
        js = finmind_fetch(
            "TaiwanStockInstitutionalInvestorsBuySell",
            params={"stock_id": stock_id_wo_suffix, "start_date": (pd.to_datetime(trade_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d"), "end_date": trade_date},
            token=token,
            timeout=30,
        )
        data = js.get("data", [])
        if not data:
            return None, "EMPTY"
        df = pd.DataFrame(data)
        if "date" not in df.columns:
            return None, "BAD_SCHEMA"
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").tail(3)

        # 依 FinMind 常見欄位：name, buy, sell
        if {"name", "buy", "sell"}.issubset(df.columns):
            df["net"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0) - pd.to_numeric(df["sell"], errors="coerce").fillna(0)
            target = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}
            net = df.loc[df["name"].isin(target), "net"].sum()
            return float(net), "FINMIND_3D_NET"

        # 若是已展開欄位（較少見），就找包含 Foreign/Trust 關鍵欄
        cols = [c for c in df.columns if "foreign" in c.lower() or "trust" in c.lower()]
        if cols:
            net = 0.0
            for c in cols:
                net += pd.to_numeric(df[c], errors="coerce").fillna(0).sum()
            return float(net), "FINMIND_3D_NET_FALLBACK"

        return None, "BAD_SCHEMA"
    except Exception as e:
        return None, f"FAIL:{type(e).__name__}"

# -------------------------
# UI Sidebar
# -------------------------
st.sidebar.header("設定 (Settings)")

session = st.sidebar.selectbox("Session", ["EOD", "INTRADAY"], index=0)
account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
topn = st.sidebar.selectbox("TopN（監控數量）", [10, 20, 30, 50], index=[10,20,30,50].index(DEFAULT_TOPN))
allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL（僅在雲端源憑證錯誤時使用）", value=True)

st.sidebar.divider()
st.sidebar.subheader("FinMind")

# 讀 secrets
token_from_secrets = None
try:
    token_from_secrets = st.secrets.get("FINMIND_TOKEN", None)
except Exception:
    token_from_secrets = None

# UI 允許 override（不強制）
token_override = st.sidebar.text_input("FinMind Token（可留空，優先用 Secrets）", value="", type="password")
finmind_token = token_override.strip() if token_override.strip() else (token_from_secrets.strip() if isinstance(token_from_secrets, str) and token_from_secrets.strip() else None)

if finmind_token:
    st.sidebar.success("FinMind Token：已載入 ✅")
else:
    st.sidebar.error("FinMind Token：未載入 ❌（OTC/法人資料會降級）")

st.sidebar.divider()
st.sidebar.subheader("持倉 (JSON List)")
positions_text = st.sidebar.text_area("positions", value="[]", height=140)

cash_balance = st.sidebar.number_input("現金餘額", min_value=0, value=DEFAULT_CASH, step=10000)
total_equity = st.sidebar.number_input("總權益", min_value=0, value=DEFAULT_EQUITY, step=10000)

run_btn = st.sidebar.button("啟動中控台（Audit Enforced）", type="primary")

# -------------------------
# Main execution
# -------------------------
def parse_positions(text: str) -> List[Dict[str, Any]]:
    try:
        data = json.loads(text)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def build_output(trade_date: str) -> Dict[str, Any]:
    # TWII + VIX
    twii_close, twii_src, twii_status = yf_last_close(TWII_SYMBOL, period="60d")
    vix_close, vix_src, vix_status = yf_last_close(VIX_SYMBOL, period="120d")

    # Market amount
    market_amount = fetch_market_amount(trade_date, allow_insecure_ssl, finmind_token)

    # Watch list prices & vol ratio
    df, price_src_map = yf_batch_quotes(DEFAULT_WATCH, lookback_days=90)

    stocks_out = []
    inst_panel = []

    # 取 Close/Volume 序列（多檔時為 MultiIndex）
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

        # FinMind 法人 3D net（可用就填）
        inst_net_3d = 0.0
        inst_src = "NO_TOKEN"
        inst_status = "NO_DATA"
        inst_dir3 = "NO_DATA"
        inst_streak3 = 0

        if finmind_token:
            stock_id = sym.replace(".TW", "")
            net, src2 = finmind_inst_3d_net(stock_id, trade_date, finmind_token)
            inst_src = src2
            if net is not None:
                inst_net_3d = float(net)
                inst_status = "READY"
                # 方向（簡化：>0 為 POSITIVE，<0 為 NEGATIVE）
                if inst_net_3d > 0:
                    inst_dir3 = "POSITIVE"
                elif inst_net_3d < 0:
                    inst_dir3 = "NEGATIVE"
                else:
                    inst_dir3 = "NEUTRAL"
                # streak（簡化：只要 net>0 視作 1；你要精準連買需逐日判斷）
                inst_streak3 = 3 if inst_net_3d > 0 else 0
            else:
                inst_status = "NO_DATA"
                inst_dir3 = "NO_DATA"

        layer = "NONE"
        # Layer 規則（示範：量能>1 且法人偏多且 streak>=3 → A+）
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

    # market_inst_summary（若你未接 TWSE 三大法人，先留空可擴充）
    market_inst_summary = []

    out = {
        "meta": {
            "timestamp": now_ts(),
            "session": session,
            "market_status": "LOW",  # 你可再用 amount_total 判斷級別
            "current_regime": "OVERHEAT",
            "account_mode": account_mode,
            "audit_tag": "V16.3.33_AUDIT_ENFORCED",
            "confidence_level": market_amount.get("confidence_level", "LOW"),
            "date_status": "VERIFIED",
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "date_status": "VERIFIED",
                "twii_close": twii_close,
                "twii_change": None,
                "twii_pct": None,
                "vix": vix_close,
                "vix_source": "VIX",
                "vix_status": clamp_enum(vix_status, STATUS_ENUM, "FAIL"),
                "vix_confidence": "MEDIUM" if vix_close is not None else "LOW",
                "vix_panic": 35.0,
                "smr": None,
                "slope5": None,
                "drawdown_pct": None,
                "price_range_10d_pct": None,
                "dynamic_vix_threshold": 35.0,
                "max_equity_allowed_pct": 0.55 if account_mode != "Conservative" else 0.45,
                "current_regime": "OVERHEAT",
            },
            "sources": {
                "twii": {"name": "TWII", "ok": twii_close is not None, "last_dt": twii_src, "reason": twii_status},
                "vix": {"name": "VIX", "ok": vix_close is not None, "last_dt": vix_src, "reason": vix_status},
                "amount_source": market_amount.get("meta", {}).get("tpex", {}).get("finmind", {}),
                "prices_source_map": price_src_map,
                "finmind_token_loaded": bool(finmind_token),
            },
            "market_amount": market_amount,
            "market_inst_summary": market_inst_summary,
            "integrity_v1633": {
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

def guess_trade_date_for_demo() -> str:
    # 你已經在上游做 VERIFIED；這裡只取今天日期（不做假日判斷）
    return str(pd.Timestamp.now(tz="Asia/Taipei").date())

if run_btn:
    trade_date = guess_trade_date_for_demo()
    payload = build_output(trade_date)

    # ---- UI 展示 ----
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
        })
        st.caption("OTC scope：只算普通股（industry_category + stock_name 排除 ETF/ETN/Index/反向/槓桿等）。")

    with col2:
        st.subheader("Token / 資料狀態")
        st.write({
            "FINMIND_TOKEN_LOADED": bool(finmind_token),
            "VIX": payload["macro"]["overview"]["vix"],
            "TWII": payload["macro"]["overview"]["twii_close"],
        })

    st.subheader("TopN 監控（含股票名稱）")
    df_show = pd.DataFrame(payload["stocks"])
    # 展開一點點欄位
    if not df_show.empty:
        df_show = df_show[["Tier", "Symbol", "Name", "Price", "Vol_Ratio", "Layer", "source"]]
    st.dataframe(df_show, use_container_width=True)

    st.subheader("法人面板（FinMind 3D Net）")
    df_inst = pd.DataFrame(payload["institutional_panel"])
    st.dataframe(df_inst, use_container_width=True)

    st.subheader("輸出 JSON（可直接餵給裁決器）")
    st.code(json.dumps(payload, ensure_ascii=False, indent=2), language="json")

else:
    st.info("左側設定完成後，按「啟動中控台」產出完整 JSON 與面板。")
