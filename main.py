# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3 Stable Hybrid）
# V16.3.2 Upgrade:
# ✅ [Fix] 解決 KeyError: 'Institutional.Inst_Net_3d' 崩潰問題
# ✅ [UI] Warnings (系統健康診斷) 加回儀表板，並提供清楚說明
# ✅ [Logic] 強化持倉 (Positions) 追蹤：持倉股強制納入監控，並自動補上中文名稱
# ✅ [Data] 確保法人數據欄位 (Capitalized Keys) 對齊前端需求
# =========================================================

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf

# =========================
# Streamlit page config
# =========================
st.set_page_config(
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3 中文戰術版）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3 Hybrid）"
st.title(APP_TITLE)

# =========================
# Constants / helpers
# =========================
EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

DEFAULT_TOPN = 20  
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}

NEUTRAL_THRESHOLD = 5_000_000 

# --- 個股中文名稱對照表 (可持續擴充) ---
STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海",   "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達",   "3231.TW": "緯創",   "2376.TW": "技嘉",   "3017.TW": "奇鋐",
    "3324.TW": "雙鴻",   "3661.TW": "世芯-KY",
    "2881.TW": "富邦金", "2882.TW": "國泰金", "2891.TW": "中信金", "2886.TW": "兆豐金",
    "2603.TW": "長榮",   "2609.TW": "陽明",   "1605.TW": "華新",   "1513.TW": "中興電",
    "1519.TW": "華城",   "2002.TW": "中鋼",
    # 若持倉股不在上面，會顯示代號，或您可在此手動追加
}

# --- 欄位中文化對照表 ---
COL_TRANSLATION = {
    "Symbol": "代號",
    "Name": "名稱",
    "Tier": "權重序",
    "Price": "價格",
    "Vol_Ratio": "量能比(Vol Ratio)",
    "Layer": "分級(Layer)",
    "Foreign_Net": "外資3日淨額",
    "Trust_Net": "投信3日淨額",
    "Inst_Streak3": "法人連買天數",
    "Inst_Status": "籌碼狀態",
    "Inst_Dir3": "籌碼方向",
    "Inst_Net_3d": "3日合計淨額",
    "inst_source": "資料來源",
    "foreign_buy": "外資買超",
    "trust_buy": "投信買超"
}

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _safe_float(x, default=None) -> Optional[float]:
    try:
        if x is None: return default
        if isinstance(x, (np.floating, float, int)): return float(x)
        if isinstance(x, str) and x.strip() == "": return default
        return float(x)
    except: return default

def _safe_int(x, default=None) -> Optional[int]:
    try:
        if x is None: return default
        if isinstance(x, (np.integer, int)): return int(x)
        if isinstance(x, (np.floating, float)): return int(float(x))
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            return int(float(s)) if s else default
        return int(x)
    except: return default

def _pct01_to_pct100(x: Optional[float]) -> Optional[float]:
    if x is None: return None
    return float(x) * 100.0

# =========================
# Warnings recorder
# =========================
class WarningBus:
    def __init__(self):
        self.items: List[Dict[str, Any]] = []

    def push(self, code: str, msg: str, meta: Optional[dict] = None):
        self.items.append({"ts": _now_ts(), "code": code, "msg": msg, "meta": meta or {}})

    def latest(self, n: int = 50) -> List[Dict[str, Any]]:
        return self.items[-n:]

warnings_bus = WarningBus()

# =========================
# Market amount (TWSE/TPEX)
# =========================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
    allow_insecure_ssl: bool

def _fetch_twse_amount(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&type=ALLBUT0999"
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()
        fields = js.get("fields9") or js.get("fields") or []
        fields = [str(x) for x in fields] if isinstance(fields, list) else []
        amt_idx = None
        for i, f in enumerate(fields):
            if "成交金額" in f:
                amt_idx = i
                break
        data = js.get("data9")
        if isinstance(data, list) and len(data) > 0 and amt_idx is not None:
            last = data[-1]
            if isinstance(last, list) and amt_idx < len(last):
                amount = _safe_int(last[amt_idx], default=None)
                if amount is not None:
                    return int(amount), "TWSE_OK:MI_INDEX"
        warnings_bus.push("TWSE_AMOUNT_PARSE_FAIL", "TWSE schema changed?", {"url": url})
        return None, "TWSE_FAIL:PARSE"
    except Exception as e:
        warnings_bus.push("TWSE_AMOUNT_FAIL", str(e), {"url": url})
        return None, "TWSE_FAIL:ERROR"

def _fetch_tpex_amount(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw"
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()
        for key in ["totalAmount", "trade_value", "amount", "amt", "成交金額"]:
            if key in js:
                v = _safe_int(js.get(key), default=None)
                if v is not None:
                    return int(v), "TPEX_OK:st43_result"
        warnings_bus.push("TPEX_AMOUNT_PARSE_FAIL", "No numeric keys", {"url": url})
        return None, "TPEX_FAIL:PARSE"
    except Exception as e:
        warnings_bus.push("TPEX_AMOUNT_FAIL", str(e), {"url": url})
        return None, "TPEX_FAIL:ERROR"

def fetch_amount_total(allow_insecure_ssl: bool = False) -> MarketAmount:
    twse_amt, twse_src = _fetch_twse_amount(allow_insecure_ssl)
    tpex_amt, tpex_src = _fetch_tpex_amount(allow_insecure_ssl)
    total = None
    if twse_amt is not None and tpex_amt is not None:
        total = int(twse_amt) + int(tpex_amt)
    elif twse_amt is not None:
        total = int(twse_amt)
    elif tpex_amt is not None:
        total = int(tpex_amt)
    return MarketAmount(twse_amt, tpex_amt, total, twse_src, tpex_src, bool(allow_insecure_ssl))

# =========================
# New Fetcher: 全市場三大法人買賣超
# =========================
def fetch_market_inst_summary(allow_insecure_ssl: bool = False) -> List[Dict[str, Any]]:
    """
    抓取證交所三大法人買賣金額統計表 (BFI82U)
    """
    url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json"
    data_list = []
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()
        if 'data' in js and isinstance(js['data'], list):
            for row in js['data']:
                if len(row) >= 4:
                    name = row[0].strip()
                    diff = _safe_int(row[3]) # 第4欄通常是買賣差額
                    if diff is not None:
                         data_list.append({"Identity": name, "Net": diff})
    except Exception as e:
        warnings_bus.push("MARKET_INST_FAIL", f"BFI82U fetch fail: {e}", {"url": url})
    
    return data_list

# =========================
# FinMind helpers
# =========================
def _finmind_headers(token: Optional[str]) -> dict:
    if token: return {"Authorization": f"Bearer {token}"}
    return {}

def _finmind_get(dataset: str, params: dict, token: Optional[str]) -> dict:
    p = {"dataset": dataset, **params}
    r = requests.get(FINMIND_URL, headers=_finmind_headers(token), params=p, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_inst_direction(net: float) -> str:
    net = float(net or 0.0)
    if abs(net) < NEUTRAL_THRESHOLD: return "NEUTRAL"
    return "POSITIVE" if net > 0 else "NEGATIVE"

def fetch_finmind_institutional(symbols: List[str], start_date: str, end_date: str, token: Optional[str] = None) -> pd.DataFrame:
    rows = []
    for sym in symbols:
        stock_id = sym.replace(".TW", "").strip()
        try:
            js = _finmind_get(
                dataset="TaiwanStockInstitutionalInvestorsBuySell",
                params={"data_id": stock_id, "start_date": start_date, "end_date": end_date},
                token=token,
            )
        except Exception as e:
            warnings_bus.push("FINMIND_FAIL", str(e), {"symbol": sym})
            continue

        data = js.get("data", []) or []
        if not data: continue
        df = pd.DataFrame(data)
        need = {"date", "stock_id", "buy", "name", "sell"}
        if not need.issubset(set(df.columns)): continue
        
        df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
        df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
        df = df[df["name"].isin(A_NAMES)].copy()
        if df.empty: continue
        
        df["net"] = df["buy"] - df["sell"]
        g = df.groupby("date", as_index=False)["net"].sum()
        for _, r in g.iterrows():
            rows.append({"date": str(r["date"]), "symbol": sym, "net_amount": float(r["net"])})

    if not rows: return pd.DataFrame(columns=["date", "symbol", "net_amount"])
    return pd.DataFrame(rows).sort_values(["symbol", "date"])

def calc_inst_3d(inst_df: pd.DataFrame, symbol: str) -> dict:
    if inst_df is None or inst_df.empty:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}
    df = inst_df[inst_df["symbol"] == symbol].copy()
    if df.empty:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}
    
    df = df.sort_values("date").tail(3)
    if len(df) < 3:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}
    
    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0)
    dirs = [normalize_inst_direction(x) for x in df["net_amount"]]
    net_sum = float(df["net_amount"].sum())
    
    if all(d == "POSITIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "POSITIVE", "Inst_Net_3d": net_sum}
    if all(d == "NEGATIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "NEGATIVE", "Inst_Net_3d": net_sum}
    
    return {"Inst_Status": "READY", "Inst_Streak3": 0, "Inst_Dir3": "NEUTRAL", "Inst_Net_3d": net_sum}

# =========================
# yfinance fetchers
# =========================
@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_history(symbol: str, period: str = "3y", interval: str = "1d") -> pd.DataFrame:
    try:
        df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False, group_by="column", threads=False)
        if df is None or df.empty: return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [' '.join([str(c) for c in col if str(c) != '']).strip() for col in df.columns.values]
            df.columns = [c.replace(f'{symbol} ', '').strip() for c in df.columns]
        df = df.reset_index()
        if "Date" in df.columns: df = df.rename(columns={"Date": "Datetime"})
        elif "index" in df.columns: df = df.rename(columns={"index": "Datetime"})
        if "Datetime" not in df.columns and df.index.name is not None:
            df.insert(0, "Datetime", pd.to_datetime(df.index))
        return df
    except Exception as e:
        warnings_bus.push("YF_HISTORY_FAIL", str(e), {"symbol": symbol})
        return pd.DataFrame()

@st.cache_data(ttl=60 * 5, show_spinner=False)
def fetch_batch_prices_volratio(symbols: List[str]) -> pd.DataFrame:
    out = pd.DataFrame({"Symbol": symbols})
    out["Price"] = None
    out["Vol_Ratio"] = None
    out["source"] = "NONE"
    if not symbols: return out
    try:
        df = yf.download(symbols, period="6mo", interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=False)
    except Exception as e:
        warnings_bus.push("YF_BATCH_FAIL", str(e), {"n": len(symbols)})
        return out
    
    if df is None or df.empty: return out
    
    for sym in symbols:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                if sym not in df.columns.get_level_values(0): continue
                close = df[(sym, "Close")].dropna()
                vol = df[(sym, "Volume")].dropna()
            else:
                close = df["Close"].dropna() if "Close" in df.columns else pd.Series(dtype=float)
                vol = df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)

            price = float(close.iloc[-1]) if len(close) else None
            vol_ratio = None
            if len(vol) >= 20:
                ma20 = float(vol.rolling(20).mean().iloc[-1])
                if ma20 and ma20 > 0: vol_ratio = float(vol.iloc[-1] / ma20)

            out.loc[out["Symbol"] == sym, "Price"] = price
            out.loc[out["Symbol"] == sym, "Vol_Ratio"] = vol_ratio
            out.loc[out["Symbol"] == sym, "source"] = "YF"
        except: continue
    return out

# =========================
# Regime & Metrics
# =========================
def _as_close_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty: raise ValueError("empty df")
    possible_names = ["Close", "close", "Adj Close", "adj close"]
    for name in possible_names:
        if name in df.columns:
            s = df[name]
            if isinstance(s, pd.DataFrame): s = s.iloc[:, 0]
            return s.astype(float)
    close_cols = [c for c in df.columns if 'close' in str(c).lower()]
    if close_cols:
        s = df[close_cols[0]]
        if isinstance(s, pd.DataFrame): s = s.iloc[:, 0]
        return s.astype(float)
    raise ValueError("No close col")

def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 260:
        return {"SMR": None, "Slope5": None, "MOMENTUM_LOCK": False, "drawdown_pct": None, "drawdown_window_days": 252}
    try:
        close = _as_close_series(market_df)
    except: return {"SMR": None, "Slope5": None, "MOMENTUM_LOCK": False, "drawdown_pct": None, "drawdown_window_days": 252}
    
    ma200 = close.rolling(200).mean()
    smr_series = ((close - ma200) / ma200).dropna()
    if len(smr_series) < 10: return {"SMR": None, "Slope5": None, "MOMENTUM_LOCK": False, "drawdown_pct": None, "drawdown_window_days": 252}

    smr = float(smr_series.iloc[-1])
    smr_ma5 = smr_series.rolling(5).mean().dropna()
    slope5 = float(smr_ma5.iloc[-1] - smr_ma5.iloc[-2]) if len(smr_ma5) >= 2 else 0.0
    
    last4 = smr_ma5.diff().dropna().iloc[-4:]
    momentum_lock = bool((last4 > EPS).all()) if len(last4) == 4 else False
    
    window = 252
    rolling_high = close.rolling(window).max()
    drawdown_pct = float(close.iloc[-1] / rolling_high.iloc[-1] - 1.0) if not np.isnan(rolling_high.iloc[-1]) else None
    
    return {
        "SMR": smr, "SMR_MA5": float(smr_ma5.iloc[-1]), "Slope5": slope5,
        "NEGATIVE_SLOPE_5D": bool(slope5 < -EPS), "MOMENTUM_LOCK": momentum_lock,
        "drawdown_pct": drawdown_pct, "drawdown_window_days": window
    }

def _calc_ma14_monthly_from_daily(df_daily: pd.DataFrame) -> Optional[float]:
    try:
        if df_daily is None or df_daily.empty: return None
        df = df_daily.copy()
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.set_index("Datetime")
        close = _as_close_series(df)
        monthly = close.resample("M").last().dropna()
        if len(monthly) < 14: return None
        ma14 = monthly.rolling(14).mean().dropna()
        return float(ma14.iloc[-1])
    except: return None

def _extract_close_price(df_daily: pd.DataFrame) -> Optional[float]:
    try:
        if df_daily is None or df_daily.empty: return None
        close = _as_close_series(df_daily)
        return float(close.iloc[-1]) if len(close) else None
    except: return None

def _count_close_below_ma_days(df_daily: pd.DataFrame, ma14_monthly: Optional[float]) -> int:
    try:
        if ma14_monthly is None or df_daily is None or df_daily.empty: return 0
        close = _as_close_series(df_daily)
        if len(close) < 2: return 0
        thresh = float(ma14_monthly) * 0.96
        recent = close.iloc[-5:].tolist()
        cnt = 0
        for v in reversed(recent):
            if float(v) < thresh: cnt += 1
            else: break
        return int(cnt)
    except: return 0

def pick_regime(metrics: dict, vix: Optional[float] = None, ma14_monthly: Optional[float] = None, 
                close_price: Optional[float] = None, close_below_ma_days: int = 0, vix_panic: float = 35.0, **kwargs) -> Tuple[str, float]:
    
    if "vixpanic" in kwargs and kwargs["vixpanic"]: vix_panic = float(kwargs["vixpanic"])
    if "vipxanic" in kwargs and kwargs["vipxanic"]: vix_panic = float(kwargs["vipxanic"])
    
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")

    if (vix and float(vix) > float(vix_panic)) or (drawdown and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10
    if ma14_monthly and close_price and int(close_below_ma_days) >= 2 and float(close_price) < float(ma14_monthly) * 0.96:
        return "HIBERNATION", 0.20
    if smr is not None and slope5 is not None:
        if float(smr) > 0.25 and float(slope5) < -EPS: return "MEAN_REVERSION", 0.45
        if float(smr) > 0.25 and float(slope5) >= -EPS: return "OVERHEAT", 0.55
    if smr is not None and 0.08 <= float(smr) <= 0.18: return "CONSOLIDATION", 0.65
    return "NORMAL", 0.85

def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    inst_streak3 = int(inst.get("inst_streak3", 0))
    if foreign_buy and trust_buy and inst_streak3 >= 3: return "A+"
    if (foreign_buy or trust_buy) and inst_streak3 >= 3: return "A"
    vr = _safe_float(vol_ratio, None)
    if momentum_lock and (vr and float(vr) > 0.8) and regime in ["NORMAL", "OVERHEAT", "CONSOLIDATION"]: return "B"
    return "NONE"

def compute_integrity_and_kill(stocks: List[dict], amount: MarketAmount) -> dict:
    n = len(stocks)
    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)
    amount_total_null = (amount.amount_total is None)
    denom = max(1, (2 * n + 1))
    core_missing = price_null + volratio_null + (1 if amount_total_null else 0)
    core_missing_pct = float(core_missing / denom)
    
    kill = False
    reasons = []
    if n > 0 and price_null == n:
        kill = True; reasons.append(f"price_null={price_null}/{n}")
    if n > 0 and volratio_null == n:
        kill = True; reasons.append(f"volratio_null={volratio_null}/{n}")
    if amount_total_null: reasons.append("amount_total_null=True")
    if core_missing_pct >= 0.50:
        kill = True; reasons.append(f"core_missing_pct={core_missing_pct:.2f}")

    return {
        "n": n, "price_null": price_null, "volratio_null": volratio_null,
        "core_missing_pct": core_missing_pct, "amount_total_null": amount_total_null,
        "kill": bool(kill), "reason": ("DATA_MISSING " + ", ".join(reasons)) if reasons else "OK"
    }

def build_active_alerts(integrity: dict, amount: MarketAmount) -> List[str]:
    alerts = []
    if integrity.get("kill"): alerts.append("KILL_SWITCH_ACTIVATED")
    if amount.amount_total is None: alerts.append("DEGRADED_AMOUNT: 成交量數據完全缺失")
    n = int(integrity.get("n") or 0)
    if n > 0 and int(integrity.get("price_null") or 0) == n: alerts.append("CRITICAL: 所有個股價格=null")
    if n > 0 and int(integrity.get("volratio_null") or 0) == n: alerts.append("CRITICAL: 所有個股量能=null")
    cm = float(integrity.get("core_missing_pct") or 0.0)
    if cm >= 0.50: alerts.append(f"DATA_INTEGRITY_FAILURE: 缺失率={cm:.2f}")
    if integrity.get("kill"): alerts.append("FORCED_ALL_CASH: 強制避險模式")
    return alerts

# =========================
# Arbiter input builder
# =========================
def _default_symbols_pool(topn: int) -> List[str]:
    pool = list(STOCK_NAME_MAP.keys())
    limit = min(len(pool), max(1, int(topn)))
    return pool[:limit]

def build_arbiter_input(session: str, account_mode: str, topn: int, positions: List[dict], 
                        cash_balance: int, total_equity: int, allow_insecure_ssl: bool, finmind_token: Optional[str]) -> Tuple[dict, List[dict]]:
    
    # 1. Market History & Metrics
    twii_df = fetch_history(TWII_SYMBOL, period="5y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")
    
    vix_last = None
    if not vix_df.empty:
        try:
            vix_close = _as_close_series(vix_df)
            vix_last = float(vix_close.iloc[-1]) if len(vix_close) else None
        except: pass

    metrics = compute_regime_metrics(twii_df)
    close_price = _extract_close_price(twii_df)
    ma14_monthly = _calc_ma14_monthly_from_daily(twii_df)
    close_below_days = _count_close_below_ma_days(twii_df, ma14_monthly)
    
    # 新增：計算大盤漲跌
    twii_change = None
    twii_pct = None
    if not twii_df.empty:
        try:
            c = _as_close_series(twii_df)
            if len(c) >= 2:
                twii_change = float(c.iloc[-1] - c.iloc[-2])
                twii_pct = float(c.iloc[-1] / c.iloc[-2] - 1.0)
        except: pass

    regime, max_equity = pick_regime(metrics, vix=vix_last, ma14_monthly=ma14_monthly, 
                                     close_price=close_price, close_below_ma_days=close_below_days)

    # 2. Market Amount & Institutions
    amount = fetch_amount_total(allow_insecure_ssl)
    market_inst_summary = fetch_market_inst_summary(allow_insecure_ssl)

    # 3. Stocks Data (TopN + Positions)
    # 合併 TopN 與 Positions 的標的，去除重複
    base_pool = _default_symbols_pool(topn)
    pos_pool = [p.get("symbol") for p in positions if p.get("symbol")]
    # 使用 dict.fromkeys 維持順序並去重
    symbols = list(dict.fromkeys(base_pool + pos_pool))

    pv = fetch_batch_prices_volratio(symbols)
    
    trade_date = None
    if not twii_df.empty and "Datetime" in twii_df.columns:
        trade_date = pd.to_datetime(twii_df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")
    end_date = trade_date or time.strftime("%Y-%m-%d", time.localtime())
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    inst_df = fetch_finmind_institutional(symbols, start_date=start_date, end_date=end_date, token=finmind_token)
    
    panel_rows = []
    inst_map = {}
    stocks = []
    
    for i, sym in enumerate(symbols, start=1):
        # Institutional
        inst3 = calc_inst_3d(inst_df, sym)
        net3 = float(inst3.get("Inst_Net_3d", 0.0))
        
        # 準備面板資料 (包含中文名稱)
        p_row = {
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym), # Add Name
            "Foreign_Net": net3, 
            "Trust_Net": net3, 
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
            "Inst_Status": inst3.get("Inst_Status", "PENDING"),
            "Inst_Dir3": inst3.get("Inst_Dir3", "PENDING"),
            "Inst_Net_3d": net3,
            "inst_source": "FINMIND_3D_NET"
        }
        panel_rows.append(p_row)
        
        # 修正：寫入正確的 Capitalized Keys，供 pd.json_normalize 與前端 disp_cols 使用
        inst_map[sym] = {
            "foreign_buy": bool(net3>0), 
            "trust_buy": bool(net3>0), 
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)), # Capitalized
            "Inst_Net_3d": net3, # Added Key
            "inst_streak3": int(inst3.get("Inst_Streak3", 0)) # keep lowercase for internal layer logic
        }

        # Stock Technicals
        row = pv[pv["Symbol"] == sym].iloc[0] if not pv.empty and (pv["Symbol"] == sym).any() else None
        price = row["Price"] if row is not None else None
        vol_ratio = row["Vol_Ratio"] if row is not None else None
        
        if price is None: warnings_bus.push("PRICE_NULL", "Missing Price", {"symbol": sym})
        if vol_ratio is None: warnings_bus.push("VOLRATIO_NULL", "Missing VolRatio", {"symbol": sym})

        inst_data = inst_map.get(sym, {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0})
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vol_ratio, inst_data)
        
        stocks.append({
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym), # Add Name
            "Tier": i,
            "Price": None if pd.isna(price) else price,
            "Vol_Ratio": None if pd.isna(vol_ratio) else vol_ratio,
            "Layer": layer,
            "Institutional": inst_data
        })

    institutional_panel = pd.DataFrame(panel_rows)
    
    integrity = compute_integrity_and_kill(stocks, amount)
    active_alerts = build_active_alerts(integrity, amount)
    current_exposure_pct = min(1.0, len(positions) * 0.05) if positions else 0.0
    
    market_status = "NORMAL" if (amount.amount_total is not None) else "DEGRADED"
    final_regime = "UNKNOWN" if integrity["kill"] else regime
    final_max_equity = 0.0 if integrity["kill"] else max_equity
    if integrity["kill"]: 
        market_status = "SHELTER"
        current_exposure_pct = 0.0

    payload = {
        "meta": {
            "timestamp": _now_ts(), "session": session, "market_status": market_status,
            "current_regime": final_regime, "account_mode": account_mode, "audit_tag": "V16.3_ZH_EDITION"
        },
        "macro": {
            "overview": {
                "trade_date": trade_date, "twii_close": close_price,
                "twii_change": twii_change, "twii_pct": twii_pct, 
                "vix": vix_last, "smr": metrics.get("SMR"), "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"), "max_equity_allowed_pct": final_max_equity
            },
            "market_amount": asdict(amount),
            "market_inst_summary": market_inst_summary, 
            "integrity": integrity
        },
        "portfolio": {
            "total_equity": int(total_equity), "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float(100.0 * max(0.0, 1.0 - current_exposure_pct)),
            "active_alerts": active_alerts
        },
        "institutional_panel": institutional_panel.to_dict(orient="records"),
        "stocks": stocks, "positions_input": positions, "decisions": [], "audit_log": []
    }
    return payload, warnings_bus.latest(50)

# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定 (Settings)")
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=0)
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
    topn = st.sidebar.selectbox("TopN（監控數量）", [8, 10, 15, 20, 30], index=3) # Default 20
    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL", value=False)
    
    st.sidebar.subheader("FinMind")
    finmind_token = st.sidebar.text_input("FinMind Token", type="password").strip() or None
    
    st.sidebar.subheader("持倉 (JSON List)")
    positions_text = st.sidebar.text_area("positions", value="[]", height=100)
    
    cash_balance = st.sidebar.number_input("現金餘額", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("總權益", min_value=0, value=DEFAULT_EQUITY, step=10000)
    
    run_btn = st.sidebar.button("啟動中控台")
    
    positions = []
    try: positions = json.loads(positions_text) if positions_text.strip() else []
    except: pass

    if run_btn or "auto_ran" not in st.session_state:
        st.session_state["auto_ran"] = True
        try:
            payload, warns = build_arbiter_input(session, account_mode, int(topn), positions, 
                                               int(cash_balance), int(total_equity), bool(allow_insecure_ssl), finmind_token)
        except Exception as e:
            st.error(f"系統錯誤: {e}")
            # 顯示完整的 traceback 方便 debug (但前端只會看到上面的 error)
            # st.exception(e) 
            return

        ov = payload.get("macro", {}).get("overview", {})
        meta = payload.get("meta", {})
        amount = payload.get("macro", {}).get("market_amount", {})
        inst_summary = payload.get("macro", {}).get("market_inst_summary", [])
        
        # --- 1. 關鍵指標 (Key Metrics) ---
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("交易日期", ov.get("trade_date", "-"))
        c2.metric("市場狀態", meta.get("market_status", "-"))
        c3.metric("策略體制 (Regime)", meta.get("current_regime", "-"))
        c4.metric("建議持倉上限", f"{_pct01_to_pct100(ov.get('max_equity_allowed_pct')):.0f}%" if ov.get("max_equity_allowed_pct") is not None else "-")

        # --- 2. 大盤與成交量 (Market Overview) ---
        st.subheader("📊 大盤觀測站 (TAIEX Overview)")
        m1, m2, m3, m4 = st.columns(4)
        
        close = ov.get("twii_close")
        chg = ov.get("twii_change")
        pct = ov.get("twii_pct")
        
        delta_color = "normal"
        if chg: delta_color = "normal" if chg >= 0 else "inverse"

        m1.metric("加權指數", f"{close:,.0f}" if close else "-", f"{chg:+.0f} ({pct:+.2%})" if chg else None, delta_color=delta_color)
        m2.metric("VIX 恐慌指數", f"{ov.get('vix'):.2f}" if ov.get("vix") else "-")
        
        amt_total = amount.get("amount_total")
        amt_str = f"{amt_total/1_0000_0000:.1f} 億" if amt_total else "數據缺失"
        m3.metric("市場總成交額", amt_str)
        m4.metric("SMR 乖離率", f"{ov.get('smr'):.4f}" if ov.get('smr') else "-")

        # --- 3. 三大法人全市場買賣超 (Market Institutions) ---
        st.subheader("🏛️ 三大法人買賣超 (全市場)")
        if inst_summary:
            cols = st.columns(len(inst_summary))
            for idx, item in enumerate(inst_summary):
                net = item.get("Net", 0)
                net_yi = net / 1_0000_0000 
                cols[idx].metric(item.get("Identity"), f"{net_yi:+.2f} 億")
        else:
            st.info("暫無今日法人統計資料 (通常下午 3 點後更新)")

        # --- 4. 警報區 (Alerts) ---
        alerts = payload.get("portfolio", {}).get("active_alerts", [])
        if alerts:
            st.subheader("⚠️ 戰術警報 (Active Alerts)")
            for a in alerts:
                if "CRITICAL" in a or "KILL" in a: st.error(a)
                else: st.warning(a)

        # --- 5. 系統診斷 (Warnings) - 加回儀表板 ---
        st.subheader("🛠️ 系統健康診斷 (System Health)")
        if not warns:
            st.success("✅ 系統運作正常，無錯誤日誌 (Clean Run)。")
        else:
            with st.expander(f"⚠️ 偵測到 {len(warns)} 條系統警示 (點擊查看詳情)", expanded=True):
                st.warning("系統遭遇部分數據抓取失敗，已自動降級或使用備援數據。")
                w_df = pd.DataFrame(warns)
                # 簡單過濾關鍵欄位
                if not w_df.empty and 'code' in w_df.columns:
                    st.dataframe(w_df[['ts', 'code', 'msg']], use_container_width=True)
                else:
                    st.write(warns)

        # --- 6. 個股分析 (Stocks) - 中文化表格 ---
        st.subheader("🎯 核心持股雷達 (Tactical Stocks)")
        s_df = pd.json_normalize(payload.get("stocks", []))
        if not s_df.empty:
            # 整理並重命名欄位 (確保 Institutional.Inst_Net_3d 存在)
            disp_cols = ["Symbol", "Name", "Price", "Vol_Ratio", "Layer", "Institutional.Inst_Net_3d", "Institutional.Inst_Streak3"]
            
            # 使用 reindex 避免 KeyError
            s_df = s_df.reindex(columns=disp_cols, fill_value=0)
            
            s_df = s_df.rename(columns=COL_TRANSLATION)
            s_df = s_df.rename(columns={
                "Institutional.Inst_Net_3d": "法人3日淨額",
                "Institutional.Inst_Streak3": "法人連買天數"
            })
            st.dataframe(s_df, use_container_width=True)

        # --- 7. 法人明細 (Institutional Panel) ---
        with st.expander("🔍 查看法人詳細數據 (Institutional Debug Panel)"):
            inst_df = pd.DataFrame(payload.get("institutional_panel", []))
            if not inst_df.empty:
                st.dataframe(inst_df.rename(columns=COL_TRANSLATION), use_container_width=True)
        
        # --- 8. AI JSON 一鍵複製 (st.code) ---
        st.markdown("---")
        c_copy1, c_copy2 = st.columns([0.8, 0.2])
        with c_copy1: st.subheader("🤖 AI JSON (Arbiter Input)")
        
        json_str = json.dumps(payload, indent=4, ensure_ascii=False)
        st.markdown("##### 📋 點擊下方代碼塊右上角的「複製圖示」即可複製完整數據")
        st.code(json_str, language="json")

if __name__ == "__main__":
    main()
