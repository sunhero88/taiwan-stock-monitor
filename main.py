# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.3 Spec-Compliant）
# 基於規格：V16.2.1 Patch Notes
#
# 本版「一次修到位」重點（針對你目前 VIX/SMR/Slope5 = null 的痛點）：
# ✅ Fix-01：yfinance 抓 TWII/VIX 增加「雙路徑 + 重試 + 稽核資訊」（避免空表無感）
# ✅ Fix-02：在 payload 加入 macro.sources（rows/cols/last_dt/ok/reason），讓 null 可回溯
# ✅ Fix-03：compute_regime_metrics 回傳 metrics_reason（明確指出是 empty / too_short / no_close）
# ✅ Fix-04：UI 顯示「資料源健康度」（TWII/VIX），不再只看到 "-" 不知原因
# ✅ Fix-05：成交額模組優先使用 market_amount.py（若存在），否則退回內建簡化版
#
# 注意：此版不「猜」指標；若 TWII/VIX 抓不到 → 會在 sources 與 warnings 明確標示原因
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
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3.3 Spec-Compliant）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.3）"
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

NEUTRAL_THRESHOLD = 5_000_000  # 法人 3 日合計 < 500 萬視為中性（可調）

# --- 個股中文名稱對照表 (可持續擴充) ---
STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海",   "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達",   "3231.TW": "緯創",   "2376.TW": "技嘉",   "3017.TW": "奇鋐",
    "3324.TW": "雙鴻",   "3661.TW": "世芯-KY",
    "2881.TW": "富邦金", "2882.TW": "國泰金", "2891.TW": "中信金", "2886.TW": "兆豐金",
    "2603.TW": "長榮",   "2609.TW": "陽明",   "1605.TW": "華新",   "1513.TW": "中興電",
    "1519.TW": "華城",   "2002.TW": "中鋼"
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
        if x is None:
            return default
        if isinstance(x, (np.floating, float, int)):
            return float(x)
        if isinstance(x, str) and x.strip() == "":
            return default
        return float(x)
    except Exception:
        return default

def _safe_int(x, default=None) -> Optional[int]:
    try:
        if x is None:
            return default
        if isinstance(x, (np.integer, int)):
            return int(x)
        if isinstance(x, (np.floating, float)):
            return int(float(x))
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            return int(float(s)) if s else default
        return int(x)
    except Exception:
        return default

def _pct01_to_pct100(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
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
# Market amount (TWSE/TPEX) - prefer external module if exists
# =========================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
    allow_insecure_ssl: bool
    meta: Optional[Dict[str, Any]] = None  # 稽核資訊

def _fetch_twse_amount_simple(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
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
        warnings_bus.push("TWSE_AMOUNT_PARSE_FAIL", "TWSE schema changed?", {"url": url, "keys": list(js.keys())[:30]})
        return None, "TWSE_FAIL:PARSE"
    except Exception as e:
        warnings_bus.push("TWSE_AMOUNT_FAIL", str(e), {"url": url})
        return None, "TWSE_FAIL:ERROR"

def _fetch_tpex_amount_simple(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
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
        warnings_bus.push("TPEX_AMOUNT_PARSE_FAIL", "No numeric keys", {"url": url, "keys": list(js.keys())[:30]})
        return None, "TPEX_FAIL:PARSE"
    except Exception as e:
        warnings_bus.push("TPEX_AMOUNT_FAIL", str(e), {"url": url})
        return None, "TPEX_FAIL:ERROR"

def fetch_amount_total(allow_insecure_ssl: bool = False) -> MarketAmount:
    """
    若同目錄已有 market_amount.py（你那份「可降級、可稽核」版），優先使用。
    否則 fallback 到簡化版（僅保底，不建議長期用）。
    """
    try:
        # 你的 repo 若有 market_amount.py，且內含 fetch_amount_total() 及 warnings_to_rows()
        import market_amount as ma  # type: ignore

        res, w_items = ma.fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl)
        # 將外部 warnings 也灌回 warnings_bus（供 UI 顯示）
        try:
            rows = ma.warnings_to_rows(w_items)
            for r in rows[-50:]:
                warnings_bus.push(r.get("code", "MARKET_AMOUNT_WARN"), r.get("msg", ""), {"meta": r.get("meta", {}), "ts": r.get("ts")})
        except Exception:
            pass

        return MarketAmount(
            amount_twse=res.amount_twse,
            amount_tpex=res.amount_tpex,
            amount_total=res.amount_total,
            source_twse=res.source_twse,
            source_tpex=res.source_tpex,
            allow_insecure_ssl=bool(allow_insecure_ssl),
            meta=res.meta if getattr(res, "meta", None) is not None else {},
        )
    except Exception as e:
        # 外部模組不可用 → 退回簡化版
        warnings_bus.push("MARKET_AMOUNT_FALLBACK", "Use simple amount fetchers (market_amount.py not used).", {"err": str(e)})

        twse_amt, twse_src = _fetch_twse_amount_simple(allow_insecure_ssl)
        tpex_amt, tpex_src = _fetch_tpex_amount_simple(allow_insecure_ssl)

        total = None
        if twse_amt is not None and tpex_amt is not None:
            total = int(twse_amt) + int(tpex_amt)
        elif twse_amt is not None:
            total = int(twse_amt)
        elif tpex_amt is not None:
            total = int(tpex_amt)

        return MarketAmount(
            amount_twse=twse_amt,
            amount_tpex=tpex_amt,
            amount_total=total,
            source_twse=twse_src,
            source_tpex=tpex_src,
            allow_insecure_ssl=bool(allow_insecure_ssl),
            meta={"fallback": "simple", "note": "建议改用你完整版 market_amount.py"},
        )

def fetch_market_inst_summary(allow_insecure_ssl: bool = False) -> List[Dict[str, Any]]:
    url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json"
    data_list = []
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()
        if "data" in js and isinstance(js["data"], list):
            for row in js["data"]:
                if len(row) >= 4:
                    name = str(row[0]).strip()
                    diff = _safe_int(row[3])
                    if diff is not None:
                        data_list.append({"Identity": name, "Net": diff})
    except Exception as e:
        warnings_bus.push("MARKET_INST_FAIL", f"BFI82U fetch fail: {e}", {"url": url})
    return data_list

# =========================
# FinMind helpers
# =========================
def _finmind_headers(token: Optional[str]) -> dict:
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}

def _finmind_get(dataset: str, params: dict, token: Optional[str]) -> dict:
    p = {"dataset": dataset, **params}
    r = requests.get(FINMIND_URL, headers=_finmind_headers(token), params=p, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_inst_direction(net: float) -> str:
    net = float(net or 0.0)
    if abs(net) < NEUTRAL_THRESHOLD:
        return "NEUTRAL"
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
        if not data:
            continue
        df = pd.DataFrame(data)
        need = {"date", "stock_id", "buy", "name", "sell"}
        if not need.issubset(set(df.columns)):
            continue

        df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
        df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
        df = df[df["name"].isin(A_NAMES)].copy()
        if df.empty:
            continue

        df["net"] = df["buy"] - df["sell"]
        g = df.groupby("date", as_index=False)["net"].sum()
        for _, r in g.iterrows():
            rows.append({"date": str(r["date"]), "symbol": sym, "net_amount": float(r["net"])})

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "net_amount"])
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
# yfinance fetchers (patched)
# =========================
def _normalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    把 yfinance 回來的表格統一成含 Datetime 欄位的扁平 DF
    目標欄位：Datetime, Open, High, Low, Close, Adj Close, Volume (不一定全有)
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # df 可能是 index=Datetime
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "Datetime"}).copy()
    elif "index" in df.columns:
        df = df.rename(columns={"index": "Datetime"}).copy()
    else:
        # 若 index 本身是 DatetimeIndex
        if df.index.name is not None:
            df = df.copy()
            df.insert(0, "Datetime", pd.to_datetime(df.index))

    # MultiIndex columns flatten（少數情況）
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(c) for c in col if str(c) != ""]).strip() for col in df.columns.values]

    # 確保 Datetime 存在
    if "Datetime" not in df.columns:
        # 最後防線
        df = df.reset_index()
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "Datetime"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "Datetime"})
    return df

@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_history(symbol: str, period: str = "3y", interval: str = "1d") -> pd.DataFrame:
    """
    重要：此函數是你 VIX/SMR/Slope5 是否為 null 的關鍵來源
    本版採「雙路徑」：
    - Path A：yf.Ticker(symbol).history(...)
    - Path B：yf.download(...)
    並加入小幅重試（最多 2 次），把空表也寫進 warnings（可稽核）
    """
    last_err = None
    for attempt in range(1, 3):  # 2 attempts
        # --- Path A: Ticker().history ---
        try:
            t = yf.Ticker(symbol)
            df_a = t.history(period=period, interval=interval, auto_adjust=False)
            df_a = _normalize_history_df(df_a.reset_index() if "Datetime" not in df_a.columns else df_a)
            if df_a is not None and not df_a.empty:
                # 稽核：成功也記錄（讓你知道是「拿到了」）
                try:
                    last_dt = str(pd.to_datetime(df_a["Datetime"].dropna().iloc[-1]))
                except Exception:
                    last_dt = None
                warnings_bus.push(
                    "YF_HISTORY_OK",
                    "History fetched via Ticker.history",
                    {"symbol": symbol, "rows": int(len(df_a)), "cols": list(df_a.columns)[:12], "last_dt": last_dt, "path": "A", "attempt": attempt},
                )
                return df_a
            else:
                warnings_bus.push(
                    "YF_HISTORY_EMPTY",
                    "Empty dataframe via Ticker.history",
                    {"symbol": symbol, "path": "A", "attempt": attempt},
                )
        except Exception as e:
            last_err = e
            warnings_bus.push("YF_HISTORY_FAIL", str(e), {"symbol": symbol, "path": "A", "attempt": attempt})

        # --- Path B: download ---
        try:
            df_b = yf.download(
                symbol,
                period=period,
                interval=interval,
                auto_adjust=False,
                progress=False,
                group_by="column",
                threads=False,
            )
            df_b = _normalize_history_df(df_b.reset_index())
            if df_b is not None and not df_b.empty:
                try:
                    last_dt = str(pd.to_datetime(df_b["Datetime"].dropna().iloc[-1]))
                except Exception:
                    last_dt = None
                warnings_bus.push(
                    "YF_HISTORY_OK",
                    "History fetched via yf.download",
                    {"symbol": symbol, "rows": int(len(df_b)), "cols": list(df_b.columns)[:12], "last_dt": last_dt, "path": "B", "attempt": attempt},
                )
                return df_b
            else:
                warnings_bus.push(
                    "YF_HISTORY_EMPTY",
                    "Empty dataframe via yf.download",
                    {"symbol": symbol, "path": "B", "attempt": attempt},
                )
        except Exception as e:
            last_err = e
            warnings_bus.push("YF_HISTORY_FAIL", str(e), {"symbol": symbol, "path": "B", "attempt": attempt})

        # 輕微退避，避免短時間重打
        time.sleep(0.6 * attempt)

    # retries exhausted
    if last_err is not None:
        warnings_bus.push("YF_HISTORY_GIVEUP", "History fetch exhausted retries", {"symbol": symbol, "err": str(last_err)})
    return pd.DataFrame()

@st.cache_data(ttl=60 * 5, show_spinner=False)
def fetch_batch_prices_volratio(symbols: List[str]) -> pd.DataFrame:
    out = pd.DataFrame({"Symbol": symbols})
    out["Price"] = None
    out["Vol_Ratio"] = None
    out["source"] = "NONE"
    if not symbols:
        return out

    try:
        df = yf.download(symbols, period="6mo", interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=False)
    except Exception as e:
        warnings_bus.push("YF_BATCH_FAIL", str(e), {"n": len(symbols)})
        return out

    if df is None or df.empty:
        warnings_bus.push("YF_BATCH_EMPTY", "Batch download returned empty dataframe", {"n": len(symbols)})
        return out

    for sym in symbols:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                if sym not in df.columns.get_level_values(0):
                    continue
                close = df[(sym, "Close")].dropna()
                vol = df[(sym, "Volume")].dropna()
            else:
                close = df["Close"].dropna() if "Close" in df.columns else pd.Series(dtype=float)
                vol = df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)

            price = float(close.iloc[-1]) if len(close) else None

            vol_ratio = None
            if len(vol) >= 20:
                ma20 = float(vol.rolling(20).mean().iloc[-1])
                if ma20 and ma20 > 0:
                    vol_ratio = float(vol.iloc[-1] / ma20)

            out.loc[out["Symbol"] == sym, "Price"] = price
            out.loc[out["Symbol"] == sym, "Vol_Ratio"] = vol_ratio
            out.loc[out["Symbol"] == sym, "source"] = "YF"
        except Exception:
            continue
    return out

# =========================
# Regime & Metrics
# =========================
def _as_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("empty df")
    if col_name in df.columns:
        s = df[col_name]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        return s.astype(float)
    cols = [c for c in df.columns if str(col_name).lower() == str(c).lower()]
    if cols:
        s = df[cols[0]]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        return s.astype(float)
    raise ValueError(f"Col {col_name} not found")

def _as_close_series(df: pd.DataFrame) -> pd.Series:
    try:
        return _as_series(df, "Close")
    except Exception:
        try:
            return _as_series(df, "Adj Close")
        except Exception:
            raise ValueError("No Close/Adj Close found")

def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    """
    本版新增：metrics_reason（讓你知道 SMR/Slope5 為什麼是 null）
    """
    if market_df is None or market_df.empty:
        return {
            "SMR": None, "SMR_MA5": None, "Slope5": None,
            "MOMENTUM_LOCK": False, "NEGATIVE_SLOPE_5D": None,
            "drawdown_pct": None, "drawdown_window_days": 252,
            "price_range_10d_pct": None, "gap_down": None,
            "metrics_reason": "TWII_EMPTY"
        }

    if len(market_df) < 260:
        return {
            "SMR": None, "SMR_MA5": None, "Slope5": None,
            "MOMENTUM_LOCK": False, "NEGATIVE_SLOPE_5D": None,
            "drawdown_pct": None, "drawdown_window_days": 252,
            "price_range_10d_pct": None, "gap_down": None,
            "metrics_reason": f"TWII_TOO_SHORT_ROWS={len(market_df)}(<260)"
        }

    try:
        close = _as_close_series(market_df)
    except Exception as e:
        return {
            "SMR": None, "SMR_MA5": None, "Slope5": None,
            "MOMENTUM_LOCK": False, "NEGATIVE_SLOPE_5D": None,
            "drawdown_pct": None, "drawdown_window_days": 252,
            "price_range_10d_pct": None, "gap_down": None,
            "metrics_reason": f"TWII_NO_CLOSE:{type(e).__name__}"
        }

    ma200 = close.rolling(200).mean()
    smr_series = ((close - ma200) / ma200).dropna()
    if len(smr_series) < 10:
        return {
            "SMR": None, "SMR_MA5": None, "Slope5": None,
            "MOMENTUM_LOCK": False, "NEGATIVE_SLOPE_5D": None,
            "drawdown_pct": None, "drawdown_window_days": 252,
            "price_range_10d_pct": None, "gap_down": None,
            "metrics_reason": f"SMR_SERIES_TOO_SHORT={len(smr_series)}(<10)"
        }

    smr = float(smr_series.iloc[-1])
    smr_ma5 = smr_series.rolling(5).mean().dropna()
    slope5 = float(smr_ma5.iloc[-1] - smr_ma5.iloc[-2]) if len(smr_ma5) >= 2 else 0.0

    last4 = smr_ma5.diff().dropna().iloc[-4:]
    momentum_lock = bool((last4 > EPS).all()) if len(last4) == 4 else False

    # Drawdown (252D)
    window_dd = 252
    rolling_high = close.rolling(window_dd).max()
    drawdown_pct = float(close.iloc[-1] / rolling_high.iloc[-1] - 1.0) if not np.isnan(rolling_high.iloc[-1]) else None

    # Consolidation price range (10D)
    price_range_10d_pct = None
    if len(close) >= 10:
        recent_10d = close.iloc[-10:]
        low_10d = float(recent_10d.min())
        high_10d = float(recent_10d.max())
        if low_10d > 0:
            price_range_10d_pct = float((high_10d - low_10d) / low_10d)

    # Gap Down (Today Open vs Yesterday Close)
    gap_down = None
    try:
        open_s = _as_series(market_df, "Open")
        if len(open_s) >= 2 and len(close) >= 2:
            today_open = float(open_s.iloc[-1])
            prev_close = float(close.iloc[-2])
            if prev_close > 0:
                gap_down = (today_open - prev_close) / prev_close
    except Exception:
        gap_down = None

    return {
        "SMR": smr,
        "SMR_MA5": float(smr_ma5.iloc[-1]) if len(smr_ma5) else None,
        "Slope5": slope5,
        "NEGATIVE_SLOPE_5D": bool(slope5 < -EPS),
        "MOMENTUM_LOCK": momentum_lock,
        "drawdown_pct": drawdown_pct,
        "drawdown_window_days": window_dd,
        "price_range_10d_pct": price_range_10d_pct,
        "gap_down": gap_down,
        "metrics_reason": "OK"
    }

def calculate_dynamic_vix(vix_df: pd.DataFrame) -> Optional[float]:
    if vix_df is None or vix_df.empty:
        return None
    try:
        vix_close = _as_close_series(vix_df)
        if len(vix_close) < 20:
            return 40.0
        ma20 = vix_close.rolling(20).mean().iloc[-1]
        std20 = vix_close.rolling(20).std().iloc[-1]
        threshold = ma20 + 2 * std20
        return max(35.0, float(threshold))
    except Exception:
        return 35.0

def _calc_ma14_monthly_from_daily(df_daily: pd.DataFrame) -> Optional[float]:
    try:
        if df_daily is None or df_daily.empty:
            return None
        df = df_daily.copy()
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.set_index("Datetime")
        close = _as_close_series(df)
        monthly = close.resample("M").last().dropna()
        if len(monthly) < 14:
            return None
        ma14 = monthly.rolling(14).mean().dropna()
        return float(ma14.iloc[-1])
    except Exception:
        return None

def _extract_close_price(df_daily: pd.DataFrame) -> Optional[float]:
    try:
        if df_daily is None or df_daily.empty:
            return None
        close = _as_close_series(df_daily)
        return float(close.iloc[-1]) if len(close) else None
    except Exception:
        return None

def _count_close_below_ma_days(df_daily: pd.DataFrame, ma14_monthly: Optional[float]) -> int:
    try:
        if ma14_monthly is None or df_daily is None or df_daily.empty:
            return 0
        close = _as_close_series(df_daily)
        if len(close) < 2:
            return 0
        thresh = float(ma14_monthly) * 0.96
        recent = close.iloc[-5:].tolist()
        cnt = 0
        for v in reversed(recent):
            if float(v) < thresh:
                cnt += 1
            else:
                break
        return int(cnt)
    except Exception:
        return 0

def pick_regime(
    metrics: dict,
    vix: Optional[float] = None,
    ma14_monthly: Optional[float] = None,
    close_price: Optional[float] = None,
    close_below_ma_days: int = 0,
    vix_panic: float = 35.0,
    **kwargs
) -> Tuple[str, float]:
    # 防呆：支援舊參數 typo
    if "vixpanic" in kwargs and kwargs["vixpanic"]:
        vix_panic = float(kwargs["vixpanic"])
    if "vipxanic" in kwargs and kwargs["vipxanic"]:
        vix_panic = float(kwargs["vipxanic"])

    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")
    price_range = metrics.get("price_range_10d_pct")

    # CRASH_RISK
    if (vix is not None and float(vix) > float(vix_panic)) or (drawdown is not None and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10

    # HIBERNATION
    if ma14_monthly is not None and close_price is not None and int(close_below_ma_days) >= 2:
        if float(close_price) < float(ma14_monthly) * 0.96:
            return "HIBERNATION", 0.20

    # MEAN_REVERSION / OVERHEAT
    if smr is not None and slope5 is not None:
        if float(smr) > 0.25 and float(slope5) < -EPS:
            return "MEAN_REVERSION", 0.45
        if float(smr) > 0.25 and float(slope5) >= -EPS:
            return "OVERHEAT", 0.55

    # CONSOLIDATION
    if smr is not None and 0.08 <= float(smr) <= 0.18:
        if price_range is not None and float(price_range) < 0.05:
            return "CONSOLIDATION", 0.65

    return "NORMAL", 0.85

def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    inst_streak3 = int(inst.get("inst_streak3", 0))

    if foreign_buy and trust_buy and inst_streak3 >= 3:
        return "A+"
    if (foreign_buy or trust_buy) and inst_streak3 >= 3:
        return "A"

    vr = _safe_float(vol_ratio, None)
    if momentum_lock and (vr is not None and float(vr) > 0.8) and regime in ["NORMAL", "OVERHEAT", "CONSOLIDATION"]:
        return "B"
    return "NONE"

def compute_integrity_and_kill(stocks: List[dict], amount: MarketAmount, metrics: dict, twii_ok: bool, vix_ok: bool) -> dict:
    """
    本版增強：把 TWII/VIX 的「可用性」納入 integrity（你要的稽核）
    - 仍維持保守：若 TWII 無法計算 SMR（metrics_reason != OK）→ 視為 critical missing（但不直接 kill，交給你的策略選擇）
    - 若你要更強硬，可把 twii_ok=False 直接 kill（我先保守：先告警 + degraded）
    """
    n = len(stocks)
    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)
    amount_total_null = (amount.amount_total is None)

    denom = max(1, (2 * n + 1))
    core_missing = price_null + volratio_null + (1 if amount_total_null else 0)
    core_missing_pct = float(core_missing / denom)

    gap_down = metrics.get("gap_down")
    is_gap_crash = bool(gap_down is not None and float(gap_down) <= -0.07)

    kill = False
    reasons = []

    # 核心：個股資料全掛
    if n > 0 and price_null == n:
        kill = True
        reasons.append(f"price_null={price_null}/{n}")
    if n > 0 and volratio_null == n:
        kill = True
        reasons.append(f"volratio_null={volratio_null}/{n}")

    # 成交額缺失（不一定 kill，但要記錄）
    if amount_total_null:
        reasons.append("amount_total_null=True")

    # 缺失率
    if core_missing_pct >= 0.50:
        kill = True
        reasons.append(f"core_missing_pct={core_missing_pct:.2f}")

    # 跳空風險
    if is_gap_crash:
        kill = True
        reasons.append(f"GAP_DOWN_CRASH({gap_down:.1%})")

    # 稽核：TWII/VIX 可用性（先不 kill，但明確輸出）
    if not twii_ok:
        reasons.append("TWII_NOT_OK")
    if not vix_ok:
        reasons.append("VIX_NOT_OK")

    return {
        "n": n,
        "price_null": price_null,
        "volratio_null": volratio_null,
        "core_missing_pct": core_missing_pct,
        "amount_total_null": amount_total_null,
        "is_gap_crash": is_gap_crash,
        "twii_ok": bool(twii_ok),
        "vix_ok": bool(vix_ok),
        "kill": bool(kill),
        "reason": ("DATA_MISSING " + ", ".join(reasons)) if reasons else "OK",
        "metrics_reason": metrics.get("metrics_reason")
    }

def build_active_alerts(integrity: dict, amount: MarketAmount) -> List[str]:
    alerts = []
    if integrity.get("kill"):
        alerts.append("KILL_SWITCH_ACTIVATED")
    if integrity.get("is_gap_crash"):
        alerts.append("CRITICAL: 市場跳空重挫 (>7%)")
    if amount.amount_total is None:
        alerts.append("DEGRADED_AMOUNT: 成交量數據完全缺失")
    if not integrity.get("twii_ok", True):
        alerts.append(f"CRITICAL: TWII 指數資料不可用（{integrity.get('metrics_reason','UNKNOWN')}）")
    if not integrity.get("vix_ok", True):
        alerts.append("DEGRADED: VIX 資料不可用（可能限流/空表）")

    n = int(integrity.get("n") or 0)
    if n > 0 and int(integrity.get("price_null") or 0) == n:
        alerts.append("CRITICAL: 所有個股價格=null")
    if n > 0 and int(integrity.get("volratio_null") or 0) == n:
        alerts.append("CRITICAL: 所有個股量能=null")
    cm = float(integrity.get("core_missing_pct") or 0.0)
    if cm >= 0.50:
        alerts.append(f"DATA_INTEGRITY_FAILURE: 缺失率={cm:.2f}")
    if integrity.get("kill"):
        alerts.append("FORCED_ALL_CASH: 強制避險模式")
    return alerts

# =========================
# Arbiter input builder
# =========================
def _default_symbols_pool(topn: int) -> List[str]:
    pool = list(STOCK_NAME_MAP.keys())
    limit = min(len(pool), max(1, int(topn)))
    return pool[:limit]

def _source_diag(df: pd.DataFrame, name: str, min_rows: int, require_close: bool = True) -> dict:
    if df is None or df.empty:
        return {"name": name, "ok": False, "rows": 0, "cols": [], "last_dt": None, "reason": "EMPTY"}
    cols = list(df.columns)
    try:
        last_dt = pd.to_datetime(df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")
    except Exception:
        last_dt = None
    ok = True
    reason = "OK"
    if len(df) < min_rows:
        ok = False
        reason = f"TOO_SHORT_ROWS={len(df)}(<{min_rows})"
    if require_close and ("Close" not in cols and "Adj Close" not in cols):
        ok = False
        reason = "NO_CLOSE_COL"
    return {"name": name, "ok": ok, "rows": int(len(df)), "cols": cols[:12], "last_dt": last_dt, "reason": reason}

def build_arbiter_input(
    session: str,
    account_mode: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
    finmind_token: Optional[str],
) -> Tuple[dict, List[dict]]:

    # 1) Market History & Metrics
    twii_df = fetch_history(TWII_SYMBOL, period="5y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")

    twii_diag = _source_diag(twii_df, "TWII", min_rows=260, require_close=True)
    vix_diag = _source_diag(vix_df, "VIX", min_rows=20, require_close=True)

    vix_last = None
    if vix_diag["ok"]:
        try:
            vix_close = _as_close_series(vix_df)
            vix_last = float(vix_close.iloc[-1]) if len(vix_close) else None
        except Exception:
            vix_last = None
            vix_diag["ok"] = False
            vix_diag["reason"] = "CLOSE_PARSE_FAIL"

    # Dynamic VIX Threshold
    dynamic_vix_threshold = calculate_dynamic_vix(vix_df)

    metrics = compute_regime_metrics(twii_df)
    close_price = _extract_close_price(twii_df)
    ma14_monthly = _calc_ma14_monthly_from_daily(twii_df)
    close_below_days = _count_close_below_ma_days(twii_df, ma14_monthly)

    # TWII change/pct
    twii_change = None
    twii_pct = None
    if twii_diag["ok"]:
        try:
            c = _as_close_series(twii_df)
            if len(c) >= 2:
                twii_change = float(c.iloc[-1] - c.iloc[-2])
                twii_pct = float(c.iloc[-1] / c.iloc[-2] - 1.0)
        except Exception:
            pass

    # Regime
    regime, max_equity = pick_regime(
        metrics,
        vix=vix_last,
        ma14_monthly=ma14_monthly,
        close_price=close_price,
        close_below_ma_days=close_below_days
    )

    # 2) Market Amount & Institutions
    amount = fetch_amount_total(allow_insecure_ssl)
    market_inst_summary = fetch_market_inst_summary(allow_insecure_ssl)

    # 3) Stocks Data (TopN + Positions)
    base_pool = _default_symbols_pool(topn)
    pos_pool = [p.get("symbol") for p in positions if p.get("symbol")]
    symbols = list(dict.fromkeys(base_pool + pos_pool))

    pv = fetch_batch_prices_volratio(symbols)

    # trade_date
    trade_date = None
    if twii_df is not None and not twii_df.empty and "Datetime" in twii_df.columns:
        try:
            trade_date = pd.to_datetime(twii_df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")
        except Exception:
            trade_date = None

    end_date = trade_date or time.strftime("%Y-%m-%d", time.localtime())
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    inst_df = fetch_finmind_institutional(symbols, start_date=start_date, end_date=end_date, token=finmind_token)

    panel_rows = []
    inst_map = {}
    stocks = []

    for i, sym in enumerate(symbols, start=1):
        inst3 = calc_inst_3d(inst_df, sym)
        net3 = float(inst3.get("Inst_Net_3d", 0.0))

        p_row = {
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym),
            "Foreign_Net": net3,
            "Trust_Net": net3,
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
            "Inst_Status": inst3.get("Inst_Status", "PENDING"),
            "Inst_Dir3": inst3.get("Inst_Dir3", "PENDING"),
            "Inst_Net_3d": net3,
            "inst_source": "FINMIND_3D_NET"
        }
        panel_rows.append(p_row)

        inst_map[sym] = {
            "foreign_buy": bool(net3 > 0),
            "trust_buy": bool(net3 > 0),
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
            "Inst_Net_3d": net3,
            "inst_streak3": int(inst3.get("Inst_Streak3", 0))
        }

        row = pv[pv["Symbol"] == sym].iloc[0] if (not pv.empty and (pv["Symbol"] == sym).any()) else None
        price = row["Price"] if row is not None else None
        vol_ratio = row["Vol_Ratio"] if row is not None else None

        if price is None:
            warnings_bus.push("PRICE_NULL", "Missing Price", {"symbol": sym})
        if vol_ratio is None:
            warnings_bus.push("VOLRATIO_NULL", "Missing VolRatio", {"symbol": sym})

        inst_data = inst_map.get(sym, {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0})
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vol_ratio, inst_data)

        stocks.append({
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym),
            "Tier": i,
            "Price": None if (price is None or pd.isna(price)) else float(price),
            "Vol_Ratio": None if (vol_ratio is None or pd.isna(vol_ratio)) else float(vol_ratio),
            "Layer": layer,
            "Institutional": inst_data
        })

    institutional_panel = pd.DataFrame(panel_rows)

    # Integrity / alerts
    integrity = compute_integrity_and_kill(
        stocks=stocks,
        amount=amount,
        metrics=metrics,
        twii_ok=bool(twii_diag["ok"]),
        vix_ok=bool(vix_diag["ok"])
    )
    active_alerts = build_active_alerts(integrity, amount)

    current_exposure_pct = min(1.0, len(positions) * 0.05) if positions else 0.0

    # Market status rule (保守：只要成交額缺失或 VIX/TWII 不 ok → DEGRADED)
    market_status = "NORMAL"
    if amount.amount_total is None or (not twii_diag["ok"]) or (not vix_diag["ok"]):
        market_status = "DEGRADED"

    final_regime = "UNKNOWN" if integrity["kill"] else regime
    final_max_equity = 0.0 if integrity["kill"] else max_equity

    if integrity["kill"]:
        market_status = "SHELTER"
        current_exposure_pct = 0.0

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": market_status,
            "current_regime": final_regime,
            "account_mode": account_mode,
            "audit_tag": "V16.3.3_SPEC_COMPLIANT"
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "twii_close": close_price,
                "twii_change": twii_change,
                "twii_pct": twii_pct,
                "vix": vix_last,
                "smr": metrics.get("SMR"),
                "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"),
                "price_range_10d_pct": metrics.get("price_range_10d_pct"),
                "dynamic_vix_threshold": dynamic_vix_threshold,
                "max_equity_allowed_pct": final_max_equity
            },
            "sources": {
                "twii": twii_diag,
                "vix": vix_diag,
                "metrics_reason": metrics.get("metrics_reason"),
                "amount_source": {
                    "source_twse": amount.source_twse,
                    "source_tpex": amount.source_tpex,
                    "amount_total": amount.amount_total
                }
            },
            "market_amount": asdict(amount),
            "market_inst_summary": market_inst_summary,
            "integrity": integrity
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float(100.0 * max(0.0, 1.0 - current_exposure_pct)),
            "active_alerts": active_alerts
        },
        "institutional_panel": institutional_panel.to_dict(orient="records"),
        "stocks": stocks,
        "positions_input": positions,
        "decisions": [],
        "audit_log": []
    }

    return payload, warnings_bus.latest(50)

# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定 (Settings)")
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=0)
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=2)
    topn = st.sidebar.selectbox("TopN（監控數量）", [8, 10, 15, 20, 30], index=3)
    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL", value=False)

    st.sidebar.subheader("FinMind")
    finmind_token = st.sidebar.text_input("FinMind Token", type="password").strip() or None

    st.sidebar.subheader("持倉 (JSON List)")
    positions_text = st.sidebar.text_area("positions", value="[]", height=120)

    cash_balance = st.sidebar.number_input("現金餘額", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("總權益", min_value=0, value=DEFAULT_EQUITY, step=10000)

    run_btn = st.sidebar.button("啟動中控台")

    positions = []
    try:
        positions = json.loads(positions_text) if positions_text.strip() else []
    except Exception:
        positions = []

    if run_btn or "auto_ran" not in st.session_state:
        st.session_state["auto_ran"] = True
        try:
            payload, warns = build_arbiter_input(
                session=session,
                account_mode=account_mode,
                topn=int(topn),
                positions=positions,
                cash_balance=int(cash_balance),
                total_equity=int(total_equity),
                allow_insecure_ssl=bool(allow_insecure_ssl),
                finmind_token=finmind_token
            )
        except Exception as e:
            st.error(f"系統錯誤: {e}")
            return

        ov = payload.get("macro", {}).get("overview", {})
        meta = payload.get("meta", {})
        amount = payload.get("macro", {}).get("market_amount", {})
        inst_summary = payload.get("macro", {}).get("market_inst_summary", [])
        sources = payload.get("macro", {}).get("sources", {})

        # --- 1) 關鍵指標 ---
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("交易日期", ov.get("trade_date", "-"))
        c2.metric("市場狀態", meta.get("market_status", "-"))
        c3.metric("策略體制 (Regime)", meta.get("current_regime", "-"))
        c4.metric(
            "建議持倉上限",
            f"{_pct01_to_pct100(ov.get('max_equity_allowed_pct')):.0f}%"
            if ov.get("max_equity_allowed_pct") is not None else "-"
        )

        # --- 2) 資料源健康度（你要的稽核核心） ---
        st.subheader("🧪 資料源健康度（Sources Health Audit）")
        s1, s2, s3 = st.columns(3)

        twii_s = sources.get("twii", {})
        vix_s = sources.get("vix", {})
        met_reason = sources.get("metrics_reason", "-")

        twii_txt = f"rows={twii_s.get('rows',0)} / last={twii_s.get('last_dt','-')} / reason={twii_s.get('reason','-')}"
        vix_txt = f"rows={vix_s.get('rows',0)} / last={vix_s.get('last_dt','-')} / reason={vix_s.get('reason','-')}"
        s1.metric("TWII 指數源", "OK" if twii_s.get("ok") else "FAIL", twii_txt)
        s2.metric("VIX 指數源", "OK" if vix_s.get("ok") else "FAIL", vix_txt)
        s3.metric("SMR 計算狀態", met_reason)

        # --- 3) 大盤與成交量 ---
        st.subheader("📊 大盤觀測站 (TAIEX Overview)")
        m1, m2, m3, m4 = st.columns(4)

        close = ov.get("twii_close")
        chg = ov.get("twii_change")
        pct = ov.get("twii_pct")

        delta_color = "normal"
        if chg is not None:
            delta_color = "normal" if float(chg) >= 0 else "inverse"

        m1.metric(
            "加權指數",
            f"{close:,.0f}" if close is not None else "-",
            f"{chg:+.0f} ({pct:+.2%})" if (chg is not None and pct is not None) else None,
            delta_color=delta_color
        )
        m2.metric("VIX 恐慌指數", f"{ov.get('vix'):.2f}" if ov.get("vix") is not None else "-")

        amt_total = amount.get("amount_total")
        amt_str = f"{amt_total/1_0000_0000:.1f} 億" if amt_total else "數據缺失"
        m3.metric("市場總成交額", amt_str)

        m4.metric("SMR 乖離率", f"{ov.get('smr'):.4f}" if ov.get("smr") is not None else "-")

        # --- 4) 三大法人（全市場） ---
        st.subheader("🏛️ 三大法人買賣超 (全市場)")
        if inst_summary:
            cols = st.columns(len(inst_summary))
            for idx, item in enumerate(inst_summary):
                net = float(item.get("Net", 0) or 0)
                net_yi = net / 1_0000_0000
                cols[idx].metric(str(item.get("Identity", "")), f"{net_yi:+.2f} 億")
        else:
            st.info("暫無今日法人統計資料（可能尚未更新或抓取失敗）")

        # --- 5) 警報區 ---
        alerts = payload.get("portfolio", {}).get("active_alerts", [])
        if alerts:
            st.subheader("⚠️ 戰術警報 (Active Alerts)")
            for a in alerts:
                if "CRITICAL" in a or "KILL" in a:
                    st.error(a)
                else:
                    st.warning(a)

        # --- 6) 系統診斷（含 meta） ---
        st.subheader("🛠️ 系統健康診斷 (System Health)")
        if not warns:
            st.success("✅ 系統運作正常，無錯誤日誌 (Clean Run)。")
        else:
            with st.expander(f"⚠️ 偵測到 {len(warns)} 條系統警示 (點擊查看詳情)", expanded=True):
                st.warning("系統遭遇部分數據抓取失敗；本版已輸出 sources 稽核資訊，請優先看 TWII/VIX 狀態。")
                w_df = pd.DataFrame(warns)
                if not w_df.empty and {"ts", "code", "msg"}.issubset(set(w_df.columns)):
                    # 額外把 meta 也顯示，避免你看不到根因
                    show_cols = ["ts", "code", "msg"]
                    if "meta" in w_df.columns:
                        show_cols.append("meta")
                    st.dataframe(w_df[show_cols], use_container_width=True)
                else:
                    st.write(warns)

        # --- 7) 個股表 ---
        st.subheader("🎯 核心持股雷達 (Tactical Stocks)")
        s_df = pd.json_normalize(payload.get("stocks", []))
        if not s_df.empty:
            disp_cols = ["Symbol", "Name", "Price", "Vol_Ratio", "Layer", "Institutional.Inst_Net_3d", "Institutional.Inst_Streak3"]
            s_df = s_df.reindex(columns=disp_cols, fill_value=0)
            s_df = s_df.rename(columns=COL_TRANSLATION)
            s_df = s_df.rename(columns={
                "Institutional.Inst_Net_3d": "法人3日淨額",
                "Institutional.Inst_Streak3": "法人連買天數"
            })
            st.dataframe(s_df, use_container_width=True)

        # --- 8) 法人明細 ---
        with st.expander("🔍 查看法人詳細數據 (Institutional Debug Panel)"):
            inst_df = pd.DataFrame(payload.get("institutional_panel", []))
            if not inst_df.empty:
                st.dataframe(inst_df.rename(columns=COL_TRANSLATION), use_container_width=True)

        # --- 9) AI JSON 一鍵複製 ---
        st.markdown("---")
        c_copy1, _ = st.columns([0.8, 0.2])
        with c_copy1:
            st.subheader("🤖 AI JSON (Arbiter Input)")

        json_str = json.dumps(payload, indent=4, ensure_ascii=False)
        st.markdown("##### 📋 點擊下方代碼塊右上角的「複製圖示」即可複製完整數據")
        st.code(json_str, language="json")

if __name__ == "__main__":
    main()
