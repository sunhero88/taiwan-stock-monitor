# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3 Stable Hybrid）
# ✅ 併入 Kill-Switch（V16.2 Enhanced）
# ✅ 修正 pick_regime(...) 參數不相容（vixpanic / vipxanic 等）
# ✅ 修正 yfinance 取價/量比率偶發 TypeError（改為批次抓取 + 防呆展開）
# ✅ drawdown_pct 改為「當前距離近一年高點回撤」(trailing 252D) 避免 ATH 卻 -28% 的誤判
# ✅ 修正 _as_close_series() MultiIndex columns 問題
# ✅ 修正 Active Alerts 顯示亂碼問題
# ✅ 新增 JSON 一鍵複製功能 (st.code)
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
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3 Stable Hybrid）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3 Stable Hybrid）"
st.title(APP_TITLE)

# =========================
# Constants / helpers
# =========================
EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

# 預設改為 20，符合您的戰術需求
DEFAULT_TOPN = 20  
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}
B_FOREIGN_NAME = "Foreign_Investor"

NEUTRAL_THRESHOLD = 5_000_000  # 5,000,000 (你指定門檻)


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
            if s == "":
                return default
            return int(float(s))
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
# Market amount (TWSE/TPEX) best-effort
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

        # heuristic: try fields9 + data9 total row
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

        warnings_bus.push(
            "TWSE_AMOUNT_PARSE_FAIL",
            "TWSE amount parse failed (schema changed?)",
            {"url": url, "keys": list(js.keys())[:30]},
        )
        return None, "TWSE_FAIL:PARSE"

    except requests.exceptions.SSLError as e:
        warnings_bus.push("TWSE_AMOUNT_SSL_ERROR", f"TWSE SSL error: {e}", {"url": url})
        return None, "TWSE_FAIL:SSLError"
    except Exception as e:
        warnings_bus.push("TWSE_AMOUNT_FETCH_FAIL", f"TWSE fetch fail: {e}", {"url": url})
        return None, "TWSE_FAIL:FETCH_ERROR"


def _fetch_tpex_amount(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw"
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()

        try:
            js = r.json()
        except Exception as e:
            warnings_bus.push(
                "TPEX_AMOUNT_PARSE_FAIL",
                f"TPEX JSON decode error: {e}",
                {"url": url, "text_head": r.text[:200]},
            )
            return None, "TPEX_FAIL:JSONDecodeError"

        # try common keys
        for key in ["totalAmount", "trade_value", "amount", "amt", "成交金額"]:
            if key in js:
                v = _safe_int(js.get(key), default=None)
                if v is not None:
                    return int(v), "TPEX_OK:st43_result"

        warnings_bus.push(
            "TPEX_AMOUNT_PARSE_FAIL",
            "TPEX amount parse failed (no numeric keys)",
            {"url": url, "keys": list(js.keys())[:30]},
        )
        return None, "TPEX_FAIL:PARSE"

    except requests.exceptions.SSLError as e:
        warnings_bus.push("TPEX_AMOUNT_SSL_ERROR", f"TPEX SSL error: {e}", {"url": url})
        return None, "TPEX_FAIL:SSLError"
    except Exception as e:
        warnings_bus.push("TPEX_AMOUNT_FETCH_FAIL", f"TPEX fetch fail: {e}", {"url": url})
        return None, "TPEX_FAIL:FETCH_ERROR"


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

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=bool(allow_insecure_ssl),
    )


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


def fetch_finmind_institutional(
    symbols: List[str],
    start_date: str,
    end_date: str,
    token: Optional[str] = None,
) -> pd.DataFrame:
    """
    個股法人淨額（外資+投信+自營(含避險)）
    回傳 columns: date, symbol, net_amount
    """
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
            warnings_bus.push("FINMIND_INST_FETCH_FAIL", f"{sym} fetch fail: {e}", {"symbol": sym})
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
    """
    回傳：
      Inst_Status: READY/PENDING
      Inst_Streak3: 3 或 0
      Inst_Dir3: POSITIVE/NEGATIVE/NEUTRAL/PENDING
      Inst_Net_3d: 三日加總
    """
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
# yfinance fetchers (robust)
# =========================
@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_history(symbol: str, period: str = "3y", interval: str = "1d") -> pd.DataFrame:
    """
    修正版：處理 yfinance 可能返回 MultiIndex columns 的問題
    """
    try:
        df = yf.download(
            symbol,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            group_by="column",
            threads=False,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        
        # 處理 MultiIndex columns (當單一 symbol 時，yfinance 有時仍會返回 MultiIndex)
        if isinstance(df.columns, pd.MultiIndex):
            # 嘗試壓平 MultiIndex
            df.columns = [' '.join([str(c) for c in col if str(c) != '']).strip() for col in df.columns.values]
            # 移除可能的 symbol 前綴
            df.columns = [c.replace(f'{symbol} ', '').strip() for c in df.columns]
        
        df = df.reset_index()
        
        # 統一日期欄位名稱
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "Datetime"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "Datetime"})
        
        if "Datetime" not in df.columns and df.index.name is not None:
            df.insert(0, "Datetime", pd.to_datetime(df.index))
        
        return df
    except Exception as e:
        warnings_bus.push("YF_HISTORY_FAIL", f"{symbol} yfinance history fail: {e}", {"symbol": symbol})
        return pd.DataFrame()


@st.cache_data(ttl=60 * 5, show_spinner=False)
def fetch_batch_prices_volratio(symbols: List[str]) -> pd.DataFrame:
    """
    批次抓：last_close、vol_ratio(20D)
    回傳 columns: Symbol, Price, Vol_Ratio, source
    """
    out = pd.DataFrame({"Symbol": symbols})
    out["Price"] = None
    out["Vol_Ratio"] = None
    out["source"] = "NONE"

    if not symbols:
        return out

    try:
        df = yf.download(
            symbols,
            period="6mo",
            interval="1d",
            auto_adjust=False,
            progress=False,
            group_by="ticker",
            threads=False,
        )
    except Exception as e:
        warnings_bus.push("YF_BATCH_FAIL", f"yfinance batch fail: {e}", {"n": len(symbols)})
        return out

    if df is None or df.empty:
        warnings_bus.push("YF_BATCH_EMPTY", "yfinance batch returned empty dataframe", {"n": len(symbols)})
        return out

    # df 可能是：
    # - 多標的：columns MultiIndex (Ticker, Field)
    # - 單標的：columns 單層 (Open/High/Low/Close/Adj Close/Volume)
    for sym in symbols:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                if sym not in df.columns.get_level_values(0):
                    warnings_bus.push("YF_SYMBOL_MISSING", f"{sym} missing in batch", {"symbol": sym})
                    continue
                close = df[(sym, "Close")].dropna()
                vol = df[(sym, "Volume")].dropna()
            else:
                # 單一標的情境：df 就是該 symbol
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

        except Exception as e:
            warnings_bus.push("YF_SYMBOL_FAIL", f"{sym} yfinance parse fail: {e}", {"symbol": sym})
            continue

    return out


# =========================
# Regime metrics (scalar-only) - 修正版
# =========================
def _as_close_series(df: pd.DataFrame) -> pd.Series:
    """
    修正版：強化處理 yfinance 返回的各種 column 格式
    """
    if df is None or df.empty:
        raise ValueError("market_df is empty")
    
    # 嘗試多種可能的欄位名稱
    possible_names = ["Close", "close", "Adj Close", "adj close"]
    
    for name in possible_names:
        if name in df.columns:
            s = df[name]
            # 如果是 DataFrame (MultiIndex 情況)，取第一欄
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            return s.astype(float)
    
    # 如果都找不到，嘗試從 columns 中尋找包含 'close' 的欄位
    close_cols = [c for c in df.columns if 'close' in str(c).lower()]
    if close_cols:
        s = df[close_cols[0]]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        return s.astype(float)
    
    raise ValueError(f"Close column not found. Available columns: {list(df.columns)}")


def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    """
    SMR / Slope5 / momentum_lock / drawdown_pct
    drawdown_pct：改為「當前距離近一年(252D)高點回撤」，避免 ATH 卻觸發 CRASH_RISK
    """
    if market_df is None or market_df.empty or len(market_df) < 260:
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
            "drawdown_window_days": 252,
        }

    try:
        close = _as_close_series(market_df)
    except ValueError as e:
        warnings_bus.push("CLOSE_SERIES_FAIL", f"Failed to extract close series: {e}", {})
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
            "drawdown_window_days": 252,
        }

    ma200 = close.rolling(200).mean()
    smr_series = ((close - ma200) / ma200).dropna()
    if len(smr_series) < 10:
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
            "drawdown_window_days": 252,
        }

    smr = float(smr_series.iloc[-1])
    smr_ma5 = smr_series.rolling(5).mean().dropna()
    slope5 = float(smr_ma5.iloc[-1] - smr_ma5.iloc[-2]) if len(smr_ma5) >= 2 else 0.0

    recent_slopes = smr_ma5.diff().dropna().iloc[-5:]
    negative_slope_5d = bool((recent_slopes < -EPS).all()) if len(recent_slopes) else True

    momentum_lock = False
    if len(smr_ma5) >= 5:
        last4 = smr_ma5.diff().dropna().iloc[-4:]
        momentum_lock = bool((last4 > EPS).all()) if len(last4) == 4 else False

    # trailing drawdown: current vs rolling 252D high
    window = 252
    rolling_high = close.rolling(window).max()
    if np.isnan(rolling_high.iloc[-1]):
        drawdown_pct = None
    else:
        drawdown_pct = float(close.iloc[-1] / rolling_high.iloc[-1] - 1.0)

    return {
        "SMR": smr,
        "SMR_MA5": float(smr_ma5.iloc[-1]) if len(smr_ma5) else None,
        "Slope5": slope5,
        "NEGATIVE_SLOPE_5D": negative_slope_5d,
        "MOMENTUM_LOCK": momentum_lock,
        "drawdown_pct": drawdown_pct,
        "drawdown_window_days": window,
    }


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
        return float(ma14.iloc[-1]) if len(ma14) else None
    except Exception as e:
        warnings_bus.push("MA14_CALC_FAIL", f"MA14 calculation failed: {e}", {})
        return None


def _extract_close_price(df_daily: pd.DataFrame) -> Optional[float]:
    try:
        if df_daily is None or df_daily.empty:
            return None
        close = _as_close_series(df_daily)
        return float(close.iloc[-1]) if len(close) else None
    except Exception as e:
        warnings_bus.push("CLOSE_PRICE_FAIL", f"Close price extraction failed: {e}", {})
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
    except Exception as e:
        warnings_bus.push("BELOW_MA_COUNT_FAIL", f"Below MA days count failed: {e}", {})
        return 0


# =========================
# pick_regime (兼容 vixpanic / vipxanic)
# =========================
def pick_regime(
    metrics: dict,
    vix: Optional[float] = None,
    ma14_monthly: Optional[float] = None,
    close_price: Optional[float] = None,
    close_below_ma_days: int = 0,
    vix_panic: float = 35.0,
    **kwargs,
) -> Tuple[str, float]:
    """
    回傳 (regime_name, max_equity_pct)
    - vix_panic：VIX panic 門檻，預設 35
    - **kwargs：吞掉你專案裡舊參數（vixpanic / vipxanic 等），避免 TypeError
    """
    # 吞掉常見錯字/舊欄位
    if "vixpanic" in kwargs and kwargs["vixpanic"] is not None:
        vix_panic = float(kwargs["vixpanic"])
    if "vipxanic" in kwargs and kwargs["vipxanic"] is not None:
        vix_panic = float(kwargs["vipxanic"])  # 兼容你截圖的參數名

    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")

    # --- CRASH_RISK (改用 trailing drawdown：<= -18%) ---
    if (vix is not None and float(vix) > float(vix_panic)) or (drawdown is not None and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10

    # --- HIBERNATION（2日 × 0.96） ---
    if (
        ma14_monthly is not None
        and close_price is not None
        and int(close_below_ma_days) >= 2
        and float(close_price) < float(ma14_monthly) * 0.96
    ):
        return "HIBERNATION", 0.20

    # --- MEAN_REVERSION / OVERHEAT ---
    if smr is not None and slope5 is not None:
        if float(smr) > 0.25 and float(slope5) < -EPS:
            return "MEAN_REVERSION", 0.45
        if float(smr) > 0.25 and float(slope5) >= -EPS:
            return "OVERHEAT", 0.55

    # --- CONSOLIDATION ---
    if smr is not None and 0.08 <= float(smr) <= 0.18:
        return "CONSOLIDATION", 0.65

    # --- NORMAL ---
    return "NORMAL", 0.85


# =========================
# Layer logic
# =========================
def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    inst_streak3 = int(inst.get("inst_streak3", 0))

    if foreign_buy and trust_buy and inst_streak3 >= 3:
        return "A+"
    if (foreign_buy or trust_buy) and inst_streak3 >= 3:
        return "A"

    vr = _safe_float(vol_ratio, None)
    if bool(momentum_lock) and (vr is not None and float(vr) > 0.8) and regime in ["NORMAL", "OVERHEAT", "CONSOLIDATION"]:
        return "B"

    return "NONE"


# =========================
# Kill-Switch (V16.2 Enhanced) — 併入 V16.3
# =========================
def compute_integrity_and_kill(
    stocks: List[dict],
    amount: MarketAmount,
) -> dict:
    n = len(stocks)
    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)

    amount_total_null = (amount.amount_total is None)
    # 核心缺失率：Price + Vol_Ratio + amount_total  三塊
    denom = max(1, (2 * n + 1))
    core_missing = price_null + volratio_null + (1 if amount_total_null else 0)
    core_missing_pct = float(core_missing / denom)

    # Kill 觸發條件（你貼的規則：核心缺失 > 50% / 兩項全滅 / amount 全滅）
    kill = False
    reasons = []
    if n > 0 and price_null == n:
        kill = True
        reasons.append(f"price_null={price_null}/{n}")
    if n > 0 and volratio_null == n:
        kill = True
        reasons.append(f"volratio_null={volratio_null}/{n}")
    if amount_total_null:
        reasons.append("amount_total_null=True")
    if core_missing_pct >= 0.50:
        kill = True
        reasons.append(f"core_missing_pct={core_missing_pct:.2f}")

    reason = "DATA_MISSING " + ", ".join(reasons) if reasons else "OK"

    return {
        "n": n,
        "price_null": price_null,
        "volratio_null": volratio_null,
        "core_missing_pct": core_missing_pct,
        "amount_total_null": amount_total_null,
        "kill": bool(kill),
        "reason": reason,
    }


def build_active_alerts(integrity: dict, amount: MarketAmount) -> List[str]:
    alerts = []
    if integrity.get("kill"):
        alerts.append("KILL_SWITCH_ACTIVATED")

    if amount.amount_total is None:
        alerts.append("DEGRADED_AMOUNT: 成交量數據完全缺失 (TWSE_FAIL + TPEX_FAIL)")

    n = int(integrity.get("n") or 0)
    if n > 0 and int(integrity.get("price_null") or 0) == n:
        alerts.append("CRITICAL: 所有個股價格 = null (無法執行任何決策)")
    if n > 0 and int(integrity.get("volratio_null") or 0) == n:
        alerts.append("CRITICAL: 所有個股 Vol_Ratio = null (Layer B 判定不可能)")

    cm = float(integrity.get("core_missing_pct") or 0.0)
    if cm >= 0.50:
        alerts.append(f"DATA_INTEGRITY_FAILURE: 核心數據缺失率={cm:.2f}")

    if integrity.get("kill"):
        alerts.append("FORCED_ALL_CASH: 資料品質不足，強制進入避險模式")

    return alerts


# =========================
# Arbiter input builder
# =========================
def _default_symbols_pool(topn: int) -> List[str]:
    # 2026 戰術核心監控名單 (Tactical 20)
    pool = [
        # --- 權值錨點 (Anchors) ---
        "2330.TW", # 台積電 (半導體核心)
        "2317.TW", # 鴻海 (AI伺服器/權值)
        "2454.TW", # 聯發科 (IC設計龍頭)
        "2308.TW", # 台達電 (綠能/電源)
        
        # --- AI 攻擊矛頭 (Alpha Leaders) ---
        "2382.TW", # 廣達 (AI伺服器代工)
        "3231.TW", # 緯創 (AI伺服器代工)
        "2376.TW", # 技嘉 (AI主機板/顯卡)
        "3017.TW", # 奇鋐 (散熱龍頭)
        "3324.TW", # 雙鴻 (散熱雙雄)
        "3661.TW", # 世芯-KY (IP/ASIC)
        
        # --- 金融防禦 (Financial Defenders) ---
        "2881.TW", # 富邦金 (獲利王)
        "2882.TW", # 國泰金 (壽險雙雄)
        "2891.TW", # 中信金 (銀行獲利穩)
        "2886.TW", # 兆豐金 (官股匯銀)

        # --- 傳產/週期 (Cyclical/Value) ---
        "2603.TW", # 長榮 (航運指標)
        "2609.TW", # 陽明 (航運)
        "1605.TW", # 華新 (電線電纜/原物料)
        "1513.TW", # 中興電 (重電/政策股)
        "1519.TW", # 華城 (重電/變壓器)
        "2002.TW"  # 中鋼 (鋼鐵龍頭/最後防線)
    ]
    # 確保返回數量不超過 pool 長度，避免 index error
    limit = min(len(pool), max(1, int(topn)))
    return pool[:limit]


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
    # --- market history ---
    twii_df = fetch_history(TWII_SYMBOL, period="5y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")

    vix_last = None
    if not vix_df.empty:
        try:
            vix_close = _as_close_series(vix_df)
            vix_last = float(vix_close.iloc[-1]) if len(vix_close) else None
        except Exception as e:
            warnings_bus.push("VIX_EXTRACT_FAIL", f"VIX extraction failed: {e}", {})

    # --- compute metrics ---
    metrics = compute_regime_metrics(twii_df) if not twii_df.empty else {
        "SMR": None, "Slope5": None, "MOMENTUM_LOCK": False, "drawdown_pct": None, "drawdown_window_days": 252
    }
    close_price = _extract_close_price(twii_df)
    ma14_monthly = _calc_ma14_monthly_from_daily(twii_df)
    close_below_days = _count_close_below_ma_days(twii_df, ma14_monthly)

    # --- regime (先依 V16.3 算；若 Kill-Switch 觸發，後面覆蓋為 UNKNOWN / 0%) ---
    regime, max_equity = pick_regime(
        metrics=metrics,
        vix=vix_last,
        ma14_monthly=ma14_monthly,
        close_price=close_price,
        close_below_ma_days=close_below_days,
        # 若你專案仍有 vixpanic/vipxanic 參數會傳進來，也不會炸
    )

    # --- market amount ---
    amount = fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl)

    # --- symbols ---
    symbols = _default_symbols_pool(topn)

    # --- prices & vol_ratio (robust batch) ---
    pv = fetch_batch_prices_volratio(symbols)

    # --- FinMind institutional (3D) ---
    # 取最近 7 天的資料以確保涵蓋 3 個交易日（遇到連假/週末不會缺）
    trade_date = None
    if not twii_df.empty and "Datetime" in twii_df.columns:
        trade_date = pd.to_datetime(twii_df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")

    end_date = trade_date or time.strftime("%Y-%m-%d", time.localtime())
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    inst_df = fetch_finmind_institutional(symbols, start_date=start_date, end_date=end_date, token=finmind_token)
    panel_rows = []
    inst_map = {}  # Symbol -> inst dict for layer
    for sym in symbols:
        inst3 = calc_inst_3d(inst_df, sym)
        # 這裡先用同一套 net 做 Foreign/Trust（你之後若拆分外資/投信，可改這兩欄）
        net3 = float(inst3.get("Inst_Net_3d", 0.0))
        panel_rows.append(
            {
                "Symbol": sym,
                "Foreign_Net": net3,
                "Trust_Net": net3,
                "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
                "Inst_Status": inst3.get("Inst_Status", "PENDING"),
                "Inst_Dir3": inst3.get("Inst_Dir3", "PENDING"),
                "Inst_Net_3d": net3,
                "inst_source": "FINMIND_3D_NET",
            }
        )
        inst_map[sym] = {
            "foreign_buy": bool(net3 > 0),
            "trust_buy": bool(net3 > 0),
            "inst_streak3": int(inst3.get("Inst_Streak3", 0)),
        }

    institutional_panel = pd.DataFrame(panel_rows)

    # --- stocks snapshot ---
    stocks = []
    for i, sym in enumerate(symbols, start=1):
        row = pv[pv["Symbol"] == sym].iloc[0] if not pv.empty and (pv["Symbol"] == sym).any() else None
        price = None if row is None else row["Price"]
        vol_ratio = None if row is None else row["Vol_Ratio"]

        if price is None:
            warnings_bus.push("PRICE_NULL", f"{sym} Price is null after yfinance batch", {"symbol": sym})
        if vol_ratio is None:
            warnings_bus.push("VOLRATIO_NULL", f"{sym} Vol_Ratio is null after yfinance batch", {"symbol": sym})

        inst = inst_map.get(sym, {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0})
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vol_ratio, inst)

        stocks.append(
            {
                "Symbol": sym,
                "Name": sym,  # TODO：你要中文名稱可在此用對照表替換
                "Tier": i,
                "Price": None if pd.isna(price) else price,
                "Vol_Ratio": None if pd.isna(vol_ratio) else vol_ratio,
                "Layer": layer,
                "Institutional": inst,
            }
        )

    # --- integrity + kill-switch ---
    integrity = compute_integrity_and_kill(stocks, amount)
    active_alerts = build_active_alerts(integrity, amount)

    # --- portfolio ---
    current_exposure_pct = 0.0
    if positions:
        current_exposure_pct = min(1.0, len(positions) * 0.05)

    # --- market_status + override when kill ---
    market_status = "NORMAL" if (amount.amount_total is not None) else "DEGRADED"
    final_regime = regime
    final_max_equity = max_equity

    audit_log = []
    if integrity["kill"]:
        market_status = "SHELTER"
        final_regime = "UNKNOWN"
        final_max_equity = 0.0
        current_exposure_pct = 0.0

        audit_log.append(
            {
                "symbol": "ALL",
                "event": "KILL_SWITCH_TRIGGERED",
                "attribution": "DATA_MISSING",
                "comment": integrity["reason"],
            }
        )
        audit_log.append(
            {
                "symbol": "ALL",
                "event": "DEGRADED_STATUS_CRITICAL",
                "attribution": "MARKET_AMOUNT_FAILURE",
                "comment": f"amount_total=None, source_twse={amount.source_twse}, source_tpex={amount.source_tpex}",
            }
        )
        audit_log.append(
            {
                "symbol": "ALL",
                "event": "ALL_CASH_FORCED",
                "attribution": "SYSTEM_PROTECTION",
                "comment": "核心哲學: In Doubt → Cash. 當前數據品質無法支持任何進場決策",
            }
        )

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": market_status,
            "current_regime": final_regime,
            "account_mode": account_mode,
            "audit_tag": "V16.3_STABLE_HYBRID_KILLSWITCH",
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "twii_close": close_price,
                "vix": vix_last,
                "smr": metrics.get("SMR"),
                "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"),
                "drawdown_window_days": metrics.get("drawdown_window_days"),
                "ma14_monthly": ma14_monthly,
                "close_below_ma_days": close_below_days,
                "max_equity_allowed_pct": final_max_equity,
            },
            "market_amount": asdict(amount),
            "integrity": {
                "n": integrity["n"],
                "price_null": integrity["price_null"],
                "volratio_null": integrity["volratio_null"],
                "core_missing_pct": integrity["core_missing_pct"],
                "kill": integrity["kill"],
                "reason": integrity["reason"],
            },
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float(100.0 * max(0.0, 1.0 - current_exposure_pct)),
            "active_alerts": active_alerts,
        },
        "institutional_panel": institutional_panel.to_dict(orient="records"),
        "stocks": stocks,
        "positions_input": positions,
        "decisions": [],
        "audit_log": audit_log,
    }

    return payload, warnings_bus.latest(50)


# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定")

    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=0)
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
    topn = st.sidebar.selectbox("TopN（固定池化數量）", [8, 10, 15, 20, 30], index=0)

    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL (verify=False)", value=False)

    st.sidebar.subheader("FinMind")
    finmind_token = st.sidebar.text_input("FinMind Token（選填）", value="", type="password")
    finmind_token = finmind_token.strip() or None

    st.sidebar.subheader("持倉（手動貼 JSON 陣列）")
    positions_text = st.sidebar.text_area("positions", value="[]", height=120)

    cash_balance = st.sidebar.number_input("現金餘額（新台幣）", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("總權益（新台幣）", min_value=0, value=DEFAULT_EQUITY, step=10000)

    run_btn = st.sidebar.button("跑步")

    # parse positions
    positions = []
    try:
        positions = json.loads(positions_text) if positions_text.strip() else []
        if not isinstance(positions, list):
            raise ValueError("positions 必須是 JSON array")
    except Exception as e:
        st.sidebar.error(f"positions JSON 解析失敗：{e}")
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
                finmind_token=finmind_token,
            )
        except Exception as e:
            st.error("App 執行期間發生例外（已捕捉，不會白屏）。")
            st.exception(e)
            return

        ov = payload.get("macro", {}).get("overview", {})
        meta = payload.get("meta", {})
        integrity = payload.get("macro", {}).get("integrity", {})
        portfolio = payload.get("portfolio", {})

        # KPI row
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("交易日", ov.get("trade_date", "-"))
        c2.metric("market_status", meta.get("market_status", "-"))
        c3.metric("regime", meta.get("current_regime", "-"))
        c4.metric("SMR", f"{_safe_float(ov.get('smr'), 0):.6f}" if ov.get("smr") is not None else "NA")
        c5.metric("Slope5", f"{_safe_float(ov.get('slope5'), 0):.6f}" if ov.get("slope5") is not None else "NA")
        c6.metric("Max Equity", f"{_pct01_to_pct100(ov.get('max_equity_allowed_pct')):.1f}%" if ov.get("max_equity_allowed_pct") is not None else "NA")

        st.caption(
            f"Integrity｜Price null={integrity.get('price_null')}/{integrity.get('n')}｜"
            f"Vol_Ratio null={integrity.get('volratio_null')}/{integrity.get('n')}｜"
            f"core_missing_pct={integrity.get('core_missing_pct'):.2f}"
        )

        # Active Alerts (已修正亂碼問題)
        st.subheader("Active Alerts")
        alerts = portfolio.get("active_alerts", []) or []
        if alerts:
            for a in alerts:
                if "CRITICAL" in a or "KILL" in a:
                    st.error(a)
                else:
                    st.warning(a)
        else:
            st.success("（目前沒有 alerts）")

        # Market amount
        st.subheader("市場成交金額（best-effort / 可稽核）")
        st.json(payload.get("macro", {}).get("market_amount", {}))

        # Indices snapshot
        st.subheader("指數快照（簡版）")
        idx_rows = [
            {"symbol": TWII_SYMBOL, "name": "TAIEX", "last": ov.get("twii_close"), "asof": ov.get("trade_date")},
            {"symbol": VIX_SYMBOL, "name": "VIX", "last": ov.get("vix"), "asof": ov.get("trade_date")},
        ]
        st.dataframe(pd.DataFrame(idx_rows), use_container_width=True)

        # FinMind panel
        st.subheader("法人面板（FinMind / Debug）")
        inst_df = pd.DataFrame(payload.get("institutional_panel", []))
        st.dataframe(inst_df, use_container_width=True)

        # Stocks
        st.subheader("今日分析清單（TopN + 持倉）— Hybrid Layer")
        s_df = pd.json_normalize(payload.get("stocks", []))
        st.dataframe(s_df, use_container_width=True)

        # Warnings
        st.subheader("Warnings（最新 50 條）")
        w_df = pd.DataFrame(warns)
        if not w_df.empty:
            key = w_df["code"].isin(
                ["TWSE_AMOUNT_SSL_ERROR", "TPEX_AMOUNT_SSL_ERROR", "TPEX_AMOUNT_PARSE_FAIL", "YF_BATCH_FAIL", "YF_SYMBOL_FAIL", "PRICE_NULL", "VOLRATIO_NULL"]
            )
            w_df = pd.concat([w_df[key], w_df[~key]], ignore_index=True)
            st.dataframe(w_df, use_container_width=True)
        else:
            st.caption("（目前沒有 warnings）")

        # AI JSON (已新增複製功能)
        st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
        
        # 1. 先將 JSON 轉為易讀的字串 (保留中文不亂碼)
        json_str = json.dumps(payload, indent=4, ensure_ascii=False)

        # 2. 使用 st.code 顯示 (自帶複製按鈕)
        st.markdown("##### 📋 點擊下方代碼塊右上角的「複製圖示」即可")
        st.code(json_str, language="json")
        
        # (選擇性) 樹狀結構檢視
        with st.expander("🔍 查看樹狀結構 (Tree View)"):
            st.json(payload)


if __name__ == "__main__":
    main()
