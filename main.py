# main.py
# =========================================================
# Sunhero | 股市智能超盤中控台
# TopN + 持倉監控 / Predator V16.3 Stable Hybrid
# （已併入 V16.2 Enhanced Kill-Switch）
# Single-file Streamlit app (drop-in runnable)
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
# Constants
# =========================
EPS = 1e-4

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

DEFAULT_TOPN = 8
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

# Kill-Switch 規則（可調，但請保守）
KILL_CORE_MISSING_THRESHOLD = 0.50  # 核心缺失率 > 50% 直接 KILL
KILL_REQUIRE_PRICE_RATIO = 1.00     # Price null 比率達 100%（全部 missing）直接 KILL
KILL_REQUIRE_VOLRATIO_RATIO = 1.00  # Vol_Ratio null 比率達 100% 直接 KILL

# 法人「中性」閾值（你指定：5,000 萬）
NEUTRAL_THRESHOLD = 50_000_000

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}
B_FOREIGN_NAME = "Foreign_Investor"


# =========================
# Basic helpers
# =========================
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


def _pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(float(x) * 100.0, 4)


def _json_copyable(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


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
        r = requests.get(url, timeout=12, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()

        # heuristic: 先找 fields9/fields1 的「成交金額」，再取 data9/data1 最末列
        fields = js.get("fields9") or js.get("fields1") or js.get("fields") or []
        fields = [str(x) for x in fields] if isinstance(fields, list) else []
        amt_idx = None
        for i, f in enumerate(fields):
            if "成交金額" in f:
                amt_idx = i
                break

        candidate = None
        if isinstance(js.get("data9"), list) and js.get("data9"):
            candidate = js.get("data9")
        elif isinstance(js.get("data1"), list) and js.get("data1"):
            candidate = js.get("data1")

        amount = None
        if candidate is not None and amt_idx is not None:
            last = candidate[-1]
            if isinstance(last, list) and amt_idx < len(last):
                amount = _safe_int(last[amt_idx], default=None)

        if amount is None:
            # fallback：掃描末端 5 列的最大數字（避免全 NULL）
            best = None
            for key in ["data9", "data1", "data2", "data3", "data4", "data5", "data6", "data7", "data8"]:
                tbl = js.get(key)
                if not (isinstance(tbl, list) and tbl):
                    continue
                for row in tbl[-5:]:
                    if not isinstance(row, list):
                        continue
                    for cell in row:
                        v = _safe_int(cell, default=None)
                        if v is None:
                            continue
                        if best is None or v > best:
                            best = v
            amount = best
            if amount is None:
                warnings_bus.push("TWSE_AMOUNT_PARSE_FAIL", "TWSE amount parse none", {"url": url})
                return None, "TWSE_FAIL:PARSE_NONE"
            warnings_bus.push("TWSE_AMOUNT_PARSE_WARN", "TWSE amount fallback max-scan", {"amount": amount, "url": url})
            return int(amount), "TWSE_WARN:FALLBACK_MAXSCAN"

        return int(amount), "TWSE_OK:MI_INDEX"

    except requests.exceptions.SSLError as e:
        warnings_bus.push("TWSE_AMOUNT_SSL_ERROR", f"TWSE SSL error: {e}", {"url": url})
        return None, "TWSE_FAIL:SSLError"
    except Exception as e:
        warnings_bus.push("TWSE_AMOUNT_FETCH_FAIL", f"TWSE fetch fail: {e}", {"url": url})
        return None, "TWSE_FAIL:FETCH_ERROR"


def _fetch_tpex_amount(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw"
    try:
        r = requests.get(url, timeout=12, verify=(not allow_insecure_ssl))
        r.raise_for_status()

        try:
            js = r.json()
        except Exception as e:
            warnings_bus.push("TPEX_AMOUNT_PARSE_FAIL", f"TPEX JSON decode error: {e}", {"url": url, "text_head": r.text[:200]})
            return None, "TPEX_FAIL:JSONDecodeError"

        # 嘗試常見 key
        for key in ["totalAmount", "成交金額", "trade_value", "amt", "amount"]:
            if key in js:
                v = _safe_int(js.get(key), default=None)
                if v is not None:
                    return int(v), "TPEX_OK:st43_result"

        # fallback：掃描 aaData/data 的最大數字
        aa = js.get("aaData") or js.get("data")
        best = None
        if isinstance(aa, list):
            for row in aa[-10:]:
                if isinstance(row, list):
                    for cell in row:
                        v = _safe_int(cell, default=None)
                        if v is None:
                            continue
                        if best is None or v > best:
                            best = v
        if best is None:
            warnings_bus.push("TPEX_AMOUNT_PARSE_FAIL", "TPEX amount parse none", {"url": url, "keys": list(js.keys())[:30]})
            return None, "TPEX_FAIL:PARSE_NONE"

        warnings_bus.push("TPEX_AMOUNT_PARSE_WARN", "TPEX amount fallback max-scan", {"amount": best, "url": url})
        return int(best), "TPEX_WARN:FALLBACK_MAXSCAN"

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
# yfinance fetchers（修掉 MultiIndex / 解析炸裂）
# =========================
def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        # 常見：('Close','^TWII') 之類 → 取第一層
        df.columns = [c[0] if isinstance(c, tuple) else str(c) for c in df.columns.to_list()]
    # 有時候欄名大小寫或空白不一致
    df = df.rename(columns={c: str(c).strip() for c in df.columns})
    return df


@st.cache_data(ttl=600, show_spinner=False)
def fetch_history(symbol: str, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
    try:
        df = yf.download(
            tickers=symbol,
            period=period,
            interval=interval,
            auto_adjust=False,
            group_by="column",
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df = _flatten_yf_columns(df.copy())
        df = df.reset_index()

        # 統一時間欄位
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "Datetime"})
        if "Datetime" not in df.columns:
            # fallback
            df.insert(0, "Datetime", pd.to_datetime(df.index))

        # 統一 Adj Close 欄位名
        if "Adj Close" in df.columns:
            df = df.rename(columns={"Adj Close": "Adj_Close"})

        # 強制存在 Close/Volume（沒有就留空）
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col not in df.columns:
                df[col] = np.nan

        return df
    except Exception as e:
        warnings_bus.push("YF_FETCH_FAIL", f"{symbol} yfinance fetch fail: {e}", {"symbol": symbol})
        return pd.DataFrame()


def _as_close_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("market_df is empty")
    if "Close" not in df.columns:
        raise ValueError("Close column not found")
    s = df["Close"]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    s = pd.to_numeric(s, errors="coerce").astype(float)
    return s


def _extract_close_price(market_df: pd.DataFrame) -> Optional[float]:
    try:
        if market_df is None or market_df.empty:
            return None
        df = market_df.copy()
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            df = df.sort_values("Datetime")
        close = pd.to_numeric(df["Close"], errors="coerce").dropna()
        if close.empty:
            return None
        return float(close.iloc[-1])
    except Exception:
        return None


def _calc_ma14_monthly_from_daily(market_df: pd.DataFrame) -> Optional[float]:
    try:
        if market_df is None or market_df.empty:
            return None
        df = market_df.copy()
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.set_index("Datetime").sort_index()
        close = _as_close_series(df).dropna()
        monthly = close.resample("M").last().dropna()
        if len(monthly) < 14:
            return None
        ma14 = monthly.rolling(14).mean().dropna()
        if ma14.empty:
            return None
        return float(ma14.iloc[-1])
    except Exception:
        return None


def _count_close_below_ma_days(market_df: pd.DataFrame, ma14_monthly: Optional[float]) -> int:
    try:
        if ma14_monthly is None or market_df is None or market_df.empty:
            return 0
        df = market_df.copy()
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.sort_values("Datetime")
        close = pd.to_numeric(df["Close"], errors="coerce").dropna()
        if close.empty:
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


def fetch_last_price_and_volratio(symbol: str) -> Tuple[Optional[float], Optional[float], str]:
    """
    回傳 (Price, Vol_Ratio, source_tag)
    Vol_Ratio = 今日量 / 20日均量
    """
    try:
        h = fetch_history(symbol, period="6mo", interval="1d")
        if h is None or h.empty:
            return None, None, "NONE"

        h = h.copy()
        h["Datetime"] = pd.to_datetime(h["Datetime"])
        h = h.sort_values("Datetime")

        close = pd.to_numeric(h["Close"], errors="coerce").dropna()
        vol = pd.to_numeric(h["Volume"], errors="coerce").dropna()

        px = float(close.iloc[-1]) if not close.empty else None

        vr = None
        if len(vol) >= 20:
            ma20 = vol.rolling(20).mean().iloc[-1]
            if pd.notna(ma20) and float(ma20) > 0:
                vr = float(vol.iloc[-1] / ma20)

        return px, vr, "YF"
    except Exception as e:
        warnings_bus.push("YF_SYMBOL_FAIL", f"{symbol} yfinance parse fail: {e}", {"symbol": symbol})
        return None, None, "NONE"


# =========================
# Predator V16.3：Regime metrics / regime pick / layer classify
# =========================
def compute_regime_metrics(market_df: pd.DataFrame = None) -> dict:
    """
    回傳 dict，所有欄位皆為 scalar（不含 Series）
    """
    try:
        if market_df is None or len(market_df) < 60:
            return {"SMR": None, "SMR_MA5": None, "Slope5": None, "NEGATIVE_SLOPE_5D": True, "MOMENTUM_LOCK": False, "drawdown_pct": None}

        df = market_df.copy()
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            df = df.sort_values("Datetime")

        close = pd.to_numeric(df["Close"], errors="coerce").dropna()
        if len(close) < 60:
            return {"SMR": None, "SMR_MA5": None, "Slope5": None, "NEGATIVE_SLOPE_5D": True, "MOMENTUM_LOCK": False, "drawdown_pct": None}

        ma200 = close.rolling(200).mean()
        smr_series = (close - ma200) / ma200
        smr_series = smr_series.dropna()
        if len(smr_series) < 6:
            return {"SMR": None, "SMR_MA5": None, "Slope5": None, "NEGATIVE_SLOPE_5D": True, "MOMENTUM_LOCK": False, "drawdown_pct": None}

        smr = float(smr_series.iloc[-1])

        smr_ma5 = smr_series.rolling(5).mean().dropna()
        slope5 = 0.0
        if len(smr_ma5) >= 2:
            slope5 = float(smr_ma5.iloc[-1] - smr_ma5.iloc[-2])

        # NEGATIVE_SLOPE_5D：最近 5 個 slope 是否全部 < -EPS
        negative_slope_5d = True
        d = smr_ma5.diff().dropna()
        if len(d) >= 5:
            negative_slope_5d = bool((d.iloc[-5:] < -EPS).all())
        else:
            negative_slope_5d = True

        # MOMENTUM_LOCK：最近 4 天 slope 是否全部 > EPS
        momentum_lock = False
        if len(d) >= 4:
            momentum_lock = bool((d.iloc[-4:] > EPS).all())

        # Drawdown：相對歷史最高點的最大回撤（應接近 0 表示創高）
        rolling_high = close.cummax()
        drawdown_series = (close - rolling_high) / rolling_high
        drawdown_pct = float(drawdown_series.min()) if len(drawdown_series) else None

        return {
            "SMR": smr,
            "SMR_MA5": float(smr_ma5.iloc[-1]) if len(smr_ma5) else None,
            "Slope5": slope5,
            "NEGATIVE_SLOPE_5D": negative_slope_5d,
            "MOMENTUM_LOCK": momentum_lock,
            "drawdown_pct": drawdown_pct,
        }
    except Exception as e:
        warnings_bus.push("REGIME_METRICS_FAIL", f"compute_regime_metrics fail: {e}", {})
        return {"SMR": None, "SMR_MA5": None, "Slope5": None, "NEGATIVE_SLOPE_5D": True, "MOMENTUM_LOCK": False, "drawdown_pct": None}


def pick_regime(
    metrics: dict,
    vix: float = None,
    ma14_monthly: float = None,
    close_price: float = None,
    close_below_ma_days: int = 0,
    **_ignored,  # ✅ 防炸：吃掉未使用參數，避免 TypeError（你現在遇到的那種）
) -> tuple:
    """
    回傳 (regime_name, max_equity_pct)
    V16.3: HIBERNATION 放寬至 2日 × 0.96
    """
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")

    # CRASH_RISK（數據層）
    if (vix is not None and float(vix) > 35) or (drawdown is not None and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10

    # HIBERNATION（長期均線跌破）
    if (
        ma14_monthly is not None
        and close_price is not None
        and int(close_below_ma_days) >= 2
        and float(close_price) < float(ma14_monthly) * 0.96
    ):
        return "HIBERNATION", 0.20

    # MEAN_REVERSION / OVERHEAT
    if smr is not None and slope5 is not None:
        if float(smr) > 0.25 and float(slope5) < -EPS:
            return "MEAN_REVERSION", 0.45
        if float(smr) > 0.25 and float(slope5) >= -EPS:
            return "OVERHEAT", 0.55

    # CONSOLIDATION（簡化：SMR 落在區間）
    if smr is not None and 0.08 <= float(smr) <= 0.18:
        return "CONSOLIDATION", 0.65

    return "NORMAL", 0.85


def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    inst_streak3 = int(inst.get("inst_streak3", 0))

    # Layer A+
    if foreign_buy and trust_buy and inst_streak3 >= 3:
        return "A+"

    # Layer A
    if (foreign_buy or trust_buy) and inst_streak3 >= 3:
        return "A"

    # Layer B
    vr = _safe_float(vol_ratio, None)
    if (
        bool(momentum_lock)
        and (vr is not None and float(vr) > 0.8)
        and regime in ["NORMAL", "OVERHEAT", "CONSOLIDATION"]
    ):
        return "B"

    return "NONE"


# =========================
# FinMind：法人資料（淨買賣）
# =========================
def _fm_headers(token: Optional[str]) -> dict:
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


def _fm_get(dataset: str, params: dict, token: Optional[str]) -> dict:
    p = {"dataset": dataset, **params}
    r = requests.get(FINMIND_URL, headers=_fm_headers(token), params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize_inst_direction(net: float) -> str:
    net = float(net or 0.0)
    if abs(net) < NEUTRAL_THRESHOLD:
        return "NEUTRAL"
    return "POSITIVE" if net > 0 else "NEGATIVE"


def fetch_finmind_institutional(symbols: List[str], start_date: str, end_date: str, token: Optional[str]) -> pd.DataFrame:
    """
    回傳欄位：date, symbol, net_amount（A_NAMES 合計）
    """
    rows = []
    for sym in symbols:
        stock_id = sym.replace(".TW", "").strip()
        try:
            js = _fm_get(
                dataset="TaiwanStockInstitutionalInvestorsBuySell",
                params={"data_id": stock_id, "start_date": start_date, "end_date": end_date},
                token=token,
            )
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
        except Exception as e:
            warnings_bus.push("FINMIND_INST_FAIL", f"{sym} finmind inst fail: {e}", {"symbol": sym})

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "net_amount"])
    return pd.DataFrame(rows).sort_values(["symbol", "date"])


def calc_inst_3d(inst_df: pd.DataFrame, symbol: str) -> dict:
    """
    回傳：
    - foreign_buy/trust_buy：布林（此版簡化：使用同一個 net_amount 當作兩者，維持一致可回溯）
    - inst_streak3：3 或 0（連 3 日同向才給 3）
    - Inst_Status, Inst_Dir3, Inst_Net_3d：除錯/稽核用
    """
    if inst_df is None or inst_df.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0, "Inst_Status": "PENDING", "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df = inst_df[inst_df["symbol"] == symbol].copy()
    if df.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0, "Inst_Status": "PENDING", "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df = df.sort_values("date").tail(3)
    if len(df) < 3:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0, "Inst_Status": "PENDING", "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0)
    dirs = [normalize_inst_direction(x) for x in df["net_amount"]]
    net_sum = float(df["net_amount"].sum())

    # 連3日同向才 streak=3
    if all(d == "POSITIVE" for d in dirs):
        return {"foreign_buy": True, "trust_buy": True, "inst_streak3": 3, "Inst_Status": "READY", "Inst_Dir3": "POSITIVE", "Inst_Net_3d": net_sum}
    if all(d == "NEGATIVE" for d in dirs):
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 3, "Inst_Status": "READY", "Inst_Dir3": "NEGATIVE", "Inst_Net_3d": net_sum}

    # NEUTRAL / 混合
    # foreign_buy / trust_buy 仍可用「三日淨額」作方向，但 streak 不給
    foreign_buy = bool(net_sum > NEUTRAL_THRESHOLD)
    trust_buy = bool(net_sum > NEUTRAL_THRESHOLD)
    return {"foreign_buy": foreign_buy, "trust_buy": trust_buy, "inst_streak3": 0, "Inst_Status": "READY", "Inst_Dir3": "NEUTRAL", "Inst_Net_3d": net_sum}


def build_institutional_panel(symbols: List[str], trade_date: str, finmind_token: Optional[str]) -> pd.DataFrame:
    """
    產出欄位（供 UI 顯示 & Layer 判定用）：
    Symbol, Foreign_Net, Trust_Net, Inst_Streak3, Inst_Status, Inst_Dir3, Inst_Net_3d
    """
    # 取 trade_date 往回抓 10 天（足夠覆蓋 3 個交易日）
    # 注意：FinMind 用 YYYY-MM-DD
    try:
        start = (pd.to_datetime(trade_date) - pd.Timedelta(days=14)).strftime("%Y-%m-%d")
        end = trade_date
    except Exception:
        start = trade_date
        end = trade_date

    inst_df = fetch_finmind_institutional(symbols, start, end, token=finmind_token)

    rows = []
    for sym in symbols:
        m = calc_inst_3d(inst_df, sym)
        # 此版的 Foreign_Net / Trust_Net 先用 Inst_Net_3d（保持可回溯一致），你之後要拆外資/投信可再擴充
        rows.append(
            {
                "Symbol": sym,
                "Foreign_Net": float(m.get("Inst_Net_3d", 0.0)),
                "Trust_Net": float(m.get("Inst_Net_3d", 0.0)),
                "Inst_Streak3": int(m.get("inst_streak3", 0)),
                "Inst_Status": m.get("Inst_Status", "PENDING"),
                "Inst_Dir3": m.get("Inst_Dir3", "PENDING"),
                "Inst_Net_3d": float(m.get("Inst_Net_3d", 0.0)),
            }
        )
    return pd.DataFrame(rows)


def inst_metrics_for_symbol(panel: pd.DataFrame, symbol: str) -> dict:
    if panel is None or panel.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    df = panel[panel["Symbol"] == symbol]
    if df.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    row = df.iloc[-1]
    foreign_buy = bool(_safe_float(row.get("Foreign_Net", 0), 0) > NEUTRAL_THRESHOLD)
    trust_buy = bool(_safe_float(row.get("Trust_Net", 0), 0) > NEUTRAL_THRESHOLD)
    inst_streak3 = int(_safe_int(row.get("Inst_Streak3", 0), 0) or 0)
    return {"foreign_buy": foreign_buy, "trust_buy": trust_buy, "inst_streak3": inst_streak3}


# =========================
# Kill-Switch：資料完整性評分/決策
# =========================
def compute_integrity(stocks: List[dict], amount: MarketAmount) -> dict:
    n = int(len(stocks))
    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)

    # 核心欄位定義（可再擴充）
    # 目前核心：Price、Vol_Ratio、amount_total
    core_total = n * 2 + 1  # 每檔 2 個 + 市場 1 個
    core_missing = price_null + volratio_null + (1 if amount.amount_total is None else 0)
    core_missing_pct = float(core_missing / core_total) if core_total > 0 else 1.0

    price_null_ratio = float(price_null / n) if n > 0 else 1.0
    volratio_null_ratio = float(volratio_null / n) if n > 0 else 1.0

    kill = False
    reasons = []

    if core_missing_pct > KILL_CORE_MISSING_THRESHOLD:
        kill = True
        reasons.append(f"core_missing_pct={core_missing_pct:.2f} > {KILL_CORE_MISSING_THRESHOLD:.2f}")

    if price_null_ratio >= KILL_REQUIRE_PRICE_RATIO and n > 0:
        kill = True
        reasons.append(f"price_null={price_null}/{n}")

    if volratio_null_ratio >= KILL_REQUIRE_VOLRATIO_RATIO and n > 0:
        kill = True
        reasons.append(f"volratio_null={volratio_null}/{n}")

    if amount.amount_total is None:
        reasons.append("amount_total_null=True")

    reason = "DATA_MISSING " + ", ".join(reasons) if reasons else "OK"

    return {
        "n": n,
        "price_null": int(price_null),
        "volratio_null": int(volratio_null),
        "core_missing_pct": float(core_missing_pct),
        "kill": bool(kill),
        "reason": reason,
    }


def build_active_alerts(integrity: dict, amount: MarketAmount) -> List[str]:
    alerts = []
    if integrity.get("kill"):
        alerts.append("KILL_SWITCH_ACTIVATED")

    if amount.amount_total is None:
        alerts.append("DEGRADED_AMOUNT: 成交量數據完全缺失 (TWSE_FAIL + TPEX_FAIL)")

    n = int(integrity.get("n", 0) or 0)
    if n > 0 and int(integrity.get("price_null", 0)) == n:
        alerts.append("CRITICAL: 所有個股價格 = null (無法執行任何決策)")
    if n > 0 and int(integrity.get("volratio_null", 0)) == n:
        alerts.append("CRITICAL: 所有個股 Vol_Ratio = null (Layer B 判定不可能)")

    cm = float(integrity.get("core_missing_pct", 0.0) or 0.0)
    if cm >= 0.50:
        alerts.append(f"DATA_INTEGRITY_FAILURE: 核心數據缺失率={cm:.2f}")

    if integrity.get("kill"):
        alerts.append("FORCED_ALL_CASH: 資料品質不足，強制進入避險模式")

    return alerts


# =========================
# Build arbiter input (with Kill-Switch)
# =========================
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
    # ---- Market data ----
    twii_df = fetch_history(TWII_SYMBOL, period="3y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")

    vix_last = None
    try:
        if not vix_df.empty:
            vix_last = float(pd.to_numeric(vix_df["Close"], errors="coerce").dropna().iloc[-1])
    except Exception:
        vix_last = None

    # ---- Metrics ----
    metrics = compute_regime_metrics(twii_df)
    ma14_monthly = _calc_ma14_monthly_from_daily(twii_df)
    close_price = _extract_close_price(twii_df)
    close_below_days = _count_close_below_ma_days(twii_df, ma14_monthly)

    regime, max_equity = pick_regime(
        metrics=metrics,
        vix=vix_last,
        ma14_monthly=ma14_monthly,
        close_price=close_price,
        close_below_ma_days=close_below_days,
    )

    # ---- Market amount ----
    amount = fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl)

    # ---- Symbols（固定池 + positions 代碼補入）----
    default_pool = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2881.TW", "2882.TW", "2603.TW", "2609.TW"]
    pos_syms = []
    for p in positions:
        if isinstance(p, dict) and p.get("symbol"):
            pos_syms.append(str(p["symbol"]).strip())
    symset = list(dict.fromkeys(pos_syms + default_pool))
    symbols = symset[: max(1, int(topn))]

    # ---- Trade date ----
    trade_date = None
    try:
        if not twii_df.empty:
            trade_date = pd.to_datetime(twii_df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")
    except Exception:
        trade_date = None

    # ---- Institutional panel（FinMind）----
    inst_panel = build_institutional_panel(symbols, trade_date or time.strftime("%Y-%m-%d"), finmind_token=finmind_token)

    # ---- Per-stock snapshot ----
    stocks = []
    for i, sym in enumerate(symbols, start=1):
        px, vr, src = fetch_last_price_and_volratio(sym)

        if px is None:
            warnings_bus.push("PRICE_NULL", f"{sym} Price is null after fallback (YF)", {"source": src, "symbol": sym})
        if vr is None:
            warnings_bus.push("VOLRATIO_NULL", f"{sym} Vol_Ratio is null after fallback (YF)", {"source": src, "symbol": sym})

        im = inst_metrics_for_symbol(inst_panel, sym)
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vr, im)

        stocks.append(
            {
                "Symbol": sym,
                "Name": sym,  # 你要中文名可之後接對照表
                "Tier": i,
                "Price": px,
                "Vol_Ratio": vr,
                "Layer": layer,
                "Institutional": im,
            }
        )

    # ---- Integrity / Kill-Switch ----
    integrity = compute_integrity(stocks, amount)
    active_alerts = build_active_alerts(integrity, amount)

    # ---- Portfolio summary ----
    current_exposure_pct = 0.0
    if positions:
        # 最小可跑：每個持倉估 5%（你之後可改成真實市值）
        current_exposure_pct = min(1.0, len(positions) * 0.05)

    cash_pct = 100.0 - float(current_exposure_pct * 100.0)
    cash_pct = max(0.0, min(100.0, cash_pct))

    # ---- Kill 覆寫（SHELTER / UNKNOWN / max_equity=0）----
    audit_log: List[dict] = []
    market_status = "NORMAL"
    final_regime = regime
    final_max_equity = max_equity

    if integrity.get("kill"):
        market_status = "SHELTER"
        final_regime = "UNKNOWN"
        final_max_equity = 0.0

        # 全部 Layer 強制 NONE（避免任何進場）
        for s in stocks:
            s["Layer"] = "NONE"

        audit_log.append(
            {
                "symbol": "ALL",
                "event": "KILL_SWITCH_TRIGGERED",
                "attribution": "DATA_MISSING",
                "comment": integrity.get("reason", "DATA_MISSING"),
            }
        )
        audit_log.append(
            {
                "symbol": "ALL",
                "event": "DEGRADED_STATUS_CRITICAL",
                "attribution": "MARKET_AMOUNT_FAILURE",
                "comment": f"amount_total={amount.amount_total}, source_twse={amount.source_twse}, source_tpex={amount.source_tpex}",
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
        # 曝險強制 0（UI 顯示一致）
        current_exposure_pct = 0.0
        cash_pct = 100.0

    else:
        # amount_total 缺失但未達 kill → 降級
        if amount.amount_total is None:
            market_status = "DEGRADED"

    # ---- Payload ----
    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": market_status,
            "current_regime": final_regime,
            "account_mode": account_mode,
            "audit_tag": "V16.3_STABLE_HYBRID_WITH_KILL_SWITCH",
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "twii_close": close_price,
                "vix": vix_last,
                "smr": metrics.get("SMR"),
                "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"),
                "ma14_monthly": ma14_monthly,
                "close_below_ma_days": close_below_days,
                "max_equity_allowed_pct": float(final_max_equity),
            },
            "market_amount": asdict(amount),
            "integrity": integrity,
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct * 100.0),  # 用百分比顯示（0~100）
            "cash_pct": float(cash_pct),
            "active_alerts": active_alerts,
        },
        "institutional_panel": inst_panel.to_dict(orient="records") if isinstance(inst_panel, pd.DataFrame) else [],
        "stocks": stocks,
        "positions_input": positions,
        "decisions": [],  # Arbiter 決策層（此 App 只輸出 input + 監控）
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
    topn = st.sidebar.selectbox("TopN（固定池化數量）", [8, 10, 15, 20, 30, 50], index=0)

    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL (verify=False)", value=False)

    st.sidebar.subheader("FinMind")
    finmind_token = st.sidebar.text_input("FinMind Token（選填）", value="", type="password")

    st.sidebar.subheader("持倉（手動貼 JSON 陣列）")
    positions_text = st.sidebar.text_area("positions", value="[]", height=140)

    cash_balance = st.sidebar.number_input("現金餘額（新台幣）", min_value=0, value=DEFAULT_CASH, step=10_000)
    total_equity = st.sidebar.number_input("總權益（新台幣）", min_value=0, value=DEFAULT_EQUITY, step=10_000)

    run_btn = st.sidebar.button("跑步")

    # ---- Parse positions ----
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
                finmind_token=(finmind_token.strip() or None),
            )
        except Exception as e:
            st.error("App 執行期間發生例外（已捕捉，不會白屏）。")
            st.exception(e)
            return

        ov = payload.get("macro", {}).get("overview", {})
        integrity = payload.get("macro", {}).get("integrity", {})
        active_alerts = payload.get("portfolio", {}).get("active_alerts", [])

        # ---- KPI ----
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("交易日", ov.get("trade_date", "-"))
        c2.metric("market_status", payload.get("meta", {}).get("market_status", "-"))
        c3.metric("regime", payload.get("meta", {}).get("current_regime", "-"))
        c4.metric("SMR", f"{_safe_float(ov.get('smr'), 0):.6f}" if ov.get("smr") is not None else "NA")
        c5.metric("Slope5", f"{_safe_float(ov.get('slope5'), 0):.6f}" if ov.get("slope5") is not None else "NA")
        c6.metric("Max Equity", f"{_pct(ov.get('max_equity_allowed_pct')):.1f}%" if ov.get("max_equity_allowed_pct") is not None else "NA")

        st.caption(
            f"Integrity | Price null={integrity.get('price_null')}/{integrity.get('n')} | "
            f"Vol_Ratio null={integrity.get('volratio_null')}/{integrity.get('n')} | "
            f"core_missing_pct={float(integrity.get('core_missing_pct', 0.0)):.2f}"
        )

        # ---- Active Alerts ----
        st.subheader("Active Alerts")
        if active_alerts:
            for a in active_alerts:
                st.error(a) if ("CRITICAL" in a or "KILL" in a or "FAILURE" in a) else st.warning(a)
        else:
            st.success("（目前沒有警示）")

        # ---- Market Amount ----
        st.subheader("市場成交金額（best-effort / 可稽核）")
        st.code(_json_copyable(payload.get("macro", {}).get("market_amount", {})), language="json")

        # ---- Indices snapshot ----
        st.subheader("指數快照（簡版）")
        idx_rows = [
            {"symbol": TWII_SYMBOL, "name": "TAIEX", "last": ov.get("twii_close"), "asof": ov.get("trade_date")},
            {"symbol": VIX_SYMBOL, "name": "VIX", "last": ov.get("vix"), "asof": ov.get("trade_date")},
        ]
        st.dataframe(pd.DataFrame(idx_rows), use_container_width=True)

        # ---- Institutional panel ----
        st.subheader("法人面板（FinMind / Debug）")
        ip = payload.get("institutional_panel", [])
        ip_df = pd.DataFrame(ip)
        if not ip_df.empty:
            st.dataframe(ip_df, use_container_width=True)
        else:
            st.info("法人資料空（FinMind token 未填或 API 無資料）。")

        # ---- Stocks table ----
        st.subheader("今日分析清單（TopN + 持倉）— Hybrid Layer")
        s_df = pd.json_normalize(payload.get("stocks", []))
        if not s_df.empty:
            if "Tier" in s_df.columns:
                s_df = s_df.sort_values("Tier", ascending=True)
            st.dataframe(s_df, use_container_width=True)
        else:
            st.info("stocks 清單為空（資料源可能暫時不可用）。")

        # ---- Warnings ----
        st.subheader("Warnings（最新 50 條）")
        w_df = pd.DataFrame(warns)
        if not w_df.empty:
            key_fail = w_df["code"].isin(
                [
                    "TWSE_AMOUNT_PARSE_FAIL",
                    "TPEX_AMOUNT_PARSE_FAIL",
                    "TWSE_AMOUNT_SSL_ERROR",
                    "TPEX_AMOUNT_SSL_ERROR",
                    "YF_FETCH_FAIL",
                    "YF_SYMBOL_FAIL",
                    "PRICE_NULL",
                    "VOLRATIO_NULL",
                    "FINMIND_INST_FAIL",
                ]
            )
            w_df = pd.concat([w_df[key_fail], w_df[~key_fail]], ignore_index=True)
            st.dataframe(w_df, use_container_width=True)
        else:
            st.caption("（目前沒有 warnings）")

        # ---- AI JSON（可複製：用 st.code 取代 st.json，會出現複製鍵）----
        st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
        st.code(_json_copyable(payload), language="json")


if __name__ == "__main__":
    main()
