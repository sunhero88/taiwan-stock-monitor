# main.py
# =========================================================
# Sunhero | 股市智能超盤中控台 (TopN + 持倉監控 / Predator V16.3 Stable Hybrid)
# Single-file Streamlit app (drop-in runnable)
# =========================================================

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
EPS = 1e-4  # 0.0001
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

DEFAULT_TOPN = 20
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

# 你關心的 Warnings 類型（置頂）
WARN_PRIORITY = [
    "TWSE_AMOUNT_PARSE_FAIL",
    "TPEX_AMOUNT_PARSE_FAIL",
    "TWSE_AMOUNT_SSL_ERROR",
    "TPEX_AMOUNT_SSL_ERROR",
    "TWSE_AMOUNT_FETCH_FAIL",
    "TPEX_AMOUNT_FETCH_FAIL",
]


def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _safe_float(x, default=None) -> Optional[float]:
    try:
        if x is None:
            return default
        if isinstance(x, (np.floating, float, int, np.integer)):
            return float(x)
        if isinstance(x, str):
            s = x.strip().replace(",", "")
            if s == "":
                return default
            return float(s)
        # ndarray / Series -> 取最後一個 scalar
        if isinstance(x, (np.ndarray, pd.Series, list, tuple)):
            if len(x) == 0:
                return default
            return _safe_float(x[-1], default=default)
        return float(x)
    except Exception:
        return default


def _safe_int(x, default=0) -> Optional[int]:
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
        if isinstance(x, (np.ndarray, pd.Series, list, tuple)):
            if len(x) == 0:
                return default
            return _safe_int(x[-1], default=default)
        return int(x)
    except Exception:
        return default


def _pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(float(x) * 100.0, 4)


def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance 有時回 MultiIndex columns，導致 df['Close'] 取不到
    這裡一律扁平化成單層欄名。
    """
    if df is None or df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(c) for c in col if str(c) != ""]) for col in df.columns.values]

        # 常見會變成 Close_^TWII / Volume_^TWII
        # 若存在 "Close_*" 且沒有 "Close"，就把第一個 Close_* rename 成 Close（同理 Volume）
        close_cols = [c for c in df.columns if c.startswith("Close")]
        vol_cols = [c for c in df.columns if c.startswith("Volume")]

        if "Close" not in df.columns and close_cols:
            df = df.rename(columns={close_cols[0]: "Close"})
        if "Volume" not in df.columns and vol_cols:
            df = df.rename(columns={vol_cols[0]: "Volume"})

    return df


# =========================
# Warnings recorder
# =========================
class WarningBus:
    def __init__(self):
        self.items: List[Dict[str, Any]] = []

    def push(self, code: str, msg: str, meta: Optional[dict] = None):
        self.items.append(
            {
                "ts": _now_ts(),
                "code": code,
                "msg": msg,
                "meta": meta or {},
            }
        )

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
    """
    TWSE 成交金額（上市）best-effort
    資料來源：TWSE /exchangeReport/MI_INDEX
    """
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&type=ALLBUT0999"
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()

        candidate_tables = []
        for k in ["data9", "data1", "data2", "data3", "data4", "data5", "data6", "data7", "data8"]:
            v = js.get(k)
            if isinstance(v, list) and len(v) > 0:
                candidate_tables.append((k, v))

        if not candidate_tables:
            warnings_bus.push(
                "TWSE_AMOUNT_PARSE_FAIL",
                "TWSE JSON missing expected tables (data1..data9)",
                {"url": url, "keys": list(js.keys())[:30]},
            )
            return None, "TWSE_FAIL:TABLE_MISSING"

        # fields9/fields1 可能對應 data9/data1
        fields = js.get("fields9") or js.get("fields1") or js.get("fields") or []
        fields = [str(x) for x in fields] if isinstance(fields, list) else []

        amt_idx = None
        for i, f in enumerate(fields):
            if "成交金額" in f:
                amt_idx = i
                break

        amount = None
        src = "TWSE_OK:MI_INDEX"

        if amt_idx is not None:
            data = js.get("data9") if isinstance(js.get("data9"), list) else candidate_tables[0][1]
            last = data[-1]
            if isinstance(last, list) and amt_idx < len(last):
                amount = _safe_int(last[amt_idx], default=None)

        # fallback：掃描候選表最後幾列，取可解析數字的最大值（best-effort）
        if amount is None:
            best = None
            for _, tbl in candidate_tables:
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
                warnings_bus.push(
                    "TWSE_AMOUNT_PARSE_FAIL",
                    "TWSE amount cannot be parsed from any candidate tables",
                    {"url": url},
                )
                return None, "TWSE_FAIL:PARSE_NONE"

            warnings_bus.push(
                "TWSE_AMOUNT_PARSE_WARN",
                "TWSE amount parsed by fallback heuristic (max-scan)",
                {"url": url, "amount": amount},
            )
            src = "TWSE_WARN:FALLBACK_MAXSCAN"

        return int(amount), src

    except requests.exceptions.SSLError as e:
        warnings_bus.push("TWSE_AMOUNT_SSL_ERROR", f"TWSE SSL error: {e}", {"url": url})
        return None, "TWSE_FAIL:SSLError"
    except Exception as e:
        warnings_bus.push("TWSE_AMOUNT_FETCH_FAIL", f"TWSE fetch fail: {e}", {"url": url})
        return None, "TWSE_FAIL:FETCH_ERROR"


def _fetch_tpex_amount(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
    """
    TPEX 成交金額（上櫃）best-effort
    資料來源：TPEX daily_trading_info/st43_result
    """
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

        amount = None
        src = "TPEX_OK:st43_result"

        # 嘗試常見 key
        for key in ["totalAmount", "成交金額", "trade_value", "amt", "amount", "total_amount"]:
            if key in js:
                amount = _safe_int(js.get(key), default=None)
                if amount is not None:
                    break

        # aaData / data fallback：掃描數字最大值
        if amount is None:
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
            amount = best

            if amount is None:
                warnings_bus.push(
                    "TPEX_AMOUNT_PARSE_FAIL",
                    "TPEX amount cannot be parsed (no numeric candidates)",
                    {"url": url, "keys": list(js.keys())[:30]},
                )
                return None, "TPEX_FAIL:PARSE_NONE"

            warnings_bus.push(
                "TPEX_AMOUNT_PARSE_WARN",
                "TPEX amount parsed by fallback heuristic (max-scan)",
                {"url": url, "amount": amount},
            )
            src = "TPEX_WARN:FALLBACK_MAXSCAN"

        return int(amount), src

    except requests.exceptions.SSLError as e:
        warnings_bus.push("TPEX_AMOUNT_SSL_ERROR", f"TPEX SSL error: {e}", {"url": url})
        return None, "TPEX_FAIL:SSLError"
    except Exception as e:
        warnings_bus.push("TPEX_AMOUNT_FETCH_FAIL", f"TPEX fetch fail: {e}", {"url": url})
        return None, "TPEX_FAIL:FETCH_ERROR"


def fetch_amount_total(allow_insecure_ssl: bool = False) -> MarketAmount:
    """
    回傳：上市、上櫃、合計成交金額（元）
    allow_insecure_ssl=True 時允許 verify=False 以繞過舊憑證/鏈問題。
    """
    twse_amt, twse_src = _fetch_twse_amount(allow_insecure_ssl)
    tpex_amt, tpex_src = _fetch_tpex_amount(allow_insecure_ssl)

    amount_twse = twse_amt if twse_amt is not None else None
    amount_tpex = tpex_amt if tpex_amt is not None else None

    total = None
    if amount_twse is not None and amount_tpex is not None:
        total = int(amount_twse) + int(amount_tpex)

    return MarketAmount(
        amount_twse=amount_twse,
        amount_tpex=amount_tpex,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=bool(allow_insecure_ssl),
    )


# =========================================================
# Predator V16.3 Stable (Hybrid Edition)
# Replace: compute_regime_metrics(), pick_regime(),
#          inst_metrics_for_symbol(), classify_layer()
# =========================================================

def _as_close_series(df: pd.DataFrame) -> pd.Series:
    """
    強制回傳單一 Close Series（float），避免 DataFrame/Series 混用造成 ambiguous truth
    """
    if df is None or df.empty:
        raise ValueError("market_df is empty")

    # 允許 index 是 Datetime
    if "Close" in df.columns:
        s = df["Close"]
    elif "close" in df.columns:
        s = df["close"]
    else:
        # 有些 yfinance 會變 Close_^TWII
        close_like = [c for c in df.columns if str(c).startswith("Close")]
        if close_like:
            s = df[close_like[0]]
        else:
            raise ValueError("Close price column not found")

    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]

    s = pd.to_numeric(s, errors="coerce").astype(float)
    return s


def _check_consolidation(smr_series: pd.Series, close_series: pd.Series) -> bool:
    """
    V16.3 CONSOLIDATION：
      - 最近 10 日 SMR 全部落在 [0.08, 0.18]
      - 最近 15 日價格波動 (max-min)/mean < 0.05
    """
    try:
        s = smr_series.dropna()
        c = close_series.dropna()
        if len(s) < 10 or len(c) < 15:
            return False

        recent_smr = s.iloc[-10:]
        if not bool(((recent_smr >= 0.08) & (recent_smr <= 0.18)).all()):
            return False

        recent_15 = c.iloc[-15:]
        m = float(recent_15.mean())
        if m <= 0:
            return False

        pr = (float(recent_15.max()) - float(recent_15.min())) / m
        return bool(pr < 0.05)
    except Exception:
        return False


def compute_regime_metrics(market_df: pd.DataFrame = None) -> dict:
    """
    回傳 dict，所有欄位皆為 scalar（不含 Series）
    """
    if market_df is None or len(market_df) < 10:
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
            "consolidation_detected": False,
        }

    close = _as_close_series(market_df)

    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    smr_series = smr_series.dropna()

    if len(smr_series) < 6:
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
            "consolidation_detected": False,
        }

    smr = float(smr_series.iloc[-1])

    smr_ma5_series = smr_series.rolling(5).mean().dropna()
    smr_ma5 = float(smr_ma5_series.iloc[-1]) if len(smr_ma5_series) > 0 else None

    if len(smr_ma5_series) < 2:
        slope5 = 0.0
    else:
        slope5 = float(smr_ma5_series.iloc[-1] - smr_ma5_series.iloc[-2])

    # NEGATIVE_SLOPE_5D：連續 5 日 slope < -EPS
    negative_slope_5d = True
    try:
        diffs = smr_ma5_series.diff().dropna()
        if len(diffs) >= 5:
            recent5 = diffs.iloc[-5:]
            negative_slope_5d = bool((recent5 < -EPS).all())
    except Exception:
        negative_slope_5d = True

    # MOMENTUM_LOCK：連續 4 日 slope > EPS
    momentum_lock = False
    try:
        diffs = smr_ma5_series.diff().dropna()
        if len(diffs) >= 4:
            last4 = diffs.iloc[-4:]
            momentum_lock = bool((last4 > EPS).all())
    except Exception:
        momentum_lock = False

    # Drawdown：從歷史高點回撤（最小值，負數）
    rolling_high = close.cummax()
    dd = (close - rolling_high) / rolling_high
    drawdown_pct = float(dd.min()) if len(dd.dropna()) > 0 else None

    consolidation_detected = _check_consolidation(smr_series, close)

    return {
        "SMR": smr,
        "SMR_MA5": smr_ma5,
        "Slope5": float(slope5),
        "NEGATIVE_SLOPE_5D": bool(negative_slope_5d),
        "MOMENTUM_LOCK": bool(momentum_lock),
        "drawdown_pct": drawdown_pct,
        "consolidation_detected": bool(consolidation_detected),
    }


def pick_regime(
    metrics: dict,
    vix: float = None,
    ma14_monthly: float = None,
    close_price: float = None,
    close_below_ma_days: int = 0,
) -> tuple:
    """
    回傳 (regime_name, max_equity_pct)
    V16.3:
      - CRASH_RISK：VIX > 35 或 drawdown <= -18%
      - HIBERNATION：close < MA14_Monthly×0.96 且連續 >=2 交易日
      - MEAN_REVERSION：SMR>0.25 且 Slope5<-0.0001
      - OVERHEAT：SMR>0.25 且 Slope5>=-0.0001
      - CONSOLIDATION：metrics['consolidation_detected']==True
      - NORMAL：其餘
    """
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")
    consolidation = bool(metrics.get("consolidation_detected", False))

    # CRASH_RISK
    if (vix is not None and float(vix) > 35) or (drawdown is not None and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10

    # HIBERNATION（2 日、0.96）
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

    # CONSOLIDATION（嚴格偵測）
    if consolidation:
        return "CONSOLIDATION", 0.65

    return "NORMAL", 0.85


def inst_metrics_for_symbol(panel: pd.DataFrame, symbol: str) -> dict:
    """
    輸出全為 scalar
    需要 panel 欄位：
      Symbol, Foreign_Net, Trust_Net, Inst_Streak3
    """
    if panel is None or panel.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    if "Symbol" not in panel.columns:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    df = panel[panel["Symbol"] == symbol]
    if df.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    row = df.iloc[-1]

    foreign_buy = bool(_safe_float(row.get("Foreign_Net", 0), 0) > 0)
    trust_buy = bool(_safe_float(row.get("Trust_Net", 0), 0) > 0)
    inst_streak3 = int(_safe_int(row.get("Inst_Streak3", 0), 0) or 0)

    return {"foreign_buy": foreign_buy, "trust_buy": trust_buy, "inst_streak3": inst_streak3}


def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    """
    V16.3 Layer：
      A+：外資+投信皆買超 且 inst_streak3>=3
      A ：外資或投信買超 且 inst_streak3>=3
      B ：momentum_lock 且 vol_ratio>0.8 且 regime in [NORMAL, OVERHEAT, CONSOLIDATION]
      NONE：其餘
    """
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    inst_streak3 = int(inst.get("inst_streak3", 0) or 0)

    if foreign_buy and trust_buy and inst_streak3 >= 3:
        return "A+"

    if (foreign_buy or trust_buy) and inst_streak3 >= 3:
        return "A"

    vr = _safe_float(vol_ratio, None)
    if (
        bool(momentum_lock)
        and (vr is not None and float(vr) > 0.8)
        and regime in ["NORMAL", "OVERHEAT", "CONSOLIDATION"]
    ):
        return "B"

    return "NONE"


# =========================
# Data fetchers (yfinance)
# =========================
@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_history(symbol: str, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
    df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()

    df = _flatten_yf_columns(df)

    df = df.reset_index()
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "Datetime"})
    if "Datetime" not in df.columns:
        df.insert(0, "Datetime", pd.to_datetime(df.index))

    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
    df = df.dropna(subset=["Datetime"])

    return df


def _extract_close_price(market_df: pd.DataFrame) -> Optional[float]:
    try:
        if market_df is None or market_df.empty:
            return None
        df = market_df.copy()
        if "Datetime" in df.columns:
            df = df.set_index("Datetime")
        close = _as_close_series(df).dropna()
        if len(close) == 0:
            return None
        return float(close.iloc[-1])
    except Exception:
        return None


def _calc_ma14_monthly_from_daily(market_df: pd.DataFrame) -> Optional[float]:
    """
    用日線近似月線 MA14：
      - resample 到月末收盤
      - rolling 14 做 MA
    """
    try:
        if market_df is None or market_df.empty:
            return None
        df = market_df.copy()
        if "Datetime" in df.columns:
            df = df.set_index(pd.to_datetime(df["Datetime"]))
        close = _as_close_series(df).dropna()
        monthly = close.resample("M").last().dropna()
        if len(monthly) < 14:
            return None
        ma14 = monthly.rolling(14).mean().dropna()
        if len(ma14) == 0:
            return None
        return float(ma14.iloc[-1])
    except Exception:
        return None


def _count_close_below_ma_days(market_df: pd.DataFrame, ma14_monthly: Optional[float]) -> int:
    """
    計算最近收盤低於 MA14*0.96 的連續天數（用最近 5 日即可）
    """
    try:
        if ma14_monthly is None or market_df is None or market_df.empty:
            return 0

        df = market_df.copy()
        if "Datetime" in df.columns:
            df = df.set_index(pd.to_datetime(df["Datetime"]))
        close = _as_close_series(df).dropna()
        recent = close.iloc[-5:]
        thresh = float(ma14_monthly) * 0.96

        cnt = 0
        for v in reversed(recent.tolist()):
            if float(v) < thresh:
                cnt += 1
            else:
                break
        return int(cnt)
    except Exception:
        return 0


# =========================
# Institutional panel stub
# =========================
def build_institutional_panel(symbols: List[str]) -> pd.DataFrame:
    """
    目前是可跑 stub（全部 0）。
    你要接回真實法人資料時，只需要確保最後 rename 成：
      Symbol, Foreign_Net, Trust_Net, Inst_Streak3
    """
    return pd.DataFrame(
        [{"Symbol": s, "Foreign_Net": 0, "Trust_Net": 0, "Inst_Streak3": 0} for s in symbols]
    )


# =========================
# Build arbiter input
# =========================
def build_arbiter_input(
    session: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
) -> Tuple[dict, List[dict]]:

    twii_df = fetch_history(TWII_SYMBOL, period="3y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")

    vix_last = None
    try:
        if not vix_df.empty:
            vix_last = _safe_float(vix_df["Close"].dropna().iloc[-1], None)
    except Exception:
        vix_last = None

    # Metrics
    twii_for_metrics = twii_df.copy()
    if "Datetime" in twii_for_metrics.columns:
        twii_for_metrics = twii_for_metrics.set_index("Datetime")

    metrics = compute_regime_metrics(twii_for_metrics)
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

    # Market amount
    amount = fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl)

    # TopN symbols（placeholder pool + positions）
    default_pool = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2881.TW", "2882.TW", "2603.TW", "2609.TW"]
    pos_syms = []
    for p in positions:
        if isinstance(p, dict) and p.get("symbol"):
            pos_syms.append(str(p["symbol"]))
    symbols = list(dict.fromkeys(pos_syms + default_pool))[: max(1, int(topn))]

    # Institutional panel (stub)
    panel = build_institutional_panel(symbols)

    # Per-stock snapshot
    stocks = []
    for i, sym in enumerate(symbols, start=1):
        px = None
        vol_ratio = None
        try:
            h = fetch_history(sym, period="6mo", interval="1d")
            if not h.empty:
                if "Close" in h.columns:
                    c = pd.to_numeric(h["Close"], errors="coerce").dropna()
                    if len(c) > 0:
                        px = float(c.iloc[-1])

                if "Volume" in h.columns:
                    v = pd.to_numeric(h["Volume"], errors="coerce").dropna()
                    if len(v) >= 20:
                        ma20 = float(v.rolling(20).mean().iloc[-1])
                        vol_ratio = float(v.iloc[-1] / ma20) if ma20 > 0 else None
        except Exception:
            px = None
            vol_ratio = None

        im = inst_metrics_for_symbol(panel, sym)
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vol_ratio, im)

        stocks.append(
            {
                "Symbol": sym,
                "Name": sym,
                "Tier": int(i),
                "Price": px,
                "Vol_Ratio": vol_ratio,
                "Layer": layer,
                "Institutional": im,
            }
        )

    # Portfolio summary（最小可跑）
    current_exposure_pct = 0.0
    if positions:
        current_exposure_pct = min(1.0, len(positions) * 0.05)

    # V16.3：只要成交金額沒抓全，就降級（避免半盲）
    degraded = (amount.amount_twse is None) or (amount.amount_tpex is None) or (amount.amount_total is None)

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": "DEGRADED" if degraded else "NORMAL",
            "current_regime": regime,
            "audit_tag": "V16.3_STABLE_HYBRID",
        },
        "macro": {
            "overview": {
                "trade_date": (twii_df["Datetime"].dropna().iloc[-1].strftime("%Y-%m-%d") if not twii_df.empty else None),
                "twii_close": close_price,
                "vix": vix_last,
                "smr": metrics.get("SMR"),
                "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"),
                "ma14_monthly": ma14_monthly,
                "close_below_ma_days": int(close_below_days),
                "consolidation_detected": bool(metrics.get("consolidation_detected", False)),
                "max_equity_allowed_pct": float(max_equity),
            },
            "market_amount": asdict(amount),
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float(max(0.0, 1.0 - current_exposure_pct)),
        },
        "stocks": stocks,
        "positions_input": positions,
    }

    return payload, warnings_bus.latest(50)


# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定")

    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=0)
    topn = st.sidebar.selectbox("TopN（固定池化數量）", [10, 15, 20, 30, 50], index=2)

    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL (verify=False)", value=False)

    st.sidebar.subheader("持倉（手動貼 JSON array）")
    positions_text = st.sidebar.text_area("positions", value="[]", height=120)

    cash_balance = st.sidebar.number_input("cash_balance (NTD)", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("total_equity (NTD)", min_value=0, value=DEFAULT_EQUITY, step=10000)

    run_btn = st.sidebar.button("Run")

    # Parse positions
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
                topn=int(topn),
                positions=positions,
                cash_balance=int(cash_balance),
                total_equity=int(total_equity),
                allow_insecure_ssl=bool(allow_insecure_ssl),
            )
        except Exception as e:
            st.error("App 執行期間發生例外（已捕捉，不會白屏）。")
            st.exception(e)
            return

        ov = payload.get("macro", {}).get("overview", {})
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("交易日", ov.get("trade_date", "-"))
        c2.metric("Regime", payload.get("meta", {}).get("current_regime", "-"))
        c3.metric("SMR", f"{_safe_float(ov.get('smr'), 0):.6f}" if ov.get("smr") is not None else "NA")
        c4.metric("Slope5", f"{_safe_float(ov.get('slope5'), 0):.6f}" if ov.get("slope5") is not None else "NA")
        c5.metric("VIX", f"{_safe_float(ov.get('vix'), 0):.2f}" if ov.get("vix") is not None else "NA")
        c6.metric("Max Equity Allowed", f"{_pct(ov.get('max_equity_allowed_pct')):.1f}%" if ov.get("max_equity_allowed_pct") is not None else "NA")

        st.subheader("市場成交金額（best-effort / 可稽核）")
        st.json(payload.get("macro", {}).get("market_amount", {}))

        st.subheader("指數快照（簡版）")
        idx_df = pd.DataFrame(
            [
                {"symbol": TWII_SYMBOL, "name": "TAIEX", "last": ov.get("twii_close"), "asof": ov.get("trade_date")},
                {"symbol": VIX_SYMBOL, "name": "VIX", "last": ov.get("vix"), "asof": ov.get("trade_date")},
            ]
        )
        st.dataframe(idx_df, use_container_width=True)

        st.subheader("今日分析清單（TopN + 持倉）— Hybrid Layer")
        s_df = pd.json_normalize(payload.get("stocks", []))
        if not s_df.empty:
            if "Tier" in s_df.columns:
                s_df = s_df.sort_values("Tier", ascending=True)
            st.dataframe(s_df, use_container_width=True)
        else:
            st.info("stocks 清單為空（資料源可能暫時不可用）。")

        st.subheader("Warnings（最新 50 條）")
        w_df = pd.DataFrame(warns)
        if not w_df.empty and "code" in w_df.columns:
            key_fail = w_df["code"].isin(WARN_PRIORITY)
            w_df = pd.concat([w_df[key_fail], w_df[~key_fail]], ignore_index=True)
            st.dataframe(w_df, use_container_width=True)
        else:
            st.caption("（目前沒有 warnings）")

        st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
        st.json(payload)


if __name__ == "__main__":
    main()
