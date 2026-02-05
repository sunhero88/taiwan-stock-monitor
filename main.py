# main.py
# =========================================================
# Sunhero | 股市智能超盤中控台 (TopN + 持倉監控 / Predator V16.3 Stable Hybrid + Kill-Switch)
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

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

# 你指定：5,000 萬閾值（50,000,000）
NEUTRAL_THRESHOLD = 50_000_000

# FinMind 三大法人資料集的常見 name
FINMIND_FOREIGN = "Foreign_Investor"
FINMIND_TRUST = "Investment_Trust"
FINMIND_DEALER_SELF = "Dealer_self"
FINMIND_DEALER_HEDGE = "Dealer_Hedging"
FINMIND_A_NAMES = {FINMIND_FOREIGN, FINMIND_TRUST, FINMIND_DEALER_SELF, FINMIND_DEALER_HEDGE}


def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _safe_float(x, default=None) -> Optional[float]:
    try:
        if x is None:
            return default
        if isinstance(x, (np.floating, float, int, np.integer)):
            return float(x)
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            if s == "":
                return default
            return float(s)
        return float(x)
    except Exception:
        return default


def _safe_int(x, default=0) -> int:
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


def _json_text(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


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
# Market amount (TWSE/TPEX) - best effort
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

        # 掃描 fields + tables 找「成交金額」欄位
        fields = js.get("fields9") or js.get("fields1") or js.get("fields") or []
        fields = [str(x) for x in fields] if isinstance(fields, list) else []
        amt_idx = None
        for i, f in enumerate(fields):
            if "成交金額" in f:
                amt_idx = i
                break

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

        amount = None
        src = "TWSE_OK:MI_INDEX"

        if amt_idx is not None:
            data = js.get("data9") if isinstance(js.get("data9"), list) else candidate_tables[0][1]
            last = data[-1]
            if isinstance(last, list) and amt_idx < len(last):
                amount = _safe_int(last[amt_idx], default=None)

        if amount is None:
            # fallback：掃描表尾數字最大值
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
                warnings_bus.push("TWSE_AMOUNT_PARSE_FAIL", "TWSE amount cannot be parsed", {"url": url})
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

        for key in ["totalAmount", "成交金額", "trade_value", "amt", "amount"]:
            if key in js:
                amount = _safe_int(js.get(key), default=None)
                if amount is not None:
                    break

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
        amount_twse=twse_amt if twse_amt is not None else None,
        amount_tpex=tpex_amt if tpex_amt is not None else None,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=bool(allow_insecure_ssl),
    )


# =========================================================
# V16.3 Stable Hybrid — Regime / Layer core
# =========================================================
def _as_close_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("market_df is empty")

    # yfinance 可能回多層欄位：做 flatten
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    if "Close" in df.columns:
        s = df["Close"]
    elif "close" in df.columns:
        s = df["close"]
    else:
        raise ValueError("Close price column not found")

    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]

    return s.astype(float)


def compute_regime_metrics(market_df: pd.DataFrame = None) -> dict:
    """
    回傳 dict，所有欄位皆為 scalar（不含 Series）
    重要修正：drawdown_pct 改為「目前距離歷史高點的回撤」，避免被「歷史最大回撤」誤判。
    同時保留 drawdown_max_pct（若你要檢視風險上限）。
    """
    if market_df is None or len(market_df) < 210:
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
            "drawdown_max_pct": None,
        }

    close = _as_close_series(market_df).dropna()
    if len(close) < 210:
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
            "drawdown_max_pct": None,
        }

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
            "drawdown_max_pct": None,
        }

    smr = float(smr_series.iloc[-1])
    smr_ma5_series = smr_series.rolling(5).mean().dropna()
    smr_ma5 = float(smr_ma5_series.iloc[-1]) if len(smr_ma5_series) else None

    if len(smr_ma5_series) >= 2:
        slope5 = float(smr_ma5_series.iloc[-1] - smr_ma5_series.iloc[-2])
    else:
        slope5 = 0.0

    recent_slopes = smr_ma5_series.diff().dropna().iloc[-5:]
    negative_slope_5d = bool(len(recent_slopes) >= 5 and (recent_slopes < -EPS).all())

    momentum_lock = False
    if len(smr_ma5_series) >= 5:
        last4 = smr_ma5_series.diff().dropna().iloc[-4:]
        momentum_lock = bool(len(last4) == 4 and (last4 > EPS).all())

    rolling_high = close.cummax()
    dd_series = (close - rolling_high) / rolling_high

    # ✅ 目前回撤（用最後一筆）
    drawdown_now = float(dd_series.iloc[-1])
    # 供稽核：歷史最大回撤（最小值）
    drawdown_max = float(dd_series.min())

    return {
        "SMR": smr,
        "SMR_MA5": smr_ma5,
        "Slope5": slope5,
        "NEGATIVE_SLOPE_5D": negative_slope_5d,
        "MOMENTUM_LOCK": momentum_lock,
        "drawdown_pct": drawdown_now,
        "drawdown_max_pct": drawdown_max,
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
    V16.3: HIBERNATION 放寬至 2日 × 0.96
    CRASH_RISK：採 drawdown_max_pct（避免「目前在高檔」但過去曾大回撤的誤判）
    """
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    dd_max = metrics.get("drawdown_max_pct")

    # --- CRASH_RISK ---
    if (vix is not None and float(vix) > 35) or (dd_max is not None and float(dd_max) <= -0.18):
        return "CRASH_RISK", 0.10

    # --- HIBERNATION ---
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

    return "NORMAL", 0.85


def classify_layer(regime: str, momentum_lock: bool, vol_ratio: float, inst: dict) -> str:
    """
    嚴格依 V16.3 規則：A+ / A / B / NONE
    """
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    inst_streak3 = int(inst.get("inst_streak3", 0))

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
# yfinance: safer fetchers
# =========================
@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_history_yf(symbol: str, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
    """
    改用 Ticker().history()，比 yf.download() 更不容易踩到多層欄位/型態坑。
    """
    try:
        t = yf.Ticker(symbol)
        df = t.history(period=period, interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.reset_index()
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "Datetime"})
        if "Datetime" not in df.columns:
            df.insert(0, "Datetime", pd.to_datetime(df.index))
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        return df
    except Exception as e:
        warnings_bus.push("YF_HISTORY_FAIL", f"{symbol} yfinance history fail: {e}", {"symbol": symbol})
        return pd.DataFrame()


def _extract_close_price(market_df: pd.DataFrame) -> Optional[float]:
    try:
        if market_df is None or market_df.empty:
            return None
        df = market_df.copy()
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            df = df.set_index("Datetime")
        close = _as_close_series(df).dropna()
        if len(close) == 0:
            return None
        return float(close.iloc[-1])
    except Exception:
        return None


def _calc_ma14_monthly_from_daily(market_df: pd.DataFrame) -> Optional[float]:
    try:
        if market_df is None or market_df.empty:
            return None
        df = market_df.copy()
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            df = df.set_index("Datetime")
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
    try:
        if ma14_monthly is None or market_df is None or market_df.empty:
            return 0
        df = market_df.copy()
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            df = df.set_index("Datetime")
        close = _as_close_series(df).dropna()
        if len(close) == 0:
            return 0

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
# FinMind helpers (institutional + price fallback)
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


def calc_inst_3d(inst_df: pd.DataFrame, symbol: str) -> dict:
    """
    inst_df 欄位需求：
    - date (YYYY-MM-DD)
    - symbol
    - net_amount  (三大法人合計日淨額)
    回傳：
    - Inst_Status: READY/PENDING
    - Inst_Streak3: 3 或 0（需連3日同向）
    - Inst_Dir3: POSITIVE/NEGATIVE/NEUTRAL/PENDING
    - Inst_Net_3d: 三日加總
    """
    if inst_df is None or inst_df.empty:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df = inst_df[inst_df["symbol"] == symbol].copy()
    if df.empty:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df = df.sort_values("date").tail(3)
    if len(df) < 3:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0.0)
    dirs = [normalize_inst_direction(x) for x in df["net_amount"].tolist()]
    net_sum = float(df["net_amount"].sum())

    if all(d == "POSITIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "POSITIVE", "Inst_Net_3d": net_sum}
    if all(d == "NEGATIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "NEGATIVE", "Inst_Net_3d": net_sum}

    return {"Inst_Status": "READY", "Inst_Streak3": 0, "Inst_Dir3": "NEUTRAL", "Inst_Net_3d": net_sum}


def fetch_finmind_institutional_components(
    symbols: List[str],
    start_date: str,
    end_date: str,
    token: Optional[str],
) -> pd.DataFrame:
    """
    dataset: TaiwanStockInstitutionalInvestorsBuySell
    盡量回傳欄位：
      date, symbol, name, net
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
            data = js.get("data", []) or []
            if not data:
                continue
            df = pd.DataFrame(data)

            need = {"date", "stock_id", "buy", "sell", "name"}
            if not need.issubset(set(df.columns)):
                continue

            df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0.0)
            df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0.0)
            df["net"] = df["buy"] - df["sell"]
            df["symbol"] = sym
            df = df[df["name"].isin(FINMIND_A_NAMES | {FINMIND_FOREIGN, FINMIND_TRUST})].copy()

            for _, r in df.iterrows():
                rows.append(
                    {
                        "date": str(r["date"]),
                        "symbol": sym,
                        "name": str(r["name"]),
                        "net": float(r["net"]),
                    }
                )
        except Exception as e:
            warnings_bus.push("FINMIND_INST_FAIL", f"{sym} finmind inst fail: {e}", {"symbol": sym})
            continue

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "name", "net"])

    return pd.DataFrame(rows).sort_values(["symbol", "date", "name"])


def build_institutional_panel_finmind(
    symbols: List[str],
    trade_date: str,
    token: Optional[str],
) -> Tuple[pd.DataFrame, List[dict]]:
    """
    回傳：
      panel_df 欄位：
        Symbol, Foreign_Net, Trust_Net, Inst_Streak3, Inst_Status, Inst_Dir3, Inst_Net_3d
      debug_rows（給 UI 顯示）
    """
    # 抓近 10 天足夠涵蓋 3 交易日（含假日）
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=14)).strftime("%Y-%m-%d")
    end_date = trade_date

    comp = fetch_finmind_institutional_components(symbols, start_date, end_date, token)
    if comp.empty:
        return pd.DataFrame(
            columns=["Symbol", "Foreign_Net", "Trust_Net", "Inst_Streak3", "Inst_Status", "Inst_Dir3", "Inst_Net_3d"]
        ), []

    # 取 Foreign / Trust 各自近 3 日合計
    foreign_3d = (
        comp[comp["name"] == FINMIND_FOREIGN]
        .groupby(["symbol"], as_index=False)["net"]
        .sum()
        .rename(columns={"net": "Foreign_Net"})
    )
    trust_3d = (
        comp[comp["name"] == FINMIND_TRUST]
        .groupby(["symbol"], as_index=False)["net"]
        .sum()
        .rename(columns={"net": "Trust_Net"})
    )

    # 三大法人合計：以 A_NAMES 做每日合計，再丟 calc_inst_3d 判斷 streak3
    a_daily = (
        comp[comp["name"].isin(FINMIND_A_NAMES)]
        .groupby(["symbol", "date"], as_index=False)["net"]
        .sum()
        .rename(columns={"net": "net_amount"})
    )

    rows = []
    debug_rows = []
    for sym in symbols:
        f_net = 0.0
        t_net = 0.0
        if not foreign_3d.empty and (foreign_3d["symbol"] == sym).any():
            f_net = float(foreign_3d.loc[foreign_3d["symbol"] == sym, "Foreign_Net"].iloc[0])
        if not trust_3d.empty and (trust_3d["symbol"] == sym).any():
            t_net = float(trust_3d.loc[trust_3d["symbol"] == sym, "Trust_Net"].iloc[0])

        inst3 = calc_inst_3d(a_daily.rename(columns={"symbol": "symbol"}), sym)

        rows.append(
            {
                "Symbol": sym,
                "Foreign_Net": float(f_net),
                "Trust_Net": float(t_net),
                "Inst_Streak3": int(inst3["Inst_Streak3"]),
                "Inst_Status": str(inst3["Inst_Status"]),
                "Inst_Dir3": str(inst3["Inst_Dir3"]),
                "Inst_Net_3d": float(inst3["Inst_Net_3d"]),
            }
        )
        debug_rows.append(rows[-1].copy())

    panel = pd.DataFrame(rows)
    return panel, debug_rows


def finmind_price_fallback(symbol: str, trade_date: str, token: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """
    以 FinMind 價量補缺（當 yfinance 失敗時）
    dataset: TaiwanStockPrice
    回傳：(close, volume)
    """
    stock_id = symbol.replace(".TW", "").strip()
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = trade_date
    try:
        js = _finmind_get(
            dataset="TaiwanStockPrice",
            params={"data_id": stock_id, "start_date": start_date, "end_date": end_date},
            token=token,
        )
        data = js.get("data", []) or []
        if not data:
            return None, None
        df = pd.DataFrame(data)
        if df.empty:
            return None, None
        # 常見欄位 close / Trading_Volume 或 volume：做耐錯
        cols = {c.lower(): c for c in df.columns}
        close_col = cols.get("close")
        vol_col = cols.get("trading_volume") or cols.get("volume")
        if close_col is None:
            return None, None
        df = df.sort_values("date")
        close = pd.to_numeric(df[close_col], errors="coerce").dropna()
        if close.empty:
            return None, None
        px = float(close.iloc[-1])

        vol = None
        if vol_col is not None:
            vv = pd.to_numeric(df[vol_col], errors="coerce").dropna()
            if not vv.empty:
                vol = float(vv.iloc[-1])

        return px, vol
    except Exception as e:
        warnings_bus.push("FINMIND_PRICE_FAIL", f"{symbol} finmind price fail: {e}", {"symbol": symbol})
        return None, None


def get_price_and_volratio(
    symbol: str,
    trade_date: str,
    finmind_token: Optional[str],
) -> Tuple[Optional[float], Optional[float], str]:
    """
    回傳：(price, vol_ratio, source)
    source 用於 warnings/meta 稽核
    """
    # 1) yfinance history
    try:
        h = fetch_history_yf(symbol, period="6mo", interval="1d")
        if h is not None and not h.empty:
            # flatten
            if isinstance(h.columns, pd.MultiIndex):
                h.columns = [c[0] if isinstance(c, tuple) else c for c in h.columns]

            px = None
            vr = None
            if "Close" in h.columns:
                close = pd.to_numeric(h["Close"], errors="coerce").dropna()
                if len(close) > 0:
                    px = float(close.iloc[-1])

            if "Volume" in h.columns:
                vol = pd.to_numeric(h["Volume"], errors="coerce").dropna()
                if len(vol) >= 20:
                    ma20 = vol.rolling(20).mean().iloc[-1]
                    if pd.notna(ma20) and float(ma20) != 0.0:
                        vr = float(vol.iloc[-1] / ma20)

            if px is not None:
                return px, vr, "YF"

    except Exception as e:
        warnings_bus.push("YF_SYMBOL_FAIL", f"{symbol} yfinance parse fail: {e}", {"symbol": symbol})

    # 2) FinMind fallback（可選）
    px2, vol2 = finmind_price_fallback(symbol, trade_date, finmind_token) if finmind_token else (None, None)
    if px2 is not None:
        vr2 = None
        # vol_ratio 需要 20 日均量；這裡用 FinMind 再抓一次 60 天計算
        try:
            stock_id = symbol.replace(".TW", "").strip()
            start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=90)).strftime("%Y-%m-%d")
            js = _finmind_get(
                dataset="TaiwanStockPrice",
                params={"data_id": stock_id, "start_date": start_date, "end_date": trade_date},
                token=finmind_token,
            )
            data = js.get("data", []) or []
            if data:
                df = pd.DataFrame(data).sort_values("date")
                cols = {c.lower(): c for c in df.columns}
                vol_col = cols.get("trading_volume") or cols.get("volume")
                if vol_col is not None:
                    vv = pd.to_numeric(df[vol_col], errors="coerce").dropna()
                    if len(vv) >= 20:
                        ma20 = vv.rolling(20).mean().iloc[-1]
                        if pd.notna(ma20) and float(ma20) != 0.0:
                            vr2 = float(vv.iloc[-1] / ma20)
        except Exception:
            vr2 = None

        return px2, vr2, "FINMIND"

    return None, None, "NONE"


def inst_metrics_for_symbol(panel: pd.DataFrame, symbol: str) -> dict:
    """
    所有輸出皆為 scalar
    """
    if panel is None or panel.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    df = panel[panel["Symbol"] == symbol]
    if df.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    row = df.iloc[-1]

    # foreign/trust buy：用 3D net 與 NEUTRAL_THRESHOLD 判斷方向（避免小額噪音）
    f_net = _safe_float(row.get("Foreign_Net", 0.0), 0.0)
    t_net = _safe_float(row.get("Trust_Net", 0.0), 0.0)

    foreign_buy = bool(f_net is not None and f_net > NEUTRAL_THRESHOLD)
    trust_buy = bool(t_net is not None and t_net > NEUTRAL_THRESHOLD)

    inst_streak3 = _safe_int(row.get("Inst_Streak3", 0), 0)

    return {"foreign_buy": foreign_buy, "trust_buy": trust_buy, "inst_streak3": inst_streak3}


# =========================
# Kill-Switch / Integrity
# =========================
def compute_integrity_and_kill(
    stocks: List[dict],
    amount: MarketAmount,
) -> dict:
    n = len(stocks)
    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)

    amount_twse_null = amount.amount_twse is None
    amount_tpex_null = amount.amount_tpex is None
    amount_total_null = amount.amount_total is None

    # 核心欄位缺失計分（和你貼的「25 格」同邏輯：n*2 + 3）
    denom = max(1, n * 2 + 3)
    missing = price_null + volratio_null + int(amount_twse_null) + int(amount_tpex_null) + int(amount_total_null)
    core_missing_pct = float(missing) / float(denom)

    # Kill-Switch 觸發（你貼的規則精神：核心缺失 > 50% 或 Price/VolRatio 全空 或 Amount 全空）
    kill = False
    reasons = []
    if n > 0 and price_null == n:
        kill = True
        reasons.append(f"price_null={price_null}/{n}")
    if n > 0 and volratio_null == n:
        kill = True
        reasons.append(f"volratio_null={volratio_null}/{n}")
    if amount_total_null and (amount_twse_null or amount_tpex_null):
        # amount_total 空且雙源至少一邊空 → 視為嚴重 degraded
        reasons.append("amount_total_null=True")
    if core_missing_pct >= 0.5:
        kill = True
        reasons.append(f"core_missing_pct={core_missing_pct:.2f}")

    reason = "DATA_MISSING " + ", ".join(reasons) if reasons else "OK"

    return {
        "n": n,
        "price_null": price_null,
        "volratio_null": volratio_null,
        "core_missing_pct": round(core_missing_pct, 4),
        "kill": bool(kill),
        "reason": reason,
    }


def build_active_alerts(integrity: dict, amount: MarketAmount) -> List[str]:
    alerts = []
    if integrity.get("kill"):
        alerts.append("KILL_SWITCH_ACTIVATED")

    if amount.amount_total is None:
        alerts.append("DEGRADED_AMOUNT: 成交量數據完全缺失 (TWSE_FAIL + TPEX_FAIL)")

    n = integrity.get("n", 0)
    if n > 0 and integrity.get("price_null", 0) == n:
        alerts.append("CRITICAL: 所有個股價格 = null (無法執行任何決策)")
    if n > 0 and integrity.get("volratio_null", 0) == n:
        alerts.append("CRITICAL: 所有個股 Vol_Ratio = null (Layer B 判定不可能)")

    if float(integrity.get("core_missing_pct", 0.0)) >= 0.5:
        alerts.append(f"DATA_INTEGRITY_FAILURE: 核心數據缺失率={integrity.get('core_missing_pct'):.2f}")

    if integrity.get("kill"):
        alerts.append("FORCED_ALL_CASH: 資料品質不足，強制進入避險模式")

    return alerts


def build_audit_log(integrity: dict, amount: MarketAmount) -> List[dict]:
    logs = []
    if integrity.get("kill"):
        logs.append(
            {
                "symbol": "ALL",
                "event": "KILL_SWITCH_TRIGGERED",
                "attribution": "DATA_MISSING",
                "comment": integrity.get("reason", ""),
            }
        )

    if amount.amount_total is None:
        logs.append(
            {
                "symbol": "ALL",
                "event": "DEGRADED_STATUS_CRITICAL",
                "attribution": "MARKET_AMOUNT_FAILURE",
                "comment": f"amount_total=None, source_twse={amount.source_twse}, source_tpex={amount.source_tpex}",
            }
        )

    if integrity.get("kill"):
        logs.append(
            {
                "symbol": "ALL",
                "event": "ALL_CASH_FORCED",
                "attribution": "SYSTEM_PROTECTION",
                "comment": "核心哲學: In Doubt → Cash. 當前數據品質無法支持任何進場決策",
            }
        )

    return logs


# =========================
# Build arbiter input (Hybrid + Kill-Switch)
# =========================
def build_arbiter_input(
    session: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
    account_mode: str,
    finmind_token: Optional[str],
) -> Tuple[dict, List[dict]]:

    # ---- Market data ----
    twii_df = fetch_history_yf(TWII_SYMBOL, period="3y", interval="1d")
    vix_df = fetch_history_yf(VIX_SYMBOL, period="2y", interval="1d")

    vix_last = None
    try:
        if not vix_df.empty and "Close" in vix_df.columns:
            v = pd.to_numeric(vix_df["Close"], errors="coerce").dropna()
            if len(v) > 0:
                vix_last = float(v.iloc[-1])
    except Exception:
        vix_last = None

    # ---- Metrics ----
    metrics = compute_regime_metrics(
        twii_df.set_index("Datetime") if (twii_df is not None and not twii_df.empty and "Datetime" in twii_df.columns) else twii_df
    )
    ma14_monthly = _calc_ma14_monthly_from_daily(twii_df)
    close_price = _extract_close_price(twii_df)
    close_below_days = _count_close_below_ma_days(twii_df, ma14_monthly)

    regime, max_equity = pick_regime(
        metrics=metrics,
        vixpanic=vix_last if vix_last is not None else None,
        vix=vix_last,
        ma14_monthly=ma14_monthly,
        close_price=close_price,
        close_below_ma_days=close_below_days,
    )

    # ---- Market amount ----
    amount = fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl)

    # ---- TopN symbols ----
    default_pool = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2881.TW", "2882.TW", "2603.TW", "2609.TW"]
    symset = list(dict.fromkeys([p.get("symbol") for p in positions if isinstance(p, dict)] + default_pool))
    symbols = symset[: max(1, int(topn))]

    # ---- Trade date ----
    trade_date = None
    try:
        if twii_df is not None and not twii_df.empty and "Datetime" in twii_df.columns:
            trade_date = pd.to_datetime(twii_df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")
    except Exception:
        trade_date = None
    trade_date = trade_date or time.strftime("%Y-%m-%d")

    # ---- Institutional panel (FinMind) ----
    panel, panel_debug = build_institutional_panel_finmind(symbols, trade_date, finmind_token)

    # ---- Per-stock snapshot (Price / Vol_Ratio robust) ----
    stocks = []
    for i, sym in enumerate(symbols, start=1):
        px, vr, src = get_price_and_volratio(sym, trade_date, finmind_token)
        if px is None:
            warnings_bus.push("PRICE_NULL", f"{sym} Price is null after fallback (YF+FinMind)", {"source": src, "symbol": sym})
        if vr is None:
            warnings_bus.push("VOLRATIO_NULL", f"{sym} Vol_Ratio is null after fallback (YF+FinMind)", {"source": src, "symbol": sym})

        im = inst_metrics_for_symbol(panel, sym)
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vr, im)

        stocks.append(
            {
                "Symbol": sym,
                "Name": sym,  # 之後你可接中文名稱表
                "Tier": i,
                "Price": px,
                "Vol_Ratio": vr,
                "Layer": layer,
                "Institutional": im,
                "Price_Source": src,
            }
        )

    # ---- Portfolio summary ----
    current_exposure_pct = 0.0
    if positions:
        current_exposure_pct = min(1.0, len(positions) * 0.05)

    # ---- Integrity + Kill-Switch ----
    integrity = compute_integrity_and_kill(stocks, amount)
    active_alerts = build_active_alerts(integrity, amount)
    audit_log = build_audit_log(integrity, amount)

    market_status = "DEGRADED" if (amount.amount_total is None) else "NORMAL"
    final_regime = regime
    final_max_equity = max_equity

    if integrity["kill"]:
        market_status = "SHELTER"
        final_regime = "UNKNOWN"
        final_max_equity = 0.0

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": market_status,
            "current_regime": final_regime,
            "account_mode": account_mode,
            "audit_tag": "V16.3_STABLE_HYBRID_KILL_SWITCH",
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "twii_close": close_price,
                "vix": vix_last,
                "smr": metrics.get("SMR"),
                "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"),          # ✅ 目前回撤
                "drawdown_max_pct": metrics.get("drawdown_max_pct"),  # ✅ 歷史最大回撤（稽核）
                "ma14_monthly": ma14_monthly,
                "close_below_ma_days": close_below_days,
                "max_equity_allowed_pct": final_max_equity,
            },
            "market_amount": asdict(amount),
            "integrity": integrity,
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct if not integrity["kill"] else 0.0),
            "cash_pct": float((1.0 - current_exposure_pct) * 100.0 if not integrity["kill"] else 100.0),
            "active_alerts": active_alerts,
        },
        "institutional_panel": panel_debug,
        "stocks": [
            {k: v for k, v in s.items() if k != "Price_Source"}  # JSON 乾淨一點
            for s in stocks
        ],
        "positions_input": positions,
        "decisions": [],  # 你之後要接 Arbiter 裁決輸出可放這裡
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
    finmind_token = finmind_token.strip() or None

    st.sidebar.subheader("持倉（手動貼 JSON 陣列）")
    positions_text = st.sidebar.text_area("positions", value="[]", height=120)

    cash_balance = st.sidebar.number_input("現金餘額（新台幣）", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("總權益（新台幣）", min_value=0, value=DEFAULT_EQUITY, step=10000)

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
                topn=int(topn),
                positions=positions,
                cash_balance=int(cash_balance),
                total_equity=int(total_equity),
                allow_insecure_ssl=bool(allow_insecure_ssl),
                account_mode=account_mode,
                finmind_token=finmind_token,
            )
        except Exception as e:
            st.error("App 執行期間發生例外（已捕捉，不會白屏）。")
            st.exception(e)
            return

        ov = payload.get("macro", {}).get("overview", {})
        meta = payload.get("meta", {})
        integrity = payload.get("macro", {}).get("integrity", {})

        # ---- Header KPI ----
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("交易日", ov.get("trade_date", "-"))
        c2.metric("market_status", meta.get("market_status", "-"))
        c3.metric("regime", meta.get("current_regime", "-"))
        c4.metric("SMR", f"{_safe_float(ov.get('smr'), 0):.6f}" if ov.get("smr") is not None else "NA")
        c5.metric("Slope5", f"{_safe_float(ov.get('slope5'), 0):.6f}" if ov.get("slope5") is not None else "NA")
        c6.metric("Max Equity", f"{_pct(ov.get('max_equity_allowed_pct')):.1f}%" if ov.get("max_equity_allowed_pct") is not None else "NA")

        st.caption(
            f"Integrity｜Price null={integrity.get('price_null')}/{integrity.get('n')} ｜ "
            f"Vol_Ratio null={integrity.get('volratio_null')}/{integrity.get('n')} ｜ "
            f"core_missing_pct={integrity.get('core_missing_pct')}"
        )

        # ---- Active Alerts ----
        st.subheader("Active Alerts")
        alerts = payload.get("portfolio", {}).get("active_alerts", [])
        if alerts:
            for a in alerts:
                st.error(a)
        else:
            st.success("（無）")

        # ---- Market Amount ----
        st.subheader("市場成交金額（best-effort / 可稽核）")
        st.json(payload.get("macro", {}).get("market_amount", {}))

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
        if ip:
            st.dataframe(pd.DataFrame(ip), use_container_width=True)
        else:
            st.info("（FinMind 法人資料目前空白：請確認 token 或 API 狀態）")

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

        # ---- Arbiter Input JSON (copyable) ----
        st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
        # ✅ st.code 會有「複製」按鈕
        st.code(_json_text(payload), language="json")

        # 另提供下載（方便你直接存檔/貼到別處）
        st.download_button(
            label="下載 Arbiter Input JSON",
            data=_json_text(payload),
            file_name=f"arbiter_input_{ov.get('trade_date','unknown')}.json",
            mime="application/json",
        )


if __name__ == "__main__":
    main()
