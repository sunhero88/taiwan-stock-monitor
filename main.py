# main.py
# =========================================================
# Sunhero | 股市智能超盤中控台 (TopN + 持倉監控 / Predator V16.3 Stable Hybrid)
# + V16.2 Enhanced Kill-Switch (SHELTER) merged into V16.3
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
import streamlit.components.v1 as components
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

# ===== Kill-Switch thresholds（可調）=====
KILL_PRICE_NULL_PCT = 0.50        # Price 缺失 >= 50% 觸發
KILL_VOLRATIO_NULL_PCT = 0.50     # Vol_Ratio 缺失 >= 50% 觸發
KILL_CORE_MISSING_PCT = 0.50      # 核心缺失率 >= 50% 觸發（Price/Vol_Ratio/Amount_total）


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
        self.items.append(
            {"ts": _now_ts(), "code": code, "msg": msg, "meta": meta or {}}
        )

    def latest(self, n: int = 50) -> List[Dict[str, Any]]:
        return self.items[-n:]


warnings_bus = WarningBus()


# =========================
# Optional external modules
# =========================
# 若 repo 內有 finmind_institutional.py / institutional_utils.py 會直接用
# 若沒有，走 stub（不會白屏）
try:
    from finmind_institutional import fetch_finmind_institutional, fetch_finmind_market_inst_net_ab  # type: ignore
except Exception:
    fetch_finmind_institutional = None
    fetch_finmind_market_inst_net_ab = None

try:
    from institutional_utils import calc_inst_3d  # type: ignore
except Exception:
    calc_inst_3d = None


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

        if amount is None:
            best = None
            for _, tbl in candidate_tables:
                for row in tbl[-8:]:
                    if not isinstance(row, list):
                        continue
                    for cell in row:
                        v = _safe_int(cell, default=None)
                        if v is None:
                            continue
                        if best is None or v > best:
                            best = v
            if best is None:
                warnings_bus.push("TWSE_AMOUNT_PARSE_FAIL", "TWSE amount cannot be parsed", {"url": url})
                return None, "TWSE_FAIL:PARSE_NONE"
            amount = best
            src = "TWSE_WARN:FALLBACK_MAXSCAN"
            warnings_bus.push("TWSE_AMOUNT_PARSE_WARN", "TWSE amount parsed by fallback (max-scan)", {"amount": amount})

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
                for row in aa[-12:]:
                    if isinstance(row, list):
                        for cell in row:
                            v = _safe_int(cell, default=None)
                            if v is None:
                                continue
                            if best is None or v > best:
                                best = v
            if best is None:
                warnings_bus.push("TPEX_AMOUNT_PARSE_FAIL", "TPEX amount cannot be parsed", {"url": url})
                return None, "TPEX_FAIL:PARSE_NONE"
            amount = best
            src = "TPEX_WARN:FALLBACK_MAXSCAN"
            warnings_bus.push("TPEX_AMOUNT_PARSE_WARN", "TPEX amount parsed by fallback (max-scan)", {"amount": amount})

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
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=bool(allow_insecure_ssl),
    )


# =========================================================
# Predator V16.3 Stable (Hybrid Edition)
# =========================================================
def _as_close_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("market_df is empty")

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
    if market_df is None or len(market_df) < 10:
        return {
            "SMR": None,
            "SMR_MA5": None,
            "Slope5": None,
            "NEGATIVE_SLOPE_5D": True,
            "MOMENTUM_LOCK": False,
            "drawdown_pct": None,
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
        }

    smr = float(smr_series.iloc[-1])
    smr_ma5_series = smr_series.rolling(5).mean().dropna()

    if len(smr_ma5_series) < 2:
        slope5 = 0.0
    else:
        slope5 = float(smr_ma5_series.iloc[-1] - smr_ma5_series.iloc[-2])

    recent_slopes = smr_ma5_series.diff().dropna().iloc[-5:]
    negative_slope_5d = bool((recent_slopes < -EPS).all())

    momentum_lock = False
    if len(smr_ma5_series) >= 5:
        last4 = smr_ma5_series.diff().dropna().iloc[-4:]
        momentum_lock = bool((last4 > EPS).all())

    rolling_high = close.cummax()
    drawdown_series = (close - rolling_high) / rolling_high
    drawdown_pct = float(drawdown_series.min())

    return {
        "SMR": smr,
        "SMR_MA5": float(smr_ma5_series.iloc[-1]) if len(smr_ma5_series) else None,
        "Slope5": slope5,
        "NEGATIVE_SLOPE_5D": negative_slope_5d,
        "MOMENTUM_LOCK": momentum_lock,
        "drawdown_pct": drawdown_pct,
    }


def pick_regime(metrics: dict, vix: float = None, ma14_monthly: float = None,
                close_price: float = None, close_below_ma_days: int = 0) -> tuple:
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")

    # CRASH_RISK
    if (vix is not None and float(vix) > 35) or (drawdown is not None and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10

    # HIBERNATION (放寬至 2日 × 0.96)
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

    # CONSOLIDATION
    if smr is not None and 0.08 <= float(smr) <= 0.18:
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
    if (
        bool(momentum_lock)
        and (vr is not None and float(vr) > 0.8)
        and regime in ["NORMAL", "OVERHEAT", "CONSOLIDATION"]
    ):
        return "B"

    return "NONE"


# =========================
# Data fetchers (yfinance) - robust
# =========================
@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_history(symbol: str, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
    try:
        df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False, threads=False)
    except Exception as e:
        warnings_bus.push("YF_DOWNLOAD_FAIL", f"yfinance download failed: {e}", {"symbol": symbol, "period": period, "interval": interval})
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "Datetime"})
    if "Datetime" not in df.columns:
        df.insert(0, "Datetime", pd.to_datetime(df.index))
    return df


def _extract_close_price(market_df: pd.DataFrame) -> Optional[float]:
    try:
        df = market_df.copy()
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            df = df.set_index("Datetime")
        close = _as_close_series(df)
        if len(close) == 0:
            return None
        return float(close.dropna().iloc[-1])
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
        close = _as_close_series(df)
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
# FinMind institutional (merged)
# =========================
def _get_trade_date_from_twii(twii_df: pd.DataFrame) -> str:
    if twii_df is None or twii_df.empty:
        return time.strftime("%Y-%m-%d", time.localtime())
    if "Datetime" in twii_df.columns:
        dt = pd.to_datetime(twii_df["Datetime"].dropna().iloc[-1])
        return dt.strftime("%Y-%m-%d")
    return time.strftime("%Y-%m-%d", time.localtime())


def build_institutional_panel_finmind(symbols: List[str], trade_date: str, token: Optional[str]) -> pd.DataFrame:
    """
    產出欄位（供 UI / Layer 判定）：
    Symbol, Foreign_Net, Trust_Net, Inst_Streak3, Inst_Status, Inst_Dir3, Inst_Net_3d
    """
    # default stub
    base = pd.DataFrame([{
        "Symbol": s, "Foreign_Net": 0.0, "Trust_Net": 0.0,
        "Inst_Streak3": 0, "Inst_Status": "PENDING", "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0
    } for s in symbols])

    if fetch_finmind_institutional is None or calc_inst_3d is None:
        warnings_bus.push("FINMIND_MODULE_MISSING", "finmind_institutional.py 或 institutional_utils.py 不存在，法人資料降級為 0", {})
        return base

    # 抓近 7 日以覆蓋 3 日連續判定
    end_date = trade_date
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    try:
        inst_df = fetch_finmind_institutional(symbols=symbols, start_date=start_date, end_date=end_date, token=token)
    except Exception as e:
        warnings_bus.push("FINMIND_FETCH_FAIL", f"FinMind 法人抓取失敗：{e}", {})
        return base

    if inst_df is None or inst_df.empty:
        warnings_bus.push("FINMIND_EMPTY", "FinMind 法人回傳空資料", {"start": start_date, "end": end_date})
        return base

    # 這裡只有「三大法人合計 net_amount」，我們把它同時餵給 Foreign_Net/Trust_Net（方向用 bool 判斷仍可用）
    # 若你要分拆外資/投信，需另抓 dataset 或你原專案已分拆則改這段。
    out_rows = []
    for s in symbols:
        r = calc_inst_3d(inst_df=inst_df, symbol=s, trade_date=trade_date)
        net3 = float(r.get("Inst_Net_3d", 0.0) or 0.0)
        streak3 = int(r.get("Inst_Streak3", 0) or 0)
        status = str(r.get("Inst_Status", "PENDING"))
        dir3 = str(r.get("Inst_Dir3", "PENDING"))

        out_rows.append({
            "Symbol": s,
            "Foreign_Net": net3,   # placeholder（合併值）
            "Trust_Net": net3,     # placeholder（合併值）
            "Inst_Streak3": streak3,
            "Inst_Status": status,
            "Inst_Dir3": dir3,
            "Inst_Net_3d": net3,
        })

    return pd.DataFrame(out_rows)


def inst_metrics_for_symbol(panel: pd.DataFrame, symbol: str) -> dict:
    if panel is None or panel.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    df = panel[panel["Symbol"] == symbol]
    if df.empty:
        return {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0}

    row = df.iloc[-1]
    foreign_buy = bool(_safe_float(row.get("Foreign_Net", 0), 0) > 0)
    trust_buy = bool(_safe_float(row.get("Trust_Net", 0), 0) > 0)
    inst_streak3 = _safe_int(row.get("Inst_Streak3", 0), 0)

    return {"foreign_buy": foreign_buy, "trust_buy": trust_buy, "inst_streak3": inst_streak3}


# =========================
# Kill-Switch integrity evaluator
# =========================
def evaluate_integrity(stocks: List[dict], amount: MarketAmount) -> Dict[str, Any]:
    n = len(stocks)
    if n <= 0:
        return {"n": 0, "price_null": 0, "volratio_null": 0, "core_missing_pct": 1.0, "kill": True, "reason": "NO_STOCKS"}

    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)

    core_missing = price_null + volratio_null + (1 if amount.amount_total is None else 0)
    core_total = (n + n + 1)
    core_missing_pct = core_missing / max(1, core_total)

    kill = (
        (price_null / n) >= KILL_PRICE_NULL_PCT
        or (volratio_null / n) >= KILL_VOLRATIO_NULL_PCT
        or (core_missing_pct >= KILL_CORE_MISSING_PCT)
    )

    reason = "OK"
    if kill:
        reason = f"DATA_MISSING price_null={price_null}/{n}, volratio_null={volratio_null}/{n}, amount_total_null={amount.amount_total is None}, core_missing_pct={core_missing_pct:.2f}"

    return {
        "n": n,
        "price_null": price_null,
        "volratio_null": volratio_null,
        "core_missing_pct": float(core_missing_pct),
        "kill": bool(kill),
        "reason": reason,
    }


# =========================
# Copy button (AI JSON)
# =========================
def render_copy_button(label: str, text: str, height_px: int = 52):
    safe = text.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    html = f"""
    <div style="display:flex; align-items:center; gap:10px;">
      <button id="copyBtn"
        style="padding:8px 12px; border-radius:8px; border:1px solid #bbb; cursor:pointer; background:white;">
        {label}
      </button>
      <span id="copyMsg" style="font-size:13px; color:#555;"></span>
    </div>
    <script>
      const txt = `{safe}`;
      const btn = document.getElementById('copyBtn');
      const msg = document.getElementById('copyMsg');
      btn.addEventListener('click', async () => {{
        try {{
          await navigator.clipboard.writeText(txt);
          msg.textContent = '已複製到剪貼簿';
          setTimeout(()=>msg.textContent='', 1500);
        }} catch (e) {{
          msg.textContent = '複製失敗（瀏覽器限制）';
          setTimeout(()=>msg.textContent='', 2000);
        }}
      }});
    </script>
    """
    components.html(html, height=height_px)


# =========================
# Build arbiter input (V16.3 + Kill-Switch merged)
# =========================
def build_arbiter_input(
    session: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
    finmind_token: Optional[str],
    account_mode: str,
) -> Tuple[dict, List[dict]]:

    # ---- Market data ----
    twii_df = fetch_history(TWII_SYMBOL, period="3y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")

    trade_date = _get_trade_date_from_twii(twii_df)

    vix_last = None
    try:
        if not vix_df.empty and "Close" in vix_df.columns:
            vix_last = float(pd.to_numeric(vix_df["Close"], errors="coerce").dropna().iloc[-1])
    except Exception:
        vix_last = None

    # ---- Metrics ----
    metrics = compute_regime_metrics(
        twii_df.set_index("Datetime")[["Close"]] if (not twii_df.empty and "Datetime" in twii_df.columns and "Close" in twii_df.columns) else None
    )
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

    # ---- TopN symbols ----
    default_pool = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2881.TW", "2882.TW", "2603.TW", "2609.TW"]
    symset = list(dict.fromkeys([p.get("symbol") for p in positions if isinstance(p, dict) and p.get("symbol")] + default_pool))
    symbols = symset[: max(1, int(topn))]

    # ---- Institutional panel (FinMind) ----
    panel = build_institutional_panel_finmind(symbols, trade_date=trade_date, token=finmind_token)

    # ---- Per-stock snapshot ----
    stocks = []
    for i, sym in enumerate(symbols, start=1):
        px = None
        vol_ratio = None
        try:
            h = fetch_history(sym, period="6mo", interval="1d")
            if not h.empty and "Close" in h.columns:
                close = pd.to_numeric(h["Close"], errors="coerce").dropna()
                if len(close) > 0:
                    px = float(close.iloc[-1])

            if not h.empty and "Volume" in h.columns:
                v = pd.to_numeric(h["Volume"], errors="coerce").dropna()
                if len(v) >= 20:
                    ma20 = v.rolling(20).mean().iloc[-1]
                    if ma20 and not (pd.isna(ma20) or float(ma20) == 0.0):
                        vol_ratio = float(v.iloc[-1] / ma20)
        except Exception as e:
            warnings_bus.push("STOCK_FETCH_FAIL", f"{sym} price/volume fetch fail: {e}", {"symbol": sym})
            px = None
            vol_ratio = None

        im = inst_metrics_for_symbol(panel, sym)
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vol_ratio, im)

        stocks.append(
            {
                "Symbol": sym,
                "Name": sym,  # 可接代碼→中文名表
                "Tier": i,
                "Price": px,
                "Vol_Ratio": vol_ratio,
                "Layer": layer,
                "Institutional": im,
            }
        )

    # ---- Integrity / Kill-Switch ----
    integrity = evaluate_integrity(stocks, amount)

    market_status = "DEGRADED" if (amount.amount_total is None) else "NORMAL"
    audit_tag = "V16.3_STABLE_HYBRID"

    # V16.3 規則：DEGRADED 禁止 BUY（你原本的哲學）
    # Kill-Switch：核心資料缺失超標 → 直接 SHELTER，強制 max_equity=0，regime=UNKNOWN
    active_alerts: List[str] = []
    audit_log: List[dict] = []
    decisions: List[dict] = []  # 本版仍維持「只產 JSON，不自動下單」

    if integrity["kill"]:
        market_status = "SHELTER"
        regime_override = "UNKNOWN"
        max_equity = 0.0
        audit_tag = "V16.2_ENHANCED_KILL_SWITCH_ACTIVATED"

        active_alerts.append("KILL_SWITCH_ACTIVATED")
        if amount.amount_total is None:
            active_alerts.append("DEGRADED_AMOUNT: 成交量數據完全缺失 (TWSE_FAIL + TPEX_FAIL)")
        if integrity["price_null"] == integrity["n"]:
            active_alerts.append("CRITICAL: 所有個股價格 = null (無法執行任何決策)")
        if integrity["volratio_null"] == integrity["n"]:
            active_alerts.append("CRITICAL: 所有個股 Vol_Ratio = null (Layer B 判定不可能)")
        active_alerts.append(f"DATA_INTEGRITY_FAILURE: 核心數據缺失率={integrity['core_missing_pct']:.2f}")
        active_alerts.append("FORCED_ALL_CASH: 資料品質不足，強制進入避險模式")

        audit_log = [
            {"symbol": "ALL", "event": "KILL_SWITCH_TRIGGERED", "attribution": "DATA_MISSING", "comment": integrity["reason"]},
            {"symbol": "ALL", "event": "DEGRADED_STATUS_CRITICAL", "attribution": "MARKET_AMOUNT_FAILURE",
             "comment": f"amount_total={amount.amount_total}, source_twse={amount.source_twse}, source_tpex={amount.source_tpex}"},
            {"symbol": "ALL", "event": "ALL_CASH_FORCED", "attribution": "SYSTEM_PROTECTION",
             "comment": "核心哲學: In Doubt → Cash. 當前數據品質無法支持任何進場決策"},
        ]
        regime = regime_override

    # ---- Portfolio summary ----
    current_exposure_pct = 0.0
    if positions and market_status not in ["SHELTER"]:
        current_exposure_pct = min(1.0, len(positions) * 0.05)

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": market_status,
            "current_regime": regime,
            "account_mode": account_mode,
            "audit_tag": audit_tag,
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
                "max_equity_allowed_pct": max_equity,
            },
            "market_amount": asdict(amount),
            "integrity": integrity,
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float((1.0 - current_exposure_pct) * 100.0),  # 0~100
            "active_alerts": active_alerts,
        },
        "institutional_panel": panel.to_dict("records") if isinstance(panel, pd.DataFrame) else [],
        "stocks": stocks,
        "positions_input": positions,
        "decisions": decisions,
        "audit_log": audit_log,
    }

    return payload, warnings_bus.latest(50)


# =========================
# UI
# =========================
def main():
    # ---- Sidebar ----
    st.sidebar.header("設定")

    session = st.sidebar.selectbox("會議", ["INTRADAY", "EOD"], index=1)
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
    topn = st.sidebar.selectbox("TopN（固定池化數量）", [8, 10, 15, 20, 30, 50], index=0)

    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL (verify=False)", value=False)

    st.sidebar.subheader("FinMind")
    finmind_token = st.sidebar.text_input("FinMind Token（選填）", value="", type="password")
    finmind_token = finmind_token.strip() or None

    st.sidebar.subheader("持倉（手動貼 JSON陣列）")
    positions_text = st.sidebar.text_area("positions", value="[]", height=120)

    cash_balance = st.sidebar.number_input("現金餘額（新台幣）", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("總權益（新台幣）", min_value=0, value=DEFAULT_EQUITY, step=10000)

    run_btn = st.sidebar.button("跑步")

    # ---- Parse positions ----
    positions: List[dict] = []
    try:
        positions = json.loads(positions_text) if positions_text.strip() else []
        if not isinstance(positions, list):
            raise ValueError("positions 必須是 JSON array")
    except Exception as e:
        st.sidebar.error(f"positions JSON 解析失敗：{e}")
        positions = []

    # ---- Auto-run on first load or button ----
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
                finmind_token=finmind_token,
                account_mode=account_mode,
            )
        except Exception as e:
            st.error("App 執行期間發生例外（已捕捉，不會白屏）。")
            st.exception(e)
            return

        ov = payload.get("macro", {}).get("overview", {})
        integ = payload.get("macro", {}).get("integrity", {})
        alerts = payload.get("portfolio", {}).get("active_alerts", [])

        # ---- Header KPI ----
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("交易日", ov.get("trade_date", "-"))
        c2.metric("market_status", payload.get("meta", {}).get("market_status", "-"))
        c3.metric("regime", payload.get("meta", {}).get("current_regime", "-"))
        c4.metric("SMR", f"{_safe_float(ov.get('smr'), 0):.6f}" if ov.get("smr") is not None else "NA")
        c5.metric("Slope5", f"{_safe_float(ov.get('slope5'), 0):.6f}" if ov.get("slope5") is not None else "NA")
        c6.metric("Max Equity", f"{_pct01_to_pct100(ov.get('max_equity_allowed_pct')):.1f}%" if ov.get("max_equity_allowed_pct") is not None else "NA")

        st.caption(
            f"Integrity｜Price null={integ.get('price_null')}/{integ.get('n')}｜"
            f"Vol_Ratio null={integ.get('volratio_null')}/{integ.get('n')}｜"
            f"core_missing_pct={_safe_float(integ.get('core_missing_pct'), 0):.2f}"
        )

        # ---- Active Alerts ----
        if alerts:
            st.subheader("Active Alerts")
            for a in alerts:
                st.error(a)

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

        # ---- Institutional panel (debug) ----
        st.subheader("法人面板（FinMind / Debug）")
        ip = payload.get("institutional_panel", [])
        if ip:
            st.dataframe(pd.DataFrame(ip), use_container_width=True)
        else:
            st.info("法人面板目前為空（未提供 Token 或資料源不可用）。")

        # ---- Stocks table ----
        st.subheader("今日分析清單（TopN + 持倉）— Hybrid Layer")
        s_df = pd.json_normalize(payload.get("stocks", []))
        if not s_df.empty:
            if "Tier" in s_df.columns:
                s_df = s_df.sort_values("Tier", ascending=True)
            st.dataframe(s_df, use_container_width=True)
        else:
            st.info("stocks 清單為空（資料源可能暫時不可用）。")

        # ---- Audit Log ----
        st.subheader("Audit Log（Kill-Switch 稽核）")
        al = payload.get("audit_log", [])
        if al:
            st.dataframe(pd.DataFrame(al), use_container_width=True)
        else:
            st.caption("（目前沒有 audit log）")

        # ---- Warnings ----
        st.subheader("Warnings（最新 50 條）")
        w_df = pd.DataFrame(warns)
        if not w_df.empty:
            st.dataframe(w_df, use_container_width=True)
        else:
            st.caption("（目前沒有 warnings）")

        # ---- Arbiter Input JSON ----
        st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
        json_text = json.dumps(payload, ensure_ascii=False, indent=2)
        render_copy_button("一鍵複製 AI JSON", json_text)
        st.download_button(
            "下載 JSON（payload.json）",
            data=json_text.encode("utf-8"),
            file_name="payload.json",
            mime="application/json",
        )
        st.code(json_text, language="json")


if __name__ == "__main__":
    main()
