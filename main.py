# -*- coding: utf-8 -*-
from __future__ import annotations

# =========================
# Predator V16.3 Stable (Hybrid Edition)
# main.py (FULL REPLACE)
# =========================

import json
import math
import os
import re
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import certifi
import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf
from bs4 import BeautifulSoup


# -----------------------------
# Streamlit UI Boot Guard
# -----------------------------
st.set_page_config(
    page_title="Sunhero｜股市智能超盤中控台",
    layout="wide",
    initial_sidebar_state="expanded",
)


# -----------------------------
# Global Config
# -----------------------------
SYSTEM_VERSION = "Predator V16.3 Stable (Hybrid Edition) - SIM/FREE"
MARKET = "tw-share"
TZ_TAIPEI = timezone(timedelta(hours=8))
EPS = 1e-4  # 0.0001


DEFAULT_UNIVERSE = [
    "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW",
    "2603.TW", "2609.TW", "2412.TW", "2881.TW", "2882.TW", "2891.TW",
    "1301.TW", "1303.TW", "2002.TW", "3711.TW", "5871.TW", "5880.TW",
    "3037.TW", "6669.TW",
]

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}


# -----------------------------
# Helpers
# -----------------------------
def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _requests_get(url: str, timeout: int = 15, allow_insecure_ssl: bool = False) -> requests.Response:
    """
    先用 certifi 驗證，若遇到 SSLError 且 allow_insecure_ssl=True 則退 verify=False
    """
    try:
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not allow_insecure_ssl:
            raise
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=False)


def _safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip().replace(",", "")
        if s in ("", "--", "None", "nan"):
            return default
        return int(float(s))
    except Exception:
        return default


def _safe_float(x, default=None) -> Optional[float]:
    try:
        if x is None:
            return default
        s = str(x).strip().replace(",", "")
        if s in ("", "--", "None", "nan"):
            return default
        return float(s)
    except Exception:
        return default


def _sign3(x: int) -> str:
    if x > 0:
        return "POSITIVE"
    if x < 0:
        return "NEGATIVE"
    return "NEUTRAL"


def _ensure_close_series(df: pd.DataFrame) -> pd.Series:
    """
    yfinance 回來可能是 DataFrame 或 MultiIndex columns
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if "Close" in df.columns:
        return df["Close"].dropna()
    # 有些狀況會是 MultiIndex
    try:
        for c in df.columns:
            if isinstance(c, tuple) and len(c) >= 2 and c[1] == "Close":
                return df[c].dropna()
    except Exception:
        pass
    return pd.Series(dtype=float)


# -----------------------------
# Market Amount (TWSE / TPEx)
# -----------------------------
@dataclass
class MarketAmount:
    amount_twse: int
    amount_tpex: int
    amount_total: int
    source_twse: str
    source_tpex: str


def _fetch_twse_amount(allow_insecure_ssl: bool, warnings: List[str]) -> Tuple[int, str]:
    """
    TWSE: https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?response=json
    回傳上市成交金額（元）
    """
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?response=json"
    try:
        r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
        r.raise_for_status()
        j = r.json()
        # 常見欄位：data = [[..., "成交金額", ...]] / 或 tables
        # 這支 API 經常改格式 → 用 regex 抓第一個像成交金額的數字
        text = json.dumps(j, ensure_ascii=False)
        # 嘗試找「成交金額」附近最大一個數字
        m = re.search(r"成交金額[^0-9]*([0-9,]{6,})", text)
        if m:
            amt = _safe_int(m.group(1), 0)
            if amt > 0:
                return amt, "TWSE_FMTQIK_JSON(regex)"
        # 備援：掃描所有可能的大數字，取最大
        nums = [int(n.replace(",", "")) for n in re.findall(r"\b[0-9,]{9,}\b", text)]
        if nums:
            amt = max(nums)
            return int(amt), "TWSE_FMTQIK_JSON(maxnum)"
        warnings.append("TWSE_AMOUNT_PARSE_FAIL")
        return 0, "TWSE_AMOUNT_PARSE_FAIL"
    except requests.exceptions.SSLError:
        warnings.append("TWSE_AMOUNT_SSL_FAIL")
        return 0, "TWSE_AMOUNT_SSL_FAIL"
    except Exception as e:
        warnings.append(f"TWSE_AMOUNT_FAIL:{type(e).__name__}")
        return 0, f"TWSE_AMOUNT_FAIL:{type(e).__name__}"


def _fetch_tpex_amount(allow_insecure_ssl: bool, warnings: List[str]) -> Tuple[int, str]:
    """
    TPEx(上櫃) 常見來源：tpex 網站盤後統計頁
    https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw
    """
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw"
    try:
        r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
        r.raise_for_status()

        # 這頁通常含「成交金額」文字，可用 soup + regex
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        m = re.search(r"成交金額[^0-9]*([0-9,]{6,})", text)
        if m:
            amt = _safe_int(m.group(1), 0)
            if amt > 0:
                return amt, "TPEX_ST43_HTML(regex)"

        # 備援：抓頁面中的大數字取最大
        nums = [int(n.replace(",", "")) for n in re.findall(r"\b[0-9,]{9,}\b", text)]
        if nums:
            amt = max(nums)
            return int(amt), "TPEX_ST43_HTML(maxnum)"

        warnings.append("TPEX_AMOUNT_PARSE_FAIL")
        return 0, "TPEX_AMOUNT_PARSE_FAIL"

    except requests.exceptions.SSLError:
        warnings.append("TPEX_AMOUNT_SSL_FAIL")
        return 0, "TPEX_AMOUNT_SSL_FAIL"
    except Exception as e:
        warnings.append(f"TPEX_AMOUNT_FAIL:{type(e).__name__}")
        return 0, f"TPEX_AMOUNT_FAIL:{type(e).__name__}"


def fetch_amount_total(allow_insecure_ssl: bool = False, warnings: Optional[List[str]] = None) -> MarketAmount:
    """
    回傳：上市、上櫃、合計成交金額（元）
    allow_insecure_ssl=True 時允許 verify=False 以繞過舊憑證/鏈問題。
    """
    warnings = warnings if warnings is not None else []
    twse_amt, twse_src = _fetch_twse_amount(allow_insecure_ssl, warnings)
    tpex_amt, tpex_src = _fetch_tpex_amount(allow_insecure_ssl, warnings)
    total = int(twse_amt) + int(tpex_amt)
    return MarketAmount(
        amount_twse=int(twse_amt),
        amount_tpex=int(tpex_amt),
        amount_total=int(total),
        source_twse=twse_src,
        source_tpex=tpex_src,
    )


# -----------------------------
# Last Trade Date
# -----------------------------
def get_last_trade_date() -> str:
    df = yf.download("^TWII", period="10d", interval="1d", progress=False)
    if df is None or df.empty:
        return (_now_taipei() - timedelta(days=1)).strftime("%Y-%m-%d")
    last_dt = df.index[-1].to_pydatetime()
    return last_dt.strftime("%Y-%m-%d")


def is_stale(trade_date: str, max_lag_days: int = 2) -> bool:
    dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
    lag = (_now_taipei().date() - dt).days
    return lag > max_lag_days


# -----------------------------
# Index Snapshot
# -----------------------------
@dataclass
class IndexSnapshot:
    symbol: str
    last: float
    prev_close: float
    change: float
    change_pct: float
    asof: str


def fetch_index_snapshot(symbol: str, mode: str = "POSTMARKET") -> Optional[IndexSnapshot]:
    """
    mode:
      - INTRADAY: 5m last
      - POSTMARKET/PREMARKET: 1d close
    """
    try:
        if mode == "INTRADAY":
            intr = yf.download(symbol, period="2d", interval="5m", progress=False)
            d1 = yf.download(symbol, period="10d", interval="1d", progress=False)
            c_intr = _ensure_close_series(intr)
            c_d1 = _ensure_close_series(d1)
            if c_intr.empty or c_d1.empty:
                return None
            last = float(c_intr.iloc[-1])
            prev_close = float(c_d1.iloc[-2]) if len(c_d1) >= 2 else float(c_d1.iloc[-1])
            asof = intr.index[-1].to_pydatetime().strftime("%Y-%m-%d %H:%M")
        else:
            d1 = yf.download(symbol, period="10d", interval="1d", progress=False)
            c_d1 = _ensure_close_series(d1)
            if c_d1.empty:
                return None
            last = float(c_d1.iloc[-1])
            prev_close = float(c_d1.iloc[-2]) if len(c_d1) >= 2 else last
            asof = d1.index[-1].to_pydatetime().strftime("%Y-%m-%d")

        chg = last - prev_close
        chg_pct = (chg / prev_close) * 100 if prev_close else 0.0
        return IndexSnapshot(symbol, last, prev_close, chg, chg_pct, asof)
    except Exception:
        return None


# -----------------------------
# Institutional (TWSE T86)
# -----------------------------
def twse_date_fmt(yyyy_mm_dd: str) -> str:
    return yyyy_mm_dd.replace("-", "")


def fetch_twse_t86_for_date(yyyy_mm_dd: str, allow_insecure_ssl: bool, warnings: List[str]) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={twse_date_fmt(yyyy_mm_dd)}&selectType=ALLBUT0999&response=json"
    try:
        r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
        r.raise_for_status()
        j = r.json()
        data = j.get("data", [])
        fields = j.get("fields", [])
        if not data or not fields:
            warnings.append(f"T86_EMPTY:{yyyy_mm_dd}")
            return pd.DataFrame()
        df = pd.DataFrame(data, columns=fields)

        def pick_col(keys: List[str]) -> Optional[str]:
            for k in keys:
                if k in df.columns:
                    return k
            return None

        col_code = pick_col(["證券代號"])
        col_name = pick_col(["證券名稱"])
        col_foreign_net = pick_col([
            "外陸資買賣超股數(不含外資自營商)",
            "外資買賣超股數",
            "外陸資買賣超股數",
        ])
        col_it_net = pick_col(["投信買賣超股數"])

        if not col_code:
            warnings.append(f"T86_NO_CODE:{yyyy_mm_dd}")
            return pd.DataFrame()

        out = pd.DataFrame({
            "code": df[col_code].astype(str).str.strip(),
            "name": df[col_name].astype(str).str.strip() if col_name else "",
            "foreign_net": df[col_foreign_net].apply(_safe_int) if col_foreign_net else 0,
            "it_net": df[col_it_net].apply(_safe_int) if col_it_net else 0,
        })
        out = out[out["code"].str.match(r"^\d{4}$")]
        out["date"] = yyyy_mm_dd
        return out.reset_index(drop=True)

    except requests.exceptions.SSLError:
        warnings.append(f"T86_SSL_FAIL:{yyyy_mm_dd}")
        return pd.DataFrame()
    except Exception as e:
        warnings.append(f"T86_FAIL:{yyyy_mm_dd}:{type(e).__name__}")
        return pd.DataFrame()


def build_institutional_panel(last_trade_date: str, allow_insecure_ssl: bool, warnings: List[str]) -> pd.DataFrame:
    """
    回傳近 7 天日曆中可抓到資料的交易日，最後取最多 5 個交易日
    """
    dt = datetime.strptime(last_trade_date, "%Y-%m-%d").date()
    dates = [(dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    dates = list(reversed(dates))

    daily = []
    for d in dates:
        df = fetch_twse_t86_for_date(d, allow_insecure_ssl, warnings)
        if not df.empty:
            daily.append(df)

    if not daily:
        return pd.DataFrame()

    all_df = pd.concat(daily, ignore_index=True)
    avail_dates = sorted(all_df["date"].unique().tolist())
    last5 = avail_dates[-5:] if len(avail_dates) >= 5 else avail_dates
    return all_df[all_df["date"].isin(last5)].copy()


# ================================
# V16.3 Core #1: compute_regime_metrics()
# ================================
def compute_regime_metrics() -> dict:
    """
    V16.3 Stable (Hybrid):
    - SMR = (Index - MA200) / MA200
    - SMR_MA5 (包含當日)
    - Slope5 = SMR_MA5[t] - SMR_MA5[t-1]
    - MOMENTUM_LOCK: (Slope5 > EPS) 連續 4 日
    - NEGATIVE_SLOPE_5D: (Slope5 < -EPS) 連續 5 日
    - drawdown_pct: 近 250 交易日高點回撤（%）
    - consolidation_flag: 0.08<=SMR<=0.18 且 15 日波動 <5% 持續 10 日
    - MA14_Monthly: 月收盤 resample('M').last() rolling(14).mean()
    - HIBERNATION: Close < MA14_Monthly*0.96 連續 2 個交易日 (用 close_below_ma_days 表示)
    """
    out = {
        "SMR": None,
        "MA200": None,
        "SMR_MA5": None,
        "Slope5": None,
        "VIX": None,
        "drawdown_pct": None,
        "consolidation_flag": False,
        "consolidation_15d_vol": None,
        "momentum_lock": False,
        "negative_slope_5d": False,
        "MA14_Monthly": None,
        "close_below_ma_days": 0,
    }

    tw = yf.download("^TWII", period="600d", interval="1d", progress=False)
    vx = yf.download("^VIX", period="120d", interval="1d", progress=False)

    close = _ensure_close_series(tw)
    if close.empty or len(close) < 220:
        return out

    # MA200 & SMR
    ma200 = close.rolling(200).mean()
    ma200_last = float(ma200.iloc[-1])
    last = float(close.iloc[-1])
    smr_series = (close - ma200) / ma200
    smr = float(smr_series.iloc[-1]) if not np.isnan(smr_series.iloc[-1]) else None

    # SMR_MA5 & Slope5
    smr_ma5_series = smr_series.rolling(5).mean()
    smr_ma5 = float(smr_ma5_series.iloc[-1]) if not np.isnan(smr_ma5_series.iloc[-1]) else None
    smr_ma5_prev = float(smr_ma5_series.iloc[-2]) if len(smr_ma5_series.dropna()) >= 2 else smr_ma5
    slope5 = (smr_ma5 - smr_ma5_prev) if (smr_ma5 is not None and smr_ma5_prev is not None) else None

    # MOMENTUM_LOCK / NEGATIVE_SLOPE_5D
    slope_series = smr_ma5_series.diff()
    recent_slope = slope_series.dropna()
    momentum_lock = False
    negative_slope_5d = False
    if len(recent_slope) >= 5:
        last4 = recent_slope.iloc[-4:]
        last5 = recent_slope.iloc[-5:]
        momentum_lock = bool((last4 > EPS).all())
        negative_slope_5d = bool((last5 < -EPS).all())

    # drawdown 250
    lookback = close.iloc[-250:] if len(close) >= 250 else close
    peak = float(lookback.max())
    dd = ((last - peak) / peak) * 100 if peak else 0.0

    # MA14_Monthly (月收盤)
    monthly_close = close.resample("M").last()
    ma14_m = monthly_close.rolling(14).mean()
    ma14_last = float(ma14_m.iloc[-1]) if len(ma14_m.dropna()) > 0 else None

    # close below MA14_Monthly*0.96 consecutive (use last trading days)
    close_below_ma_days = 0
    if ma14_last is not None and ma14_last > 0:
        thresh = ma14_last * 0.96
        # 檢查最後 10 個交易日內的連續
        tail = close.iloc[-10:].tolist()
        for v in reversed(tail):
            if v < thresh:
                close_below_ma_days += 1
            else:
                break

    # consolidation_flag: 最近 10 日 SMR 都在 0.08~0.18 且 15 日波動<5%
    consolidation_flag = False
    vol15 = None
    if len(close) >= 15:
        lb15 = close.iloc[-15:]
        vol15 = ((float(lb15.max()) - float(lb15.min())) / float(lb15.mean())) * 100 if float(lb15.mean()) else None

    if len(smr_series.dropna()) >= 10 and vol15 is not None:
        last10_smr = smr_series.dropna().iloc[-10:]
        smr_in_range = bool(((last10_smr >= 0.08) & (last10_smr <= 0.18)).all())
        consolidation_flag = bool(smr_in_range and (vol15 < 5.0))

    # VIX
    vix_close = _ensure_close_series(vx)
    vix = float(vix_close.iloc[-1]) if not vix_close.empty else None

    out.update({
        "SMR": round(smr, 6) if smr is not None else None,
        "MA200": round(ma200_last, 2),
        "SMR_MA5": round(smr_ma5, 6) if smr_ma5 is not None else None,
        "Slope5": round(slope5, 6) if slope5 is not None else None,
        "VIX": round(vix, 2) if vix is not None else None,
        "drawdown_pct": round(dd, 2),
        "consolidation_flag": consolidation_flag,
        "consolidation_15d_vol": round(vol15, 2) if vol15 is not None else None,
        "momentum_lock": momentum_lock,
        "negative_slope_5d": negative_slope_5d,
        "MA14_Monthly": round(ma14_last, 2) if ma14_last is not None else None,
        "close_below_ma_days": int(close_below_ma_days),
    })
    return out


# ================================
# V16.3 Core #2: pick_regime()
# ================================
def pick_regime(metrics: dict) -> Tuple[str, float, str]:
    """
    V16.3 Regime priority:
    CRASH_RISK > HIBERNATION > MEAN_REVERSION > OVERHEAT > CONSOLIDATION > NORMAL

    Returns:
      (regime, max_equity_allowed_pct, account_mode)
      max_equity_allowed_pct: 10/20/45/55/65/85  (percent)
      account_mode: Aggressive / Balanced / Defensive
    """
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    vix = metrics.get("VIX")
    dd = metrics.get("drawdown_pct")
    cons = bool(metrics.get("consolidation_flag"))
    below_ma_days = int(metrics.get("close_below_ma_days") or 0)

    # Defaults
    regime = "NORMAL"
    max_eq = 85.0

    # CRASH_RISK
    if (vix is not None and vix > 35) or (dd is not None and dd <= -18.0):
        regime = "CRASH_RISK"
        max_eq = 10.0

    # HIBERNATION (2 days below MA14M*0.96)
    elif below_ma_days >= 2:
        regime = "HIBERNATION"
        max_eq = 20.0

    # MEAN_REVERSION
    elif (smr is not None and slope5 is not None) and (smr > 0.25 and slope5 < -0.0001):
        regime = "MEAN_REVERSION"
        max_eq = 45.0

    # OVERHEAT
    elif (smr is not None and slope5 is not None) and (smr > 0.25 and slope5 >= -0.0001):
        regime = "OVERHEAT"
        max_eq = 55.0

    # CONSOLIDATION
    elif cons:
        regime = "CONSOLIDATION"
        max_eq = 65.0

    # NORMAL
    else:
        regime = "NORMAL"
        max_eq = 85.0

    # Hybrid account_mode
    if regime in ("NORMAL", "CONSOLIDATION"):
        account_mode = "Aggressive"
    elif regime in ("OVERHEAT", "MEAN_REVERSION"):
        account_mode = "Balanced"
    else:
        account_mode = "Defensive"

    return regime, float(max_eq), account_mode


# ================================
# V16.3 Core #3: inst_metrics_for_symbol()
# ================================
def inst_metrics_for_symbol(panel: pd.DataFrame, symbol_tw: str) -> dict:
    """
    Output fields aligned to V16.3 doc:
      foreign_buy (bool)
      trust_buy (bool)
      inst_streak3 (0~3)
      inst_dir3 ("POSITIVE"/"NEGATIVE"/"NEUTRAL"/"MISSING")
      inst_net_3d (int)
      inst_status ("READY"/"UNAVAILABLE")
      inst_dates_5 (list)
      foreign_net_last (int), it_net_last (int), inst_net_last (int)
    """
    out = {
        "foreign_buy": False,
        "trust_buy": False,
        "inst_streak3": 0,
        "inst_dir3": "MISSING",
        "inst_net_3d": 0,
        "inst_status": "UNAVAILABLE",
        "inst_dates_5": [],
        "foreign_net_last": 0,
        "it_net_last": 0,
        "inst_net_last": 0,
    }

    if panel is None or panel.empty:
        return out

    code = symbol_tw.replace(".TW", "").strip()
    df = panel[panel["code"] == code].copy()
    if df.empty:
        return out

    df = df.sort_values("date")
    df["foreign_net"] = df["foreign_net"].astype(int)
    df["it_net"] = df["it_net"].astype(int)
    df["inst_net"] = df["foreign_net"] + df["it_net"]

    dates = df["date"].tolist()
    out["inst_dates_5"] = dates[-5:] if len(dates) >= 5 else dates

    inst = df["inst_net"].tolist()
    foreign = df["foreign_net"].tolist()
    trust = df["it_net"].tolist()

    # last day signals
    out["foreign_net_last"] = int(foreign[-1]) if foreign else 0
    out["it_net_last"] = int(trust[-1]) if trust else 0
    out["inst_net_last"] = int(inst[-1]) if inst else 0
    out["foreign_buy"] = bool(out["foreign_net_last"] > 0)
    out["trust_buy"] = bool(out["it_net_last"] > 0)

    # inst_dir3 / inst_net_3d
    if len(inst) >= 3:
        s3 = int(np.sum(inst[-3:]))
        out["inst_net_3d"] = s3
        out["inst_dir3"] = _sign3(s3)
    else:
        out["inst_dir3"] = "MISSING"
        out["inst_net_3d"] = int(np.sum(inst)) if inst else 0

    # inst_streak3 (consecutive inst_net>0)
    streak = 0
    for v in reversed(inst):
        if v > 0:
            streak += 1
        else:
            break
    out["inst_streak3"] = int(min(streak, 3))

    out["inst_status"] = "READY" if len(inst) >= 3 else "UNAVAILABLE"
    return out


# ================================
# V16.3 Core #4: classify_layer()
# ================================
def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], im: dict) -> str:
    """
    V16.3 Layer rules:
      A+ : foreign_buy AND trust_buy AND inst_streak3>=3
      A  : (foreign_buy OR trust_buy) AND inst_streak3>=3
      B  : momentum_lock AND vol_ratio>0.8 AND regime IN ["NORMAL","OVERHEAT","CONSOLIDATION"]
      NONE
    """
    foreign_buy = bool(im.get("foreign_buy", False))
    trust_buy = bool(im.get("trust_buy", False))
    inst_streak3 = int(im.get("inst_streak3", 0))

    if foreign_buy and trust_buy and inst_streak3 >= 3:
        return "A+"
    if (foreign_buy or trust_buy) and inst_streak3 >= 3:
        return "A"
    if momentum_lock and (vol_ratio is not None) and float(vol_ratio) > 0.8 and regime in ("NORMAL", "OVERHEAT", "CONSOLIDATION"):
        return "B"
    return "NONE"


# -----------------------------
# Stock Features (FREE)
# -----------------------------
def fetch_stock_daily(tickers: List[str]) -> pd.DataFrame:
    return yf.download(tickers, period="300d", interval="1d", progress=False, group_by="ticker", auto_adjust=False)


def stock_features(data: pd.DataFrame, symbol: str) -> dict:
    out = {
        "Price": None,
        "MA_Bias": 0.0,
        "Vol_Ratio": None,
        "Score": 0.0,
        "Tag": "○觀察(觀望)",
    }
    try:
        df = data[symbol] if isinstance(data.columns, pd.MultiIndex) else data
        close = df["Close"].dropna()
        vol = df["Volume"].dropna()
        if close.empty:
            return out

        price = float(close.iloc[-1])
        ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else float(close.mean())
        ma60 = float(close.rolling(60).mean().iloc[-1]) if len(close) >= 60 else ma20
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else ma60

        bias = (price - ma20) / ma20 * 100 if ma20 else 0.0

        vr = None
        if len(vol) >= 20:
            vma20 = float(vol.rolling(20).mean().iloc[-1])
            vr = float(vol.iloc[-1]) / vma20 if vma20 else None

        score = 0.0
        if price > ma20 > ma60:
            score += 10
        if price > ma200:
            score += 10
        if bias > 0:
            score += 10
        if vr is not None and vr > 1.0:
            score += 10

        tag = "○觀察(觀望)"
        if score >= 30 and bias > 0:
            tag = "🟢起漲(觀望)"
        if vr is not None and vr >= 1.5:
            tag = "🔥主力(觀望)"

        out.update({
            "Price": round(price, 4),
            "MA_Bias": round(bias, 2),
            "Vol_Ratio": round(vr, 4) if vr is not None else None,
            "Score": round(score, 1),
            "Tag": tag,
        })
        return out
    except Exception:
        return out


def build_universe(positions: List[dict]) -> List[str]:
    s = set(DEFAULT_UNIVERSE)
    for p in positions or []:
        sym = str(p.get("symbol", "")).strip()
        if sym:
            s.add(sym)
    return sorted(s)


def rank_topn(features_map: Dict[str, dict], inst_map: Dict[str, dict], topn: int) -> List[str]:
    rows = []
    for sym, f in features_map.items():
        score = float(f.get("Score") or 0.0)
        vr = f.get("Vol_Ratio")
        vr_bonus = 0.0
        if vr is not None:
            vr_bonus = min(10.0, max(0.0, (float(vr) - 0.8) * 10.0))

        im = inst_map.get(sym, {})
        inst_bonus = 0.0
        if im.get("inst_status") == "READY":
            inst_bonus += float(im.get("inst_streak3", 0)) * 2.0
            if im.get("inst_dir3") == "POSITIVE":
                inst_bonus += 4.0

        total = score + vr_bonus + inst_bonus
        rows.append((sym, total))

    rows.sort(key=lambda x: x[1], reverse=True)
    return [r[0] for r in rows[:topn]]


# -----------------------------
# Data Health Gate (degraded)
# -----------------------------
def data_health_gate(trade_date: str, stale_flag: bool, amount_ok: bool, inst_any_ready: bool) -> Tuple[bool, str]:
    reasons = []
    if stale_flag:
        reasons.append("DATA_STALE")
    if not amount_ok:
        reasons.append("AMOUNT_UNAVAILABLE")
    if not inst_any_ready:
        reasons.append("INST_UNAVAILABLE")
    degraded = len(reasons) > 0
    return degraded, "; ".join(reasons) if reasons else "OK"


# -----------------------------
# Build Arbiter Input JSON
# -----------------------------
def build_arbiter_input(
    session: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
) -> Tuple[dict, List[str]]:
    warnings: List[str] = []

    ts_str = _now_taipei().strftime("%Y-%m-%d %H:%M")
    trade_date = get_last_trade_date()
    stale_flag = is_stale(trade_date, max_lag_days=2)

    # Indices
    mode_tw = "INTRADAY" if session == "INTRADAY" else "POSTMARKET"
    twii = fetch_index_snapshot("^TWII", mode_tw)
    spx = fetch_index_snapshot("^GSPC", "POSTMARKET")
    ixic = fetch_index_snapshot("^IXIC", "POSTMARKET")
    dji = fetch_index_snapshot("^DJI", "POSTMARKET")
    vix = fetch_index_snapshot("^VIX", "POSTMARKET")

    indices = []
    def add_idx(snap: Optional[IndexSnapshot], name: str):
        if not snap:
            return
        indices.append({
            "symbol": snap.symbol,
            "name": name,
            "last": round(snap.last, 2),
            "prev_close": round(snap.prev_close, 2),
            "change": round(snap.change, 2),
            "change_pct": round(snap.change_pct, 3),
            "asof": snap.asof,
        })
    add_idx(twii, "TAIEX")
    add_idx(spx, "S&P 500")
    add_idx(ixic, "NASDAQ")
    add_idx(dji, "DJIA")
    add_idx(vix, "VIX")

    # Amount
    amount_ok = True
    amount_twse = None
    amount_tpex = None
    amount_total = None
    amount_sources = {"twse": None, "tpex": None, "error": None}
    try:
        ma = fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl, warnings=warnings)
        amount_twse = int(ma.amount_twse)
        amount_tpex = int(ma.amount_tpex)
        amount_total = int(ma.amount_total)
        amount_sources["twse"] = ma.source_twse
        amount_sources["tpex"] = ma.source_tpex
        if amount_total <= 0:
            amount_ok = False
    except Exception as e:
        amount_ok = False
        amount_sources["error"] = f"{type(e).__name__}: {str(e)[:160]}"
        warnings.append(f"AMOUNT_FAIL:{type(e).__name__}")

    # Regime metrics (V16.3)
    regime_metrics = compute_regime_metrics()
    regime, max_equity_allowed_pct, account_mode = pick_regime(regime_metrics)

    # Institutional panel
    panel = build_institutional_panel(trade_date, allow_insecure_ssl=allow_insecure_ssl, warnings=warnings)

    # Universe + stock features
    universe = build_universe(positions)
    data = fetch_stock_daily(universe)

    features_map: Dict[str, dict] = {}
    inst_map: Dict[str, dict] = {}
    inst_any_ready = False

    for sym in universe:
        f = stock_features(data, sym)
        features_map[sym] = f

        im = inst_metrics_for_symbol(panel, sym)
        inst_map[sym] = im
        if im.get("inst_status") == "READY":
            inst_any_ready = True

    # TopN
    top_list = rank_topn(features_map, inst_map, topn=topn)

    # momentum_lock from regime_metrics
    momentum_lock = bool(regime_metrics.get("momentum_lock", False))

    # degraded mode
    degraded_mode, gate_comment = data_health_gate(
        trade_date=trade_date,
        stale_flag=stale_flag,
        amount_ok=amount_ok,
        inst_any_ready=inst_any_ready,
    )

    # tracked = TopN + holdings
    tracked = list(dict.fromkeys(top_list + [p.get("symbol") for p in positions if p.get("symbol")]))

    stocks_out = []
    for i, sym in enumerate(tracked, start=1):
        f = features_map.get(sym, {})
        im = inst_map.get(sym, {})
        layer = classify_layer(regime, momentum_lock, f.get("Vol_Ratio"), im)

        tier = "A" if i <= max(1, topn // 2) else "B"
        top_flag = sym in top_list
        orphan = (not top_flag) and any(str(p.get("symbol")) == sym for p in positions)

        stocks_out.append({
            "Symbol": sym,
            "Price": f.get("Price"),
            "ranking": {"rank": i, "tier": tier, "top20_flag": bool(top_flag), "topn_actual": len(top_list)},
            "Technical": {
                "Tag": f.get("Tag"),
                "Score": f.get("Score"),
                "Vol_Ratio": f.get("Vol_Ratio"),
                "MA_Bias": f.get("MA_Bias"),
            },
            "Institutional": {
                "Inst_Status": im.get("inst_status"),
                "Inst_Streak3": im.get("inst_streak3"),
                "Inst_Dir3": im.get("inst_dir3"),
                "Inst_Net_3d": im.get("inst_net_3d"),
                "foreign_buy": im.get("foreign_buy"),
                "trust_buy": im.get("trust_buy"),
                "Layer": layer,
            },
            "risk": {"trial_flag": True},
            "orphan_holding": bool(orphan),
        })

    account = {
        "cash_balance": int(cash_balance),
        "total_equity": int(total_equity),
        "positions": positions,
    }

    overview = {
        "trade_date": trade_date,
        "data_mode": session,
        "amount_twse": amount_twse,
        "amount_tpex": amount_tpex,
        "amount_total": amount_total,
        "amount_sources": amount_sources,
        "degraded_mode": bool(degraded_mode),
        "market_comment": gate_comment,
        "regime": regime,
        "account_mode": account_mode,
        "max_equity_allowed_pct": float(max_equity_allowed_pct),
        "regime_metrics": regime_metrics,
    }

    payload = {
        "meta": {
            "system": SYSTEM_VERSION,
            "market": MARKET,
            "timestamp": ts_str,
            "session": session,
        },
        "macro": {"overview": overview, "indices": indices},
        "account": account,
        "stocks": stocks_out,
        "warnings": warnings[-50:],  # 僅保留最新 50
    }
    return payload, warnings


# -----------------------------
# UI
# -----------------------------
st.title("Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3 Stable Hybrid）")

with st.sidebar:
    st.header("設定")
    session = st.selectbox("Session", ["PREMARKET", "INTRADAY", "POSTMARKET"], index=1)
    topn = st.selectbox("TopN（固定池化數量）", [10, 20, 30, 50], index=1)

    allow_insecure_ssl = st.checkbox("允許不安全 SSL (verify=False)", value=False)

    st.subheader("持倉（手動貼 JSON array）")
    st.caption('格式範例： [{"symbol":"2330.TW","shares":100,"avg_cost":1000}]')
    pos_text = st.text_area("positions", value="[]", height=140)

    cash_balance = st.number_input("cash_balance（NTD）", min_value=0, value=2_000_000, step=10_000)
    total_equity = st.number_input("total_equity（NTD）", min_value=0, value=2_000_000, step=10_000)

    run = st.button("Run", type="primary")


def _parse_positions(pos_text: str) -> List[dict]:
    try:
        arr = json.loads(pos_text)
        if isinstance(arr, list):
            return arr
        return []
    except Exception:
        return []


# -----------------------------
# Main Run
# -----------------------------
if run:
    try:
        positions = _parse_positions(pos_text)

        payload, warnings = build_arbiter_input(
            session=session,
            topn=int(topn),
            positions=positions,
            cash_balance=int(cash_balance),
            total_equity=int(total_equity),
            allow_insecure_ssl=bool(allow_insecure_ssl),
        )

        ov = payload.get("macro", {}).get("overview", {})
        cols = st.columns(6)
        cols[0].metric("交易日（最後收盤）", ov.get("trade_date", "-"))
        cols[1].metric("Regime", ov.get("regime", "-"))
        cols[2].metric("SMR", ov.get("regime_metrics", {}).get("SMR"))
        cols[3].metric("Slope5", ov.get("regime_metrics", {}).get("Slope5"))
        cols[4].metric("VIX", ov.get("regime_metrics", {}).get("VIX"))
        cols[5].metric("Max Equity Allowed", f"{ov.get('max_equity_allowed_pct', 0):.1f}%")

        # Gate status banner
        if ov.get("degraded_mode", False):
            st.error(f"Gate：DEGRADED（禁止 BUY/TRIAL）｜原因：{ov.get('market_comment','-')}")
        else:
            st.success("Gate：OK（資料品質允許進行裁決）")

        st.subheader("市場成交金額（best-effort / 可稽核）")
        st.json({
            "amount_twse": ov.get("amount_twse"),
            "amount_tpex": ov.get("amount_tpex"),
            "amount_total": ov.get("amount_total"),
            "sources": ov.get("amount_sources"),
            "allow_insecure_ssl": allow_insecure_ssl,
        })

        st.subheader("指數快照")
        idx_df = pd.DataFrame(payload.get("macro", {}).get("indices", []))
        st.dataframe(idx_df, use_container_width=True)

        st.subheader(f"今日分析清單（Top{topn} + 持倉）→ 含 A+ Layer")
        s_df = pd.json_normalize(payload.get("stocks", []))
        show_cols = [
            "ranking.rank", "ranking.tier", "Symbol", "Price",
            "Technical.Tag", "Technical.Score", "Technical.MA_Bias", "Technical.Vol_Ratio",
            "Institutional.Inst_Status", "Institutional.Inst_Streak3", "Institutional.Inst_Dir3",
            "Institutional.Inst_Net_3d", "Institutional.foreign_buy", "Institutional.trust_buy",
            "Institutional.Layer", "orphan_holding",
        ]
        show_cols = [c for c in show_cols if c in s_df.columns]
        st.dataframe(s_df[show_cols], use_container_width=True, height=420)

        st.subheader("Warnings（最新 50 條）")
        # 你要看的 TWSE_AMOUNT_PARSE_FAIL / TPEX_AMOUNT_PARSE_FAIL 會出現在這裡
        w = payload.get("warnings", [])
        st.json({
            "warning_count": len(w),
            "warnings": w[-50:],
        })

        st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
        st.json(payload)

    except Exception as e:
        st.error("App 執行期間發生例外（已捕捉，不會白屏）。")
        st.code(f"{type(e).__name__}: {e}")
        st.code(traceback.format_exc()[:4000])

else:
    st.info("左側設定完成後按 Run。若抓取資料失敗，會以 Warnings 可稽核呈現，不會白屏。")
