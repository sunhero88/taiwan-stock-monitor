# main.py
# -*- coding: utf-8 -*-

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import certifi
import numpy as np
import pandas as pd
import requests
import yfinance as yf

from market_amount import fetch_amount_total, TZ_TAIPEI


# -----------------------------
# 基本設定（SIM/FREE）
# -----------------------------
SYSTEM_VERSION = "Predator V16.2 Enhanced (SIM/FREE)"
MARKET = "tw-share"

DEFAULT_UNIVERSE = [
    "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW",
    "2603.TW", "2609.TW", "2412.TW", "2881.TW", "2882.TW", "2891.TW",
    "1301.TW", "1303.TW", "2002.TW", "3711.TW", "5871.TW", "5880.TW",
    "3037.TW", "6669.TW",
]

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

# 預設：不允許 verify=False（除非 UI 勾選或 env 開啟）
ALLOW_INSECURE_SSL_ENV = str(os.getenv("ALLOW_INSECURE_SSL", "0")).strip() in ("1", "true", "TRUE", "yes", "YES")


# -----------------------------
# 低階工具：requests（帶 certifi，必要時 verify=False）
# -----------------------------
def _requests_get(url: str, timeout: int = 15, allow_insecure_ssl: bool = False) -> requests.Response:
    try:
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not allow_insecure_ssl:
            raise
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=False)


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


# -----------------------------
# 交易日：用 ^TWII 最新K棒日期當作「最後收盤日」
# -----------------------------
def get_last_trade_date() -> str:
    df = yf.download("^TWII", period="10d", interval="1d", progress=False)
    if df is None or df.empty:
        return (_now_taipei() - timedelta(days=1)).strftime("%Y-%m-%d")
    last_dt = df.index[-1].to_pydatetime()
    return last_dt.strftime("%Y-%m-%d")


def is_stale(last_trade_date: str, max_lag_trading_days: int = 1) -> bool:
    dt = datetime.strptime(last_trade_date, "%Y-%m-%d").replace(tzinfo=TZ_TAIPEI)
    lag_days = (_now_taipei().date() - dt.date()).days
    return lag_days > 2 or lag_days > (max_lag_trading_days + 1)


# -----------------------------
# 中文名稱：抓 TWSE 上市清單做對照（容錯）
# -----------------------------
def fetch_twse_stock_names(allow_insecure_ssl: bool = False) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
        r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
        r.raise_for_status()
        tables = pd.read_html(r.text)
        if not tables:
            return out
        t = tables[0]
        col0 = t.columns[0]
        for v in t[col0].astype(str).tolist():
            if len(v) < 6:
                continue
            parts = [p for p in v.replace("\u3000", " ").split(" ") if p.strip()]
            if len(parts) >= 2 and parts[0].isdigit():
                code = parts[0].strip()
                name = parts[1].strip()
                out[f"{code}.TW"] = name
    except Exception:
        return out
    return out


# -----------------------------
# 指數快照
# -----------------------------
@dataclass
class IndexSnapshot:
    symbol: str
    last: float
    prev_close: float
    change: float
    change_pct: float
    asof: str


def fetch_index_snapshot(symbol: str, session: str) -> Optional[IndexSnapshot]:
    try:
        if session == "INTRADAY":
            intr = yf.download(symbol, period="2d", interval="5m", progress=False)
            d1 = yf.download(symbol, period="5d", interval="1d", progress=False)
            if intr is None or intr.empty or d1 is None or d1.empty:
                return None
            last = float(intr["Close"].dropna().iloc[-1])
            prev_close = float(d1["Close"].dropna().iloc[-2]) if len(d1) >= 2 else float(d1["Close"].iloc[-1])
            ts = intr.index[-1].to_pydatetime()
            asof = ts.strftime("%Y-%m-%d %H:%M")
        else:
            d1 = yf.download(symbol, period="10d", interval="1d", progress=False)
            if d1 is None or d1.empty:
                return None
            last = float(d1["Close"].dropna().iloc[-1])
            prev_close = float(d1["Close"].dropna().iloc[-2]) if len(d1) >= 2 else last
            ts = d1.index[-1].to_pydatetime()
            asof = ts.strftime("%Y-%m-%d")
        change = last - prev_close
        change_pct = (change / prev_close) * 100 if prev_close else 0.0
        return IndexSnapshot(symbol=symbol, last=last, prev_close=prev_close, change=change, change_pct=change_pct, asof=asof)
    except Exception:
        return None


# =========================================================
# ✅ 你要求「整段取代」：compute_regime_metrics() + pick_regime()
# 並含 Momentum Lock（Slope5 > EPS 連續 4 日）
# =========================================================
def compute_regime_metrics() -> dict:
    """
    指標（依 V16.2 文檔）：
      - MA200, SMR
      - SMR_MA5, Slope5
      - drawdown_pct：近 250 交易日高點回撤（%）
      - consolidation_flag：SMR 近10天都在 0.08~0.18 且 15日波動 < 5%
      - VIX：^VIX Close
      - momentum_lock_active：Slope5 > EPS(1e-4) 連續4日
    """
    out = {
        "SMR": None,
        "MA200": None,
        "SMR_MA5": None,
        "Slope5": None,
        "VIX": None,
        "drawdown_pct": None,
        "consolidation_15d_vol": None,
        "consolidation_flag": False,
        "momentum_lock_active": False,
        # HIBERNATION 介面預留（若你後續補月收盤MA14）
        "MA14_Monthly": None,
        "hibernation_days": None,
        "last_close": None,
    }

    tw = yf.download("^TWII", period="420d", interval="1d", progress=False)
    vx = yf.download("^VIX", period="90d", interval="1d", progress=False)

    if tw is None or tw.empty or len(tw) < 220:
        return out

    close = tw["Close"].dropna()
    last = float(close.iloc[-1])
    out["last_close"] = round(last, 2)

    ma200 = float(close.rolling(200).mean().iloc[-1])
    smr = (last - ma200) / ma200 if ma200 else 0.0

    smr_series = (close - close.rolling(200).mean()) / close.rolling(200).mean()
    smr_ma5_series = smr_series.rolling(5).mean().dropna()

    smr_ma5 = float(smr_ma5_series.iloc[-1]) if len(smr_ma5_series) else None
    slope5 = None
    if len(smr_ma5_series) >= 2:
        slope5 = float(smr_ma5_series.iloc[-1] - smr_ma5_series.iloc[-2])

    # drawdown: 近 250 交易日高點回撤（%），回撤為負值
    lookback = close.iloc[-250:] if len(close) >= 250 else close
    peak = float(lookback.max()) if len(lookback) else last
    dd_pct = ((last - peak) / peak * 100) if peak else 0.0

    # CONSOLIDATION（文檔）：SMR 近10天都在 0.08~0.18 且 15日波動 < 5%
    cons_flag = False
    vol15 = None
    if len(close) >= 15 and len(smr_series.dropna()) >= 10:
        recent_smr10 = smr_series.dropna().iloc[-10:].tolist()
        smr_in_range_10 = all(0.08 <= float(s) <= 0.18 for s in recent_smr10)

        lb15 = close.iloc[-15:]
        m15 = float(lb15.mean()) if float(lb15.mean()) else None
        vol15 = (float(lb15.max()) - float(lb15.min())) / m15 * 100 if m15 else None
        vol_ok = (vol15 is not None) and (vol15 < 5.0)
        cons_flag = bool(smr_in_range_10 and vol_ok)

    # Momentum Lock（文檔）：Slope5 > EPS 連續4日
    EPS = 1e-4
    momentum_lock_active = False
    if len(smr_ma5_series) >= 5:
        slope5_series = (smr_ma5_series.diff()).dropna()
        if len(slope5_series) >= 4:
            last4 = slope5_series.iloc[-4:].tolist()
            if len(last4) == 4 and all(float(x) > EPS for x in last4):
                momentum_lock_active = True

    vix = float(vx["Close"].dropna().iloc[-1]) if (vx is not None and not vx.empty) else None

    out.update({
        "SMR": round(float(smr), 6),
        "MA200": round(float(ma200), 2),
        "SMR_MA5": round(float(smr_ma5), 6) if smr_ma5 is not None else None,
        "Slope5": round(float(slope5), 6) if slope5 is not None else None,
        "VIX": round(float(vix), 2) if vix is not None else None,
        "drawdown_pct": round(float(dd_pct), 2),
        "consolidation_15d_vol": round(float(vol15), 2) if vol15 is not None else None,
        "consolidation_flag": bool(cons_flag),
        "momentum_lock_active": bool(momentum_lock_active),
    })
    return out


def pick_regime(metrics: dict) -> Tuple[str, float]:
    """
    文檔優先序：
    CRASH_RISK > HIBERNATION > MEAN_REVERSION > OVERHEAT > CONSOLIDATION > NORMAL
    Max Equity（%）：10 / 20 / 45 / 55 / 65 / 85
    """
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    vix = metrics.get("VIX")
    dd_pct = metrics.get("drawdown_pct")
    cons = bool(metrics.get("consolidation_flag"))

    max_equity_map = {
        "CRASH_RISK": 10.0,
        "HIBERNATION": 20.0,
        "MEAN_REVERSION": 45.0,
        "OVERHEAT": 55.0,
        "CONSOLIDATION": 65.0,
        "NORMAL": 85.0,
    }

    # 缺資料：保守 NORMAL（後續 Gate 仍會擋 BUY/TRIAL）
    if smr is None or slope5 is None:
        return "NORMAL", max_equity_map["NORMAL"]

    # 1) CRASH_RISK：VIX>=35 或 drawdown>=18%
    # dd_pct 是 % 且回撤為負值 → dd_pct <= -18
    if (vix is not None and float(vix) >= 35.0) or (dd_pct is not None and float(dd_pct) <= -18.0):
        return "CRASH_RISK", max_equity_map["CRASH_RISK"]

    # 2) HIBERNATION（接口預留：你補 MA14_Monthly 與連續日後可啟用）
    ma14m = metrics.get("MA14_Monthly")
    hib_days = metrics.get("hibernation_days")
    last_close = metrics.get("last_close")
    if ma14m is not None and hib_days is not None and last_close is not None:
        if int(hib_days) >= 3 and float(last_close) < float(ma14m) * 0.97:
            return "HIBERNATION", max_equity_map["HIBERNATION"]

    # 3) MEAN_REVERSION（文檔）：SMR > 0.25 AND Slope5 < -0.0001
    if float(smr) > 0.25 and float(slope5) < -0.0001:
        return "MEAN_REVERSION", max_equity_map["MEAN_REVERSION"]

    # 4) OVERHEAT（文檔）：SMR > 0.25 AND Slope5 >= -0.0001
    if float(smr) > 0.25 and float(slope5) >= -0.0001:
        return "OVERHEAT", max_equity_map["OVERHEAT"]

    # 5) CONSOLIDATION
    if cons:
        return "CONSOLIDATION", max_equity_map["CONSOLIDATION"]

    return "NORMAL", max_equity_map["NORMAL"]


def vix_stop_pct(vix: Optional[float]) -> float:
    """
    動態停損（文檔）：
      VIX<20: 6%
      20-30: 8%
      >30: 10%
    """
    if vix is None:
        return 0.08
    if vix < 20:
        return 0.06
    if vix <= 30:
        return 0.08
    return 0.10


# -----------------------------
# 法人：TWSE T86（外資/投信/自營商）
# -----------------------------
def twse_date_fmt(yyyy_mm_dd: str) -> str:
    return yyyy_mm_dd.replace("-", "")


def fetch_twse_t86_for_date(yyyy_mm_dd: str, allow_insecure_ssl: bool = False) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={twse_date_fmt(yyyy_mm_dd)}&selectType=ALLBUT0999&response=json"
    r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
    r.raise_for_status()
    j = r.json()
    data = j.get("data", [])
    fields = j.get("fields", [])
    if not data or not fields:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=fields)

    def pick_col(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in df.columns:
                return k
        return None

    col_code = pick_col(["證券代號"])
    col_name = pick_col(["證券名稱"])
    col_foreign_net = pick_col(["外陸資買賣超股數(不含外資自營商)", "外資買賣超股數", "外陸資買賣超股數"])
    col_it_net = pick_col(["投信買賣超股數"])

    if not col_code:
        return pd.DataFrame()

    def to_int(s) -> int:
        try:
            ss = str(s).replace(",", "").strip()
            if ss in ("", "--", "None", "nan"):
                return 0
            return int(float(ss))
        except Exception:
            return 0

    out = pd.DataFrame({
        "code": df[col_code].astype(str).str.strip(),
        "name": df[col_name].astype(str).str.strip() if col_name else "",
        "foreign_net": df[col_foreign_net].apply(to_int) if col_foreign_net else 0,
        "it_net": df[col_it_net].apply(to_int) if col_it_net else 0,
    })
    out = out[out["code"].str.match(r"^\d{4}$")]
    return out.reset_index(drop=True)


def build_institutional_panel(last_trade_date: str, lookback_days: int = 7, allow_insecure_ssl: bool = False) -> Tuple[pd.DataFrame, List[str]]:
    warnings = []
    dates = []
    dt = datetime.strptime(last_trade_date, "%Y-%m-%d").date()
    for i in range(lookback_days):
        dates.append((dt - timedelta(days=i)).strftime("%Y-%m-%d"))
    dates = list(reversed(dates))

    daily = []
    for d in dates:
        try:
            df = fetch_twse_t86_for_date(d, allow_insecure_ssl=allow_insecure_ssl)
            if df.empty:
                continue
            df["date"] = d
            daily.append(df)
        except Exception as e:
            warnings.append(f"T86_FAIL:{d}:{type(e).__name__}")
            continue

    if not daily:
        return pd.DataFrame(), warnings

    all_df = pd.concat(daily, ignore_index=True)
    avail_dates = sorted(all_df["date"].unique().tolist())
    last5 = avail_dates[-5:] if len(avail_dates) >= 5 else avail_dates
    panel = all_df[all_df["date"].isin(last5)].copy()
    return panel, warnings


# =========================================================
# ✅ 你要求「整段取代」：inst_metrics_for_symbol() + classify_layer()
# （啟用 A+，且 B 覆蓋 NORMAL/OVERHEAT/CONSOLIDATION）
# =========================================================
def inst_metrics_for_symbol(panel: pd.DataFrame, symbol_tw: str) -> dict:
    """
    依文檔輸出：
      - inst_streak3：法人合計(外資+投信)連續買超天數（截到3）
      - inst_dir3：近3日法人合計方向
      - foreign_buy / trust_buy：以「最近一個交易日」是否買超作為布林
      - foreign_dir：保留相容（不再作為 Layer 依據）
    """
    out = {
        "inst_streak3": 0,
        "inst_dir3": "MISSING",
        "foreign_buy": False,
        "trust_buy": False,
        "foreign_dir": "MISSING",
        "inst_status": "UNAVAILABLE",
        "inst_dates_5": [],
    }
    if panel is None or panel.empty:
        return out

    code = symbol_tw.replace(".TW", "")
    df = panel[panel["code"] == code].copy()
    if df.empty:
        return out

    df["foreign_net"] = df["foreign_net"].astype(int)
    df["it_net"] = df["it_net"].astype(int)
    df["inst_net"] = df["foreign_net"] + df["it_net"]
    df = df.sort_values("date")

    dates = df["date"].tolist()
    out["inst_dates_5"] = dates[-5:]

    inst = df["inst_net"].tolist()
    foreign = df["foreign_net"].tolist()
    trust = df["it_net"].tolist()

    out["foreign_buy"] = bool(len(foreign) >= 1 and int(foreign[-1]) > 0)
    out["trust_buy"] = bool(len(trust) >= 1 and int(trust[-1]) > 0)

    last3 = inst[-3:] if len(inst) >= 3 else inst
    if len(last3) >= 3:
        s3 = int(np.sum(last3))
        out["inst_dir3"] = "POSITIVE" if s3 > 0 else ("NEGATIVE" if s3 < 0 else "NEUTRAL")

    f3 = foreign[-3:] if len(foreign) >= 3 else foreign
    if len(f3) >= 3:
        sf3 = int(np.sum(f3))
        out["foreign_dir"] = "POSITIVE" if sf3 > 0 else ("NEGATIVE" if sf3 < 0 else "NEUTRAL")

    streak = 0
    for v in reversed(inst):
        if int(v) > 0:
            streak += 1
        else:
            break
    out["inst_streak3"] = int(min(streak, 3))
    out["inst_status"] = "READY" if len(inst) >= 3 else "UNAVAILABLE"
    return out


def classify_layer(
    regime: str,
    momentum_lock: bool,
    recovery_mode: bool,
    vol_ratio: Optional[float],
    inst: dict
) -> str:
    """
    文檔版 Layer：
      - recovery_mode=True：只允許 A，其餘 NONE
      - A+：foreign_buy & trust_buy & inst_streak3>=3
      - A ：(foreign_buy or trust_buy) & inst_streak3>=3
      - B ：momentum_lock & vol_ratio>0.8 & regime in NORMAL/OVERHEAT/CONSOLIDATION
      - else NONE
    """
    inst_streak3 = int(inst.get("inst_streak3", 0))
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))

    if recovery_mode:
        if (foreign_buy or trust_buy) and inst_streak3 >= 3:
            return "A"
        return "NONE"

    if foreign_buy and trust_buy and inst_streak3 >= 3:
        return "A+"

    if (foreign_buy or trust_buy) and inst_streak3 >= 3:
        return "A"

    if momentum_lock and (vol_ratio is not None) and float(vol_ratio) > 0.8:
        if regime in ("NORMAL", "OVERHEAT", "CONSOLIDATION"):
            return "B"

    return "NONE"


# -----------------------------
# 個股技術資料（yfinance）
# -----------------------------
def fetch_stock_daily(tickers: List[str]) -> pd.DataFrame:
    return yf.download(tickers, period="260d", interval="1d", progress=False, group_by="ticker", auto_adjust=False)


def stock_features(data: pd.DataFrame, symbol: str) -> dict:
    out = {
        "Price": None,
        "MA_Bias": 0.0,
        "Vol_Ratio": None,
        "Score": 0.0,
        "Body_Power": 0.0,
        "Tag": "○觀察(觀望)",
    }

    try:
        df = data[symbol].copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()
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


# -----------------------------
# TopN：免費/模擬期動態掃描
# -----------------------------
def build_universe(positions: List[dict]) -> List[str]:
    s = set(DEFAULT_UNIVERSE)
    for p in positions or []:
        sym = str(p.get("symbol", "")).strip()
        if sym:
            s.add(sym)
    return sorted(s)


def rank_topn(features_map: Dict[str, dict], inst_map: Dict[str, dict], n: int = 20) -> List[str]:
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
            # A+ 額外加權（讓榜單更容易看到 A+ 候選）
            if bool(im.get("foreign_buy")) and bool(im.get("trust_buy")) and int(im.get("inst_streak3", 0)) >= 3:
                inst_bonus += 6.0

        total = score + vr_bonus + inst_bonus
        rows.append((sym, total))

    rows.sort(key=lambda x: x[1], reverse=True)
    return [r[0] for r in rows[:n]]


# -----------------------------
# 帳戶：SIM（可用 configs/account.json）
# -----------------------------
def load_account() -> dict:
    path = "configs/account.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "cash_balance": 2000000,
        "total_equity": 2000000,
        "positions": [],
    }


# -----------------------------
# Data Health Gate（絕對防線）
# -----------------------------
def data_health_gate(trade_date: str, inst_ready_any: bool, amount_ok: bool, stale_flag: bool) -> Tuple[bool, str]:
    reasons = []
    if stale_flag:
        reasons.append("DATA_STALE")
    if not amount_ok:
        reasons.append("AMOUNT_UNAVAILABLE")
    if not inst_ready_any:
        reasons.append("INST_UNAVAILABLE")
    degraded = len(reasons) > 0
    comment = "; ".join(reasons) if reasons else "OK"
    return degraded, comment


# -----------------------------
# 主流程：產出 Arbiter Input JSON
# -----------------------------
def build_arbiter_input(
    session: str = "PREMARKET",
    topn: int = 20,
    allow_insecure_ssl: bool = False
) -> dict:
    now = _now_taipei()
    ts_str = now.strftime("%Y-%m-%d %H:%M")

    trade_date = get_last_trade_date()
    stale_flag = is_stale(trade_date, max_lag_trading_days=1)

    account = load_account()
    positions = account.get("positions", [])

    # 指數（盤前看昨收）
    twii = fetch_index_snapshot("^TWII", session if session != "PREMARKET" else "POSTMARKET")
    spx = fetch_index_snapshot("^GSPC", "POSTMARKET")
    ixic = fetch_index_snapshot("^IXIC", "POSTMARKET")
    dji = fetch_index_snapshot("^DJI", "POSTMARKET")
    vix_snap = fetch_index_snapshot("^VIX", "POSTMARKET")

    # 成交金額（上市+上櫃）
    amount_ok = True
    amount_twse = None
    amount_tpex = None
    amount_total = None
    amount_sources = {"twse": None, "tpex": None, "warnings": []}

    try:
        ma = fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl)
        amount_twse = int(ma.amount_twse)
        amount_tpex = int(ma.amount_tpex)
        amount_total = int(ma.amount_total)
        amount_sources["twse"] = ma.source_twse
        amount_sources["tpex"] = ma.source_tpex
        amount_sources["warnings"] = ma.warnings
    except Exception as e:
        amount_ok = False
        amount_sources["warnings"] = [f"AMOUNT_FATAL:{type(e).__name__}:{str(e)[:160]}"]

    # Regime（文檔版）
    regime_metrics = compute_regime_metrics()
    regime, max_equity_allowed_pct = pick_regime(regime_metrics)

    # ✅ Momentum Lock：直接用 regime_metrics 的連續4日判定（可稽核）
    momentum_lock = bool(regime_metrics.get("momentum_lock_active", False))

    # 法人 panel（T86）
    panel, inst_warn = build_institutional_panel(trade_date, lookback_days=7, allow_insecure_ssl=allow_insecure_ssl)

    # 名稱對照
    name_map = fetch_twse_stock_names(allow_insecure_ssl=allow_insecure_ssl)

    # Universe + 技術
    universe = build_universe(positions)
    data = fetch_stock_daily(universe)

    features_map: Dict[str, dict] = {}
    inst_map: Dict[str, dict] = {}

    inst_ready_any = False
    for sym in universe:
        f = stock_features(data, sym)
        features_map[sym] = f

        im = inst_metrics_for_symbol(panel, sym)
        inst_map[sym] = im
        if im.get("inst_status") == "READY":
            inst_ready_any = True

    # TopN（預設20）
    topn_list = rank_topn(features_map, inst_map, n=int(topn))

    # Data Health Gate
    degraded_mode, gate_comment = data_health_gate(
        trade_date=trade_date,
        inst_ready_any=inst_ready_any,
        amount_ok=amount_ok,
        stale_flag=stale_flag,
    )

    # tracked：TopN + 持倉補入
    tracked = list(dict.fromkeys(topn_list + [p.get("symbol") for p in positions if p.get("symbol")]))
    stocks_out = []

    # Recovery（SIM/FREE 先留 false；你之後可把前一日 regime 記在檔案裡再打開）
    recovery_mode = False
    recovery_days = 0

    for i, sym in enumerate(tracked, start=1):
        f = features_map.get(sym, {})
        im = inst_map.get(sym, {})
        name = name_map.get(sym, sym.replace(".TW", ""))

        tier = "A" if i <= max(1, int(topn) // 2) else "B"
        top_flag = sym in topn_list
        orphan_holding = (not top_flag) and any(str(p.get("symbol")) == sym for p in positions)

        layer = classify_layer(
            regime=regime,
            momentum_lock=momentum_lock,
            recovery_mode=recovery_mode,
            vol_ratio=f.get("Vol_Ratio"),
            inst=im,
        )

        stocks_out.append({
            "Symbol": sym,
            "Name": name,
            "Price": f.get("Price"),
            "ranking": {
                "symbol": sym,
                "rank": i,
                "tier": tier,
                "top20_flag": bool(top_flag),
                "topn_actual": len(topn_list),
            },
            "Technical": {
                "MA_Bias": f.get("MA_Bias", 0.0),
                "Vol_Ratio": f.get("Vol_Ratio"),
                "Body_Power": f.get("Body_Power", 0.0),
                "Score": f.get("Score", 0.0),
                "Tag": f.get("Tag", "○觀察(觀望)"),
            },
            "Institutional": {
                "Inst_Status": im.get("inst_status", "UNAVAILABLE"),
                "Inst_Streak3": int(im.get("inst_streak3", 0)),
                "Inst_Dir3": im.get("inst_dir3", "MISSING"),
                "Foreign_Buy": bool(im.get("foreign_buy", False)),
                "Trust_Buy": bool(im.get("trust_buy", False)),
                "Foreign_Dir": im.get("foreign_dir", "MISSING"),
                "Inst_Dates_5": im.get("inst_dates_5", []),
                "Layer": layer,  # A+ / A / B / NONE
            },
            "risk": {
                "position_pct_max": 15 if layer == "A+" else (10 if layer == "A" else 5),
                "risk_per_trade_max": 1.0,
                "trial_flag": True,
            },
            "orphan_holding": bool(orphan_holding),
            "weaken_flags": {
                "technical_weaken": False,
                "structure_weaken": False,
            }
        })

    # indices list
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
    add_idx(vix_snap, "VIX")

    # overview
    overview = {
        "trade_date": trade_date,
        "data_mode": session,
        "amount_twse": amount_twse if amount_twse is not None else "UNAVAILABLE",
        "amount_tpex": amount_tpex if amount_tpex is not None else "UNAVAILABLE",
        "amount_total": amount_total if amount_total is not None else "UNAVAILABLE",
        "amount_sources": amount_sources,  # ✅ 這裡會帶出 TWSE_AMOUNT_PARSE_FAIL / TPEX... 詳情
        "inst_status": "READY" if inst_ready_any else "UNAVAILABLE",
        "degraded_mode": bool(degraded_mode),
        "market_comment": gate_comment,  # 依你 Arbiter 規則：會被忽略，不得用作裁決條件
        "regime": regime,
        "regime_metrics": regime_metrics,
        "max_equity_allowed_pct": float(max_equity_allowed_pct),
        "momentum_lock": bool(momentum_lock),
        "recovery_mode": bool(recovery_mode),
        "recovery_days": int(recovery_days),
        "vix_stop_pct": float(vix_stop_pct(regime_metrics.get("VIX"))),
        "warnings": (inst_warn or []) + (amount_sources.get("warnings") or []),
    }

    payload = {
        "meta": {
            "system": SYSTEM_VERSION,
            "market": MARKET,
            "timestamp": ts_str,
            "session": session,
            "audit_tag": "V16.2_ENHANCED_CLEAN",
        },
        "macro": {
            "overview": overview,
            "indices": indices
        },
        "account": account,
        "stocks": stocks_out
    }
    return payload


def main():
    session = os.getenv("SESSION", "PREMARKET").strip().upper()
    if session not in ("PREMARKET", "INTRADAY", "POSTMARKET"):
        session = "PREMARKET"
    topn = int(os.getenv("TOPN", "20"))
    allow_insecure_ssl = ALLOW_INSECURE_SSL_ENV
    payload = build_arbiter_input(session=session, topn=topn, allow_insecure_ssl=allow_insecure_ssl)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
