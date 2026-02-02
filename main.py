# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import certifi
import numpy as np
import pandas as pd
import requests
import streamlit as st
import urllib3
import yfinance as yf

from market_amount import fetch_amount_total, TZ_TAIPEI

# -----------------------------
# 基本設定（免費/模擬期）
# -----------------------------
SYSTEM_VERSION = "Predator V16.2 Enhanced (FREE/SIM)"
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

def _bool_env(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "y", "on")

# UI / ENV：允許 verify=False（為了解 TWSE 舊憑證 SSLError）
# 1) Streamlit sidebar checkbox（若有 UI）
# 2) 環境變數 ALLOW_INSECURE_SSL=1（Streamlit Cloud 建議用這個）
allow_insecure_env = _bool_env("ALLOW_INSECURE_SSL", "0")
ALLOW_INSECURE_SSL = allow_insecure_env

# 若在 streamlit run 下，提供 UI 開關（不影響雲端用 env 控制）
try:
    # sidebar 可能在你另一個 app.py 生成；這裡做容錯，不存在就略過
    ui_flag = st.sidebar.checkbox("允許不安全 SSL (verify=False)", value=allow_insecure_env)
    ALLOW_INSECURE_SSL = bool(ui_flag)
except Exception:
    ALLOW_INSECURE_SSL = allow_insecure_env

if ALLOW_INSECURE_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -----------------------------
# 低階工具：requests（帶 certifi，必要時退 verify=False）
# -----------------------------
def _requests_get(url: str, timeout: int = 15) -> requests.Response:
    """
    預設使用 certifi.where() 做 SSL 驗證。
    若遇到 requests.exceptions.SSLError，且 ALLOW_INSECURE_SSL=True 才允許 verify=False 降級。
    """
    try:
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not ALLOW_INSECURE_SSL:
            raise
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=False)

def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def _floor_int(x: float) -> int:
    return int(math.floor(x))

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
# 中文名稱：抓 TWSE/TPEx 股票清單做對照（免費）
# -----------------------------
def fetch_twse_stock_names() -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
        r = _requests_get(url, timeout=15)
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
# 大盤指數
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

# -----------------------------
# Regime 計算（V16.2）
# -----------------------------
def compute_regime_metrics() -> dict:
    out = {
        "SMR": None,
        "MA200": None,
        "SMR_MA5": None,
        "Slope5": None,
        "VIX": None,
        "drawdown_pct": None,
        "consolidation_15d_vol": None,
        "consolidation_flag": False,
    }

    tw = yf.download("^TWII", period="400d", interval="1d", progress=False)
    vx = yf.download("^VIX", period="60d", interval="1d", progress=False)

    if tw is None or tw.empty or len(tw) < 220:
        return out

    close = tw["Close"].dropna()
    ma200 = float(close.rolling(200).mean().iloc[-1])
    last = float(close.iloc[-1])
    smr = (last - ma200) / ma200 if ma200 else 0.0

    ma200_series = close.rolling(200).mean()
    smr_series = (close - ma200_series) / ma200_series
    smr_ma5_series = smr_series.rolling(5).mean().dropna()
    smr_ma5 = float(smr_ma5_series.iloc[-1]) if len(smr_ma5_series) >= 1 else float(smr)
    smr_ma5_prev = float(smr_ma5_series.iloc[-2]) if len(smr_ma5_series) >= 2 else smr_ma5
    slope5 = smr_ma5 - smr_ma5_prev

    lookback = close.iloc[-250:] if len(close) >= 250 else close
    peak = float(lookback.max())
    dd = (last - peak) / peak * 100 if peak else 0.0

    lb15 = close.iloc[-15:] if len(close) >= 15 else close
    if len(lb15) >= 10:
        vol15 = (float(lb15.max()) - float(lb15.min())) / float(lb15.mean()) * 100
    else:
        vol15 = None
    consolidation_flag = (vol15 is not None) and (vol15 < 5.0) and (0.08 <= smr <= 0.18)

    vix = float(vx["Close"].dropna().iloc[-1]) if (vx is not None and not vx.empty) else None

    out.update({
        "SMR": round(smr, 6),
        "MA200": round(ma200, 2),
        "SMR_MA5": round(smr_ma5, 6),
        "Slope5": round(slope5, 6),
        "VIX": round(vix, 2) if vix is not None else None,
        "drawdown_pct": round(dd, 2),
        "consolidation_15d_vol": round(vol15, 2) if vol15 is not None else None,
        "consolidation_flag": bool(consolidation_flag),
    })
    return out

def pick_regime(metrics: dict) -> Tuple[str, float]:
    """
    V16.2 優先序：
    CRASH > HIBERNATION > MEAN_REVERSION > OVERHEAT > CONSOLIDATION > NORMAL
    """
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    vix = metrics.get("VIX")
    dd = metrics.get("drawdown_pct")
    cons = bool(metrics.get("consolidation_flag"))

    max_equity_map = {
        "CRASH_RISK": 10.0,
        "HIBERNATION": 20.0,
        "MEAN_REVERSION": 45.0,
        "OVERHEAT": 55.0,
        "CONSOLIDATION": 65.0,
        "NORMAL": 85.0,
    }

    if smr is None or slope5 is None:
        return "NORMAL", max_equity_map["NORMAL"]

    if (vix is not None and vix > 35) or (dd is not None and dd <= -18.0):
        return "CRASH_RISK", max_equity_map["CRASH_RISK"]

    # HIBERNATION：需要 MA14_Monthly（每月收盤）資料。免費版先不硬觸發（避免錯殺）
    # 你若要「真 MA14_Monthly」，我可以再提供月線取樣版本。

    if 0.15 <= smr <= 0.25 and slope5 < -0.0001:
        return "MEAN_REVERSION", max_equity_map["MEAN_REVERSION"]

    if smr > 0.25:
        return "OVERHEAT", max_equity_map["OVERHEAT"]

    if cons:
        return "CONSOLIDATION", max_equity_map["CONSOLIDATION"]

    return "NORMAL", max_equity_map["NORMAL"]

def vix_stop_pct(vix: Optional[float]) -> float:
    if vix is None:
        return 0.08
    if vix < 20:
        return 0.06
    if vix <= 30:
        return 0.08
    return 0.10

# -----------------------------
# 法人：TWSE T86（三大法人買賣超）
# -----------------------------
def twse_date_fmt(yyyy_mm_dd: str) -> str:
    return yyyy_mm_dd.replace("-", "")

def fetch_twse_t86_for_date(yyyy_mm_dd: str) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={twse_date_fmt(yyyy_mm_dd)}&selectType=ALLBUT0999&response=json"
    r = _requests_get(url, timeout=15)
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

def build_institutional_panel(last_trade_date: str, lookback_days: int = 7) -> Tuple[pd.DataFrame, List[str]]:
    warnings = []
    dates = []
    dt = datetime.strptime(last_trade_date, "%Y-%m-%d").date()
    for i in range(lookback_days):
        dates.append((dt - timedelta(days=i)).strftime("%Y-%m-%d"))
    dates = list(reversed(dates))

    daily = []
    for d in dates:
        try:
            df = fetch_twse_t86_for_date(d)
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

def inst_metrics_for_symbol(panel: pd.DataFrame, symbol_tw: str) -> dict:
    out = {
        "inst_streak3": 0,
        "inst_streak5": 0,
        "inst_dir3": "MISSING",
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

    df["inst_net"] = df["foreign_net"].astype(int) + df["it_net"].astype(int)
    df = df.sort_values("date")
    dates = df["date"].tolist()
    inst = df["inst_net"].tolist()
    foreign = df["foreign_net"].tolist()
    out["inst_dates_5"] = dates[-5:]

    last3 = inst[-3:] if len(inst) >= 3 else inst
    s3 = int(np.sum(last3)) if last3 else 0
    out["inst_dir3"] = "POSITIVE" if (len(last3) >= 3 and s3 > 0) else ("NEGATIVE" if (len(last3) >= 3 and s3 < 0) else ("NEUTRAL" if len(last3) >= 3 else "MISSING"))

    f3 = foreign[-3:] if len(foreign) >= 3 else foreign
    sf3 = int(np.sum(f3)) if f3 else 0
    out["foreign_dir"] = "POSITIVE" if (len(f3) >= 3 and sf3 > 0) else ("NEGATIVE" if (len(f3) >= 3 and sf3 < 0) else ("NEUTRAL" if len(f3) >= 3 else "MISSING"))

    def streak(xs: List[int]) -> int:
        c = 0
        for v in reversed(xs):
            if v > 0:
                c += 1
            else:
                break
        return c

    out["inst_streak3"] = min(streak(inst), 3)
    out["inst_streak5"] = min(streak(inst), 5)
    out["inst_status"] = "READY" if len(inst) >= 3 else "UNAVAILABLE"
    return out

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
            vr = (float(vol.iloc[-1]) / vma20) if vma20 else None

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
# Top20：免費/模擬期的「動態掃描」
# -----------------------------
def build_universe(positions: List[dict]) -> List[str]:
    s = set(DEFAULT_UNIVERSE)
    for p in positions or []:
        sym = str(p.get("symbol", "")).strip()
        if sym:
            s.add(sym)
    return sorted(s)

def rank_top20(features_map: Dict[str, dict], inst_map: Dict[str, dict]) -> List[str]:
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
    return [r[0] for r in rows[:20]]

# -----------------------------
# Layer 判定（含 A+）
# -----------------------------
def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    """
    A+：inst_streak5>=5 且 foreign_dir=POSITIVE
    A ：inst_streak3>=3 且 inst_dir3=POSITIVE
    B ：(regime in OVERHEAT/CONSOLIDATION) 且 momentum_lock=True 且 vol_ratio>0.8
    NONE：其他
    """
    if int(inst.get("inst_streak5", 0)) >= 5 and inst.get("foreign_dir") == "POSITIVE":
        return "A+"
    if int(inst.get("inst_streak3", 0)) >= 3 and inst.get("inst_dir3") == "POSITIVE":
        return "A"
    if regime in ("OVERHEAT", "CONSOLIDATION"):
        if momentum_lock and (vol_ratio is not None) and float(vol_ratio) > 0.8:
            return "B"
    return "NONE"

def compute_momentum_lock(features_map: Dict[str, dict]) -> bool:
    if not features_map:
        return False
    scores = [float(v.get("Score") or 0.0) for v in features_map.values()]
    if not scores:
        return False
    strong = sum(1 for s in scores if s >= 30.0)
    return (strong / len(scores)) >= 0.5

# -----------------------------
# 帳戶：免費模擬
# -----------------------------
def load_account() -> dict:
    path = "configs/account.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"cash_balance": 2000000, "total_equity": 2000000, "positions": []}

# -----------------------------
# Data Health Gate
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
def build_arbiter_input(session: str = "PREMARKET") -> dict:
    now = _now_taipei()
    ts_str = now.strftime("%Y-%m-%d %H:%M")

    trade_date = get_last_trade_date()
    stale_flag = is_stale(trade_date, max_lag_trading_days=1)

    account = load_account()
    positions = account.get("positions", [])

    # 指數
    twii = fetch_index_snapshot("^TWII", "INTRADAY" if session == "INTRADAY" else "POSTMARKET")
    spx = fetch_index_snapshot("^GSPC", "POSTMARKET")
    ixic = fetch_index_snapshot("^IXIC", "POSTMARKET")
    dji = fetch_index_snapshot("^DJI", "POSTMARKET")
    vix = fetch_index_snapshot("^VIX", "POSTMARKET")

    # 成交金額（上市+上櫃合計）— 會受 market_amount.py 影響
    amount_ok = True
    amount_twse = None
    amount_tpex = None
    amount_total = None
    amount_sources = {"twse": None, "tpex": None, "error": None, "allow_insecure_ssl": ALLOW_INSECURE_SSL}
    try:
        ma = fetch_amount_total(allow_insecure_ssl=ALLOW_INSECURE_SSL)
        amount_twse = int(ma.amount_twse)
        amount_tpex = int(ma.amount_tpex)
        amount_total = int(ma.amount_total)
        amount_sources["twse"] = ma.source_twse
        amount_sources["tpex"] = ma.source_tpex
    except Exception as e:
        amount_ok = False
        amount_sources["error"] = f"{type(e).__name__}: {str(e)[:160]}"

    # Regime
    regime_metrics = compute_regime_metrics()
    regime, max_equity_allowed_pct = pick_regime(regime_metrics)

    # 法人 panel（T86）
    panel, inst_warn = build_institutional_panel(trade_date, lookback_days=7)

    # 名稱對照
    name_map = fetch_twse_stock_names()

    # Universe + 個股技術
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

    # Top20（每日更新）
    top20 = rank_top20(features_map, inst_map)

    # momentum_lock（簡化版）
    top20_features = {s: features_map[s] for s in top20 if s in features_map}
    momentum_lock = compute_momentum_lock(top20_features)

    # Gate → degraded_mode
    degraded_mode, gate_comment = data_health_gate(
        trade_date=trade_date,
        inst_ready_any=inst_ready_any,
        amount_ok=amount_ok,
        stale_flag=stale_flag,
    )

    # stocks（Top20 + 持倉補入）
    tracked = list(dict.fromkeys(top20 + [p.get("symbol") for p in positions if p.get("symbol")]))

    stocks_out = []
    a_plus_hits = 0

    for i, sym in enumerate(tracked, start=1):
        f = features_map.get(sym, {})
        im = inst_map.get(sym, {})
        name = name_map.get(sym, sym.replace(".TW", ""))

        tier = "A" if i <= 10 else "B"
        top20_flag = sym in top20
        orphan_holding = (not top20_flag) and any(str(p.get("symbol")) == sym for p in positions)

        layer = classify_layer(
            regime=regime,
            momentum_lock=momentum_lock,
            vol_ratio=f.get("Vol_Ratio"),
            inst=im,
        )
        if layer == "A+":
            a_plus_hits += 1

        stocks_out.append({
            "Symbol": sym,
            "Name": name,
            "Price": f.get("Price"),
            "ranking": {
                "symbol": sym,
                "rank": i,
                "tier": tier,
                "top20_flag": bool(top20_flag),
                "topn_actual": len(top20),
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
                "Inst_Streak5": int(im.get("inst_streak5", 0)),
                "Inst_Dir3": im.get("inst_dir3", "MISSING"),
                "Foreign_Dir": im.get("foreign_dir", "MISSING"),
                "Inst_Dates_5": im.get("inst_dates_5", []),
                "Layer": layer,  # A+ / A / B / NONE
            },
            "Structure": {
                "OPM": 0.0,
                "Rev_Growth": 0.0,
                "PE": 0.0,
                "Sector": "Unknown",
            },
            "risk": {
                "position_pct_max": 12,
                "risk_per_trade_max": 1.0,
                "trial_flag": True,
            },
            "orphan_holding": bool(orphan_holding),
            "weaken_flags": {
                "technical_weaken": False,
                "structure_weaken": False,
            }
        })

    # indices
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

    # overview
    overview = {
        "amount_twse": amount_twse if amount_twse is not None else None,
        "amount_tpex": amount_tpex if amount_tpex is not None else None,
        "amount_total": amount_total if amount_total is not None else None,
        "amount_sources": amount_sources,
        "trade_date": trade_date,
        "data_mode": session,
        "inst_status": "READY" if inst_ready_any else "UNAVAILABLE",
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": bool(degraded_mode),
        "market_comment": gate_comment,  # Arbiter 會忽略
        "regime": regime,
        "regime_metrics": regime_metrics,
        "max_equity_allowed_pct": float(max_equity_allowed_pct),
        "vix_stop_pct": round(vix_stop_pct(regime_metrics.get("VIX")), 4),
        "momentum_lock": bool(momentum_lock),
        "a_plus_hits": int(a_plus_hits),
        "warnings": inst_warn,
    }

    payload = {
        "meta": {
            "system": SYSTEM_VERSION,
            "market": MARKET,
            "timestamp": ts_str,
            "session": session
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
    """
    你在 Streamlit Cloud 的 entrypoint 用 main.py 沒問題。
    若你要本機直接跑：python main.py 會輸出 JSON 到 stdout。
    """
    session = os.getenv("SESSION", "PREMARKET").strip().upper()
    if session not in ("PREMARKET", "INTRADAY", "POSTMARKET"):
        session = "PREMARKET"
    payload = build_arbiter_input(session=session)
    print(json.dumps(payload, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
