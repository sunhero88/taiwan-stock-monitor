# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

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
import streamlit as st
import yfinance as yf

# =========================================================
# Predator V16.2 Enhanced (FREE/SIM) - main.py 直接跑版
# 目標：
# 1) 修復你現在卡住的 SyntaxError（__future__ 已在檔首）
# 2) 直接用 Streamlit 跑 main.py 也能出畫面（不需要 app.py）
# 3) 內建 SSL fallback（可控，預設安全；允許時才 verify=False）
# 4) A+ Layer 必須啟用（已硬寫入 classify_layer）
# 5) 即使 Top20=0，也要顯示「原因」(Data Health Gate)
# =========================================================

SYSTEM_VERSION = "Predator V16.2 Enhanced (FREE/SIM)"
MARKET = "tw-share"
TZ_TAIPEI = "Asia/Taipei"

DEFAULT_UNIVERSE = [
    "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW",
    "2603.TW", "2609.TW", "2412.TW", "2881.TW", "2882.TW", "2891.TW",
    "1301.TW", "1303.TW", "2002.TW", "3711.TW", "5871.TW", "5880.TW",
    "3037.TW", "6669.TW",
]

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

# === 重要：SSL 開關（預設關閉，只有你明確允許才會 verify=False）
# 可用：
# 1) Streamlit sidebar 勾選
# 2) 環境變數：ALLOW_INSECURE_SSL=1
ENV_ALLOW_INSECURE_SSL = str(os.getenv("ALLOW_INSECURE_SSL", "0")).strip().lower() in ("1", "true", "yes")


# -----------------------------
# 低階工具：requests（先用 certifi，必要時才 verify=False）
# -----------------------------
def _requests_get(url: str, timeout: int = 15, allow_insecure_ssl: bool = False) -> requests.Response:
    try:
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not allow_insecure_ssl:
            raise
        # 允許 verify=False 時，才退到不驗證
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=False)


def _now_taipei() -> datetime:
    # 以系統時區顯示即可（Streamlit Cloud 多半是 UTC；我們只做字串顯示）
    # 如需嚴格 +0800，可再補 zoneinfo，但這版先求穩定可跑。
    return datetime.now()


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


def is_stale(last_trade_date: str, max_lag_days: int = 2) -> bool:
    dt = datetime.strptime(last_trade_date, "%Y-%m-%d").date()
    lag_days = (_now_taipei().date() - dt).days
    return lag_days > max_lag_days


# -----------------------------
# 成交金額（TWSE / TPEx）— FREE best-effort
# 你之前的截圖顯示 TWSE SSL 會炸：Missing Subject Key Identifier
# => 這裡用 allow_insecure_ssl 控制 fallback verify=False
# -----------------------------
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str


def fetch_amount_total(trade_date: str, allow_insecure_ssl: bool) -> MarketAmount:
    """
    回傳金額單位：元（取整數）
    若抓不到，回 None，並在 source_* 留原因
    """
    # TWSE: MI_INDEX
    amount_twse = None
    src_twse = "TWSE:MI_INDEX"
    try:
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={trade_date.replace('-','')}&type=ALL&response=json"
        r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
        r.raise_for_status()
        j = r.json()

        # fields/data 會變，這裡用「容錯掃描」找「成交金額」
        # 常見位置：各類指數總表內的「成交金額」
        # 我們用最粗暴但穩定的方式：在 data 中找「成交金額」欄位的數值最大者（近似全市場）
        fields = j.get("fields", [])
        data = j.get("data", [])
        if fields and data:
            # 找欄位 index
            idx_amt = None
            for i, f in enumerate(fields):
                if "成交金額" in str(f):
                    idx_amt = i
                    break
            if idx_amt is not None:
                vals = []
                for row in data:
                    if idx_amt < len(row):
                        s = str(row[idx_amt]).replace(",", "").strip()
                        if s.isdigit():
                            vals.append(int(s))
                if vals:
                    amount_twse = max(vals)
    except Exception as e:
        src_twse = f"TWSE_FAIL:{type(e).__name__}"

    # TPEx：日成交資訊（格式易變，做 best-effort）
    amount_tpex = None
    src_tpex = "TPEX:daily_trade_sum"
    try:
        # TPEx 日期格式：民國年/月份/日期（常見）
        yyyy, mm, dd = trade_date.split("-")
        roc = str(int(yyyy) - 1911)
        roc_date = f"{roc}/{int(mm):02d}/{int(dd):02d}"

        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trade_sum/st43_result.php"
        params = f"?l=zh-tw&d={roc_date}&o=json"
        r = _requests_get(url + params, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
        r.raise_for_status()
        j = r.json()

        # 常見：{"aaData":[...]}，其中有「成交金額」欄位
        aa = j.get("aaData", [])
        # 嘗試掃描數字最大者（當作全市場成交金額近似）
        vals = []
        for row in aa:
            for cell in row:
                s = str(cell).replace(",", "").strip()
                if s.isdigit():
                    v = int(s)
                    # 過小通常不是金額；用 >1e8 當濾網（1億）
                    if v > 100_000_000:
                        vals.append(v)
        if vals:
            amount_tpex = max(vals)
    except Exception as e:
        src_tpex = f"TPEX_FAIL:{type(e).__name__}"

    amount_total = None
    if amount_twse is not None and amount_tpex is not None:
        amount_total = int(amount_twse + amount_tpex)

    return MarketAmount(
        amount_twse=amount_twse,
        amount_tpex=amount_tpex,
        amount_total=amount_total,
        source_twse=src_twse,
        source_tpex=src_tpex,
    )


# -----------------------------
# 指數快照（盤前/盤中/盤後）
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
    """
    session:
      - PREMARKET / POSTMARKET: 抓 1d 收盤
      - INTRADAY: 抓 5m 最後一筆 + 昨收
    """
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
# Regime 計算（V16.2 Enhanced）
# -----------------------------
def compute_regime_metrics() -> dict:
    """
    指標：
      - MA200
      - SMR = (Index - MA200) / MA200
      - SMR_MA5, Slope5
      - drawdown_pct（250日高點回撤）
      - CONSOLIDATION: 15日波動 < 5% 且 SMR 0.08~0.18
      - VIX（^VIX）
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
    }

    tw = yf.download("^TWII", period="420d", interval="1d", progress=False)
    vx = yf.download("^VIX", period="60d", interval="1d", progress=False)

    if tw is None or tw.empty or len(tw) < 220:
        return out

    close = tw["Close"].dropna()
    ma200 = float(close.rolling(200).mean().iloc[-1])
    last = float(close.iloc[-1])
    smr = (last - ma200) / ma200 if ma200 else 0.0

    smr_series = (close - close.rolling(200).mean()) / close.rolling(200).mean()
    smr_ma5_series = smr_series.rolling(5).mean().dropna()
    if smr_ma5_series.empty:
        return out

    smr_ma5 = float(smr_ma5_series.iloc[-1])
    smr_ma5_prev = float(smr_ma5_series.iloc[-2]) if len(smr_ma5_series) >= 2 else smr_ma5
    slope5 = smr_ma5 - smr_ma5_prev

    lookback = close.iloc[-250:] if len(close) >= 250 else close
    peak = float(lookback.max())
    dd = (last - peak) / peak * 100 if peak else 0.0

    lb15 = close.iloc[-15:] if len(close) >= 15 else close
    vol15 = None
    if len(lb15) >= 10:
        vol15 = (float(lb15.max()) - float(lb15.min())) / float(lb15.mean()) * 100

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

    注意：HIBERNATION 需要「MA14_Monthly」月收盤，免費端難完整；
    這版先不在這裡硬觸發，避免錯殺。你若要真月線，我再幫你補「月收盤序列」算法。
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

    # CRASH: VIX>35 或 回撤>=18%
    if (vix is not None and vix > 35) or (dd is not None and dd <= -18.0):
        return "CRASH_RISK", max_equity_map["CRASH_RISK"]

    # MEAN_REVERSION: SMR 0.15~0.25 且 Slope5 < -0.0001
    if 0.15 <= smr <= 0.25 and slope5 < -0.0001:
        return "MEAN_REVERSION", max_equity_map["MEAN_REVERSION"]

    # OVERHEAT: SMR > 0.25
    if smr > 0.25:
        return "OVERHEAT", max_equity_map["OVERHEAT"]

    # CONSOLIDATION
    if cons:
        return "CONSOLIDATION", max_equity_map["CONSOLIDATION"]

    return "NORMAL", max_equity_map["NORMAL"]


def vix_stop_pct(vix: Optional[float]) -> float:
    """
    動態停損（V16.2）：
      VIX < 20: 6%
      20~30: 8%
      >30: 10%
    回傳正值（例如 0.06）
    """
    if vix is None:
        return 0.08
    if vix < 20:
        return 0.06
    if vix <= 30:
        return 0.08
    return 0.10


# -----------------------------
# 中文名稱（TWSE ISIN page，best-effort）
# -----------------------------
def fetch_twse_stock_names(allow_insecure_ssl: bool) -> Dict[str, str]:
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
# 法人：TWSE T86（三大法人買賣超）— best-effort
# -----------------------------
def twse_date_fmt(yyyy_mm_dd: str) -> str:
    return yyyy_mm_dd.replace("-", "")


def fetch_twse_t86_for_date(yyyy_mm_dd: str, allow_insecure_ssl: bool) -> pd.DataFrame:
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


def build_institutional_panel(last_trade_date: str, allow_insecure_ssl: bool, lookback_days: int = 7) -> Tuple[pd.DataFrame, List[str]]:
    warnings = []
    dt = datetime.strptime(last_trade_date, "%Y-%m-%d").date()
    dates = [(dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
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


def inst_metrics_for_symbol(panel: pd.DataFrame, symbol_tw: str) -> dict:
    """
    回傳：
      inst_streak3, inst_streak5, inst_dir3, foreign_dir, inst_status, inst_dates_5
    """
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
    """
    回傳：
      Price, MA_Bias(%), Vol_Ratio(當日量/20日均量), Score(簡易), Tag
    """
    out = {"Price": None, "MA_Bias": 0.0, "Vol_Ratio": None, "Score": 0.0, "Tag": "○觀察(觀望)"}
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
            vma = float(vol.rolling(20).mean().iloc[-1])
            vr = float(vol.iloc[-1]) / vma if vma else None

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
# TopN（免費版用可解釋排名）
# -----------------------------
def build_universe(positions: List[dict]) -> List[str]:
    s = set(DEFAULT_UNIVERSE)
    for p in positions or []:
        sym = str(p.get("symbol", "")).strip()
        if sym:
            s.add(sym)
    return sorted(s)


def rank_topn(features_map: Dict[str, dict], inst_map: Dict[str, dict], topn: int) -> List[str]:
    """
    排名分數 = 技術 Score + 量能加權 + 法人加權
    量能加權：vol_ratio 0.8->0, 1.8->10（上限 10）
    法人加權：streak3*2 + POSITIVE +4（最多 10）
    """
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
# Momentum Lock（免費簡化版）
# -----------------------------
def compute_momentum_lock(top_features: Dict[str, dict]) -> bool:
    """
    先用 TopN 中 Score>=30 的比例判定動能鎖：
    強勢占比 >= 50% -> True
    """
    if not top_features:
        return False
    scores = [float(v.get("Score") or 0.0) for v in top_features.values()]
    if not scores:
        return False
    strong = sum(1 for s in scores if s >= 30.0)
    return (strong / len(scores)) >= 0.5


# -----------------------------
# Layer 判定（A+ 必須啟用）
# -----------------------------
def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    """
    A+：inst_streak5>=5 AND foreign_dir=POSITIVE  （必須啟用）
    A ：inst_streak3>=3 AND inst_dir3=POSITIVE
    B ：(regime in OVERHEAT/CONSOLIDATION) AND momentum_lock AND vol_ratio>0.8
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


# -----------------------------
# Account（UI JSON 輸入：positions）
# -----------------------------
def normalize_positions(raw_positions: list) -> list:
    out = []
    for p in raw_positions or []:
        sym = str(p.get("symbol", "")).strip()
        if not sym:
            continue
        out.append({
            "symbol": sym,
            "shares": int(p.get("shares", 0) or 0),
            "avg_cost": float(p.get("avg_cost", 0) or 0),
            "entry_date": str(p.get("entry_date", "")) if p.get("entry_date") is not None else "",
            "status": str(p.get("status", "")) if p.get("status") is not None else "",
            "sector": str(p.get("sector", "")) if p.get("sector") is not None else "",
        })
    return out


# -----------------------------
# Data Health Gate（保命邏輯）
# -----------------------------
def data_health_gate(
    trade_date: str,
    stale_flag: bool,
    amount_ok: bool,
    inst_ready_any: bool,
    ssl_broken: bool,
) -> Tuple[bool, str]:
    reasons = []
    if stale_flag:
        reasons.append("DATA_STALE")
    if not amount_ok:
        reasons.append("AMOUNT_UNAVAILABLE")
    if not inst_ready_any:
        reasons.append("INST_UNAVAILABLE")
    if ssl_broken:
        reasons.append("SSL_BROKEN")

    degraded = len(reasons) > 0
    return degraded, "; ".join(reasons) if reasons else "OK"


# -----------------------------
# 主流程：產出 Arbiter Input JSON（供你下一段 Arbiter 用）
# -----------------------------
def build_arbiter_input(
    session: str,
    topn: int,
    allow_insecure_ssl: bool,
    account: dict,
) -> dict:
    now = _now_taipei()
    ts_str = now.strftime("%Y-%m-%d %H:%M")

    trade_date = get_last_trade_date()
    stale_flag = is_stale(trade_date, max_lag_days=2)

    positions = account.get("positions", [])

    # 1) 指數
    sess_idx = "INTRADAY" if session == "INTRADAY" else "POSTMARKET"
    twii = fetch_index_snapshot("^TWII", sess_idx)
    spx = fetch_index_snapshot("^GSPC", "POSTMARKET")
    ixic = fetch_index_snapshot("^IXIC", "POSTMARKET")
    dji = fetch_index_snapshot("^DJI", "POSTMARKET")
    vix_snap = fetch_index_snapshot("^VIX", "POSTMARKET")

    # 2) 成交金額（SSL 會影響）
    amount_ok = True
    ssl_broken = False
    ma = None
    try:
        ma = fetch_amount_total(trade_date, allow_insecure_ssl=allow_insecure_ssl)
        # 如果兩邊都抓不到，視為 amount 不可用
        if ma.amount_twse is None and ma.amount_tpex is None:
            amount_ok = False
    except requests.exceptions.SSLError:
        amount_ok = False
        ssl_broken = True
        ma = MarketAmount(None, None, None, "TWSE_SSL_FAIL", "TPEX_SSL_FAIL")
    except Exception:
        amount_ok = False
        ma = MarketAmount(None, None, None, "TWSE_FAIL", "TPEX_FAIL")

    # 3) Regime
    regime_metrics = compute_regime_metrics()
    regime, max_equity_allowed_pct = pick_regime(regime_metrics)

    # 4) 法人 panel
    panel, inst_warn = build_institutional_panel(trade_date, allow_insecure_ssl=allow_insecure_ssl, lookback_days=7)

    # 5) 名稱
    name_map = fetch_twse_stock_names(allow_insecure_ssl=allow_insecure_ssl)

    # 6) Universe + 技術
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

    # 7) TopN
    top_list = rank_topn(features_map, inst_map, topn=topn)
    top_features = {s: features_map.get(s, {}) for s in top_list}
    momentum_lock = compute_momentum_lock(top_features)

    # 8) Gate
    degraded_mode, gate_comment = data_health_gate(
        trade_date=trade_date,
        stale_flag=stale_flag,
        amount_ok=amount_ok,
        inst_ready_any=inst_ready_any,
        ssl_broken=ssl_broken,
    )

    # 9) tracked = TopN + 持倉（避免 orphan）
    tracked = list(dict.fromkeys(top_list + [p.get("symbol") for p in positions if p.get("symbol")]))

    stocks_out = []
    for i, sym in enumerate(tracked, start=1):
        f = features_map.get(sym, {})
        im = inst_map.get(sym, {})
        name = name_map.get(sym, sym.replace(".TW", ""))

        tier = "A" if i <= 10 else "B"
        top_flag = sym in top_list
        orphan_holding = (not top_flag) and any(str(p.get("symbol")) == sym for p in positions)

        layer = classify_layer(
            regime=regime,
            momentum_lock=momentum_lock,
            vol_ratio=f.get("Vol_Ratio"),
            inst=im,
        )

        stocks_out.append({
            "symbol": sym,
            "name": name,
            "price": f.get("Price"),
            "ranking": {
                "rank": i,
                "tier": tier,
                "top_flag": bool(top_flag),
                "topn_actual": len(top_list),
            },
            "technical": {
                "ma_bias": f.get("MA_Bias", 0.0),
                "vol_ratio": f.get("Vol_Ratio"),
                "score": f.get("Score", 0.0),
                "tag": f.get("Tag", "○觀察(觀望)"),
            },
            "institutional": {
                "inst_status": im.get("inst_status", "UNAVAILABLE"),
                "inst_streak3": int(im.get("inst_streak3", 0)),
                "inst_streak5": int(im.get("inst_streak5", 0)),
                "inst_dir3": im.get("inst_dir3", "MISSING"),
                "foreign_dir": im.get("foreign_dir", "MISSING"),
                "inst_dates_5": im.get("inst_dates_5", []),
                "layer": layer,  # A+ / A / B / NONE
            },
            "orphan_holding": bool(orphan_holding),
        })

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

    payload = {
        "meta": {
            "system": SYSTEM_VERSION,
            "market": MARKET,
            "timestamp": ts_str,
            "session": session,
            "allow_insecure_ssl": bool(allow_insecure_ssl),
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "data_mode": session,
                "regime": regime,
                "regime_metrics": regime_metrics,
                "max_equity_allowed_pct": float(max_equity_allowed_pct),
                "degraded_mode": bool(degraded_mode),
                "gate_comment": gate_comment,
                "amount_twse": ma.amount_twse if ma else None,
                "amount_tpex": ma.amount_tpex if ma else None,
                "amount_total": ma.amount_total if ma else None,
                "amount_sources": {
                    "twse": ma.source_twse if ma else None,
                    "tpex": ma.source_tpex if ma else None,
                },
                "inst_ready_any": bool(inst_ready_any),
                "momentum_lock": bool(momentum_lock),
                "vix_stop_pct": float(vix_stop_pct(regime_metrics.get("VIX"))),
                "warnings_count": len(inst_warn),
                "warnings": inst_warn[:50],
            },
            "indices": indices,
        },
        "account": account,
        "stocks": stocks_out,
        "audit": {
            "layer_a_plus_enabled": True,
            "a_plus_hits": sum(1 for s in stocks_out if s.get("institutional", {}).get("layer") == "A+"),
        }
    }
    return payload


# -----------------------------
# Streamlit UI（main.py 直接跑）
# -----------------------------
def run_app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")

    st.title("Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.2 Enhanced SIM-FREE）")

    with st.sidebar:
        st.header("設定")
        session = st.selectbox("Session", ["PREMARKET", "INTRADAY", "POSTMARKET"], index=0)
        topn = st.selectbox("TopN（固定池化數量）", [10, 20, 30, 50], index=1)

        # SSL 開關：環境變數 OR 手動勾選
        allow_insecure_ssl = st.checkbox(
            "允許不安全 SSL（verify=False）",
            value=ENV_ALLOW_INSECURE_SSL,
            help="只在 TWSE/TPEx 憑證導致 requests.SSLError 時才會退到 verify=False。預設應保持關閉。",
        )

        st.subheader("持倉（手動貼 JSON array）")
        st.caption("格式範例：[{\"symbol\":\"2330.TW\",\"shares\":100,\"avg_cost\":1000}]")
        positions_text = st.text_area("positions", value="[]", height=120)

        cash_balance = st.number_input("cash_balance（NTD）", min_value=0, value=2000000, step=10000)
        total_equity = st.number_input("total_equity（NTD）", min_value=0, value=2000000, step=10000)

        run_btn = st.button("Run")

    # 解析持倉
    try:
        raw_positions = json.loads(positions_text) if positions_text.strip() else []
        positions = normalize_positions(raw_positions)
    except Exception as e:
        st.error(f"positions JSON 解析失敗：{type(e).__name__}")
        st.stop()

    account = {
        "cash_balance": int(cash_balance),
        "total_equity": int(total_equity),
        "positions": positions,
    }

    if not run_btn and "last_payload" in st.session_state:
        payload = st.session_state["last_payload"]
    elif run_btn:
        try:
            payload = build_arbiter_input(
                session=session,
                topn=int(topn),
                allow_insecure_ssl=bool(allow_insecure_ssl),
                account=account,
            )
            st.session_state["last_payload"] = payload
        except requests.exceptions.SSLError as e:
            st.error("SSLError：你目前遇到的 Missing Subject Key Identifier 類型問題。")
            st.error("若你確認要暫時恢復抓取，請在左側勾選「允許不安全 SSL（verify=False）」再 Run。")
            st.stop()
        except Exception as e:
            st.exception(e)
            st.stop()
    else:
        st.info("請在左側設定後按 Run。")
        return

    ov = payload["macro"]["overview"]
    metrics = ov.get("regime_metrics", {}) or {}

    # ====== 關鍵數據（用數字講話）
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("交易日（最後收盤）", ov.get("trade_date", "NA"))
    c2.metric("Regime", ov.get("regime", "NA"))
    c3.metric("SMR", f"{metrics.get('SMR')}", help="(Index-MA200)/MA200")
    c4.metric("Slope5", f"{metrics.get('Slope5')}", help="SMR_MA5[t]-SMR_MA5[t-1]")
    c5.metric("VIX", f"{metrics.get('VIX')}")

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("Max Equity Allowed", f"{ov.get('max_equity_allowed_pct')}%")
    c7.metric("Degraded Mode", str(ov.get("degraded_mode")))
    c8.metric("TopN Actual", str(payload["stocks"][0]["ranking"]["topn_actual"]) if payload["stocks"] else "0")
    c9.metric("A+ 命中檔數", str(payload["audit"]["a_plus_hits"]))
    c10.metric("VIX Stop(%)", f"{round(ov.get('vix_stop_pct', 0)*100, 1)}%")

    # ====== Gate 狀態
    if ov.get("degraded_mode"):
        st.error(f"Gate：DEGRADED（禁止 BUY/TRIAL）｜原因：{ov.get('gate_comment')}")
    else:
        st.success("Gate：NORMAL（資料健康通過）")

    # ====== 成交金額與來源（你之前最常壞在這裡）
    st.subheader("市場成交金額（best-effort / 可稽核）")
    st.write({
        "amount_twse": ov.get("amount_twse"),
        "amount_tpex": ov.get("amount_tpex"),
        "amount_total": ov.get("amount_total"),
        "sources": ov.get("amount_sources"),
        "allow_insecure_ssl": payload["meta"]["allow_insecure_ssl"],
    })

    # ====== 指數列表
    st.subheader("指數快照")
    idx_df = pd.DataFrame(payload["macro"]["indices"])
    if not idx_df.empty:
        st.dataframe(idx_df, use_container_width=True)
    else:
        st.warning("指數資料不足（yfinance 可能暫時無回應）。")

    # ====== TopN + 持倉監控表（含 A+）
    st.subheader(f"今日分析清單（Top{topn} + 持倉）— 含 A+ Layer")
    rows = []
    for s in payload["stocks"]:
        rows.append({
            "rank": s["ranking"]["rank"],
            "tier": s["ranking"]["tier"],
            "symbol": s["symbol"],
            "name": s["name"],
            "price": s["price"],
            "score": s["technical"]["score"],
            "ma_bias(%)": s["technical"]["ma_bias"],
            "vol_ratio": s["technical"]["vol_ratio"],
            "inst_status": s["institutional"]["inst_status"],
            "streak3": s["institutional"]["inst_streak3"],
            "streak5": s["institutional"]["inst_streak5"],
            "inst_dir3": s["institutional"]["inst_dir3"],
            "foreign_dir": s["institutional"]["foreign_dir"],
            "layer": s["institutional"]["layer"],  # A+ / A / B / NONE
            "orphan_holding": s["orphan_holding"],
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=520)
    else:
        st.warning("stocks 清單為空：通常是 Universe 建立失敗或資料源完全不可用。")

    # ====== 警告（你要看的「為什麼沒東西」會在這裡）
    st.subheader("Warnings（最多顯示 50 條）")
    warns = ov.get("warnings", []) or []
    st.write({"warnings_count": ov.get("warnings_count", 0), "warnings": warns[:50]})

    # ====== Arbiter Input JSON（給你下一步裁決引擎餵入）
    st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
    st.json(payload, expanded=False)


# 直接跑 main.py（Streamlit 會從頂層執行）
run_app()
