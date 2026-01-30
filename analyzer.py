# analyzer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import time as _time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf

TZ_TAIPEI = timezone(timedelta(hours=8))

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

# --- Utilities ----------------------------------------------------------------

def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def ymd(dt: date) -> str:
    return dt.strftime("%Y-%m-%d")

def yyyymmdd(dt: date) -> str:
    return dt.strftime("%Y%m%d")

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float, np.floating)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() == "nan":
            return default
        return float(s)
    except Exception:
        return default

def safe_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, (int, np.integer)):
            return int(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() == "nan":
            return default
        return int(float(s))
    except Exception:
        return default

# --- Data Models ---------------------------------------------------------------

@dataclass
class MacroIndex:
    symbol: str
    name: str
    asof_date: str
    close: float
    change: float
    chg_pct: float
    source: str

@dataclass
class MarketDayAll:
    trade_date: str
    df: pd.DataFrame
    source: str

# --- TWSE OpenAPI: STOCK_DAY_ALL (全市場日行情) --------------------------------
# 官方 OpenAPI：全體上市股票當日行情 (含成交金額/成交股數/收盤等)
# 這是免費階段做「真正全市場排名」最穩定的來源。

def fetch_twse_stock_day_all(trade_dt: date, timeout: int = 20) -> MarketDayAll:
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    r = requests.get(url, headers=USER_AGENT, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        raise RuntimeError("TWSE OpenAPI STOCK_DAY_ALL 回傳空資料")

    df = pd.DataFrame(data)

    # 欄位常見：Code/Name/TradeVolume/TradeValue/Open/High/Low/Close/Change/Transaction
    # 做最小兼容映射
    colmap = {
        "Code": "symbol",
        "Name": "name",
        "TradeVolume": "volume",
        "TradeValue": "turnover",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Change": "change",
    }
    for k, v in colmap.items():
        if k in df.columns:
            df[v] = df[k]

    if "symbol" not in df.columns or "close" not in df.columns:
        raise RuntimeError(f"TWSE STOCK_DAY_ALL 欄位異動，缺 symbol/close，現有欄位={list(df.columns)}")

    # 標準化
    df["symbol"] = df["symbol"].astype(str).str.strip() + ".TW"
    if "name" not in df.columns:
        df["name"] = ""
    df["name"] = df["name"].astype(str).str.strip()

    for c in ["close", "open", "high", "low", "change"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: safe_float(x, default=np.nan))

    for c in ["volume", "turnover"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: safe_int(x, default=0))

    # STOCK_DAY_ALL 是「最新交易日」資料，不一定等於你傳入的 trade_dt
    # 我們用 yfinance 的 ^TWII 最新交易日去對齊，並在上層做稽核。
    return MarketDayAll(
        trade_date=ymd(trade_dt),
        df=df,
        source="TWSE OpenAPI STOCK_DAY_ALL",
    )

def find_latest_trade_date(max_lookback_days: int = 10) -> date:
    """
    找最新可用交易日（免費階段用「可取到資料」當作交易日判定）。
    """
    today = now_taipei().date()
    for i in range(max_lookback_days + 1):
        d = today - timedelta(days=i)
        # 週末直接略過可加速
        if d.weekday() >= 5:
            continue
        try:
            _ = fetch_twse_stock_day_all(d)
            return d
        except Exception:
            continue
    # 最後保底：今天
    return today

# --- Index / Global Summary via yfinance --------------------------------------

def fetch_index_yf(symbol: str, name: str, period: str = "7d") -> MacroIndex:
    """
    用 yfinance 拉指數，抓最近一筆 close + 前一筆 close -> change/chg_pct
    """
    tk = yf.Ticker(symbol)
    hist = tk.history(period=period, interval="1d", auto_adjust=False)
    if hist is None or hist.empty or "Close" not in hist.columns:
        raise RuntimeError(f"yfinance 無法取得 {symbol} 歷史資料")

    hist = hist.dropna(subset=["Close"])
    if len(hist) < 1:
        raise RuntimeError(f"yfinance {symbol} 無有效 Close")

    last = hist.iloc[-1]
    last_close = float(last["Close"])
    last_date = hist.index[-1].date()

    if len(hist) >= 2:
        prev_close = float(hist.iloc[-2]["Close"])
    else:
        prev_close = last_close

    chg = last_close - prev_close
    chg_pct = (chg / prev_close * 100.0) if prev_close != 0 else 0.0

    return MacroIndex(
        symbol=symbol,
        name=name,
        asof_date=ymd(last_date),
        close=round(last_close, 4),
        change=round(chg, 4),
        chg_pct=round(chg_pct, 4),
        source="yfinance",
    )

def fetch_global_summary() -> pd.DataFrame:
    rows = []
    # US
    for sym, nm in [
        ("^GSPC", "S&P500"),
        ("^IXIC", "NASDAQ"),
        ("^DJI", "DOW"),
        ("^SOX", "SOX"),
        ("^VIX", "VIX"),
    ]:
        try:
            x = fetch_index_yf(sym, nm, period="10d")
            rows.append({
                "Market": "US",
                "Name": x.name,
                "Symbol": x.symbol,
                "Date": x.asof_date,
                "Close": x.close,
                "Chg%": x.chg_pct,
                "Source": x.source,
            })
        except Exception as e:
            rows.append({"Market": "US", "Name": nm, "Symbol": sym, "Date": None, "Close": None, "Chg%": None, "Source": f"ERR:{e}"})

    # ASIA (reference)
    for sym, nm in [
        ("^N225", "Nikkei_225"),
        ("JPY=X", "USD/JPY"),
        ("TWD=X", "USD/TWD"),
        ("^TWII", "TWSE_TAIEX"),
    ]:
        try:
            x = fetch_index_yf(sym, nm, period="10d")
            rows.append({
                "Market": "ASIA",
                "Name": x.name,
                "Symbol": x.symbol,
                "Date": x.asof_date,
                "Close": x.close,
                "Chg%": x.chg_pct,
                "Source": x.source,
            })
        except Exception as e:
            rows.append({"Market": "ASIA", "Name": nm, "Symbol": sym, "Date": None, "Close": None, "Chg%": None, "Source": f"ERR:{e}"})

    return pd.DataFrame(rows)

# --- Institution (TWSE T86) ---------------------------------------------------

def fetch_twse_institutional_all(trade_dt: date, timeout: int = 20) -> pd.DataFrame:
    """
    TWSE 三大法人買賣超 (T86) - 官方 JSON
    """
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={yyyymmdd(trade_dt)}&selectType=ALL&response=json"
    r = requests.get(url, headers=USER_AGENT, timeout=timeout)
    r.raise_for_status()
    js = r.json()

    data = js.get("data", [])
    fields = js.get("fields", [])
    if not data or not fields:
        raise RuntimeError("TWSE T86 回傳空資料")

    df = pd.DataFrame(data, columns=fields)

    # 常見欄位：證券代號、證券名稱、外資及陸資買賣超股數、投信買賣超股數、自營商買賣超股數、三大法人買賣超股數
    # 我們只取「三大法人買賣超股數」
    code_col = None
    name_col = None
    net_col = None
    for c in df.columns:
        if "證券代號" in c:
            code_col = c
        if "證券名稱" in c:
            name_col = c
        if "三大法人" in c and "買賣超" in c and "股數" in c:
            net_col = c

    if code_col is None or net_col is None:
        raise RuntimeError(f"TWSE T86 欄位異動，現有欄位={list(df.columns)}")

    out = pd.DataFrame({
        "symbol": df[code_col].astype(str).str.strip() + ".TW",
        "name": df[name_col].astype(str).str.strip() if name_col else "",
        "inst_net": df[net_col].apply(safe_int),
    })
    return out

# --- Ranking / Scoring --------------------------------------------------------

def _history_for_symbols(symbols: List[str], period: str = "3mo") -> Dict[str, pd.DataFrame]:
    """
    用 yfinance 批次抓資料（Streamlit Cloud 需節制）
    """
    joined = " ".join(symbols)
    data = yf.download(joined, period=period, interval="1d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
    out = {}
    if isinstance(data.columns, pd.MultiIndex):
        for s in symbols:
            if s in data.columns.levels[0]:
                df = data[s].copy()
                df = df.dropna(subset=["Close"])
                out[s] = df
    else:
        # 只有 1 檔
        df = data.copy()
        df = df.dropna(subset=["Close"])
        out[symbols[0]] = df
    return out

def build_topn_from_market_dayall(dayall: MarketDayAll, topn: int = 20, preselect_by_turnover: int = 250) -> pd.DataFrame:
    """
    先用 STOCK_DAY_ALL 的成交金額做預篩（加速），再用 yfinance 算 20D 指標。
    輸出欄位：
      symbol, name, date, close, ret20_pct, vol_ratio, ma_bias_pct, volume, score, rank, tag
    """
    df0 = dayall.df.copy()

    # 預篩：turnover 最大的前 N 檔（避免全市場逐檔 yfinance）
    if "turnover" in df0.columns:
        df0 = df0.sort_values("turnover", ascending=False).head(preselect_by_turnover)
    else:
        # 沒 turnover 就用 volume
        df0 = df0.sort_values("volume", ascending=False).head(preselect_by_turnover)

    symbols = df0["symbol"].astype(str).tolist()
    hist_map = _history_for_symbols(symbols, period="6mo")

    rows = []
    for sym in symbols:
        h = hist_map.get(sym)
        if h is None or h.empty:
            continue

        closes = h["Close"].dropna()
        vols = h["Volume"].dropna() if "Volume" in h.columns else None
        if len(closes) < 25:
            continue

        last_close = float(closes.iloc[-1])
        last_date = h.index[-1].date()

        close_20 = float(closes.iloc[-21]) if len(closes) >= 21 else float(closes.iloc[0])
        ret20 = (last_close / close_20 - 1.0) * 100.0 if close_20 != 0 else 0.0

        ma20 = float(closes.tail(20).mean())
        ma_bias = (last_close / ma20 - 1.0) * 100.0 if ma20 != 0 else 0.0

        vol_ratio = None
        last_vol = None
        if vols is not None and len(vols) >= 21:
            last_vol = float(vols.iloc[-1])
            vma20 = float(vols.tail(20).mean())
            vol_ratio = (last_vol / vma20) if vma20 != 0 else None

        name = df0.loc[df0["symbol"] == sym, "name"].iloc[0] if "name" in df0.columns and (df0["symbol"] == sym).any() else ""

        rows.append({
            "symbol": sym,
            "name": name,
            "date": ymd(last_date),
            "close": round(last_close, 4),
            "ret20_pct": round(ret20, 4),
            "vol_ratio": None if vol_ratio is None else round(float(vol_ratio), 4),
            "ma_bias_pct": round(ma_bias, 4),
            "volume": None if last_vol is None else int(last_vol),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # score：用可解釋、可稽核的線性組合（免費模擬版）
    # score = 0.45*ret20 + 0.35*ma_bias + 0.20*(vol_ratio-1)*100
    out["vol_boost"] = out["vol_ratio"].fillna(1.0).apply(lambda x: (x - 1.0) * 100.0)
    out["score"] = 0.45 * out["ret20_pct"] + 0.35 * out["ma_bias_pct"] + 0.20 * out["vol_boost"]

    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    out["rank"] = np.arange(1, len(out) + 1)

    # tag：簡單規則（可在 arbiter 再做裁決）
    def tag_row(r):
        if r["ret20_pct"] >= 15 and (r["vol_ratio"] is not None and r["vol_ratio"] >= 1.5):
            return "🔥Volume_Alert"
        if r["ret20_pct"] >= 10 and r["ma_bias_pct"] >= 2:
            return "🟢Strong_Relative"
        return "○Neutral"

    out["tag"] = out.apply(tag_row, axis=1)

    return out.head(topn).copy()

def merge_with_positions(top_df: pd.DataFrame, positions: List[str]) -> pd.DataFrame:
    """
    positions: ["2330.TW","2317.TW",...]
    TopN + positions 去重合併；若 position 不在 top_df，補抓 yfinance 當日指標。
    """
    positions = [p.strip() for p in positions if p and str(p).strip() != ""]
    positions = [p if p.endswith(".TW") else p + ".TW" for p in positions]
    positions = list(dict.fromkeys(positions))  # preserve order

    if top_df is None or top_df.empty:
        base = pd.DataFrame(columns=["symbol","name","date","close","ret20_pct","vol_ratio","ma_bias_pct","volume","score","rank","tag"])
    else:
        base = top_df.copy()

    existing = set(base["symbol"].astype(str).tolist())
    need = [p for p in positions if p not in existing]
    if not need:
        return base

    hist_map = _history_for_symbols(need, period="6mo")
    extra = []
    for sym in need:
        h = hist_map.get(sym)
        if h is None or h.empty or "Close" not in h.columns:
            continue
        closes = h["Close"].dropna()
        if len(closes) < 25:
            continue
        last_close = float(closes.iloc[-1])
        last_date = h.index[-1].date()

        close_20 = float(closes.iloc[-21]) if len(closes) >= 21 else float(closes.iloc[0])
        ret20 = (last_close / close_20 - 1.0) * 100.0 if close_20 != 0 else 0.0

        ma20 = float(closes.tail(20).mean())
        ma_bias = (last_close / ma20 - 1.0) * 100.0 if ma20 != 0 else 0.0

        vols = h["Volume"].dropna() if "Volume" in h.columns else None
        vol_ratio = None
        last_vol = None
        if vols is not None and len(vols) >= 21:
            last_vol = float(vols.iloc[-1])
            vma20 = float(vols.tail(20).mean())
            vol_ratio = (last_vol / vma20) if vma20 != 0 else None

        # yfinance 名稱（有時是英文）；台股中文名我們優先用 TWSE OpenAPI 的 name（top_df 已帶）
        nm = ""
        try:
            info = yf.Ticker(sym).fast_info
            _ = info  # no-op
        except Exception:
            pass

        vol_boost = ((vol_ratio or 1.0) - 1.0) * 100.0
        score = 0.45 * ret20 + 0.35 * ma_bias + 0.20 * vol_boost

        extra.append({
            "symbol": sym,
            "name": nm,
            "date": ymd(last_date),
            "close": round(last_close, 4),
            "ret20_pct": round(ret20, 4),
            "vol_ratio": None if vol_ratio is None else round(float(vol_ratio), 4),
            "ma_bias_pct": round(ma_bias, 4),
            "volume": None if last_vol is None else int(last_vol),
            "vol_boost": round(vol_boost, 4),
            "score": round(score, 4),
            "rank": None,
            "tag": "POSITION",
        })

    if extra:
        base = pd.concat([base, pd.DataFrame(extra)], ignore_index=True)

    return base
