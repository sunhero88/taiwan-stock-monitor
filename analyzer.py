# analyzer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf

TZ_TAIPEI = timezone(timedelta(hours=8))

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

# ----------------------------
# Utils
# ----------------------------
def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def ymd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def ymd_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def is_number(x) -> bool:
    try:
        _ = float(x)
        return True
    except Exception:
        return False

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.strip() in ("", "--", "None", "nan", "NaN"):
            return default
        return float(str(x).replace(",", "").strip())
    except Exception:
        return default

def safe_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.strip() in ("", "--", "None", "nan", "NaN"):
            return default
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return default

# ----------------------------
# Official: TWSE daily snapshot (all listed)
# ----------------------------
def fetch_twse_stock_day_all(
    trade_date: datetime,
    verify_ssl: bool = True,
    timeout: int = 20,
) -> pd.DataFrame:
    """
    TWSE 全市場日行情（上市）：
    https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date=YYYYMMDD

    欄位通常含：
    證券代號/證券名稱/成交股數/成交金額/開盤價/最高價/最低價/收盤價/漲跌價差/成交筆數
    """
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": ymd_compact(trade_date)}
    r = requests.get(url, params=params, headers=USER_AGENT, timeout=timeout, verify=verify_ssl)
    r.raise_for_status()
    j = r.json()

    data = j.get("data", []) or []
    fields = j.get("fields", []) or []
    if not data or not fields:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=fields)

    # normalize column names
    col_map = {}
    for c in df.columns:
        cc = str(c).strip()
        if "證券代號" in cc:
            col_map[c] = "symbol"
        elif "證券名稱" in cc:
            col_map[c] = "name"
        elif "成交股數" in cc:
            col_map[c] = "volume"
        elif "成交金額" in cc:
            col_map[c] = "amount"
        elif "收盤價" in cc:
            col_map[c] = "close"
        elif "開盤價" in cc:
            col_map[c] = "open"
        elif "最高價" in cc:
            col_map[c] = "high"
        elif "最低價" in cc:
            col_map[c] = "low"
        elif "漲跌價差" in cc:
            col_map[c] = "chg"
    df = df.rename(columns=col_map)

    # add market + date
    df["date"] = ymd(trade_date)
    df["market"] = "TWSE"

    # type convert
    for c in ("volume", "amount"):
        if c in df.columns:
            df[c] = df[c].apply(safe_int)
    for c in ("open", "high", "low", "close", "chg"):
        if c in df.columns:
            df[c] = df[c].apply(safe_float)

    # Symbol unify: add .TW for yfinance
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip()
        df["symbol_yf"] = df["symbol"] + ".TW"

    # drop invalid rows
    df = df[df["symbol"].str.len() > 0].copy()
    return df

def find_latest_trade_date_twse(
    max_back_days: int = 10,
    verify_ssl: bool = True,
) -> Tuple[Optional[datetime], str]:
    """
    從今天往回找「第一個有有效 STOCK_DAY_ALL data」的交易日。
    有效判定：資料筆數 >= 500（上市通常上千筆；用 500 當保守門檻）
    """
    base = now_taipei().replace(hour=0, minute=0, second=0, microsecond=0)
    last_err = ""
    for back in range(0, max_back_days + 1):
        dt = base - timedelta(days=back)
        try:
            df = fetch_twse_stock_day_all(dt, verify_ssl=verify_ssl)
            if len(df) >= 500:
                return dt, "TWSE STOCK_DAY_ALL"
            else:
                last_err = f"TWSE STOCK_DAY_ALL empty/too small ({len(df)}) at {ymd(dt)}"
        except Exception as e:
            last_err = f"TWSE STOCK_DAY_ALL error at {ymd(dt)}: {type(e).__name__}: {e}"
            continue
    return None, last_err or "TWSE latest trade date not found"

# ----------------------------
# Macro: TWII via yfinance (free)
# ----------------------------
def fetch_twii_latest(
    session: str,
) -> Dict:
    """
    session: PREOPEN / INTRADAY / EOD
    使用 yfinance ^TWII。
    - 盤前：取最新可用日(通常是前一交易日)收盤
    - 盤中：取今日最新（yf 有時為延遲）
    - 盤後：取今日（或最新可用）收盤
    """
    ticker = "^TWII"
    # 取 10 天夠用
    df = yf.download(ticker, period="10d", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        return {"symbol": "^TWII", "date": None, "close": None, "chg": None, "chg_pct": None, "source": "yfinance", "error": "TWII empty"}

    df = df.dropna()
    df.index = pd.to_datetime(df.index)
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None

    close = float(latest["Close"])
    prev_close = float(prev["Close"]) if prev is not None else None
    chg = (close - prev_close) if prev_close is not None else None
    chg_pct = ((chg / prev_close) * 100.0) if (chg is not None and prev_close not in (None, 0)) else None

    return {
        "symbol": "^TWII",
        "date": latest.name.strftime("%Y-%m-%d"),
        "close": round(close, 2),
        "chg": None if chg is None else round(chg, 2),
        "chg_pct": None if chg_pct is None else round(chg_pct, 4),
        "source": "yfinance",
        "error": None,
    }

# ----------------------------
# Institutional: TWSE T86 (free, official)
# ----------------------------
def fetch_twse_t86(
    trade_date: datetime,
    verify_ssl: bool = True,
    timeout: int = 20,
) -> pd.DataFrame:
    """
    TWSE 三大法人買賣超（個股明細）
    https://www.twse.com.tw/fund/T86?response=json&date=YYYYMMDD&selectType=ALL
    """
    url = "https://www.twse.com.tw/fund/T86"
    params = {"response": "json", "date": ymd_compact(trade_date), "selectType": "ALL"}
    r = requests.get(url, params=params, headers=USER_AGENT, timeout=timeout, verify=verify_ssl)
    r.raise_for_status()
    j = r.json()
    data = j.get("data", []) or []
    fields = j.get("fields", []) or []
    if not data or not fields:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=fields)

    # 常見欄位：證券代號 / 證券名稱 / 外陸資買賣超股數(不含外資自營商) / 投信買賣超股數 / 自營商買賣超股數 / 三大法人買賣超股數
    # 我們只取「三大法人買賣超股數」
    col_map = {}
    for c in df.columns:
        cc = str(c).strip()
        if "證券代號" in cc:
            col_map[c] = "symbol"
        elif "證券名稱" in cc:
            col_map[c] = "name"
        elif "三大法人買賣超股數" in cc:
            col_map[c] = "inst_net_shares"

    df = df.rename(columns=col_map)
    if "symbol" not in df.columns or "inst_net_shares" not in df.columns:
        return pd.DataFrame()

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["inst_net_shares"] = df["inst_net_shares"].apply(safe_int)
    df["date"] = ymd(trade_date)
    return df[["symbol", "inst_net_shares", "date"]].copy()

def fetch_inst_streak_3d(
    trade_date: datetime,
    verify_ssl: bool = True,
    max_back_days: int = 10,
) -> Tuple[Dict[str, int], List[str], Optional[str]]:
    """
    取最近 3 個交易日的 T86，計算「連續買超天數（正）/連續賣超天數（負）」。
    回傳：streak_map, used_dates, error
    """
    # 找最近 3 個可用日
    used = []
    dfs = []
    last_err = None

    base = trade_date.replace(hour=0, minute=0, second=0, microsecond=0)
    for back in range(0, max_back_days + 1):
        dt = base - timedelta(days=back)
        try:
            df = fetch_twse_t86(dt, verify_ssl=verify_ssl)
            if not df.empty:
                dfs.append(df)
                used.append(ymd(dt))
                if len(dfs) >= 3:
                    break
        except Exception as e:
            last_err = f"T86 error {ymd(dt)}: {type(e).__name__}: {e}"
            continue

    if not dfs:
        return {}, [], last_err or "T86 unavailable"

    # 合併成 dict: {date: {symbol: net}}
    by_date = []
    for ddf in dfs:
        d = ddf["date"].iloc[0]
        m = dict(zip(ddf["symbol"], ddf["inst_net_shares"]))
        by_date.append((d, m))

    # streak: 以 used 的時間順序（最新在前）計算
    streak = {}
    # 先收集符號全集
    symbols = set()
    for _, m in by_date:
        symbols |= set(m.keys())

    # for each symbol: check latest -> older
    for sym in symbols:
        s = 0
        sign = 0
        for d, m in by_date:
            v = m.get(sym, 0)
            cur_sign = 1 if v > 0 else (-1 if v < 0 else 0)
            if cur_sign == 0:
                break
            if sign == 0:
                sign = cur_sign
                s = 1
            else:
                if cur_sign == sign:
                    s += 1
                else:
                    break
        streak[sym] = s * sign if sign != 0 else 0

    return streak, used, None

# ----------------------------
# Top20 builder (SIM-FREE)
# ----------------------------
@dataclass
class TopBuildResult:
    top_df: pd.DataFrame
    top_actual: int
    snapshot_date: str
    snapshot_source: str
    warnings: List[str]

def build_topn_sim_free(
    topn: int = 20,
    liquidity_pool: int = 200,
    verify_ssl: bool = True,
    max_back_days: int = 10,
) -> TopBuildResult:
    """
    1) 用 TWSE 官方日行情 (STOCK_DAY_ALL) 找最新交易日 + 全市場資料
    2) 先用成交金額 amount 排出流動性前 liquidity_pool 名（避免全市場逐檔抓歷史）
    3) 對這批 tickers 用 yfinance 批次抓 60D 日線，計算：
        - ret20_pct
        - vol_ratio (當日成交量 / 20D均量)
        - ma_bias_pct (close / MA20 - 1)
      再用簡化分數選 TopN
    """
    warnings: List[str] = []

    latest_dt, src = find_latest_trade_date_twse(max_back_days=max_back_days, verify_ssl=verify_ssl)
    if latest_dt is None:
        return TopBuildResult(pd.DataFrame(), 0, None, "TWSE", [f"找不到最新交易日：{src}"])

    snap = fetch_twse_stock_day_all(latest_dt, verify_ssl=verify_ssl)
    if snap.empty:
        return TopBuildResult(pd.DataFrame(), 0, ymd(latest_dt), src, [f"TWSE 日行情為空：{ymd(latest_dt)}"])

    # liquidity pool
    if "amount" not in snap.columns:
        snap["amount"] = 0
    pool = snap.sort_values("amount", ascending=False).head(max(liquidity_pool, topn)).copy()
    tickers = pool["symbol_yf"].dropna().astype(str).tolist()

    # yfinance batch
    # 60d 足夠算 MA20/Vol20/Ret20
    hist = yf.download(
        tickers=tickers,
        period="90d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )

    if hist is None or hist.empty:
        return TopBuildResult(pd.DataFrame(), 0, ymd(latest_dt), "TWSE+YF", ["yfinance 回傳空資料（可能被限流或網路問題）"])

    # yfinance output shape: MultiIndex columns if multiple tickers
    rows = []
    for t in tickers:
        try:
            if isinstance(hist.columns, pd.MultiIndex):
                if t not in hist.columns.get_level_values(0):
                    continue
                h = hist[t].dropna()
            else:
                # single ticker fallback
                h = hist.dropna()

            if h is None or h.empty or len(h) < 25:
                continue

            h = h.dropna()
            close = float(h["Close"].iloc[-1])
            vol = float(h["Volume"].iloc[-1]) if "Volume" in h.columns else None

            ma20 = float(h["Close"].tail(20).mean())
            ma_bias_pct = (close / ma20 - 1.0) * 100.0 if ma20 > 0 else None

            prev20 = float(h["Close"].iloc[-21]) if len(h) >= 21 else None
            ret20_pct = ((close / prev20) - 1.0) * 100.0 if (prev20 and prev20 > 0) else None

            vol20 = float(h["Volume"].tail(20).mean()) if ("Volume" in h.columns and vol is not None) else None
            vol_ratio = (vol / vol20) if (vol20 and vol20 > 0) else None

            # merge name/amount from official snapshot
            sym = t.replace(".TW", "")
            ss = pool[pool["symbol"] == sym]
            name = ss["name"].iloc[0] if (not ss.empty and "name" in ss.columns) else None
            amount = int(ss["amount"].iloc[0]) if (not ss.empty and "amount" in ss.columns) else 0

            # score (SIM-FREE): 動能 + 放量 + 趨勢偏離 + 流動性
            # 用可讀的數字權重（避免過度神祕化）
            # ret20 佔 45%，vol_ratio 佔 30%，ma_bias 佔 15%，log(amount) 佔 10%
            score = 0.0
            if ret20_pct is not None:
                score += 0.45 * ret20_pct
            if vol_ratio is not None:
                score += 0.30 * (vol_ratio * 10.0)  # 放大到同量級
            if ma_bias_pct is not None:
                score += 0.15 * ma_bias_pct
            if amount and amount > 0:
                score += 0.10 * math.log10(amount)

            rows.append({
                "symbol": f"{sym}.TW",
                "name": name,
                "date": ymd(latest_dt),
                "close": round(close, 2),
                "volume": int(vol) if vol is not None else None,
                "amount": amount,
                "ret20_pct": None if ret20_pct is None else round(ret20_pct, 4),
                "vol_ratio": None if vol_ratio is None else round(vol_ratio, 4),
                "ma_bias_pct": None if ma_bias_pct is None else round(ma_bias_pct, 4),
                "score": round(score, 4),
            })
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return TopBuildResult(pd.DataFrame(), 0, ymd(latest_dt), "TWSE+YF", ["TopN 建立失敗：候選歷史資料不足（yfinance 可能限流）"])

    df = df.sort_values("score", ascending=False).head(topn).reset_index(drop=True)
    df["rank"] = df.index + 1

    return TopBuildResult(df, len(df), ymd(latest_dt), "TWSE(STOCK_DAY_ALL)+yfinance(90d)", warnings)

# ----------------------------
# Market amount (TWSE official from snapshot sum)
# ----------------------------
def compute_market_amount_from_twse_snapshot(snap_df: pd.DataFrame) -> int:
    if snap_df is None or snap_df.empty:
        return 0
    if "amount" not in snap_df.columns:
        return 0
    return int(snap_df["amount"].fillna(0).astype(int).sum())

# ----------------------------
# Arbiter Input builder
# ----------------------------
def build_arbiter_input_sim_free(
    session: str,
    topn: int,
    positions: List[Dict],
    verify_ssl: bool,
    max_back_days: int,
    liquidity_pool: int = 200,
) -> Dict:
    """
    產生你要餵給 Arbiter 的 JSON（SIM-FREE）
    """
    ts = now_taipei().strftime("%Y-%m-%d %H:%M")
    warnings: List[str] = []

    # Macro: TWII
    twii = fetch_twii_latest(session=session)
    if twii.get("error"):
        warnings.append(f"TWII 取得失敗：{twii.get('error')}")

    # TopN
    top_res = build_topn_sim_free(
        topn=topn,
        liquidity_pool=liquidity_pool,
        verify_ssl=verify_ssl,
        max_back_days=max_back_days,
    )
    if top_res.warnings:
        warnings.extend(top_res.warnings)

    # Latest official trade date used by TopN
    trade_date = top_res.snapshot_date

    # Institutional streak (3d)
    inst_streak_map, inst_dates_3d, inst_err = ({} , [], None)
    if trade_date:
        try:
            dt = datetime.strptime(trade_date, "%Y-%m-%d").replace(tzinfo=TZ_TAIPEI)
            inst_streak_map, inst_dates_3d, inst_err = fetch_inst_streak_3d(
                dt, verify_ssl=verify_ssl, max_back_days=max_back_days
            )
        except Exception as e:
            inst_err = f"inst streak error: {type(e).__name__}: {e}"
    if inst_err:
        warnings.append(f"法人資料不可用：{inst_err}")

    # merge positions into analysis list
    top_df = top_res.top_df.copy() if top_res.top_df is not None else pd.DataFrame()
    top_symbols = set(top_df["symbol"].astype(str).tolist()) if (not top_df.empty and "symbol" in top_df.columns) else set()

    # positions list: [{"symbol":"2330.TW","qty":100,"avg_cost":1000}]
    pos_symbols = []
    for p in positions or []:
        s = str(p.get("symbol", "")).strip()
        if s:
            if not s.endswith(".TW") and s.isdigit():
                s = f"{s}.TW"
            pos_symbols.append(s)

    extra = [s for s in pos_symbols if s not in top_symbols]
    analysis_symbols = (list(top_symbols) + extra)

    # Data freshness audit
    # Rule: trade_date must equal TWII latest date (or within 1 day allowed due to timezone mismatch)
    degraded_mode = False
    stale_reason = None
    if trade_date and twii.get("date"):
        try:
            d1 = datetime.strptime(trade_date, "%Y-%m-%d")
            d2 = datetime.strptime(twii["date"], "%Y-%m-%d")
            gap = abs((d2 - d1).days)
            if gap >= 2:
                degraded_mode = True
                stale_reason = f"DATA_STALE_{gap}D (TopN={trade_date} vs TWII={twii['date']})"
        except Exception:
            pass

    # attach inst streak into top list
    if not top_df.empty:
        top_df["inst_streak"] = top_df["symbol"].apply(lambda s: int(inst_streak_map.get(str(s).replace(".TW",""), 0)))

    # Compose JSON
    arb = {
        "meta": {
            "system": "Predator V15.7 (SIM-FREE / Top20+Positions)",
            "timestamp": ts,
            "session": session,
            "market": "tw-share",
            "topn_target": topn,
            "topn_actual": int(top_res.top_actual),
            "snapshot_date": trade_date,
            "snapshot_source": top_res.snapshot_source,
            "verify_ssl": bool(verify_ssl),
        },
        "macro": {
            "twii": twii,  # {date, close, chg, chg_pct, source}
            "warnings": warnings,
            "stale_reason": stale_reason,
            "degraded_mode": bool(degraded_mode),
            "inst_dates_3d": inst_dates_3d,
            "inst_status": "READY" if not inst_err else "UNAVAILABLE",
        },
        "top_watchlist": top_df.to_dict(orient="records") if not top_df.empty else [],
        "positions": positions or [],
        "analysis_symbols": analysis_symbols,
    }
    return arb
