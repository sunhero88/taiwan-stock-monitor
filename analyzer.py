# analyzer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import requests
import yfinance as yf

TZ_TAIPEI = timezone(timedelta(hours=8))

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

# ======== 指數與全球摘要（yfinance）========
GLOBAL_TICKERS = [
    ("US", "S&P500", "^GSPC"),
    ("US", "NASDAQ", "^IXIC"),
    ("US", "DOW", "^DJI"),
    ("US", "SOX", "^SOX"),
    ("US", "VIX", "^VIX"),
    ("ASIA", "Nikkei_225", "^N225"),
    ("ASIA", "USD_JPY", "JPY=X"),
    ("ASIA", "USD_TWD", "TWD=X"),
]

TW_INDEX_TICKER = "^TWII"  # 台股加權指數（yfinance）


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _ymd(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _to_int(x) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, (int,)):
            return int(x)
        s = str(x).replace(",", "").strip()
        if s == "" or s == "--":
            return 0
        # 有些欄位可能含括號或空白
        s = s.replace("(", "").replace(")", "")
        return int(float(s))
    except Exception:
        return 0


def _to_float(x) -> float:
    try:
        if x is None:
            return float("nan")
        s = str(x).replace(",", "").strip()
        if s == "" or s == "--":
            return float("nan")
        return float(s)
    except Exception:
        return float("nan")


# ==========================================================
# 1) 台股大盤（自動輸入）：依 session 取值（PREOPEN/INTRADAY/EOD）
# ==========================================================

def fetch_tw_index_auto(session: str) -> Dict[str, Any]:
    """
    回傳：
    - index_level
    - index_change
    - index_chg_pct
    - index_date（最新可用交易日）
    - source
    """
    session = (session or "PREOPEN").upper()

    try:
        t = yf.Ticker(TW_INDEX_TICKER)

        if session == "INTRADAY":
            # 盤中：抓 1m / 1d 最後一筆（yfinance 可能延遲，仍以 best-effort）
            df = t.history(period="1d", interval="1m")
            if df is None or df.empty:
                # fallback：用最近 5d 的日線最後一筆
                df2 = t.history(period="7d", interval="1d")
                if df2 is None or df2.empty:
                    raise RuntimeError("TW index yfinance history empty")
                last = df2.iloc[-1]
                prev = df2.iloc[-2] if len(df2) >= 2 else last
                level = float(last["Close"])
                change = float(last["Close"] - prev["Close"])
                chg_pct = (change / float(prev["Close"])) * 100 if float(prev["Close"]) != 0 else 0.0
                idx_date = df2.index[-1].date().isoformat()
                return {
                    "index_level": round(level, 4),
                    "index_change": round(change, 4),
                    "index_chg_pct": round(chg_pct, 4),
                    "index_date": idx_date,
                    "source": "yfinance(1d fallback)",
                }

            # intraday 最後一筆 close
            last_price = float(df["Close"].iloc[-1])
            # 用昨日收盤做比較
            df_daily = t.history(period="7d", interval="1d")
            if df_daily is None or df_daily.empty or len(df_daily) < 2:
                prev_close = last_price
                idx_date = _now_taipei().date().isoformat()
            else:
                prev_close = float(df_daily["Close"].iloc[-2])
                idx_date = df_daily.index[-1].date().isoformat()

            change = last_price - prev_close
            chg_pct = (change / prev_close) * 100 if prev_close != 0 else 0.0
            return {
                "index_level": round(last_price, 4),
                "index_change": round(change, 4),
                "index_chg_pct": round(chg_pct, 4),
                "index_date": idx_date,
                "source": "yfinance(1m)",
            }

        # PREOPEN / EOD：用最近日線（最後一個交易日收盤）
        df = t.history(period="10d", interval="1d")
        if df is None or df.empty or len(df) < 2:
            raise RuntimeError("TW index yfinance daily history too short")
        last = df.iloc[-1]
        prev = df.iloc[-2]
        level = float(last["Close"])
        change = float(last["Close"] - prev["Close"])
        chg_pct = (change / float(prev["Close"])) * 100 if float(prev["Close"]) != 0 else 0.0
        idx_date = df.index[-1].date().isoformat()
        return {
            "index_level": round(level, 4),
            "index_change": round(change, 4),
            "index_chg_pct": round(chg_pct, 4),
            "index_date": idx_date,
            "source": "yfinance(1d)",
        }
    except Exception as e:
        return {
            "index_level": None,
            "index_change": None,
            "index_chg_pct": None,
            "index_date": None,
            "source": f"ERROR: {type(e).__name__}: {e}",
        }


def fetch_global_summary() -> pd.DataFrame:
    rows = []
    for market, name, symbol in GLOBAL_TICKERS:
        try:
            t = yf.Ticker(symbol)
            df = t.history(period="10d", interval="1d")
            if df is None or df.empty or len(df) < 2:
                raise RuntimeError("history empty/short")
            last = df.iloc[-1]
            prev = df.iloc[-2]
            close = float(last["Close"])
            chg = float(last["Close"] - prev["Close"])
            chg_pct = (chg / float(prev["Close"])) * 100 if float(prev["Close"]) != 0 else 0.0
            d = df.index[-1].date().isoformat()
            rows.append({
                "Market": market,
                "Name": name,
                "Symbol": symbol,
                "Date": d,
                "Close": round(close, 4),
                "Chg%": round(chg_pct, 4),
                "Source": "yfinance",
            })
        except Exception as e:
            rows.append({
                "Market": market,
                "Name": name,
                "Symbol": symbol,
                "Date": None,
                "Close": None,
                "Chg%": None,
                "Source": f"ERROR: {type(e).__name__}: {e}",
            })
    return pd.DataFrame(rows)


# ==========================================================
# 2) 全市場日行情（上市 + 上櫃）：用 TWSE/TPEx 網站端點（避開 openapi.twse.com.tw）
#    - 先找「最新可用交易日」
#    - 再抓當日全市場，做 TopN
# ==========================================================

def _req_get(url: str, *, verify_ssl: bool, timeout: int = 20) -> requests.Response:
    return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=verify_ssl)


def fetch_twse_all_daily_quotes(trade_date: date, *, verify_ssl: bool, allow_ssl_bypass: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    TWSE 上市：抓 rwd/afterTrading/STOCK_DAY_ALL (JSON)
    常見 URL 形式（較穩）：
      https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?date=YYYYMMDD&response=json
    回傳 DataFrame 欄位至少含：symbol,name,close,volume,open,high,low
    """
    ymd = _yyyymmdd(trade_date)
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?date={ymd}&response=json"
    meta = {"url": url, "verify_ssl": verify_ssl, "ssl_bypassed": False, "error": None}

    def _parse(j: Dict[str, Any]) -> pd.DataFrame:
        data = j.get("data", [])
        fields = j.get("fields", [])
        if not isinstance(data, list) or len(data) == 0:
            return pd.DataFrame()

        # 欄位位置（依官方表格常見欄位名）
        # 代號 / 名稱 / 成交股數 / 成交金額 / 開盤 / 最高 / 最低 / 收盤 ...
        def idx_of(key: str) -> Optional[int]:
            for i, f in enumerate(fields):
                if key in str(f):
                    return i
            return None

        i_code = idx_of("證券代號") or idx_of("代號") or 0
        i_name = idx_of("證券名稱") or idx_of("名稱") or 1
        i_vol = idx_of("成交股數")  # shares
        i_open = idx_of("開盤價")
        i_high = idx_of("最高價")
        i_low = idx_of("最低價")
        i_close = idx_of("收盤價")

        rows = []
        for r in data:
            if not isinstance(r, list):
                continue
            code = str(r[i_code]).strip()
            name = str(r[i_name]).strip() if i_name is not None else ""
            if code == "" or code.lower() == "nan":
                continue
            # 過濾掉權證/特別標的可視需要再加規則；此處先保留
            rows.append({
                "symbol": f"{code}.TW",
                "name": name,
                "volume": _to_int(r[i_vol]) if i_vol is not None else 0,
                "open": _to_float(r[i_open]) if i_open is not None else float("nan"),
                "high": _to_float(r[i_high]) if i_high is not None else float("nan"),
                "low": _to_float(r[i_low]) if i_low is not None else float("nan"),
                "close": _to_float(r[i_close]) if i_close is not None else float("nan"),
            })
        df = pd.DataFrame(rows)
        return df

    try:
        r = _req_get(url, verify_ssl=verify_ssl)
        r.raise_for_status()
        j = r.json()
        df = _parse(j)
        return df, meta
    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        if verify_ssl and allow_ssl_bypass:
            try:
                r = _req_get(url, verify_ssl=False)
                r.raise_for_status()
                j = r.json()
                df = _parse(j)
                meta["verify_ssl"] = False
                meta["ssl_bypassed"] = True
                meta["error"] = None
                return df, meta
            except Exception as e2:
                meta["error"] = f"{type(e2).__name__}: {e2}"
        return pd.DataFrame(), meta


def fetch_tpex_all_daily_quotes(trade_date: date, *, verify_ssl: bool, allow_ssl_bypass: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    TPEx 上櫃：抓 daily_close_quotes JSON
    常見端點：
      https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=json&d=YYY/MM/DD
    """
    d_str = trade_date.strftime("%Y/%m/%d")
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=json&d={d_str}"
    meta = {"url": url, "verify_ssl": verify_ssl, "ssl_bypassed": False, "error": None}

    def _parse(j: Dict[str, Any]) -> pd.DataFrame:
        data = j.get("aaData") or j.get("data") or []
        if not isinstance(data, list) or len(data) == 0:
            return pd.DataFrame()

        # 常見欄位順序（可能會變動）：代號, 名稱, 收盤, 漲跌, 開盤, 最高, 最低, 成交股數...
        rows = []
        for r in data:
            if not isinstance(r, list) or len(r) < 8:
                continue
            code = str(r[0]).strip()
            name = str(r[1]).strip()
            close = _to_float(r[2])
            # 成交股數通常在某個位置；這裡採用「嘗試找最大可解析整數」策略（保守）
            # 常見：成交股數在 r[8] 或 r[9]
            vol = 0
            for cand in r[::-1]:
                v = _to_int(cand)
                if v > vol:
                    vol = v
            open_p = _to_float(r[4]) if len(r) > 4 else float("nan")
            high_p = _to_float(r[5]) if len(r) > 5 else float("nan")
            low_p = _to_float(r[6]) if len(r) > 6 else float("nan")

            if code == "" or code.lower() == "nan":
                continue
            rows.append({
                "symbol": f"{code}.TWO",
                "name": name,
                "volume": vol,
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close,
            })
        return pd.DataFrame(rows)

    try:
        r = _req_get(url, verify_ssl=verify_ssl)
        r.raise_for_status()
        j = r.json()
        df = _parse(j)
        return df, meta
    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        if verify_ssl and allow_ssl_bypass:
            try:
                r = _req_get(url, verify_ssl=False)
                r.raise_for_status()
                j = r.json()
                df = _parse(j)
                meta["verify_ssl"] = False
                meta["ssl_bypassed"] = True
                meta["error"] = None
                return df, meta
            except Exception as e2:
                meta["error"] = f"{type(e2).__name__}: {e2}"
        return pd.DataFrame(), meta


def find_latest_trade_date_tw(
    lookback_days: int = 10,
    *,
    verify_ssl: bool = True,
    allow_ssl_bypass: bool = True
) -> Tuple[Optional[date], Dict[str, Any]]:
    """
    依序回推日期，找到可以成功抓到「上市或上櫃」任一側全市場行情的日期。
    """
    meta = {"attempts": [], "error": None}
    today = _now_taipei().date()

    for i in range(0, max(1, lookback_days)):
        d = today - timedelta(days=i)
        twse_df, twse_m = fetch_twse_all_daily_quotes(d, verify_ssl=verify_ssl, allow_ssl_bypass=allow_ssl_bypass)
        tpex_df, tpex_m = fetch_tpex_all_daily_quotes(d, verify_ssl=verify_ssl, allow_ssl_bypass=allow_ssl_bypass)

        meta["attempts"].append({
            "date": d.isoformat(),
            "twse_rows": int(len(twse_df)),
            "tpex_rows": int(len(tpex_df)),
            "twse_err": twse_m.get("error"),
            "tpex_err": tpex_m.get("error"),
            "twse_ssl_bypass": bool(twse_m.get("ssl_bypassed")),
            "tpex_ssl_bypass": bool(tpex_m.get("ssl_bypassed")),
        })

        if len(twse_df) > 100 or len(tpex_df) > 100:
            return d, meta

    meta["error"] = "找不到最新可用交易日（lookback 範圍內全市場行情皆不可用）"
    return None, meta


# ==========================================================
# 3) TopN 建立：先用全市場當日成交金額做熱度池，再用 yfinance 計算分數
# ==========================================================

def build_topn_tw_market(
    topn: int,
    trade_date: date,
    *,
    verify_ssl: bool = True,
    allow_ssl_bypass: bool = True,
    pool_size: int = 200
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    TopN 定義（SIM-FREE）：
      Step1 全市場掃描：上市+上櫃，計算當日成交金額 = volume * close，取 Top{pool_size} 作為熱度池。
      Step2 對熱度池逐檔抓 60 交易日 yfinance，計算：
        - ret20_pct：近 20 交易日報酬
        - ma_bias_pct：收盤相對 MA20 偏離%
        - vol_ratio：當日量 / 20日均量
        - score：0.45*ret20 + 0.35*ma_bias + 0.20*(vol_ratio-1)*100（並做 NaN 防護）
      Step3 score 由高到低取 TopN
    """
    meta: Dict[str, Any] = {
        "trade_date": trade_date.isoformat(),
        "topn": topn,
        "pool_size": pool_size,
        "sources": {},
        "warnings": [],
        "ssl_bypassed": False,
    }

    twse_df, twse_m = fetch_twse_all_daily_quotes(trade_date, verify_ssl=verify_ssl, allow_ssl_bypass=allow_ssl_bypass)
    tpex_df, tpex_m = fetch_tpex_all_daily_quotes(trade_date, verify_ssl=verify_ssl, allow_ssl_bypass=allow_ssl_bypass)
    meta["sources"]["twse_all"] = twse_m
    meta["sources"]["tpex_all"] = tpex_m

    if bool(twse_m.get("ssl_bypassed")) or bool(tpex_m.get("ssl_bypassed")):
        meta["ssl_bypassed"] = True
        meta["warnings"].append("全市場行情抓取已發生 SSL bypass（requests verify=False）")

    if twse_df.empty and tpex_df.empty:
        meta["warnings"].append("全市場行情不可用：TWSE/TPEx 均無資料")
        return pd.DataFrame(), meta

    uni = pd.concat([twse_df, tpex_df], ignore_index=True)
    # 基本清洗
    uni = uni.dropna(subset=["close"]).copy()
    uni["close"] = uni["close"].astype(float)
    uni["volume"] = uni["volume"].fillna(0).astype(int)
    uni["turnover_amt"] = (uni["close"] * uni["volume"]).fillna(0.0)

    # 熱度池：成交金額 Top pool_size
    pool = uni.sort_values("turnover_amt", ascending=False).head(pool_size).copy()
    pool_symbols = pool["symbol"].tolist()

    # 逐檔抓 yfinance（只抓 pool，避免全市場爆量）
    rows = []
    for sym in pool_symbols:
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="4mo", interval="1d")  # 約 80 交易日
            if hist is None or hist.empty or len(hist) < 25:
                continue

            close_series = hist["Close"].dropna()
            vol_series = hist["Volume"].dropna()

            last_close = float(close_series.iloc[-1])
            prev_close = float(close_series.iloc[-2]) if len(close_series) >= 2 else last_close

            # 20D return
            if len(close_series) >= 21:
                c20 = float(close_series.iloc[-21])
                ret20 = (last_close / c20 - 1.0) * 100 if c20 != 0 else float("nan")
            else:
                ret20 = float("nan")

            # MA20 bias
            ma20 = float(close_series.tail(20).mean()) if len(close_series) >= 20 else float("nan")
            ma_bias = (last_close / ma20 - 1.0) * 100 if ma20 and not math.isnan(ma20) and ma20 != 0 else float("nan")

            # volume ratio（當日 vs 20D avg）
            v_last = float(vol_series.iloc[-1]) if len(vol_series) >= 1 else float("nan")
            v_avg20 = float(vol_series.tail(20).mean()) if len(vol_series) >= 20 else float("nan")
            vol_ratio = (v_last / v_avg20) if v_avg20 and not math.isnan(v_avg20) and v_avg20 != 0 else float("nan")

            # score（NaN 防護：任何 NaN 直接降級為 -inf）
            if any(math.isnan(x) for x in [ret20, ma_bias, vol_ratio]):
                score = float("-inf")
            else:
                score = 0.45 * ret20 + 0.35 * ma_bias + 0.20 * ((vol_ratio - 1.0) * 100.0)

            rows.append({
                "symbol": sym,
                "name": pool.loc[pool["symbol"] == sym, "name"].iloc[0] if (pool["symbol"] == sym).any() else "",
                "date": hist.index[-1].date().isoformat(),
                "close": round(last_close, 4),
                "ret20_pct": round(ret20, 4) if not math.isnan(ret20) else None,
                "vol_ratio": round(vol_ratio, 4) if not math.isnan(vol_ratio) else None,
                "ma_bias_pct": round(ma_bias, 4) if not math.isnan(ma_bias) else None,
                "volume": int(v_last) if not math.isnan(v_last) else None,
                "score": round(score, 4) if score != float("-inf") else None,
            })
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        meta["warnings"].append("熱度池成功，但 yfinance 指標計算後無有效股票")
        return df, meta

    # 排名：score desc
    df = df.dropna(subset=["score"]).copy()
    df = df.sort_values(["score", "vol_ratio"], ascending=[False, False]).reset_index(drop=True)
    df["rank"] = df.index + 1
    df = df.head(topn).copy()

    # trade_date 稽核：若 df 的 date 與 trade_date 差距過大，標記 stale（避免拿到舊行情）
    # 允許最多 1 天差（時區/晚間抓取）
    try:
        max_date = pd.to_datetime(df["date"]).max().date()
        gap = abs((max_date - trade_date).days)
        if gap >= 2:
            meta["warnings"].append(f"DATA_STALE: TopN 指標日期={max_date.isoformat()} 與官方交易日={trade_date.isoformat()} 差距={gap}天")
            meta["stale"] = True
        else:
            meta["stale"] = False
    except Exception:
        meta["stale"] = True
        meta["warnings"].append("DATA_STALE: 無法稽核 TopN 指標日期")

    return df, meta


# ==========================================================
# 4) AI JSON（Arbiter Input）組包：Top20 + 持倉 + macro
# ==========================================================

def normalize_symbols_list(text: str) -> List[str]:
    if not text:
        return []
    parts = []
    for s in str(text).replace("，", ",").replace(" ", "").split(","):
        s = s.strip()
        if not s:
            continue
        parts.append(s)
    # 也支援換行
    out = []
    for p in parts:
        for line in p.splitlines():
            line = line.strip()
            if line:
                out.append(line)
    # 去重保持順序
    seen = set()
    res = []
    for x in out:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res


def merge_topn_with_positions(topn_df: pd.DataFrame, positions: List[str]) -> pd.DataFrame:
    """
    TopN + positions 合併後輸出（Top20 + N）
    """
    pos_syms = set(positions)
    df = topn_df.copy()

    # 若持倉不在 TopN：追加（以 yfinance 補基本欄位）
    missing = [s for s in positions if s not in set(df["symbol"].tolist())]
    rows = []
    for sym in missing:
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="10d", interval="1d")
            if hist is None or hist.empty:
                continue
            last_close = float(hist["Close"].iloc[-1])
            d = hist.index[-1].date().isoformat()
            rows.append({
                "symbol": sym,
                "name": "",
                "date": d,
                "close": round(last_close, 4),
                "ret20_pct": None,
                "vol_ratio": None,
                "ma_bias_pct": None,
                "volume": None,
                "score": None,
                "rank": None,
            })
        except Exception:
            rows.append({
                "symbol": sym,
                "name": "",
                "date": None,
                "close": None,
                "ret20_pct": None,
                "vol_ratio": None,
                "ma_bias_pct": None,
                "volume": None,
                "score": None,
                "rank": None,
            })

    if rows:
        df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)

    # 增加欄位：is_position
    df["is_position"] = df["symbol"].apply(lambda x: x in pos_syms)
    return df


def build_arbiter_input(
    *,
    session: str,
    market: str,
    topn_df: pd.DataFrame,
    merged_df: pd.DataFrame,
    trade_date: Optional[str],
    index_info: Dict[str, Any],
    market_amount: Dict[str, Any],
    topn_meta: Dict[str, Any],
    positions: List[str],
    verify_ssl: bool
) -> Dict[str, Any]:
    """
    依你需求的 Arbiter Input JSON：
    - meta
    - macro.overview（trade_date / index / amount / inst_status 等）
    - stocks（Top20 + positions 合併後清單）
    """
    # 稽核：TopN stale 或 amount不可用 或 index不可用 → degraded
    stale = bool(topn_meta.get("stale")) or ("DATA_STALE" in " ".join(topn_meta.get("warnings", [])))
    amount_total = market_amount.get("amount_total")
    amount_sources = market_amount.get("sources", {})
    amount_warning = amount_sources.get("warning")
    ssl_bypass_any = bool(amount_sources.get("twse", {}).get("ssl_bypassed")) or bool(amount_sources.get("tpex", {}).get("ssl_bypassed")) or bool(topn_meta.get("ssl_bypassed"))

    degraded = False
    reasons = []

    if stale:
        degraded = True
        reasons.append("DATA_STALE")
    if index_info.get("index_level") is None:
        degraded = True
        reasons.append("INDEX_UNAVAILABLE")
    if amount_total is None:
        # 量能缺失：在你的裁決邏輯中屬於絕對防線 → degraded
        degraded = True
        reasons.append("AMOUNT_UNAVAILABLE")

    # 模擬期：法人資料預設不可用（避免 FinMind 402）
    inst_status = "UNAVAILABLE(FREE_SIM)"
    inst_net = "A:0.00億 | B:0.00億"

    overview = {
        "trade_date": trade_date,
        "data_mode": session.upper(),
        "index_level": index_info.get("index_level"),
        "index_change": index_info.get("index_change"),
        "index_chg_pct": index_info.get("index_chg_pct"),
        "index_source": index_info.get("source"),
        "amount_twse": market_amount.get("amount_twse"),
        "amount_tpex": market_amount.get("amount_tpex"),
        "amount_total": amount_total,
        "amount_sources": amount_sources,
        "inst_status": inst_status,
        "inst_net": inst_net,
        "verify_ssl": bool(verify_ssl),
        "ssl_bypass_any": ssl_bypass_any,
        "degraded_mode": bool(degraded),
        "degraded_reasons": reasons,
        "topn_definition": {
            "pool_size": topn_meta.get("pool_size"),
            "scoring": "score=0.45*ret20 + 0.35*ma_bias + 0.20*(vol_ratio-1)*100 ; pool=Top成交金額",
        },
        "warnings": (topn_meta.get("warnings") or []) + ([amount_warning] if amount_warning else []),
    }

    # stocks：輸出給其他 AI 的清單（Top20 + positions）
    stocks = []
    for _, r in merged_df.iterrows():
        stocks.append({
            "symbol": r.get("symbol"),
            "name": r.get("name"),
            "date": r.get("date"),
            "close": r.get("close"),
            "ret20_pct": r.get("ret20_pct"),
            "vol_ratio": r.get("vol_ratio"),
            "ma_bias_pct": r.get("ma_bias_pct"),
            "volume": r.get("volume"),
            "score": r.get("score"),
            "rank": r.get("rank"),
            "is_position": bool(r.get("is_position")),
        })

    return {
        "meta": {
            "system": "Predator V15.7 (SIM-FREE / Top20+Positions)",
            "market": market,
            "timestamp": _now_taipei().strftime("%Y-%m-%d %H:%M"),
            "session": session.upper(),
        },
        "macro": {
            "overview": overview,
            "global_summary": [],  # main.py 會另外顯示表格，此處可不塞以免過大
        },
        "stocks": stocks,
        "positions": positions,
    }
