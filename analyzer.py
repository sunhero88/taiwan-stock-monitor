# analyzer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

TZ_TAIPEI = timezone(timedelta(hours=8))


# -----------------------
# 基礎工具：交易日 / 新鮮度
# -----------------------
def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def normalize_trade_date(d: pd.Timestamp) -> str:
    return pd.Timestamp(d).strftime("%Y-%m-%d")


def trading_stale_days(latest_trade_date: Optional[str], now: Optional[datetime] = None) -> Optional[int]:
    """
    以「交易日」估算 stale_days（簡化版：用日曆差，但可接受於 SIM-FREE）
    你若日後要嚴格的「交易日計數」，可改成用交易日曆/自建 trading calendar。
    """
    if not latest_trade_date:
        return None
    now = now or now_taipei()
    try:
        d = pd.to_datetime(latest_trade_date).tz_localize(None)
        n = pd.to_datetime(now.astimezone(TZ_TAIPEI).strftime("%Y-%m-%d"))
        return int((n - d).days)
    except Exception:
        return None


# -----------------------
# 大盤：TWII（自動）
# -----------------------
def fetch_twii(session: str) -> Dict[str, Any]:
    """
    session:
      - PREOPEN/EOD：取最後一根收盤
      - INTRADAY：嘗試取 1d intraday 最後價，並與前一收盤計算 change
    """
    t = "^TWII"
    out = {
        "symbol": t,
        "date": None,
        "index": None,
        "change": None,
        "change_pct": None,
        "source": "yfinance",
        "mode": session,
        "error": None,
    }

    try:
        if session == "INTRADAY":
            # intraday 嘗試 1d/5m（若被限制可能回空）
            df = yf.download(tickers=t, period="2d", interval="5m", progress=False, auto_adjust=False, threads=False)
            if df is None or df.empty:
                raise RuntimeError("TWII intraday 無資料（可能被 yfinance 限制或延遲）")

            df = df.dropna()
            # last price
            last_px = float(df["Close"].iloc[-1])
            last_ts = df.index[-1]
            out["index"] = round(last_px, 2)
            out["date"] = normalize_trade_date(pd.Timestamp(last_ts).tz_localize(None))

            # prev close：用前一天最後一筆 close
            # 如果 df 只有同一天資料，改用 2d daily
            prev_close = None
            try:
                df_daily = yf.download(tickers=t, period="5d", interval="1d", progress=False, auto_adjust=False, threads=False)
                df_daily = df_daily.dropna()
                if len(df_daily) >= 2:
                    prev_close = float(df_daily["Close"].iloc[-2])
            except Exception:
                prev_close = None

            if prev_close is not None and prev_close > 0:
                chg = last_px - prev_close
                out["change"] = round(chg, 4)
                out["change_pct"] = round((chg / prev_close) * 100.0, 4)
            else:
                out["change"] = None
                out["change_pct"] = None

        else:
            # PREOPEN / EOD：最後一根收盤
            df = yf.download(tickers=t, period="10d", interval="1d", progress=False, auto_adjust=False, threads=False)
            if df is None or df.empty:
                raise RuntimeError("TWII daily 無資料")
            df = df.dropna()
            last_close = float(df["Close"].iloc[-1])
            last_date = df.index[-1]
            out["index"] = round(last_close, 2)
            out["date"] = normalize_trade_date(pd.Timestamp(last_date).tz_localize(None))

            if len(df) >= 2:
                prev = float(df["Close"].iloc[-2])
                chg = last_close - prev
                out["change"] = round(chg, 4)
                out["change_pct"] = round((chg / prev) * 100.0, 4) if prev > 0 else None

    except Exception as e:
        out["error"] = str(e)

    return out


# -----------------------
# Universe / TopN：資料載入
# -----------------------
def _candidate_files(market: str) -> List[str]:
    """
    盡量相容你 repo 可能出現的命名：
      data/data_tw-share.csv
      data/data_tw.csv
      data/tw-share.csv
      data/tw.csv
      reports/*.csv
    """
    cands = []
    # data folder
    cands += [
        f"data/data_{market}.csv",
        f"data/{market}.csv",
        f"data/data_{market.replace('-', '_')}.csv",
        f"data/{market.replace('-', '_')}.csv",
        "data/data_tw-share.csv",
        "data/data_tw.csv",
        "data/tw-share.csv",
        "data/tw.csv",
    ]
    # reports (latest)
    if os.path.isdir("reports"):
        for fn in sorted(os.listdir("reports"), reverse=True):
            if fn.endswith(".csv") and ("tw" in fn or "tw-share" in fn):
                cands.append(os.path.join("reports", fn))
    return cands


def load_market_snapshot(market: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    讀入你每天產出的市場快照 CSV（建議由 GitHub Actions 生成）。
    需至少包含：
      symbol, date, close
    若有以下欄位可加分：
      name, ret20_pct, vol_ratio, ma_bias_pct, volume, score
    """
    meta = {"source": None, "path": None, "error": None}
    for p in _candidate_files(market):
        if os.path.isfile(p):
            try:
                df = pd.read_csv(p)
                if df is None or df.empty:
                    continue
                # normalize columns
                df.columns = [str(c).strip() for c in df.columns]
                if "symbol" not in df.columns:
                    continue
                # ensure date column
                if "date" not in df.columns:
                    # 嘗試用 Date / trade_date
                    for alt in ["Date", "trade_date", "TradeDate"]:
                        if alt in df.columns:
                            df["date"] = df[alt]
                            break
                if "close" not in df.columns:
                    for alt in ["Close", "price", "Price"]:
                        if alt in df.columns:
                            df["close"] = df[alt]
                            break

                if "date" not in df.columns or "close" not in df.columns:
                    continue

                # clean
                df["symbol"] = df["symbol"].astype(str).str.strip()
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                df = df.dropna(subset=["symbol", "date", "close"])

                meta["source"] = "repo_csv"
                meta["path"] = p
                return df, meta
            except Exception as e:
                meta["error"] = f"{p}: {e}"
                continue
    meta["error"] = meta["error"] or "找不到可用市場快照 CSV"
    return None, meta


def build_topn(df: pd.DataFrame, topn: int = 20) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    以快照資料建 TopN。
    排序優先：
      1) 若有 score：score desc
      2) 否則用 ret20_pct desc + vol_ratio desc + ma_bias_pct desc
      3) 再不行就 volume desc
    """
    meta = {"method": None}

    d = df.copy()

    # ensure numeric
    for col in ["score", "ret20_pct", "vol_ratio", "ma_bias_pct", "volume", "close"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    # choose latest trade_date in snapshot
    latest_date = d["date"].dropna().max()
    d = d[d["date"] == latest_date].copy()

    if "score" in d.columns and d["score"].notna().any():
        meta["method"] = "score_desc"
        d = d.sort_values(["score"], ascending=False)
    elif all(c in d.columns for c in ["ret20_pct", "vol_ratio", "ma_bias_pct"]) and (
        d["ret20_pct"].notna().any() or d["vol_ratio"].notna().any() or d["ma_bias_pct"].notna().any()
    ):
        meta["method"] = "ret20+vol_ratio+ma_bias"
        d = d.sort_values(["ret20_pct", "vol_ratio", "ma_bias_pct"], ascending=False)
    elif "volume" in d.columns and d["volume"].notna().any():
        meta["method"] = "volume_desc"
        d = d.sort_values(["volume"], ascending=False)
    else:
        meta["method"] = "close_desc_fallback"
        d = d.sort_values(["close"], ascending=False)

    d = d.head(topn).copy()

    # ensure columns exist
    if "name" not in d.columns:
        d["name"] = None

    d = d.reset_index(drop=True)
    d.insert(0, "rank", np.arange(1, len(d) + 1))
    return d, {"latest_date": latest_date, **meta}


# -----------------------
# Positions（持倉）解析
# -----------------------
def parse_positions_json(text: str) -> List[Dict[str, Any]]:
    if not text:
        return []
    try:
        x = json.loads(text)
        if isinstance(x, list):
            out = []
            for it in x:
                if isinstance(it, dict) and "symbol" in it:
                    out.append(
                        {
                            "symbol": str(it["symbol"]).strip(),
                            "qty": float(it.get("qty", 0)),
                            "avg_cost": float(it.get("avg_cost", 0)),
                        }
                    )
            return out
        return []
    except Exception:
        return []


# -----------------------
# Gate（稽核）
# -----------------------
def gate_eval(
    session: str,
    topn_df: Optional[pd.DataFrame],
    twii: Dict[str, Any],
    latest_trade_date: Optional[str],
    required_topn: int = 20,
) -> Dict[str, Any]:
    """
    Gate：
      - DATA_STALE
      - DATE_MISMATCH
      - TOPN_INCOMPLETE
    """
    now = now_taipei()
    stale = trading_stale_days(latest_trade_date, now=now)

    out = {
        "DATA_STALE": False,
        "DATE_MISMATCH": False,
        "TOPN_INCOMPLETE": False,
        "stale_days": stale,
        "latest_trade_date": latest_trade_date,
        "twii_date": twii.get("date"),
        "reason": [],
        "allow_trade": True,
    }

    # TopN incomplete
    if topn_df is None or len(topn_df) < required_topn:
        out["TOPN_INCOMPLETE"] = True
        out["reason"].append(f"TopN 不足 {len(topn_df) if topn_df is not None else 0}/{required_topn}")

    # Date mismatch
    if latest_trade_date and twii.get("date") and latest_trade_date != twii.get("date"):
        # PREOPEN 容許 TWII 日期與 TopN 日期同為「最後可用交易日」，一般應一致；不一致就標記
        out["DATE_MISMATCH"] = True
        out["reason"].append(f"日期不一致：TopN={latest_trade_date} vs TWII={twii.get('date')}")

    # DATA_STALE rule
    if stale is None:
        out["DATA_STALE"] = True
        out["reason"].append("無法判定資料新鮮度（缺 trade_date）")
    else:
        if session == "PREOPEN":
            if stale > 1:
                out["DATA_STALE"] = True
                out["reason"].append(f"PREOPEN stale_days={stale} > 1（盤前只允許昨收盤）")
        else:
            # INTRADAY / EOD
            if stale != 0:
                out["DATA_STALE"] = True
                out["reason"].append(f"{session} stale_days={stale} != 0（盤中/盤後必須當日）")

    # allow_trade：只要觸發任何高風險 Gate 就禁止
    if out["DATA_STALE"] or out["DATE_MISMATCH"] or (session in ["INTRADAY", "EOD"] and out["TOPN_INCOMPLETE"]):
        out["allow_trade"] = False

    return out


# -----------------------
# 產出 Arbiter Input JSON（核心）
# -----------------------
def build_arbiter_input(
    system_name: str,
    market: str,
    session: str,
    topn_df: Optional[pd.DataFrame],
    positions: List[Dict[str, Any]],
    twii: Dict[str, Any],
    amount_pack: Dict[str, Any],
    gates: Dict[str, Any],
    snapshot_meta: Dict[str, Any],
) -> Dict[str, Any]:
    ts = now_taipei().strftime("%Y-%m-%d %H:%M")

    # merge: TopN ∪ Positions（去重）
    stocks = []
    seen = set()

    def add_row(symbol: str, payload: Dict[str, Any]):
        sym = str(symbol).strip()
        if not sym or sym in seen:
            return
        seen.add(sym)
        stocks.append(payload)

    if topn_df is not None and not topn_df.empty:
        for _, r in topn_df.iterrows():
            add_row(
                r.get("symbol"),
                {
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
                    "is_position": False,
                },
            )

    for p in positions:
        add_row(
            p["symbol"],
            {
                "symbol": p["symbol"],
                "name": None,
                "date": None,
                "close": None,
                "ret20_pct": None,
                "vol_ratio": None,
                "ma_bias_pct": None,
                "volume": None,
                "score": None,
                "rank": None,
                "is_position": True,
                "qty": p.get("qty", 0),
                "avg_cost": p.get("avg_cost", 0),
            },
        )

    out = {
        "meta": {
            "system": system_name,
            "timestamp": ts,
            "market": market,
            "session": session,
            "topn_target": 20,
            "topn_actual": len(topn_df) if topn_df is not None else 0,
            "snapshot_source": snapshot_meta.get("source"),
            "snapshot_path": snapshot_meta.get("path"),
        },
        "macro": {
            "trade_date": gates.get("latest_trade_date"),
            "twii": twii,
            "amount": amount_pack,
            "gates": gates,
        },
        "stocks": stocks,
    }
    return out
