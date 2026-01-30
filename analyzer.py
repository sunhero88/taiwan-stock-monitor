# analyzer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SESSION_PREOPEN = "PREOPEN"   # 盤前（輸入昨日/最後收盤日的大盤）
SESSION_INTRADAY = "INTRADAY" # 盤中（輸入當下大盤）
SESSION_EOD = "EOD"           # 盤後（輸入當日收盤大盤）


@dataclass
class AnalyzerConfig:
    topn: int = 20
    min_history_days: int = 60
    vol_lookback: int = 20
    ma_fast: int = 20
    ma_slow: int = 60
    mom_5d: int = 5
    mom_20d: int = 20


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = {"Date", "Symbol", "Close", "Volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Market CSV 缺欄位：{sorted(list(missing))}；需要 Date, Symbol, Close, Volume")
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out["Symbol"] = out["Symbol"].astype(str)
    out["Close"] = pd.to_numeric(out["Close"], errors="coerce")
    out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce")
    out = out.dropna(subset=["Date", "Symbol", "Close"]).copy()
    out["Volume"] = out["Volume"].fillna(0)
    return out


def _calc_features(df: pd.DataFrame, cfg: AnalyzerConfig) -> pd.DataFrame:
    """
    以「每檔股票」做 rolling 指標，再取最後一筆作為當日(或盤中最新)快照。
    """
    d = df.sort_values(["Symbol", "Date"]).copy()

    g = d.groupby("Symbol", group_keys=False)
    d["ma20"] = g["Close"].transform(lambda s: s.rolling(cfg.ma_fast, min_periods=cfg.ma_fast).mean())
    d["ma60"] = g["Close"].transform(lambda s: s.rolling(cfg.ma_slow, min_periods=cfg.ma_slow).mean())
    d["vol20"] = g["Volume"].transform(lambda s: s.rolling(cfg.vol_lookback, min_periods=cfg.vol_lookback).mean())

    # 報酬/動能
    d["ret_5d"] = g["Close"].transform(lambda s: s.pct_change(cfg.mom_5d))
    d["ret_20d"] = g["Close"].transform(lambda s: s.pct_change(cfg.mom_20d))

    # MA_Bias：用 MA20
    d["ma_bias_pct"] = (d["Close"] / d["ma20"] - 1.0) * 100.0

    # 量能比
    d["vol_ratio"] = np.where(d["vol20"] > 0, d["Volume"] / d["vol20"], np.nan)

    return d


def _score_row(r: pd.Series) -> float:
    """
    免費/模擬期：用可回溯、可穩定的因子做「相對排名」。
    Score 不用追求金融完美，重點是「每天都能算出全市場 Top20」。
    """
    ma_bias = float(r.get("ma_bias_pct", np.nan))
    vol_ratio = float(r.get("vol_ratio", np.nan))
    ret_5d = float(r.get("ret_5d", np.nan))
    ret_20d = float(r.get("ret_20d", np.nan))

    # 缺值保守處理（避免亂給高分）
    if np.isnan(ma_bias):
        ma_bias = -999
    if np.isnan(vol_ratio):
        vol_ratio = 0
    if np.isnan(ret_5d):
        ret_5d = 0
    if np.isnan(ret_20d):
        ret_20d = 0

    # 核心：趨勢 + 量能 + 動能（權重可調）
    # - ma_bias：-10%~+10% 常見，放大到可比較
    # - vol_ratio：1.0=正常，>1.2偏放量
    # - ret_5d/20d：動能
    score = 0.0
    score += 0.55 * ma_bias
    score += 10.0 * np.tanh((vol_ratio - 1.0) * 1.5)   # 避免極端量爆掉
    score += 40.0 * ret_5d
    score += 20.0 * ret_20d

    # 基本防護：極低量（例如 vol_ratio < 0.2）扣分
    if vol_ratio < 0.2:
        score -= 8.0

    return float(score)


def _tag_row(r: pd.Series) -> str:
    ma_bias = float(r.get("ma_bias_pct", np.nan))
    vol_ratio = float(r.get("vol_ratio", np.nan))
    ret_5d = float(r.get("ret_5d", np.nan))

    # 缺值→觀察
    if np.isnan(ma_bias) or np.isnan(vol_ratio):
        return "○觀察(觀望)"

    # 你原本的標籤語系（保持一致）
    # 🔥：趨勢強 + 放量
    if ma_bias >= 5 and vol_ratio >= 1.2:
        return "🔥主力(觀望)"
    # 起漲：剛翻正、且短線動能>0
    if ma_bias > 0 and ret_5d > 0:
        return "🟢起漲(觀望)"
    return "○觀察(觀望)"


def run_analysis(
    df_market: pd.DataFrame,
    session: str = SESSION_EOD,
    cfg: Optional[AnalyzerConfig] = None
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    輸入：全市場歷史資料（Date, Symbol, Close, Volume）
    輸出：TopN 快照表（含 Score/Tag 等）
    """
    cfg = cfg or AnalyzerConfig()

    try:
        df = _ensure_columns(df_market)
        if df.empty:
            return pd.DataFrame(), "market dataframe is empty"

        df_feat = _calc_features(df, cfg)

        # 取每檔最後一筆（代表最新快照）
        last = df_feat.sort_values(["Symbol", "Date"]).groupby("Symbol", as_index=False).tail(1).copy()

        # 歷史不足者剔除
        hist_cnt = df.groupby("Symbol")["Date"].count()
        last["hist_cnt"] = last["Symbol"].map(hist_cnt).fillna(0).astype(int)
        last = last[last["hist_cnt"] >= cfg.min_history_days].copy()

        if last.empty:
            return pd.DataFrame(), f"no symbols meet min_history_days >= {cfg.min_history_days}"

        last["Score"] = last.apply(_score_row, axis=1)
        last["Tag"] = last.apply(_tag_row, axis=1)

        # 組出 TopN
        top = last.sort_values("Score", ascending=False).head(cfg.topn).copy()

        # 整理輸出欄位
        top_out = pd.DataFrame({
            "Date": top["Date"].dt.strftime("%Y-%m-%d"),
            "Symbol": top["Symbol"].astype(str),
            "Price": top["Close"].astype(float),
            "Volume": top["Volume"].astype(float),
            "MA_Bias": top["ma_bias_pct"].astype(float).round(2),
            "Vol_Ratio": top["vol_ratio"].astype(float).round(2),
            "Body_Power": 0.0,
            "Score": top["Score"].astype(float).round(1),
            "Tag": top["Tag"].astype(str),
        })

        return top_out.reset_index(drop=True), None

    except Exception as e:
        return pd.DataFrame(), f"{type(e).__name__}: {str(e)}"


def generate_ai_json(
    df_top: pd.DataFrame,
    market: str,
    session: str,
    macro_data: dict,
    name_map: Optional[Dict[str, str]] = None,
    account: Optional[dict] = None,
) -> str:
    """
    產出 Arbiter Input JSON（保持你既有 V15.x 格式習慣）
    - Top20 + positions(去重) 的邏輯在 main.py 做（這裡假設 df_top 已是要輸出的清單）
    """
    import json
    from datetime import datetime

    name_map = name_map or {}
    account = account or {
        "cash_balance": 2_000_000,
        "total_equity": 2_000_000,
        "positions": []
    }

    # ranking：依 df_top 順序給 rank / tier / top20_flag
    stocks = []
    for i, r in df_top.reset_index(drop=True).iterrows():
        sym = str(r.get("Symbol"))
        price = float(r.get("Price", 0.0))
        score = float(r.get("Score", 0.0))
        tag = str(r.get("Tag", "○觀察(觀望)"))

        rank = int(i + 1)
        tier = "A" if rank <= 10 else "B"
        top20_flag = True if rank <= 20 else False

        # 中文名：優先本地映射
        name = name_map.get(sym, sym)

        stocks.append({
            "Symbol": sym,
            "Name": name,
            "Price": price,
            "ranking": {
                "symbol": sym,
                "rank": rank,
                "tier": tier,
                "top20_flag": top20_flag
            },
            "Technical": {
                "MA_Bias": float(r.get("MA_Bias", 0.0)),
                "Vol_Ratio": float(r.get("Vol_Ratio", 0.0)),
                "Body_Power": float(r.get("Body_Power", 0.0)),
                "Score": score,
                "Tag": tag
            },
            # 免費/模擬期：法人先允許 UNAVAILABLE，不要讓系統爆掉
            "Institutional": {
                "Inst_Visual": "PENDING",
                "Inst_Net_3d": 0.0,
                "Inst_Streak3": 0,
                "Inst_Dir3": "PENDING",
                "Inst_Status": "PENDING"
            },
            "Structure": {
                "OPM": 0.0,
                "Rev_Growth": 0.0,
                "PE": 0.0,
                "Sector": "Unknown",
                "Rev_Growth_Source": "raw"
            },
            "risk": {
                "position_pct_max": 12,
                "risk_per_trade_max": 1.0,
                "trial_flag": True
            },
            "orphan_holding": False,
            "weaken_flags": {
                "technical_weaken": False,
                "structure_weaken": False
            }
        })

    payload = {
        "meta": {
            "system": "Predator V15.7 (SIM/FREE)",
            "market": market,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "session": session
        },
        "macro": macro_data,
        "account": account,
        "stocks": stocks
    }

    return json.dumps(payload, ensure_ascii=False, indent=2)
