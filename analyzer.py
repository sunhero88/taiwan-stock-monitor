# analyzer.py
# -*- coding: utf-8 -*-
"""
Predator Analyzer V15.6.3
- 技術面篩選 + 結構面補強 + 法人3日欄位準備
- 不做 BUY/SELL（裁決由 arbiter.py 負責）
"""

import json
from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf

from institutional_utils import calc_inst_3d  # 你已新增

# ======================================================
# Parameters
# ======================================================

VOL_THRESHOLD_WEIGHTED = 1.2
VOL_THRESHOLD_SMALL = 1.8

MA_BIAS_GREEN_WEIGHTED = (0, 8)
MA_BIAS_GREEN_SMALL = (0, 12)

MA_BIAS_PENALTY_START = 10
MA_BIAS_PENALTY_FULL = 15
MA_BIAS_HARD_CAP = 20

BODY_POWER_STRONG = 75
BODY_POWER_DISTRIBUTE = 20
DISTRIBUTE_VOL_RATIO = 2.5

SESSION_INTRADAY = "INTRADAY"
SESSION_EOD = "EOD"

TOPN_FINAL = 20


# ======================================================
# Helpers
# ======================================================

def calc_body_power(row: dict) -> float:
    try:
        high = float(row.get("High", np.nan))
        low = float(row.get("Low", np.nan))
        close = float(row.get("Close", np.nan))
        open_ = float(row.get("Open", np.nan))

        if not np.isfinite(high) or not np.isfinite(low) or not np.isfinite(close) or not np.isfinite(open_):
            return 0.0

        span = high - low
        if span <= 0:
            return 0.0

        return abs(close - open_) / span * 100.0
    except Exception:
        return 0.0


def calc_ma_bias_penalty(ma_bias) -> float:
    try:
        ma_bias = float(ma_bias)
    except Exception:
        return 0.0

    if ma_bias <= MA_BIAS_PENALTY_START:
        return 0.0
    if ma_bias >= MA_BIAS_PENALTY_FULL:
        return 1.0
    return (ma_bias - MA_BIAS_PENALTY_START) / (MA_BIAS_PENALTY_FULL - MA_BIAS_PENALTY_START)


def enrich_fundamentals(symbol: str) -> dict:
    """
    注意：Rev_Growth 來源為 yfinance.info['revenueGrowth']
    口徑可能是 YoY 或 trailing growth，故不命名 QoQ。
    """
    data = {
        "OPM": 0.0,
        "Rev_Growth": 0.0,
        "PE": 0.0,
        "Sector": "Unknown",
        "Rev_Growth_Source": "yfinance:revenueGrowth",
    }
    try:
        info = (yf.Ticker(symbol).info) or {}
        data["OPM"] = round((info.get("operatingMargins") or 0) * 100, 2)
        data["Rev_Growth"] = round((info.get("revenueGrowth") or 0) * 100, 2)
        data["PE"] = round((info.get("trailingPE") or 0), 2)
        data["Sector"] = info.get("sector", "Unknown") or "Unknown"
    except Exception:
        pass
    return data


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = ["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]
    for c in required:
        if c not in df.columns:
            df[c] = np.nan
    return df


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=False)
    df["Symbol"] = df["Symbol"].astype(str)
    return df


# ======================================================
# Core
# ======================================================

def run_analysis(
    df: pd.DataFrame,
    session: str = SESSION_INTRADAY,
    market: str = "tw-share",
    trade_date: str = "",
    inst_df_3d: pd.DataFrame = None,
    inst_status: str = "PENDING",
    inst_dates_3d=None,
):
    """
    Return: (df_top20_records, err_msg)
    df_top20_records: DataFrame，每列是一檔股票的完整欄位（含 Institutional/Structure/Technical）
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame(), "Input DataFrame is empty"

        df = df.copy()
        if "Date" not in df.columns:
            df = df.reset_index(drop=False)
            if "Date" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "Date"})

        df = _ensure_columns(df)
        df = _coerce_types(df)

        if df["Date"].isna().all():
            return pd.DataFrame(), "Date invalid (all NaT)"

        latest_date = df["Date"].max()
        df_today = df[df["Date"] == latest_date].copy()
        if df_today.empty:
            return pd.DataFrame(), "No rows for latest_date"

        # 動態權值：成交額前 50
        df_today["Amount"] = (df_today["Close"] * df_today["Volume"]).fillna(0)
        top_50_amt_threshold = df_today["Amount"].nlargest(50).min() if len(df_today) > 50 else 0
        weighted_symbols = set(df_today.loc[df_today["Amount"] >= top_50_amt_threshold, "Symbol"].dropna().astype(str))

        # 技術快篩
        results = []
        for symbol, g in df.groupby("Symbol"):
            g = g.sort_values("Date")
            if len(g) < 20:
                continue

            latest_row = g.iloc[-1]
            close_v = latest_row.get("Close", np.nan)
            vol_v = latest_row.get("Volume", np.nan)
            if not np.isfinite(close_v) or not np.isfinite(vol_v) or float(vol_v) == 0:
                continue

            ma20 = g["Close"].rolling(20).mean().iloc[-1]
            vol_ma20 = g["Volume"].rolling(20).mean().iloc[-1]
            if not np.isfinite(ma20) or not np.isfinite(vol_ma20) or vol_ma20 <= 0:
                continue

            latest = latest_row.to_dict()
            latest["MA_Bias"] = ((latest["Close"] - ma20) / ma20) * 100.0
            latest["Vol_Ratio"] = latest["Volume"] / vol_ma20
            latest["Body_Power"] = calc_body_power(latest)

            is_weighted = str(symbol) in weighted_symbols

            # Kill Switch I
            if latest["Body_Power"] < BODY_POWER_DISTRIBUTE and latest["Vol_Ratio"] > DISTRIBUTE_VOL_RATIO:
                continue
            # Kill Switch II
            if latest["MA_Bias"] > MA_BIAS_HARD_CAP:
                continue

            penalty = calc_ma_bias_penalty(latest["MA_Bias"])
            ers = (
                (latest["Vol_Ratio"] * 20.0)
                + (max(0.0, 15.0 - abs(latest["MA_Bias"])) * 2.0)
            ) * (1.0 - 0.5 * penalty)

            latest["Score"] = round(float(ers), 2)
            latest["_Is_Weighted"] = bool(is_weighted)
            results.append(latest)

        if not results:
            return pd.DataFrame(), "no_results_after_tech_filter"

        # 準決賽：前 30 做結構
        candidates = pd.DataFrame(results).sort_values("Score", ascending=False).head(30).to_dict("records")

        final_list = []
        for row in candidates:
            symbol = str(row.get("Symbol", ""))

            # 結構面
            fundamentals = enrich_fundamentals(symbol)
            row["Structure"] = fundamentals

            # Tag
            weighted = bool(row.get("_Is_Weighted", False))
            vol_threshold = VOL_THRESHOLD_WEIGHTED if weighted else VOL_THRESHOLD_SMALL
            green_range = MA_BIAS_GREEN_WEIGHTED if weighted else MA_BIAS_GREEN_SMALL

            tags = []
            ma_bias = float(row.get("MA_Bias", 0))
            vol_ratio = float(row.get("Vol_Ratio", 0))
            body_power = float(row.get("Body_Power", 0))

            if green_range[0] < ma_bias <= green_range[1]:
                tags.append("🟢起漲")
            if vol_ratio >= vol_threshold:
                tags.append("🔥主力")
            if body_power >= BODY_POWER_STRONG:
                tags.append("⚡真突破")

            suffix = "(觀望)" if session == SESSION_INTRADAY else "(確認)"
            row["Predator_Tag"] = (" ".join(tags) + suffix) if tags else f"○觀察{suffix}"

            # 法人（3日）
            if market == "tw-share" and inst_df_3d is not None and not inst_df_3d.empty:
                inst_info = calc_inst_3d(inst_df_3d, symbol, trade_date)
            else:
                inst_info = {
                    "Inst_Status": "PENDING",
                    "Inst_Streak3": 0,
                    "Inst_Dir3": "PENDING",
                    "Inst_Net_3d": 0.0,
                }

            row["Institutional"] = {
                "Inst_Visual": inst_info.get("Inst_Dir3", "PENDING"),
                "Inst_Net_3d": float(inst_info.get("Inst_Net_3d", 0.0) or 0.0),
                "Inst_Streak3": int(inst_info.get("Inst_Streak3", 0) or 0),
                "Inst_Dir3": inst_info.get("Inst_Dir3", "PENDING"),
                "Inst_Status": inst_info.get("Inst_Status", "PENDING"),
            }

            final_list.append(row)

        if not final_list:
            return pd.DataFrame(), "no_results_after_structure_build"

        df_final = pd.DataFrame(final_list).sort_values("Score", ascending=False).head(TOPN_FINAL)

        # Ranking: Top20 Tier A/B
        df_final = df_final.reset_index(drop=True)
        df_final["rank"] = df_final.index + 1
        df_final["tier"] = df_final["rank"].apply(lambda r: "A" if r <= 10 else "B")
        df_final["top20_flag"] = True

        return df_final, ""

    except Exception as e:
        return pd.DataFrame(), f"Analyzer Crash: {type(e).__name__}: {str(e)}"


# ======================================================
# JSON payload (V15.6.3)
# ======================================================

def generate_ai_json_v1563(stocks: list, market: str, session: str, macro: dict) -> str:
    payload = {
        "meta": {
            "system": "Predator V15.6.3",
            "market": market,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "session": session,
        },
        "macro": macro,
        "stocks": []
    }

    for s in stocks:
        symbol = str(s.get("Symbol", "Unknown"))
        payload["stocks"].append({
            "Symbol": symbol,
            "Price": float(s.get("Close", 0) or 0),
            "ranking": {
                "symbol": symbol,
                "rank": int(s.get("rank", 0) or 0),
                "tier": s.get("tier", "B"),
                "top20_flag": bool(s.get("top20_flag", False)),
            },
            "Technical": {
                "MA_Bias": round(float(s.get("MA_Bias", 0) or 0), 2),
                "Vol_Ratio": round(float(s.get("Vol_Ratio", 0) or 0), 2),
                "Body_Power": round(float(s.get("Body_Power", 0) or 0), 1),
                "Score": round(float(s.get("Score", 0) or 0), 1),
                "Tag": s.get("Predator_Tag", ""),
            },
            "Institutional": s.get("Institutional", {}),
            "Structure": s.get("Structure", {}),
            "FinalDecision": s.get("FinalDecision", {}),
        })

    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
