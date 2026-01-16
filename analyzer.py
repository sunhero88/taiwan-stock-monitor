# =========================
# analyzer.py
# Predator V15.5.3 Patch (Inst pass-through + Rev_Growth rename)
# =========================
# -*- coding: utf-8 -*-
"""
Filename: analyzer.py
Version: Predator V15.5.3 (Inst-Aware + Rev_Growth)
"""
import pandas as pd
import numpy as np
import json
import yfinance as yf
from datetime import datetime

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
    data = {"OPM": 0, "Rev_Growth": 0, "PE": 0, "Sector": "Unknown"}
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
    optional = ["Inst_Net", "Inst_Status"]
    for c in required + optional:
        if c not in df.columns:
            df[c] = np.nan
    return df


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["Open", "High", "Low", "Close", "Volume", "Inst_Net"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=False)
    df["Symbol"] = df["Symbol"].astype(str)
    df["Inst_Status"] = df["Inst_Status"].astype(str)
    return df


def run_analysis(df: pd.DataFrame, session: str = SESSION_EOD):
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

        df_today["Amount"] = (df_today["Close"] * df_today["Volume"]).fillna(0)
        if len(df_today) > 50:
            top_50_amt_threshold = df_today["Amount"].nlargest(50).min()
        else:
            top_50_amt_threshold = 0

        weighted_symbols = set(df_today.loc[df_today["Amount"] >= top_50_amt_threshold, "Symbol"].dropna().astype(str))

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

            if latest["Body_Power"] < BODY_POWER_DISTRIBUTE and latest["Vol_Ratio"] > DISTRIBUTE_VOL_RATIO:
                continue
            if latest["MA_Bias"] > MA_BIAS_HARD_CAP:
                continue

            penalty = calc_ma_bias_penalty(latest["MA_Bias"])
            ers = (
                (latest["Vol_Ratio"] * 20.0) +
                (max(0.0, 15.0 - abs(latest["MA_Bias"])) * 2.0)
            ) * (1.0 - 0.5 * penalty)

            latest["Score"] = round(float(ers), 2)
            latest["_Is_Weighted"] = bool(is_weighted)

            # 保留籌碼欄位（上游已合併）
            latest["Inst_Net"] = float(latest.get("Inst_Net", 0) or 0)
            latest["Inst_Status"] = str(latest.get("Inst_Status", "N/A"))

            results.append(latest)

        if not results:
            return pd.DataFrame(), "no_results_after_tech_filter"

        candidates = pd.DataFrame(results).sort_values("Score", ascending=False).head(15).to_dict("records")

        final_list = []
        for row in candidates:
            symbol = str(row.get("Symbol", ""))
            fundamentals = enrich_fundamentals(symbol)
            row["Structure"] = fundamentals

            # 結構濾網：Rev_Growth < 0 剔除（你要放寬可註解）
            try:
                if fundamentals.get("Rev_Growth", 0) is not None and float(fundamentals.get("Rev_Growth", 0)) < 0:
                    continue
            except Exception:
                pass

            weighted = bool(row.get("_Is_Weighted", False))
            vol_threshold = VOL_THRESHOLD_WEIGHTED if weighted else VOL_THRESHOLD_SMALL
            green_range = MA_BIAS_GREEN_WEIGHTED if weighted else MA_BIAS_GREEN_SMALL

            tags = []
            ma_bias = float(row.get("MA_Bias", 0))
            vol_ratio = float(row.get("Vol_Ratio", 0))
            body_power = float(row.get("Body_Power", 0))

            if green_range[0] < ma_bias <= green_range[1]:
                tags.append("起漲")
            if vol_ratio >= vol_threshold:
                tags.append("主力")
            if body_power >= BODY_POWER_STRONG:
                tags.append("真突破")

            suffix = "(確認)" if session == SESSION_EOD else "(觀望)"
            row["Predator_Tag"] = (" ".join(tags) + suffix) if tags else "觀察"

            final_list.append(row)

        if not final_list:
            return pd.DataFrame(), "no_results_after_fundamental_filter"

        df_final = pd.DataFrame(final_list).sort_values("Score", ascending=False).head(10)
        if "Inst_Status" not in df_final.columns:
            df_final["Inst_Status"] = "N/A"
        if "Inst_Net" not in df_final.columns:
            df_final["Inst_Net"] = 0.0

        return df_final, ""

    except Exception as e:
        return pd.DataFrame(), f"Analyzer Crash: {type(e).__name__}: {str(e)}"


def generate_ai_json(df_top10: pd.DataFrame, market: str = "tw-share", session: str = SESSION_EOD, macro_data=None) -> str:
    if df_top10 is None or df_top10.empty:
        return json.dumps({"error": "No data"}, ensure_ascii=False, indent=2)

    records = df_top10.to_dict("records")
    stocks = []
    for r in records:
        stocks.append({
            "Symbol": str(r.get("Symbol", "Unknown")),
            "Price": float(r.get("Close", 0) or 0),
            "Technical": {
                "MA_Bias": round(float(r.get("MA_Bias", 0) or 0), 2),
                "Vol_Ratio": round(float(r.get("Vol_Ratio", 0) or 0), 2),
                "Body_Power": round(float(r.get("Body_Power", 0) or 0), 1),
                "Score": round(float(r.get("Score", 0) or 0), 1),
                "Tag": r.get("Predator_Tag", ""),
            },
            "Inst_Visual": r.get("Inst_Status", "N/A"),
            "Inst_Net_Raw": float(r.get("Inst_Net", 0) or 0),
            "Structure": r.get("Structure", {}),
        })

    payload = {
        "meta": {
            "system": "Predator V15.5.3",
            "market": market,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "session": session,
        },
        "macro": macro_data if macro_data else {},
        "stocks": stocks,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
