# -*- coding: utf-8 -*-
"""
Filename: analyzer.py
Version: Predator V15.6 (Inst 3D + Dual Engine + Rev_Growth)
"""

import pandas as pd
import numpy as np
import json
import yfinance as yf
from datetime import datetime, timedelta

# ======================================================
# 1) Parameters
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

# ======================================================
# 2) FinMind trade date helpers (pure logic; main.py calls API)
# ======================================================

def get_recent_finmind_trade_dates(anchor_date: str, lookback_days: int = 12, need_days: int = 3):
    """
    Returns last `need_days` date strings <= anchor_date by probing date list.
    main.py will validate data existence per date; here we just generate candidates.
    """
    try:
        d0 = datetime.strptime(anchor_date, "%Y-%m-%d")
    except Exception:
        d0 = datetime.now()

    candidates = [(d0 - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
    # main.py will keep the dates where API returns data; but for caching we pass candidates.
    # Here return full candidate list; main.py will choose actual 3.
    # However in V15.6 we want analyzer to supply a stable list -> main will filter.
    # We'll still return candidates and let main.py decide; but for simplicity we return candidates.
    return candidates[:lookback_days]

def inst_direction_3d(d0: float, d1: float, d2: float) -> str:
    """
    Direction classification for 3 days: BUY / SELL / FLAT
    """
    a = [float(d2), float(d1), float(d0)]  # oldest -> newest
    # consider tiny values as flat
    eps = 1e-9
    signs = []
    for x in a:
        if abs(x) <= eps:
            signs.append(0)
        elif x > 0:
            signs.append(1)
        else:
            signs.append(-1)

    if signs == [1, 1, 1]:
        return "BUY"
    if signs == [-1, -1, -1]:
        return "SELL"
    if signs == [0, 0, 0]:
        return "FLAT"
    return "MIXED"

def inst_streak_3d(d0: float, d1: float, d2: float) -> int:
    """
    Streak of same direction ending at most recent day (d0).
    Returns 0~3:
      - If d0 == 0 => 0
      - Else count consecutive days backwards with same sign as d0
    """
    vals = [float(d0), float(d1), float(d2)]  # newest -> oldest
    eps = 1e-9

    def sgn(x):
        if abs(x) <= eps:
            return 0
        return 1 if x > 0 else -1

    s0 = sgn(vals[0])
    if s0 == 0:
        return 0

    streak = 1
    for x in vals[1:]:
        if sgn(x) == s0:
            streak += 1
        else:
            break
    return streak

# ======================================================
# 3) Helpers
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
    Structure fields:
      - OPM: operatingMargins * 100
      - Rev_Growth: revenueGrowth * 100 (SOURCE MUST BE DECLARED)
      - PE: trailingPE
      - Sector
      - Rev_Growth_Source
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
# 4) Dual decision engine (V15.6)
# ======================================================

def decide_conservative(row: dict, session: str, inst_ready: bool) -> str:
    """
    Conservative account:
      - Must be EOD AND inst_ready
      - Must have inst_streak3 == 3 AND direction BUY
      - Prefer tags include 主力 and not extreme MA_Bias
    Output: BUY / WATCH / AVOID
    """
    if session != SESSION_EOD or not inst_ready:
        return "WATCH"

    streak = int(row.get("Inst_Streak3", 0) or 0)
    ddir = str(row.get("Inst_Dir3", "PENDING"))
    ma_bias = float(row.get("MA_Bias", 0) or 0)
    vol_ratio = float(row.get("Vol_Ratio", 0) or 0)

    if streak == 3 and ddir == "BUY" and ma_bias <= 12 and vol_ratio >= 1.0:
        return "BUY"
    if ma_bias > MA_BIAS_HARD_CAP:
        return "AVOID"
    return "WATCH"

def decide_aggressive(row: dict, session: str, inst_ready: bool) -> str:
    """
    Aggressive account:
      - INTRADAY: can allow TRIAL if Vol_Ratio >= threshold or Body_Power strong
      - EOD: prefer inst_streak3 >= 2 with BUY
    Output: BUY / TRIAL / WATCH / AVOID
    """
    ma_bias = float(row.get("MA_Bias", 0) or 0)
    vol_ratio = float(row.get("Vol_Ratio", 0) or 0)
    body = float(row.get("Body_Power", 0) or 0)

    if ma_bias > MA_BIAS_HARD_CAP:
        return "AVOID"

    if session == SESSION_EOD and inst_ready:
        streak = int(row.get("Inst_Streak3", 0) or 0)
        ddir = str(row.get("Inst_Dir3", "PENDING"))
        if streak >= 2 and ddir == "BUY" and vol_ratio >= 1.0:
            return "BUY"
        return "WATCH"

    # INTRADAY or inst not ready: allow TRIAL with strict risk
    if vol_ratio >= 1.5 or body >= BODY_POWER_STRONG:
        return "TRIAL"
    return "WATCH"

# ======================================================
# 5) Core analysis
# ======================================================

def run_analysis(df: pd.DataFrame, session: str = SESSION_INTRADAY, inst_ready: bool = False):
    """
    Return: (df_top10, err_msg)
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

            # Kill Switch I: distribute trap
            if latest["Body_Power"] < BODY_POWER_DISTRIBUTE and latest["Vol_Ratio"] > DISTRIBUTE_VOL_RATIO:
                continue

            # Kill Switch II: MA_Bias hard cap
            if latest["MA_Bias"] > MA_BIAS_HARD_CAP:
                continue

            penalty = calc_ma_bias_penalty(latest["MA_Bias"])
            ers = ((latest["Vol_Ratio"] * 20.0) + (max(0.0, 15.0 - abs(latest["MA_Bias"])) * 2.0)) * (1.0 - 0.5 * penalty)

            latest["Score"] = round(float(ers), 2)
            latest["_Is_Weighted"] = bool(is_weighted)
            results.append(latest)

        if not results:
            return pd.DataFrame(), "no_results_after_tech_filter"

        candidates = pd.DataFrame(results).sort_values("Score", ascending=False).head(15).to_dict("records")

        final_list = []
        for row in candidates:
            symbol = str(row.get("Symbol", ""))

            fundamentals = enrich_fundamentals(symbol)
            row["Structure"] = fundamentals

            # Optional: if you still want a hard filter based on Rev_Growth < 0, do it here
            # For now, DO NOT hard-kill to avoid data-definition risk.

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

            # V15.5.6+ confirmation gate
            if session == SESSION_EOD and inst_ready:
                suffix = "(確認)"
            else:
                suffix = "(觀望)"

            row["Predator_Tag"] = (" ".join(tags) + suffix) if tags else ("觀察" + suffix)

            # V15.6 dual decisions (inst streak will be merged in main.py; default 0 if missing)
            row["Decision_Conservative"] = decide_conservative(row, session=session, inst_ready=inst_ready)
            row["Decision_Aggressive"] = decide_aggressive(row, session=session, inst_ready=inst_ready)

            final_list.append(row)

        if not final_list:
            return pd.DataFrame(), "no_results_after_structure_step"

        df_final = pd.DataFrame(final_list).sort_values("Score", ascending=False).head(10)
        return df_final, ""

    except Exception as e:
        return pd.DataFrame(), f"Analyzer Crash: {type(e).__name__}: {str(e)}"


# ======================================================
# 6) JSON payload
# ======================================================

def generate_ai_json(df_top10: pd.DataFrame, market: str = "tw-share", session: str = SESSION_INTRADAY, macro_data=None, inst_ready: bool = False) -> str:
    if df_top10 is None or df_top10.empty:
        return json.dumps({"error": "No data"}, ensure_ascii=False, indent=2)

    records = df_top10.to_dict("records")
    stocks = []

    for r in records:
        # Inst fields (if merged)
        inst_visual = r.get("Inst_Visual", "PENDING")
        inst_net_3d = float(r.get("Inst_Net_3d", 0) or 0)
        inst_streak3 = int(r.get("Inst_Streak3", 0) or 0)
        inst_dir3 = r.get("Inst_Dir3", "PENDING")

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
            "Institutional": {
                "Inst_Visual": inst_visual,
                "Inst_Net_3d": inst_net_3d,
                "Inst_Streak3": inst_streak3,
                "Inst_Dir3": inst_dir3,
                "Inst_Status": "READY" if inst_ready else "PENDING",
            },
            "Structure": r.get("Structure", {}),
            "Decision": {
                "Conservative": r.get("Decision_Conservative", "WATCH"),
                "Aggressive": r.get("Decision_Aggressive", "WATCH"),
            }
        })

    payload = {
        "meta": {
            "system": "Predator V15.6",
            "market": market,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "session": session,
        },
        "macro": macro_data if macro_data else {},
        "stocks": stocks,
    }

    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
