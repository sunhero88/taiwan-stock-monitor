# analyzer.py
# -*- coding: utf-8 -*-
"""
Filename: analyzer.py
Version: Predator V15.6.3 (Frozen / Production) - Analyzer Engine
Goal:
- Keep core selection logic intact
- Output JSON schema compatible with Arbiter (V15.6.3)
- Minimal-risk additions: macro.overview fields, per-stock ranking/inst/risk/orphan/weaken fields

Notes:
- JSON is the only trusted source for Arbiter. This module must be deterministic on the same input DF.
- Fundamentals via yfinance.info can be flaky; failures are tolerated (values default to 0/Unknown).
"""
from __future__ import annotations

import json
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

# ======================================================
# 1) Parameters (keep original logic)
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
# 2) Helpers
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
    Important: yfinance revenueGrowth is NOT guaranteed QoQ; label as Rev_Growth and note source.
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
        opm = info.get("operatingMargins")
        rev_g = info.get("revenueGrowth")
        pe = info.get("trailingPE")
        sector = info.get("sector", "Unknown")

        data["OPM"] = round(float(opm or 0.0) * 100.0, 2)
        data["Rev_Growth"] = round(float(rev_g or 0.0) * 100.0, 2)
        data["PE"] = round(float(pe or 0.0), 2)
        data["Sector"] = (sector or "Unknown")
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


def _safe_float(x, default=0.0) -> float:
    try:
        v = float(x)
        if np.isfinite(v):
            return v
        return float(default)
    except Exception:
        return float(default)


# ======================================================
# 3) Core
# ======================================================


def run_analysis(df: pd.DataFrame, session: str = SESSION_INTRADAY):
    """
    Return: (df_topN, err_msg)
    - df_topN: top 10 with required columns for UI/JSON
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame(), "Input DataFrame is empty"

        df = df.copy()

        # Normalize Date column existence
        if "Date" not in df.columns:
            df = df.reset_index(drop=False)
            if "Date" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "Date"})

        df = _ensure_columns(df)
        df = _coerce_types(df)

        if df["Date"].isna().all():
            return pd.DataFrame(), "Date invalid (all NaT)"

        # Latest trading day
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

        # Tech filter
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
            ers = ((latest["Vol_Ratio"] * 20.0) + (max(0.0, 15.0 - abs(latest["MA_Bias"])) * 2.0)) * (
                1.0 - 0.5 * penalty
            )

            latest["Score"] = round(float(ers), 2)
            latest["_Is_Weighted"] = bool(is_weighted)
            results.append(latest)

        if not results:
            return pd.DataFrame(), "no_results_after_tech_filter"

        # Rank candidates by Score
        df_candidates = pd.DataFrame(results).sort_values("Score", ascending=False).head(20).copy()
        if df_candidates.empty:
            return pd.DataFrame(), "no_candidates"

        # Add Rank (1..N)
        df_candidates["Rank"] = np.arange(1, len(df_candidates) + 1)

        # Fundamentals filter (keep original QoQ<0 kill-switch but applied to Rev_Growth)
        final_list = []
        for _, row in df_candidates.iterrows():
            symbol = str(row.get("Symbol", ""))

            fundamentals = enrich_fundamentals(symbol)
            row_dict = row.to_dict()
            row_dict["Structure"] = fundamentals

            # Kill Switch III: Rev_Growth < 0 (previously QoQ < 0)
            try:
                if fundamentals.get("Rev_Growth", 0) is not None and float(fundamentals.get("Rev_Growth", 0)) < 0:
                    # Keep behavior: filter out negative growth candidates
                    continue
            except Exception:
                pass

            weighted = bool(row_dict.get("_Is_Weighted", False))
            vol_threshold = VOL_THRESHOLD_WEIGHTED if weighted else VOL_THRESHOLD_SMALL
            green_range = MA_BIAS_GREEN_WEIGHTED if weighted else MA_BIAS_GREEN_SMALL

            tags = []
            ma_bias = _safe_float(row_dict.get("MA_Bias", 0))
            vol_ratio = _safe_float(row_dict.get("Vol_Ratio", 0))
            body_power = _safe_float(row_dict.get("Body_Power", 0))

            if green_range[0] < ma_bias <= green_range[1]:
                tags.append("起漲")
            if vol_ratio >= vol_threshold:
                tags.append("主力")
            if body_power >= BODY_POWER_STRONG:
                tags.append("真突破")

            suffix = "(觀望)" if session == SESSION_INTRADAY else "(確認)"
            row_dict["Predator_Tag"] = (" ".join(tags) + suffix) if tags else ("觀察" + suffix)

            final_list.append(row_dict)

        if not final_list:
            return pd.DataFrame(), "no_results_after_fundamental_filter"

        df_final = pd.DataFrame(final_list).sort_values("Score", ascending=False).head(10).copy()
        # Ensure Rank exists in final output
        if "Rank" not in df_final.columns:
            df_final["Rank"] = np.arange(1, len(df_final) + 1)

        return df_final, ""

    except Exception as e:
        return pd.DataFrame(), f"Analyzer Crash: {type(e).__name__}: {str(e)}"


# ======================================================
# 4) JSON payload (V15.6.3 schema for Arbiter)
# ======================================================


def generate_ai_json(
    df_top: pd.DataFrame,
    market: str = "tw-share",
    session: str = SESSION_INTRADAY,
    macro_data: dict | None = None,
) -> str:
    """
    Output schema highlights:
    - meta: system/market/timestamp/session
    - macro.overview: amount/inst_net/trade_date/inst_status/degraded_mode/kill_switch/v14_watch/inst_dates_3d
    - stocks[]: Symbol/Price/Technical/Institutional/Structure/risk/orphan_holding/weaken_flags/ranking
    """
    if df_top is None or df_top.empty:
        return json.dumps({"error": "No data"}, ensure_ascii=False, indent=2)

    macro_data = macro_data or {}
    overview = (macro_data.get("overview") or {}).copy()

    # --- Normalize macro.overview to Arbiter-required fields
    inst_status = overview.get("inst_status", "PENDING")
    degraded_mode = bool(overview.get("degraded_mode", inst_status != "READY"))

    overview.setdefault("amount", overview.get("amount", "待更新"))
    overview.setdefault("inst_net", overview.get("inst_net", "待更新"))
    overview.setdefault("trade_date", overview.get("trade_date", datetime.now().strftime("%Y-%m-%d")))
    overview["inst_status"] = inst_status
    overview["degraded_mode"] = degraded_mode
    overview.setdefault("kill_switch", False)
    overview.setdefault("v14_watch", False)
    overview.setdefault("inst_dates_3d", overview.get("inst_dates_3d", []))

    macro_out = {
        "overview": overview,
        "indices": macro_data.get("indices", []),
    }

    records = df_top.to_dict("records")
    stocks = []

    for r in records:
        symbol = str(r.get("Symbol", "Unknown"))
        rank = int(_safe_float(r.get("Rank", 999), default=999))

        # --- Technical block
        tech = {
            "MA_Bias": round(_safe_float(r.get("MA_Bias", 0)), 2),
            "Vol_Ratio": round(_safe_float(r.get("Vol_Ratio", 0)), 2),
            "Body_Power": round(_safe_float(r.get("Body_Power", 0)), 1),
            "Score": round(_safe_float(r.get("Score", 0)), 1),
            "Tag": r.get("Predator_Tag", ""),
        }

        # --- Structure block (already embedded by run_analysis)
        struct = r.get("Structure", {}) or {}
        # Ensure Rev_Growth key exists consistently
        if "Rev_Growth" not in struct and "QoQ" in struct:
            struct["Rev_Growth"] = struct.pop("QoQ")
            struct["Rev_Growth_Source"] = "legacy:QoQ_renamed"

        # --- Institutional block
        # If upstream main.py already merged inst fields, keep them; else default PENDING.
        inst = r.get("Institutional", {}) or {}
        inst_out = {
            "Inst_Visual": inst.get("Inst_Visual", inst_status if inst_status != "READY" else "N/A"),
            "Inst_Net_3d": _safe_float(inst.get("Inst_Net_3d", 0.0)),
            "Inst_Streak3": int(_safe_float(inst.get("Inst_Streak3", 0), default=0)),
            "Inst_Dir3": inst.get("Inst_Dir3", "PENDING"),
            "Inst_Status": inst.get("Inst_Status", "PENDING"),
        }

        # --- Risk defaults (Arbiter reads these caps)
        risk = r.get("risk", {}) or {}
        risk_out = {
            "position_pct_max": int(_safe_float(risk.get("position_pct_max", 12), default=12)),
            "risk_per_trade_max": _safe_float(risk.get("risk_per_trade_max", 1), default=1),
            "trial_flag": bool(risk.get("trial_flag", True)),
        }

        # --- Orphan & weaken flags (default safe)
        orphan_holding = bool(r.get("orphan_holding", False))
        weaken_flags = r.get("weaken_flags", {}) or {
            "technical_weaken": False,
            "structure_weaken": False,
        }
        weaken_flags.setdefault("technical_weaken", False)
        weaken_flags.setdefault("structure_weaken", False)

        # --- Ranking block (Top20 pool)
        ranking = {
            "symbol": symbol,
            "rank": rank,
            "tier": "A" if rank <= 10 else "B",
            "top20_flag": rank <= 20,
        }

        stocks.append(
            {
                "Symbol": symbol,
                "Price": _safe_float(r.get("Close", 0.0)),
                "ranking": ranking,
                "Technical": tech,
                "Institutional": inst_out,
                "Structure": struct,
                "risk": risk_out,
                "orphan_holding": orphan_holding,
                "weaken_flags": weaken_flags,
            }
        )

    payload = {
        "meta": {
            "system": "Predator V15.6.3",
            "market": market,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "session": session,
        },
        "macro": macro_out,
        "stocks": stocks,
    }

    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
