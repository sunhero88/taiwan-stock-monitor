# main.py
# -*- coding: utf-8 -*-
import json
from datetime import datetime

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_EOD, SESSION_INTRADAY
from arbiter import arbitrate
from institutional_utils import calc_inst_3d

# =========================
# UI Header
# =========================
st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
st.title("Sunhero｜股市智能超盤中控台")

# =========================
# Settings
# =========================
DATA_CSV = "data_tw-share.csv"  # your repo file
SESSION = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)

# Optional: you can swap to data_tw.csv if you want broader coverage
# DATA_CSV = st.sidebar.text_input("Data CSV", value=DATA_CSV)

# =========================
# Load price data
# =========================
@st.cache_data(show_spinner=False)
def load_price_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Expect columns: Symbol, Date, Open, High, Low, Close, Volume (your dataset should match)
    return df


# =========================
# Institutional data source (placeholder)
# You must implement get_institutional_df() to return:
# columns: date (YYYY-MM-DD), symbol (e.g. 2330.TW), net_amount (float)
# =========================
@st.cache_data(show_spinner=False, ttl=300)
def get_institutional_df(trade_date: str) -> pd.DataFrame:
    """
    Minimal placeholder: return empty DF => inst_status will remain PENDING.
    Replace this with your existing FinMind / TWSE institutional fetch.
    """
    return pd.DataFrame(columns=["date", "symbol", "net_amount"])


def compute_market_amount(df: pd.DataFrame, latest_date: pd.Timestamp) -> float:
    d = df[df["Date"] == latest_date].copy()
    for c in ["Close", "Volume"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["Amount"] = (d["Close"] * d["Volume"]).fillna(0)
    return float(d["Amount"].sum())


def normalize_trade_date(ts: pd.Timestamp) -> str:
    # ts expected as pandas Timestamp
    return ts.strftime("%Y-%m-%d")


def decide_macro_inst_status(top20_inst_ready_ratio: float) -> str:
    # Conservative: require at least 80% ready to claim READY
    return "READY" if top20_inst_ready_ratio >= 0.8 else "PENDING"


# =========================
# Main pipeline
# =========================
try:
    price_df = load_price_data(DATA_CSV)
except Exception as e:
    st.error(f"讀取 {DATA_CSV} 失敗：{e}")
    st.stop()

# Ensure Date exists and parse
if "Date" not in price_df.columns:
    st.error("CSV 缺少 Date 欄位")
    st.stop()

price_df["Date"] = pd.to_datetime(price_df["Date"], errors="coerce")
latest_date = price_df["Date"].max()
if pd.isna(latest_date):
    st.error("Date 解析失敗（全是空值）")
    st.stop()

trade_date = normalize_trade_date(latest_date)

# Analyzer => Top10 (+ Score/Tag/Structure)
df_top10, err = run_analysis(price_df, session=SESSION)
if err:
    st.warning(f"Analyzer: {err}")

# Analyzer internally ranks candidates; but we need Top20 for arbiter universe
# We will reconstruct Top20 by re-running selection inside analyzer is complex.
# Simple approach: rely on analyzer's internal head(20) — but run_analysis returns Top10.
# Therefore: we load Top20 by calling run_analysis twice is not available.
# If you want Top20, extend analyzer.run_analysis to return both Top20 & Top10.
# For now, we proceed with Top10 output and treat them as rank 1..10 Tier A.

if df_top10 is None or df_top10.empty:
    st.error("Top10 空白，無法產出 JSON")
    st.stop()

# Market amount (approx)
market_amount = compute_market_amount(price_df, latest_date)

# Institutional fetch (3-day)
inst_df = get_institutional_df(trade_date)

# Merge Institutional 3D into each stock
stocks_out = []
ready_count = 0
total_count = 0

# Build macro overview first; inst_status decided after we compute readiness
macro_overview = {
    "amount": f"{market_amount:,.0f}",
    "inst_net": "待更新",  # you can compute total net from inst_df if you have it
    "trade_date": trade_date,
    "inst_status": "PENDING",
    "inst_dates_3d": [],
    "kill_switch": False,
    "v14_watch": False,
    "degraded_mode": True,
}

# Generate base JSON from analyzer (without institutional)
base_json_str = generate_ai_json(df_top10, market="tw-share", session=SESSION, macro_data={"overview": macro_overview, "indices": []})
base = json.loads(base_json_str)

# Now enrich each stock with Institutional + FinalDecision
for s in base.get("stocks", []):
    total_count += 1
    symbol = s.get("Symbol")

    # Institutional 3D
    if inst_df is not None and (not inst_df.empty):
        inst3 = calc_inst_3d(inst_df, symbol, trade_date)
    else:
        inst3 = {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
        }

    if inst3.get("Inst_Status") == "READY":
        ready_count += 1

    # Visual formatting
    net3 = float(inst3.get("Inst_Net_3d", 0.0))
    s["Institutional"] = {
        "Inst_Visual": "PENDING" if inst3.get("Inst_Status") != "READY" else f"{net3/1_000_000:.1f}M",
        "Inst_Net_3d": net3,
        "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
        "Inst_Dir3": inst3.get("Inst_Dir3", "PENDING"),
        "Inst_Status": inst3.get("Inst_Status", "PENDING"),
    }

    stocks_out.append(s)

# Decide inst_status & degraded_mode
ready_ratio = (ready_count / total_count) if total_count else 0.0
inst_status_final = decide_macro_inst_status(ready_ratio)
degraded_mode_final = (inst_status_final != "READY")

base["macro"]["overview"]["inst_status"] = inst_status_final
base["macro"]["overview"]["degraded_mode"] = degraded_mode_final
base["macro"]["overview"]["inst_dates_3d"] = sorted(inst_df["date"].unique().tolist())[-3:] if (inst_df is not None and not inst_df.empty and "date" in inst_df.columns) else []

# Apply arbiter per stock, dual accounts
for i, s in enumerate(stocks_out):
    cons = arbitrate(s, base["macro"]["overview"], account="Conservative")
    aggr = arbitrate(s, base["macro"]["overview"], account="Aggressive")
    s["FinalDecision"] = {"Conservative": cons, "Aggressive": aggr}

base["stocks"] = stocks_out

# Output JSON
st.subheader("AI JSON（V15.6.3）")
st.code(json.dumps(base, ensure_ascii=False, indent=2), language="json")

# Simple table view
st.subheader("Top10 概覽")
rows = []
for s in base["stocks"]:
    rows.append(
        {
            "Symbol": s["Symbol"],
            "Price": s.get("Price"),
            "Rank": s.get("ranking", {}).get("rank"),
            "Tag": s.get("Technical", {}).get("Tag"),
            "Inst_Status": s.get("Institutional", {}).get("Inst_Status"),
            "Inst_Dir3": s.get("Institutional", {}).get("Inst_Dir3"),
            "Cons_Decision": s.get("FinalDecision", {}).get("Conservative", {}).get("Decision"),
            "Aggr_Decision": s.get("FinalDecision", {}).get("Aggressive", {}).get("Decision"),
        }
    )
st.dataframe(pd.DataFrame(rows))
