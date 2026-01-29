# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d
from market_amount import fetch_amount_total, intraday_norm

# =========================
# V15.7 固定設定
# =========================
SYSTEM_VERSION = "Predator V15.7"
MAX_TOP_N = 20

# =========================
# 工具
# =========================
def fmt_date(d):
    return pd.to_datetime(d).strftime("%Y-%m-%d")

def yesterday_if_before_open():
    now = datetime.now()
    if now.hour < 9:
        return (now - timedelta(days=1)).date()
    return now.date()

# =========================
# Macro Gate（裁決核心）
# =========================
def decide_macro_gate(macro: dict) -> dict:
    """
    回傳：
    - macro_gate: NORMAL / DEGRADED / BLOCK
    - degraded_mode: bool
    - market_comment: str
    """
    inst_status = macro.get("inst_status")
    amount_label = macro.get("amount_norm_label")
    amount_total = macro.get("amount_total")

    # 絕對防線
    if inst_status == "UNAVAILABLE":
        return {
            "macro_gate": "BLOCK",
            "degraded_mode": True,
            "market_comment": "法人資料不可用，系統進入資料降級：禁止 BUY / TRIAL。"
        }

    if amount_total in (None, "待更新"):
        return {
            "macro_gate": "DEGRADED",
            "degraded_mode": True,
            "market_comment": "成交金額資料不足，僅允許觀察與持倉管理。"
        }

    if amount_label == "LOW" and inst_status != "READY":
        return {
            "macro_gate": "DEGRADED",
            "degraded_mode": True,
            "market_comment": "量能偏低且法人未就位，策略降級。"
        }

    return {
        "macro_gate": "NORMAL",
        "degraded_mode": False,
        "market_comment": "市場條件正常，可依個股訊號操作。"
    }

# =========================
# App
# =========================
def app():
    st.set_page_config(page_title="Sunhero｜Predator V15.7", layout="wide")
    st.title("Sunhero｜Predator V15.7（模擬期）")

    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD])
    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("點選 Run 以產生分析結果")
        return

    # =========================
    # 1) 交易日判定
    # =========================
    trade_date = yesterday_if_before_open()
    trade_date_str = fmt_date(trade_date)

    # =========================
    # 2) 市場成交金額（免費來源）
    # =========================
    amount_total = None
    amount_sources = {}

    try:
        amt = fetch_amount_total()
        amount_total = amt.amount_total
        amount_sources = {
            "twse": amt.source_twse,
            "tpex": amt.source_tpex,
        }
    except Exception as e:
        amount_sources = {"error": str(e)}

    # =========================
    # 3) 載入市場資料（Yahoo）
    # =========================
    df = pd.read_csv("data/data_tw-share.csv")
    df["Date"] = pd.to_datetime(df["Date"])
    df = df[df["Date"] <= pd.to_datetime(trade_date_str)]

    # =========================
    # 4) 個股掃描（全市場 → Top20）
    # =========================
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(err)
        return

    df_top = df_top.head(MAX_TOP_N)

    symbols = df_top["Symbol"].tolist()

    # =========================
    # 5) 法人資料（免費 FinMind）
    # =========================
    inst_status = "UNAVAILABLE"
    inst_dates = []

    try:
        inst_df = fetch_finmind_institutional(
            symbols=symbols,
            start_date=(pd.to_datetime(trade_date_str) - pd.Timedelta(days=10)).strftime("%Y-%m-%d"),
            end_date=trade_date_str,
            token=os.getenv("FINMIND_TOKEN")
        )

        for s in symbols:
            r = calc_inst_3d(inst_df, s, trade_date_str)
            if r.get("Inst_Status") == "READY":
                inst_status = "READY"

        inst_dates = sorted(inst_df["date"].astype(str).unique())[-3:]

    except Exception:
        inst_status = "UNAVAILABLE"

    # =========================
    # 6) 量能正規化（僅資訊用）
    # =========================
    amount_norm = {}
    if amount_total:
        amount_norm = intraday_norm(
            amount_total_now=amount_total,
            amount_total_prev=None,
            avg20_amount_total=None
        )

    # =========================
    # 7) Macro Overview
    # =========================
    macro_overview = {
        "trade_date": trade_date_str,
        "amount_total": amount_total or "待更新",
        "amount_sources": amount_sources,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates,
        **amount_norm
    }

    macro_gate = decide_macro_gate(macro_overview)
    macro_overview.update(macro_gate)

    # =========================
    # 8) JSON 輸出
    # =========================
    payload = generate_ai_json(
        df_top,
        market="tw-share",
        session=session,
        macro_data={"overview": macro_overview, "indices": []}
    )

    # =========================
    # UI
    # =========================
    st.subheader("市場裁決狀態")
    st.info(macro_overview["market_comment"])

    st.subheader("Top20（全市場排名）")
    st.dataframe(df_top, use_container_width=True)

    st.subheader("AI JSON（Arbiter Input）")
    st.code(payload, language="json")

if __name__ == "__main__":
    app()
