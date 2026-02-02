# app.py
# -*- coding: utf-8 -*-

import json
import streamlit as st
import pandas as pd

from main import build_arbiter_input

st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")

st.title("Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.2 Enhanced SIM-FREE）")

with st.sidebar:
    st.header("設定")
    session = st.selectbox("Session", ["PREMARKET", "INTRADAY", "POSTMARKET"], index=1)
    topn = st.selectbox("TopN（固定池化數量）", [10, 20, 30, 50], index=1)
    allow_insecure_ssl = st.checkbox("允許不安全 SSL (verify=False)", value=False)
    st.divider()

    st.subheader("持倉（手動貼 JSON array）")
    st.caption('格式範例：\n[{"symbol":"2330.TW","shares":100,"avg_cost":1000}]')
    positions_text = st.text_area("positions", value="[]", height=120)

    cash_balance = st.number_input("cash_balance (NTD)", value=2000000, step=10000)
    total_equity = st.number_input("total_equity (NTD)", value=2000000, step=10000)

    run = st.button("Run")

def _safe_load_positions(s: str):
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return v
    except Exception:
        pass
    return []

if run:
    positions = _safe_load_positions(positions_text)

    # 覆寫 account（不改 main.py 的 load_account，也能直接測）
    payload = build_arbiter_input(
        session=session,
        topn=int(topn),
        allow_insecure_ssl=bool(allow_insecure_ssl),
    )
    payload["account"]["cash_balance"] = int(cash_balance)
    payload["account"]["total_equity"] = int(total_equity)
    payload["account"]["positions"] = positions

    ov = payload.get("macro", {}).get("overview", {})
    indices = payload.get("macro", {}).get("indices", [])
    stocks = payload.get("stocks", [])

    # 頂部 KPI
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("交易日（最後收盤）", ov.get("trade_date", "-"))
    c2.metric("Regime", ov.get("regime", "-"))
    c3.metric("SMR", ov.get("regime_metrics", {}).get("SMR", "-"))
    c4.metric("Slope5", ov.get("regime_metrics", {}).get("Slope5", "-"))
    c5.metric("VIX", ov.get("regime_metrics", {}).get("VIX", "-"))
    c6.metric("Max Equity Allowed", f'{ov.get("max_equity_allowed_pct","-")}%')

    st.write("")
    if ov.get("degraded_mode", False):
        st.error(f'Gate：DEGRADED（禁止 BUY/TRIAL）｜原因：{ov.get("market_comment","-")}')

    # 成交金額 + sources（可稽核）
    st.subheader("市場成交金額（best-effort / 可稽核）")
    st.json({
        "amount_twse": ov.get("amount_twse"),
        "amount_tpex": ov.get("amount_tpex"),
        "amount_total": ov.get("amount_total"),
        "sources": ov.get("amount_sources", {}),
        "allow_insecure_ssl": bool(allow_insecure_ssl),
    }, expanded=True)

    # 指數表
    st.subheader("指數快照")
    if indices:
        st.dataframe(pd.DataFrame(indices), use_container_width=True)
    else:
        st.info("indices 無資料")

    # A+ 命中數
    aplus_count = 0
    for s in stocks:
        if s.get("Institutional", {}).get("Layer") == "A+":
            aplus_count += 1

    st.subheader(f"今日分析清單（Top{topn} + 持倉）— 含 A+ Layer（命中數：{aplus_count}）")
    if stocks:
        rows = []
        for s in stocks:
            inst = s.get("Institutional", {})
            tech = s.get("Technical", {})
            rk = s.get("ranking", {})
            rows.append({
                "rank": rk.get("rank"),
                "tier": rk.get("tier"),
                "symbol": s.get("Symbol"),
                "name": s.get("Name"),
                "price": s.get("Price"),
                "tag": tech.get("Tag"),
                "score": tech.get("Score"),
                "vol_ratio": tech.get("Vol_Ratio"),
                "ma_bias(%)": tech.get("MA_Bias"),
                "inst_status": inst.get("Inst_Status"),
                "foreign_buy": inst.get("Foreign_Buy"),
                "trust_buy": inst.get("Trust_Buy"),
                "inst_streak3": inst.get("Inst_Streak3"),
                "inst_dir3": inst.get("Inst_Dir3"),
                "layer": inst.get("Layer"),
                "orphan_holding": s.get("orphan_holding"),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

    # Warnings（最新 50 條）— 你要看的就在這裡
    st.subheader("Warnings（最新 50 條）")
    warns = ov.get("warnings", []) or []
    st.json({
        "warning_count": len(warns),
        "warnings": warns[-50:]
    }, expanded=True)

    st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
    st.json(payload, expanded=False)
