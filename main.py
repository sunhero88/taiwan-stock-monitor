# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, date
import streamlit as st

from downloader_tw import get_market_snapshot  # ✅ 只依賴這個統一入口
from arbiter import arbiter_run               # ✅ 你的統一裁決入口


# -----------------------------
# Streamlit config
# -----------------------------
st.set_page_config(layout="wide")
st.title("Sunhero｜股市智能超盤中控台（Data-Layer + Arbiter Orchestrator）")


# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.header("模式 / 交易日")
run_mode = st.sidebar.radio("RUN 模式", ["L1", "L2", "L3"], index=0)
session = st.sidebar.selectbox("Session", ["EOD", "INTRADAY"], index=0)

target_dt = st.sidebar.date_input("目標日期（台北）", value=date.today())
topn = st.sidebar.slider("TopN（上市成交額排序）", min_value=5, max_value=50, value=20, step=1)

refresh = st.sidebar.button("立即更新")


# -----------------------------
# Caching snapshot
# -----------------------------
@st.cache_data(ttl=180, show_spinner=False)
def get_snapshot_cached(target_iso: str, session: str, topn: int):
    # ✅ 這裡的呼叫方式固定為：get_market_snapshot(target_iso, session=..., topn=...)
    return get_market_snapshot(target_iso, session=session, topn=topn)


# -----------------------------
# UI
# -----------------------------
col_left, col_right = st.columns([1.15, 0.85], gap="large")

with col_left:
    st.subheader("市場狀態（以資料層輸出為準）")

    target_iso = target_dt.strftime("%Y-%m-%d")

    try:
        if refresh:
            # ✅ 強制清掉 cache，避免你一直「改了檔，但 cache 還抓舊函式」
            st.cache_data.clear()

        snapshot = get_snapshot_cached(target_iso, session, int(topn))
    except Exception as e:
        st.error(f"Snapshot 取得失敗：{e}")
        st.stop()

    # --- KPI (安全顯示) ---
    k1, k2, k3, k4 = st.columns(4)

    twii = snapshot.get("macro", {}).get("overview", {}).get("twii_close")
    twii_chg = snapshot.get("macro", {}).get("overview", {}).get("twii_chg")
    twii_pct = snapshot.get("macro", {}).get("overview", {}).get("twii_pct")
    twii_src = snapshot.get("audit", {}).get("TWII", {}).get("source", "—")
    twii_err = snapshot.get("audit", {}).get("TWII", {}).get("error", None)

    amount_twse = snapshot.get("macro", {}).get("market_amount", {}).get("amount_twse")
    amount_tpex = snapshot.get("macro", {}).get("market_amount", {}).get("amount_tpex")
    amount_total = snapshot.get("macro", {}).get("market_amount", {}).get("amount_total")

    with k1:
        st.metric(
            "加權指數 TWII",
            value="—" if twii is None else f"{twii:,.2f}",
            delta=None if (twii_chg is None or twii_pct is None) else f"{twii_chg:+.2f} ({twii_pct:+.2%})",
        )
        st.caption(f"來源：{twii_src}" + (f"｜錯誤：{twii_err}" if twii_err else ""))

    with k2:
        st.metric("上市成交額（TWSE）", value="—" if amount_twse is None else f"{amount_twse:,.0f}")

    with k3:
        st.metric("上櫃成交額（TPEX）", value="—" if amount_tpex is None else f"{amount_tpex:,.0f}")

    with k4:
        st.metric("總成交額", value="—" if amount_total is None else f"{amount_total:,.0f}")
        st.caption(f"effective_trade_date={snapshot.get('meta', {}).get('effective_trade_date','—')}")

    # --- 三大法人（若你資料層有提供） ---
    st.markdown("### 三大法人")
    inst = snapshot.get("macro", {}).get("institutional", {})
    inst_err = snapshot.get("audit", {}).get("INSTITUTIONAL", {}).get("error")
    if inst_err:
        st.error(f"法人讀取失敗：{inst_err}")
    else:
        st.json(inst)

    st.markdown("---")
    st.subheader("輸入 JSON Payload（可貼 Arbiter JSON 或用範本生成）")

    # 提供一個可用範本（把 snapshot 塞進去）
    example_payload = snapshot.get("arb_input") or {
        "meta": snapshot.get("meta", {}),
        "macro": snapshot.get("macro", {}),
        "stocks": snapshot.get("stocks", []),
    }

    if st.button("載入標準範本（以 TopN + 市場資料層組裝）"):
        st.session_state["json_input"] = json.dumps(example_payload, ensure_ascii=False, indent=2)

    json_input = st.text_area("JSON 內容", height=450, key="json_input")


with col_right:
    st.subheader("執行結果（統一入口：arbiter_run）")

    if st.button("執行（arbiter_run）"):
        try:
            payload = json.loads(st.session_state.get("json_input", "") or "{}")
        except Exception as e:
            st.error(f"JSON 解析錯誤：{e}")
            st.stop()

        try:
            result = arbiter_run(payload, run_mode)
        except Exception as e:
            st.error(f"裁決引擎錯誤：{e}")
            st.stop()

        # 顯示摘要
        verdict = result.get("VERDICT") or result.get("verdict") or "—"
        risk_reason = result.get("RISK_REASON") or result.get("risk_reason") or "—"
        st.success(f"{verdict}｜{risk_reason}")

        st.markdown("### Arbiter 統一輸出")
        st.json(result)
