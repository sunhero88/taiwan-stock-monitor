# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import streamlit as st
import pandas as pd

from analyzer import (
    TZ_TAIPEI,
    now_taipei,
    build_arbiter_input_sim_free,
)

st.set_page_config(
    page_title="Sunhero | 股市智能超盤中控台",
    layout="wide",
)

# ----------------------------
# UI helpers
# ----------------------------
def parse_positions_json(text: str) -> List[Dict]:
    """
    positions JSON array:
    [
      {"symbol":"2330.TW","qty":100,"avg_cost":1100},
      {"symbol":"2308.TW","qty":450,"avg_cost":1110}
    ]
    """
    t = (text or "").strip()
    if not t:
        return []
    try:
        obj = json.loads(t)
        if isinstance(obj, list):
            out = []
            for p in obj:
                if not isinstance(p, dict):
                    continue
                sym = str(p.get("symbol","")).strip()
                if sym.isdigit():
                    sym = f"{sym}.TW"
                if sym and not sym.endswith(".TW") and sym.replace(".","").isdigit():
                    # keep as-is for non-TW
                    pass
                out.append({
                    "symbol": sym,
                    "qty": int(p.get("qty", 0)),
                    "avg_cost": float(p.get("avg_cost", 0)),
                })
            return out
        return []
    except Exception:
        # fallback: comma separated symbols
        syms = [x.strip() for x in t.split(",") if x.strip()]
        out = []
        for s in syms:
            if s.isdigit():
                s = f"{s}.TW"
            out.append({"symbol": s, "qty": 0, "avg_cost": 0})
        return out

def json_copy_block(obj: Dict):
    """
    顯示 JSON + 複製按鈕（Streamlit 內建 copy）
    """
    st.code(json.dumps(obj, ensure_ascii=False, indent=2), language="json")

# ----------------------------
# Sidebar
# ----------------------------
st.sidebar.header("設定")

session = st.sidebar.selectbox(
    "Session",
    options=["PREOPEN", "INTRADAY", "EOD"],
    index=0,
)

topn = st.sidebar.selectbox(
    "TopN（固定追蹤數量）",
    options=[10, 20, 30, 50],
    index=1,  # default 20
)

st.sidebar.subheader("持倉（會納入追蹤）")
pos_text = st.sidebar.text_area(
    "positions（JSON array，或逗號分隔代碼）",
    value='[{"symbol":"2330.TW","qty":0,"avg_cost":0}]',
    height=140,
)

st.sidebar.subheader("資料取得策略（SIM-FREE）")
verify_ssl = st.sidebar.checkbox(
    "SSL 驗證（官方資料）",
    value=True,
    help="若 Streamlit Cloud 對 TWSE/TPEx 出現 SSL_CERTIFICATE_VERIFY_FAILED，可先關閉以恢復抓取（仍為官方來源，但不驗證憑證）。",
)

max_back_days = st.sidebar.slider(
    "官方資料回溯天數（找最新交易日）",
    min_value=3,
    max_value=20,
    value=10,
    help="系統會從今天往回找，直到抓到有效的『全市場日行情』交易日。",
)

liquidity_pool = st.sidebar.slider(
    "流動性候選池（前N名）",
    min_value=50,
    max_value=500,
    value=200,
    step=50,
    help="Top20 不是固定清單：先用全市場成交金額抓出前N名，再用這批做 60D 指標與打分。",
)

run = st.sidebar.button("Run")

# ----------------------------
# Main
# ----------------------------
st.title("Sunhero｜股市智能超盤中控台（Top20 + 持倉監控 / V15.7 SIM-FREE）")

now_str = now_taipei().strftime("%Y-%m-%d %H:%M")
st.info(f"目前台北時間：{now_str}｜模式：{session}｜TopN：{topn}")

if not run:
    st.caption("請按左側 Run 產生最新資料。")
    st.stop()

positions = parse_positions_json(pos_text)

arb = build_arbiter_input_sim_free(
    session=session,
    topn=int(topn),
    positions=positions,
    verify_ssl=bool(verify_ssl),
    max_back_days=int(max_back_days),
    liquidity_pool=int(liquidity_pool),
)

macro = arb.get("macro", {})
twii = macro.get("twii", {})
warnings = macro.get("warnings", []) or []
degraded = bool(macro.get("degraded_mode", False))
stale_reason = macro.get("stale_reason")

# ----------------------------
# Section: Macro (TWII)
# ----------------------------
st.subheader("台股大盤指數（自動）")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("TWII 日期", twii.get("date") or "None")
c2.metric("TWII 收盤", twii.get("close") if twii.get("close") is not None else "None")
c3.metric("漲跌", twii.get("chg") if twii.get("chg") is not None else "None")
c4.metric("漲跌幅(%)", twii.get("chg_pct") if twii.get("chg_pct") is not None else "None")
c5.metric("來源", twii.get("source") or "None")

if stale_reason:
    st.error(f"資料新鮮度稽核失敗：{stale_reason} → degraded_mode=true（禁止 BUY/TRIAL）")

if warnings:
    with st.expander("警告/降級原因（點開）", expanded=True):
        for w in warnings:
            st.write(f"- {w}")

# ----------------------------
# Section: Top20 + Positions
# ----------------------------
st.subheader("今日分析清單（Top20 + 持倉追加）— 每日動態更新（非固定）")

top = arb.get("top_watchlist", []) or []
top_df = pd.DataFrame(top)

if top_df.empty:
    st.error("TopN 建立失敗：目前無可用 TopN 清單（請看上方警告原因）。")
else:
    # 標記持倉
    pos_syms = set([p.get("symbol","") for p in positions if p.get("symbol")])
    top_df["is_position"] = top_df["symbol"].apply(lambda s: (s in pos_syms))
    st.dataframe(top_df, use_container_width=True)

# ----------------------------
# Section: System decision gate
# ----------------------------
st.subheader("今日系統判斷（白話解釋）")

if degraded:
    st.error("Gate 未通過：degraded_mode=true → 強制禁止 BUY/TRIAL（避免舊資料造成裁決失真）")
else:
    st.success("Gate 通過：資料稽核通過（SIM-FREE）")

# ----------------------------
# Section: AI JSON (copyable)
# ----------------------------
st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")

st.caption("下方 JSON 可直接複製貼給其他 AI；包含：Top20（每日更新）、持倉追加、TWII 大盤資料、法人連續性（近3交易日）、資料日期與稽核結果。")
json_copy_block(arb)
