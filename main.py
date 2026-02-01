# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any, Dict, List

import streamlit as st
import pandas as pd

from analyzer import build_arbiter_input, now_taipei

st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")

APP_TITLE = "Sunhero｜股市智能超盤中控台（Top20 + 持倉監控 / SIM-FREE）"

# ---------------------------
# UI helpers
# ---------------------------
def parse_positions_json(raw: str) -> List[Dict[str, Any]]:
    raw = (raw or "").strip()
    if raw == "":
        return []
    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            return obj
        return []
    except Exception:
        return []

def clipboard_button(label: str, text: str, key: str):
    # Streamlit 沒有原生 copy-button，這用 components + JS clipboard
    import streamlit.components.v1 as components
    safe = text.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    html = f"""
    <button id="{key}" style="padding:8px 12px;border-radius:10px;border:1px solid #ddd;background:#fff;cursor:pointer;">
      {label}
    </button>
    <script>
      const btn = document.getElementById("{key}");
      btn.addEventListener("click", async () => {{
        try {{
          await navigator.clipboard.writeText(`{safe}`);
          btn.innerText = "已複製 ✅";
          setTimeout(()=>btn.innerText="{label}", 1200);
        }} catch (e) {{
          btn.innerText = "複製失敗（瀏覽器限制）";
          setTimeout(()=>btn.innerText="{label}", 1500);
        }}
      }});
    </script>
    """
    components.html(html, height=48)

# ---------------------------
# Sidebar
# ---------------------------
st.title(APP_TITLE)

with st.sidebar:
    st.subheader("設定")

    session = st.selectbox("Session", ["PREOPEN", "INTRADAY", "EOD"], index=2)
    topn = st.selectbox("TopN（固定追蹤數量）", [10, 20, 30, 50], index=1)

    st.divider()
    st.subheader("帳戶 / 持倉（會納入追蹤）")

    cash_balance = st.number_input("cash_balance（NTD）", min_value=0, value=2000000, step=10000)
    total_equity = st.number_input("total_equity（NTD）", min_value=0, value=2000000, step=10000)

    st.caption("positions（JSON array），格式：[{symbol, shares, avg_cost, entry_date, trailing_high(optional)}]")
    positions_raw = st.text_area(
        "positions (JSON array)",
        value='[]',
        height=140
    )

    st.divider()
    st.subheader("資料取得策略（SIM-FREE）")
    verify_ssl = st.checkbox("SSL 驗證（官方資料）", value=True)
    st.caption("你要求「正確最新」→建議保持勾選。若遇到 Streamlit Cloud 憑證鏈問題，可暫時關閉，但會被標示風險。")

    run = st.button("Run")

# ---------------------------
# Main
# ---------------------------
st.info(f"目前台北時間：{now_taipei().strftime('%Y-%m-%d %H:%M')}｜模式：{session}｜TopN：{topn}")

if run:
    positions = parse_positions_json(positions_raw)

    arb = build_arbiter_input(
        session=session,
        topn=int(topn),
        positions=positions,
        cash_balance=int(cash_balance),
        total_equity=int(total_equity),
        verify_ssl=bool(verify_ssl),
        sim_free=True,
    )

    # 1) Market Meta
    st.subheader("台股大盤指數（自動）")
    twii = arb.get("market_meta", {}).get("taiex", {})
    cols = st.columns(5)
    cols[0].metric("TWII 日期", twii.get("date"))
    cols[1].metric("TWII 收盤/最新", twii.get("close"))
    cols[2].metric("漲跌", twii.get("chg"))
    cols[3].metric("漲跌幅(%)", twii.get("chg_pct"))
    cols[4].metric("來源", twii.get("source"))

    st.subheader("Regime / 指標（生成端先算好，Arbiter 只讀）")
    mm = arb.get("market_meta", {})
    rm = mm.get("regime_metrics", {})
    cols2 = st.columns(6)
    cols2[0].metric("current_regime", mm.get("current_regime"))
    cols2[1].metric("SMR", round(rm.get("SMR", 0.0), 6) if rm.get("SMR") is not None else None)
    cols2[2].metric("MA200", round(rm.get("MA200", 0.0), 2) if rm.get("MA200") is not None else None)
    cols2[3].metric("Slope5", round(rm.get("Slope5", 0.0), 6) if rm.get("Slope5") is not None else None)
    cols2[4].metric("MA14_Monthly", round(rm.get("MA14_Monthly", 0.0), 2) if rm.get("MA14_Monthly") is not None else None)
    cols2[5].metric("drawdown_pct(%)", round(rm.get("drawdown_pct", 0.0), 2))

    vix = mm.get("vix", {})
    st.caption(f"VIX：{vix.get('value')}（{vix.get('date')}）｜dynamic_vix_threshold：{vix.get('dynamic_vix_threshold')}｜source：{vix.get('source')}")

    # 2) Amount
    st.subheader("市場成交金額（官方優先 / SIM-FREE best-effort）")
    ov = arb.get("macro", {}).get("overview", {})
    c3 = st.columns(4)
    c3[0].metric("TWSE 上市（億）", ov.get("amount_twse_yi"))
    c3[1].metric("TPEx 上櫃（億）", ov.get("amount_tpex_yi"))
    c3[2].metric("Total 合計（億）", ov.get("amount_total_yi"))
    c3[3].metric("交易日（代理）", ov.get("data_date_proxy"))

    if ov.get("amount_warning") or (isinstance(ov.get("amount_sources"), dict) and ("ERR" in str(ov.get("amount_sources")))):
        st.warning(f"成交金額來源狀態：{ov.get('amount_sources')}｜warning：{ov.get('amount_warning')}")

    # 3) Top list
    st.subheader("今日分析清單（TopN + 持倉追加）— 以全市場成交金額做真排名")
    stocks = arb.get("stocks", [])
    df = pd.DataFrame(stocks)

    if df.empty:
        st.error("TopN 建立失敗：stocks 空。請查看 gate / risk_alerts。")
    else:
        show_cols = [c for c in ["rank", "symbol", "name", "tier_level", "top20_flag", "price",
                                 "ret20_pct", "vol_ratio", "ma_bias_pct", "volume", "score"] if c in df.columns]
        st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

    # 4) Gate
    st.subheader("今日系統判斷（白話解釋）")
    gate = arb.get("gate", {})
    if gate.get("degraded_mode"):
        st.error(f"Gate 觸發：degraded_mode=true → **禁止 BUY/TRIAL**｜原因：{gate.get('degraded_reason')}")
    else:
        st.success("Gate 通過：資料可用（SIM-FREE）")

    ra = arb.get("risk_alerts", [])
    if ra:
        st.warning("風險警示 / 資料缺口：\n- " + "\n- ".join(ra))

    # 5) Arbiter Input JSON
    st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
    json_text = json.dumps(arb, ensure_ascii=False, indent=2)

    # 一鍵複製 + 下載
    colA, colB = st.columns([1, 1])
    with colA:
        clipboard_button("複製 Arbiter JSON", json_text, key="copy_json_btn")
    with colB:
        st.download_button(
            "下載 Arbiter JSON",
            data=json_text.encode("utf-8"),
            file_name=f"arbiter_input_{arb.get('meta', {}).get('timestamp', '').replace(':','')}.json",
            mime="application/json",
        )

    st.code(json_text, language="json")

else:
    st.caption("左側設定完成後按 Run。")
