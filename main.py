# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import streamlit as st

APP_TITLE = "Sunhero｜股市智能超盤中控台（Top20 + 持倉監控 / V15.7 SIM-FREE）"


def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    # ---------------------------------------------------------
    # 延遲 import：讓 Streamlit UI 先起來，匯入錯誤能顯示完整 traceback
    # ---------------------------------------------------------
    try:
        import analyzer as az
    except Exception as e:
        st.error("❌ 匯入 analyzer.py 失敗（請看下方完整錯誤）")
        st.exception(e)
        st.stop()

    try:
        from market_amount import fetch_amount_total_latest
    except Exception as e:
        st.error("❌ 匯入 market_amount.py 失敗（請看下方完整錯誤）")
        st.exception(e)
        st.stop()

    # ===== Sidebar =====
    st.sidebar.header("設定")

    session = st.sidebar.selectbox("Session", ["PREOPEN", "INTRADAY", "EOD"], index=0)
    topn = st.sidebar.selectbox("TopN（固定追蹤數量）", [10, 20, 30], index=1)

    st.sidebar.markdown("---")
    st.sidebar.subheader("持倉（會納入追蹤）")
    positions_text = st.sidebar.text_area("輸入代碼（逗號/換行分隔）", value="2330.TW", height=90)
    positions = az.normalize_symbols_list(positions_text)

    st.sidebar.markdown("---")
    st.sidebar.subheader("資料取得策略（SIM-FREE）")
    verify_ssl = st.sidebar.checkbox("SSL 驗證（官方資料）", value=True)
    lookback = st.sidebar.slider("官方資料回溯天數", min_value=3, max_value=14, value=10, step=1)

    run = st.sidebar.button("Run")

    # ===== 狀態列 =====
    now_str = datetime.now(tz=az.TZ_TAIPEI).strftime("%Y-%m-%d %H:%M")
    st.info(f"目前台北時間：{now_str} ｜ 模式：{session} ｜ TopN：{topn} ｜ 持倉數：{len(positions)}")

    # ===== 0) 全球摘要 =====
    st.subheader("全球市場摘要（美股/日經/匯率）— 最新可用交易日收盤")
    try:
        gdf = az.fetch_global_summary()
        st.dataframe(gdf, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning("全球摘要抓取失敗（不影響台股 Top20 主流程）")
        st.exception(e)

    # ===== 1) 台股大盤（自動） =====
    st.subheader("台股大盤指數（自動）")
    idx = az.fetch_tw_index_auto(session=session)
    if idx.get("index_level") is None:
        st.warning(f"台股大盤指數抓取失敗：{idx.get('source')}")
    else:
        st.write(
            f"**TWSE_TAIEX**：{idx['index_level']}，漲跌 {idx['index_change']}（{idx['index_chg_pct']}%）"
            f"｜日期 {idx['index_date']}｜來源 {idx['source']}"
        )

    # ===== 2) 找最新可用交易日（官方行情） =====
    st.subheader("市場成交金額（官方優先 / 免費 best-effort）")

    latest_trade_date, td_meta = az.find_latest_trade_date_tw(
        lookback_days=lookback,
        verify_ssl=verify_ssl,
        allow_ssl_bypass=True,
    )

    if latest_trade_date is None:
        st.error("找不到最新可用交易日（官方全市場行情不可用）。Top20/成交金額將無法建立。")
        st.code(json.dumps(td_meta, ensure_ascii=False, indent=2), language="json")
        st.stop()

    st.caption(f"官方最新可用交易日：**{latest_trade_date.isoformat()}**（lookback={lookback} 天）")

    # ===== 3) 成交金額 =====
    ma = fetch_amount_total_latest(
        trade_date=latest_trade_date,
        verify_ssl=verify_ssl,
        allow_ssl_bypass=True,
    )

    def _fmt_amt_100m(n: Optional[int]) -> str:
        if n is None:
            return "待更新"
        return f"{round(n / 1e8):,} 億"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", _fmt_amt_100m(ma.amount_twse))
    c2.metric("TPEx 上櫃", _fmt_amt_100m(ma.amount_tpex))
    c3.metric("Total 合計", _fmt_amt_100m(ma.amount_total))
    c4.metric("最新交易日", ma.trade_date or "未知")

    warn = (ma.sources or {}).get("warning")
    if warn:
        st.warning(warn)

    # ===== 4) TopN 建立 =====
    st.subheader("今日分析清單（Top20 + 持倉監控）— 以全市場日行情做真正排名")

    topn_df, topn_meta = az.build_topn_tw_market(
        topn=topn,
        trade_date=latest_trade_date,
        verify_ssl=verify_ssl,
        allow_ssl_bypass=True,
        pool_size=200,
    )

    if topn_df.empty:
        st.error("TopN 建立失敗：全市場行情或 yfinance 指標不足。")
        st.code(json.dumps(topn_meta, ensure_ascii=False, indent=2), language="json")
        st.stop()

    merged_df = az.merge_topn_with_positions(topn_df, positions)

    show_cols = ["rank", "symbol", "name", "date", "close", "ret20_pct", "vol_ratio", "ma_bias_pct", "volume", "score", "is_position"]
    # 某些欄位可能不存在（避免 KeyError）
    show_cols = [c for c in show_cols if c in merged_df.columns]
    st.dataframe(merged_df[show_cols], use_container_width=True, hide_index=True)

    # ===== 5) Degraded 稽核（核心防線） =====
    st.subheader("今日系統判斷（白話解釋）")
    degraded = False
    reasons = []

    if bool(topn_meta.get("stale")):
        degraded = True
        reasons.append("DATA_STALE（TopN 指標日期與官方交易日差距過大）")
    if ma.amount_total is None:
        degraded = True
        reasons.append("AMOUNT_UNAVAILABLE（成交金額不可用）")
    if idx.get("index_level") is None:
        degraded = True
        reasons.append("INDEX_UNAVAILABLE（大盤指數不可用）")

    if degraded:
        st.error("資料健康門觸發 → degraded_mode = true → 強制禁止 BUY/TRIAL。")
        for r in reasons:
            st.write(f"- {r}")
    else:
        st.success("資料稽核通過：可進入裁決（仍受你 Arbiter 規則約束）。")

    if topn_meta.get("warnings"):
        st.warning("警告/稽核訊息：\n- " + "\n- ".join(topn_meta["warnings"]))

    # ===== 6) AI JSON（Arbiter Input）— 可複製 =====
    st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")

    arb = az.build_arbiter_input(
        session=session,
        market="tw-share",
        topn_df=topn_df,
        merged_df=merged_df,
        trade_date=latest_trade_date.isoformat(),
        index_info=idx,
        market_amount={
            "amount_twse": ma.amount_twse,
            "amount_tpex": ma.amount_tpex,
            "amount_total": ma.amount_total,
            "sources": ma.sources,
        },
        topn_meta=topn_meta,
        positions=positions,
        verify_ssl=verify_ssl,
    )

    st.code(json.dumps(arb, ensure_ascii=False, indent=2), language="json")


if __name__ == "__main__":
    app()
