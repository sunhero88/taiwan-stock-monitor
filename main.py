# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, Optional, List

import pandas as pd
import streamlit as st

from analyzer import (
    TZ_TAIPEI,
    fetch_global_summary,
    fetch_tw_index_auto,
    find_latest_trade_date_tw,
    build_topn_tw_market,
    normalize_symbols_list,
    merge_topn_with_positions,
    build_arbiter_input,
)

# 你需確保 repo 內有新版 market_amount.py（含 fetch_amount_total_latest）
from market_amount import fetch_amount_total_latest

APP_TITLE = "Sunhero｜股市智能超盤中控台（Top20 + 持倉監控 / V15.7 SIM-FREE）"


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _fmt_amt_100m(n: Optional[int]) -> str:
    if n is None:
        return "待更新"
    # 元 -> 億（四捨五入）
    return f"{round(n / 1e8):,} 億"


def _as_trade_date(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


@st.cache_data(ttl=180, show_spinner=False)
def cached_global_summary() -> pd.DataFrame:
    return fetch_global_summary()


def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    # ===== Sidebar =====
    st.sidebar.header("設定")

    session = st.sidebar.selectbox("Session", ["PREOPEN", "INTRADAY", "EOD"], index=0)
    topn = st.sidebar.selectbox("TopN（固定追蹤數量）", [10, 20, 30], index=1)

    st.sidebar.markdown("---")
    st.sidebar.subheader("持倉（會納入追蹤）")
    positions_text = st.sidebar.text_area("輸入代碼（逗號/換行分隔）", value="2330.TW", height=90)
    positions = normalize_symbols_list(positions_text)

    st.sidebar.markdown("---")
    st.sidebar.subheader("資料取得策略（SIM-FREE）")
    verify_ssl = st.sidebar.checkbox("SSL 驗證（官方資料）", value=True)
    lookback = st.sidebar.slider("官方資料回溯天數", min_value=3, max_value=14, value=10, step=1)

    run = st.sidebar.button("Run")

    # 預設也跑一次（避免使用者沒按 Run 看不到）
    if not run:
        st.caption("提示：你可以按左側 Run 強制重新抓取。")

    # ===== 主畫面狀態列 =====
    now_str = _now_taipei().strftime("%Y-%m-%d %H:%M")
    st.info(f"目前台北時間：{now_str} ｜ 模式：{session} ｜ TopN：{topn} ｜ 持倉數：{len(positions)}")

    # ===== 0) 全球摘要 =====
    st.subheader("全球市場摘要（美股/日經/匯率）— 最新可用交易日收盤")
    gdf = cached_global_summary()
    st.dataframe(gdf, use_container_width=True, hide_index=True)

    # ===== 1) 自動台股大盤 =====
    st.subheader("台股大盤指數（自動）")
    idx = fetch_tw_index_auto(session=session)
    if idx.get("index_level") is None:
        st.warning(f"台股大盤指數抓取失敗：{idx.get('source')}")
    else:
        st.write(
            f"**TWSE_TAIEX**：{idx['index_level']}，漲跌 {idx['index_change']}（{idx['index_chg_pct']}%）"
            f"｜日期 {idx['index_date']}｜來源 {idx['source']}"
        )

    # ===== 2) 取得最新可用台股交易日（官方行情） =====
    st.subheader("市場成交金額（官方優先 / 免費 best-effort）")

    latest_trade_date, td_meta = find_latest_trade_date_tw(
        lookback_days=lookback,
        verify_ssl=verify_ssl,
        allow_ssl_bypass=True,  # B 方案：允許 best-effort
    )

    if latest_trade_date is None:
        st.error("找不到最新可用交易日（官方全市場行情不可用）。Top20/成交金額將無法建立。")
        st.code(json.dumps(td_meta, ensure_ascii=False, indent=2), language="json")
        return

    st.caption(f"官方最新可用交易日：**{latest_trade_date.isoformat()}**（lookback={lookback} 天）")

    # ===== 3) 成交金額（TWSE+TPEx） =====
    ma = fetch_amount_total_latest(
        trade_date=latest_trade_date,
        verify_ssl=verify_ssl,
        allow_ssl_bypass=True,  # B 方案
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", _fmt_amt_100m(ma.amount_twse))
    c2.metric("TPEx 上櫃", _fmt_amt_100m(ma.amount_tpex))
    c3.metric("Total 合計", _fmt_amt_100m(ma.amount_total))
    c4.metric("最新交易日", ma.trade_date or "未知")

    # SSL bypass 警告（明確呈現）
    warn = (ma.sources or {}).get("warning")
    if warn:
        st.warning(warn)

    st.caption(f"來源/稽核：{(ma.sources or {}).get('twse')} ｜ {(ma.sources or {}).get('tpex')}")

    # ===== 4) 建立 TopN（全市場） =====
    st.subheader("今日分析清單（Top20 + 持倉監控）— 以全市場日行情做真正排名")

    topn_df, topn_meta = build_topn_tw_market(
        topn=topn,
        trade_date=latest_trade_date,
        verify_ssl=verify_ssl,
        allow_ssl_bypass=True,  # B 方案
        pool_size=200,
    )

    if topn_df.empty:
        st.error("TopN 建立失敗：全市場行情或 yfinance 指標不足。")
        st.code(json.dumps(topn_meta, ensure_ascii=False, indent=2), language="json")
        return

    merged_df = merge_topn_with_positions(topn_df, positions)

    # 顯示（表格）
    show_cols = ["rank", "symbol", "name", "date", "close", "ret20_pct", "vol_ratio", "ma_bias_pct", "volume", "score", "is_position"]
    st.dataframe(merged_df[show_cols], use_container_width=True, hide_index=True)

    # ===== 5) 今日系統判斷（白話解釋） =====
    st.subheader("今日系統判斷（白話解釋）")

    # 稽核：TopN stale / amount unavailable / index unavailable
    degraded = False
    reasons = []

    if topn_meta.get("stale"):
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

    arb = build_arbiter_input(
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

    # 用 st.code 才有 Copy
    st.code(json.dumps(arb, ensure_ascii=False, indent=2), language="json")


if __name__ == "__main__":
    app()
