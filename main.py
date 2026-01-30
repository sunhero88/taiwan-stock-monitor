# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import streamlit as st
import pandas as pd

from analyzer import (
    now_taipei,
    fetch_twii,
    load_market_snapshot,
    build_topn,
    parse_positions_json,
    gate_eval,
    build_arbiter_input,
)

from market_amount import (
    TZ_TAIPEI,
    fetch_amount_total_latest,
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（Top20 + 持倉監控 / V15.7 SIM-FREE）"
SYSTEM_NAME = "Predator V15.7 (SIM-FREE / Top20+Positions)"

DEFAULT_MARKET = "tw-share"
DEFAULT_TOPN = 20

# -----------------------
# UI helper
# -----------------------
def _money_fmt(n: Optional[int]) -> str:
    if n is None:
        return "待更新"
    if n >= 10**8:
        return f"{n/10**8:,.0f} 億"
    return f"{n:,}"

def _safe_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None

def _json_dump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

# -----------------------
# App
# -----------------------
def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    # Sidebar
    st.sidebar.header("設定")

    session = st.sidebar.selectbox("Session", ["PREOPEN", "INTRADAY", "EOD"], index=0)
    topn = st.sidebar.selectbox("TopN（固定追蹤數量）", [10, 15, 20, 25, 30], index=[10,15,20,25,30].index(DEFAULT_TOPN))

    st.sidebar.subheader("持倉（會納入追蹤）")
    st.sidebar.caption("輸入 JSON array（至少 symbol/qty/avg_cost）例如："
                       '\n[{"symbol":"2330.TW","qty":100,"avg_cost":600}]')
    positions_text = st.sidebar.text_area("positions（JSON array）", value="[]", height=160)
    positions = parse_positions_json(positions_text)

    st.sidebar.subheader("資料取得策略（SIM-FREE）")
    verify_ssl = st.sidebar.checkbox("SSL 驗證（官方資料）", value=True)
    st.sidebar.caption("若官方站台 SSL 在 Streamlit Cloud 失敗，可取消勾選以 best-effort 抓取。")

    run = st.sidebar.button("Run")

    # auto-run first load
    if "ran_once" not in st.session_state:
        st.session_state["ran_once"] = True
        run = True

    if not run:
        st.stop()

    now = now_taipei()
    st.info(f"目前台北時間：{now.strftime('%Y-%m-%d %H:%M')} ｜ 模式：{session}")

    # -----------------------
    # 1) Macro：TWII 自動
    # -----------------------
    st.subheader("台股大盤指數（自動）")
    twii = fetch_twii(session=session)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWII 日期", twii.get("date") or "未知")
    c2.metric("TWII 指數", twii.get("index") if twii.get("index") is not None else "待更新")
    c3.metric("漲跌", twii.get("change") if twii.get("change") is not None else "待更新")
    c4.metric("漲跌幅(%)", twii.get("change_pct") if twii.get("change_pct") is not None else "待更新")

    if twii.get("error"):
        st.warning(f"TWII 取得失敗：{twii['error']}（source=yfinance）")

    # -----------------------
    # 2) 成交金額（官方優先 / best-effort）
    # -----------------------
    st.subheader("市場成交金額（官方優先 / 免費 best-effort）")
    # 盤前：用 TWII 日期當作「最後可用交易日」的 anchor
    # 盤中/盤後：也用 TWII 日期作為當日對齊
    trade_date = None
    try:
        if twii.get("date"):
            trade_date = datetime.strptime(twii["date"], "%Y-%m-%d").replace(tzinfo=TZ_TAIPEI)
        else:
            trade_date = now
    except Exception:
        trade_date = now

    amt_obj = fetch_amount_total_latest(trade_date=trade_date, verify_ssl=verify_ssl)

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("TWSE 上市", _money_fmt(amt_obj.amount_twse))
    a2.metric("TPEx 上櫃", _money_fmt(amt_obj.amount_tpex))
    a3.metric("Total 合計", _money_fmt(amt_obj.amount_total))
    a4.metric("成交金額交易日", amt_obj.trade_date or "未知")

    st.caption(f"來源：TWSE={amt_obj.source_twse} ｜ TPEx={amt_obj.source_tpex}")
    if amt_obj.warning:
        st.warning(amt_obj.warning)

    amount_pack = {
        "trade_date": amt_obj.trade_date,
        "amount_twse": amt_obj.amount_twse,
        "amount_tpex": amt_obj.amount_tpex,
        "amount_total": amt_obj.amount_total,
        "source_twse": amt_obj.source_twse,
        "source_tpex": amt_obj.source_tpex,
        "warning": amt_obj.warning,
        "verify_ssl": verify_ssl,
    }

    # -----------------------
    # 3) TopN（真全市場排序：以你每日產出的快照為準）
    # -----------------------
    st.subheader("今日分析清單（Top20 + 持倉監控）— 以全市場日行情做真正排名")
    snapshot_df, snapshot_meta = load_market_snapshot(DEFAULT_MARKET)
    top_df = None
    top_meta: Dict[str, Any] = {}

    if snapshot_df is None:
        st.error(f"TopN 建立失敗：{snapshot_meta.get('error')}")
    else:
        top_df, top_meta = build_topn(snapshot_df, topn=topn)
        st.caption(f"TopN 來源：{snapshot_meta.get('source')} ｜ 檔案：{snapshot_meta.get('path')} ｜ 方法：{top_meta.get('method')}")

        # 顯示 TopN
        show_cols = ["rank", "symbol", "name", "date", "close"]
        for extra in ["ret20_pct", "vol_ratio", "ma_bias_pct", "volume", "score"]:
            if extra in top_df.columns:
                show_cols.append(extra)

        st.dataframe(top_df[show_cols], use_container_width=True, height=420)

    # -----------------------
    # 4) Gate（稽核 / 禁止失真）
    # -----------------------
    latest_trade_date = None
    if top_meta.get("latest_date"):
        latest_trade_date = str(top_meta.get("latest_date"))
    elif twii.get("date"):
        latest_trade_date = twii.get("date")

    gates = gate_eval(
        session=session,
        topn_df=top_df,
        twii=twii,
        latest_trade_date=latest_trade_date,
        required_topn=topn,
    )

    st.subheader("今日系統判斷（白話解釋）")
    if gates["allow_trade"]:
        st.success("Gate 通過：允許 BUY/TRIAL（仍需個股訊號符合）")
    else:
        st.error("Gate 未通過：強制禁止 BUY/TRIAL（避免舊或錯資料導致裁決失真）")

    with st.expander("Gate 詳細"):
        st.json(gates)

    # -----------------------
    # 5) Arbiter Input JSON（可回溯 / 可複製）
    # -----------------------
    st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")

    arbiter_input = build_arbiter_input(
        system_name=SYSTEM_NAME,
        market=DEFAULT_MARKET,
        session=session,
        topn_df=top_df,
        positions=positions,
        twii=twii,
        amount_pack=amount_pack,
        gates=gates,
        snapshot_meta=snapshot_meta,
    )

    # Streamlit st.code 右上角自帶 copy 按鈕（你標的「缺了複製鍵」就是這個）
    json_text = _json_dump(arbiter_input)
    st.code(json_text, language="json")

    st.download_button(
        label="下載 Arbiter Input JSON",
        data=json_text.encode("utf-8"),
        file_name=f"arbiter_input_{DEFAULT_MARKET}_{session}_{now.strftime('%Y%m%d_%H%M')}.json",
        mime="application/json",
    )

    # -----------------------
    # 6) 交易行為提示（SIM-FREE）
    # -----------------------
    st.subheader("交易行為（SIM-FREE）")
    if not gates["allow_trade"]:
        st.warning("目前狀態：禁止 BUY/TRIAL。原因：\n- " + "\n- ".join(gates.get("reason", [])))
    else:
        st.info("目前狀態：允許 BUY/TRIAL（但你仍需依策略訊號與風控執行）。")

    # 持倉提醒
    if positions:
        st.caption(f"持倉已納入分析：{', '.join([p['symbol'] for p in positions])}（TopN ∪ Positions）")

if __name__ == "__main__":
    app()
