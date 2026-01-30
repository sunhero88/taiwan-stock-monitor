# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import streamlit as st
import pandas as pd

from analyzer import (
    TZ_TAIPEI,
    now_taipei,
    find_latest_trade_date,
    fetch_twse_stock_day_all,
    build_topn_from_market_dayall,
    merge_with_positions,
    fetch_global_summary,
    fetch_index_yf,
    fetch_twse_institutional_all,
)
from market_amount import fetch_amount_total_latest

APP_TITLE = "Sunhero｜股市智能超盤中控台（Top20 + 持倉監控 / V15.7 SIM-FREE）"

def _days_stale(latest_trade_date: str, asof_date: Optional[str]) -> Optional[int]:
    if not latest_trade_date or not asof_date:
        return None
    try:
        d0 = datetime.strptime(latest_trade_date, "%Y-%m-%d").date()
        d1 = datetime.strptime(asof_date, "%Y-%m-%d").date()
        return (d0 - d1).days
    except Exception:
        return None

def _copy_button(label: str, text: str, key: str):
    # Streamlit 原生沒有 clipboard；用 HTML/JS
    html = f"""
    <div style="display:flex; gap:8px; align-items:center;">
      <button onclick="navigator.clipboard.writeText(document.getElementById('{key}').textContent)"
              style="padding:6px 10px;border-radius:8px;border:1px solid #ccc;background:#fff;cursor:pointer;">
        {label}
      </button>
      <span style="color:#666;font-size:12px;">（若瀏覽器阻擋剪貼簿，請改用 Download）</span>
    </div>
    <pre id="{key}" style="display:none;">{text}</pre>
    """
    st.components.v1.html(html, height=45)

def _session_to_data_mode(session: str) -> str:
    # 盤前/盤中/盤後映射
    if session == "PREOPEN":
        return "PREOPEN"
    if session == "INTRADAY":
        return "INTRADAY"
    return "EOD"

def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    # ---------------- Sidebar Controls ----------------
    st.sidebar.header("設定")

    session = st.sidebar.selectbox("Session", ["PREOPEN", "INTRADAY", "EOD"], index=0)
    topn = st.sidebar.selectbox("TopN（固定追蹤數量）", [10, 20, 30, 50], index=1)

    positions_text = st.sidebar.text_area("持倉（會納入追蹤）\n輸入代碼（逗號分隔）", value="2330.TW", height=90)
    positions = [x.strip() for x in positions_text.split(",") if x.strip()]

    st.sidebar.caption("免費模擬期：法人資料改用 TWSE T86（免 FinMind 402）。")

    # ---------------- Core: Latest Trade Date ----------------
    now = now_taipei()
    latest_dt = find_latest_trade_date(max_lookback_days=10)
    latest_trade_date = latest_dt.strftime("%Y-%m-%d")

    st.info(f"目前台北時間：{now.strftime('%Y-%m-%d %H:%M')} ｜模式：{session}｜最新可用交易日：{latest_trade_date}")

    # ---------------- Global Summary (US/FX etc) ----------------
    st.subheader("全球市場摘要（美股/日經/匯率）— 最新可用交易日收盤")
    gdf = fetch_global_summary()
    st.dataframe(gdf, use_container_width=True)

    # ---------------- TW Index (Auto) ----------------
    # 台股大盤：^TWII（免費階段最穩）
    # PREOPEN/EOD：用最近日 K（收盤）
    # INTRADAY：yfinance 有時提供延遲即時；若抓不到就仍顯示最近日並標示延遲
    idx = None
    idx_err = None
    try:
        idx = fetch_index_yf("^TWII", "TWSE_TAIEX", period="10d")
    except Exception as e:
        idx_err = str(e)

    st.subheader("台股大盤指數（自動）")
    if idx is None:
        st.error(f"台股指數抓取失敗：{idx_err}")
        idx_asof = None
        index_level = None
        index_change = None
        index_chg_pct = None
        idx_source = f"ERR:{idx_err}"
    else:
        idx_asof = idx.asof_date
        index_level = idx.close
        index_change = idx.change
        index_chg_pct = idx.chg_pct
        idx_source = idx.source

        stale = _days_stale(latest_trade_date, idx_asof)
        note = ""
        if stale is not None and stale > 0:
            note = f"（⚠️ DATA_STALE_{stale}D）"
        st.write(f"**{idx.name}**：{index_level}，漲跌 {index_change}（{index_chg_pct}%）｜日期 {idx_asof}｜來源 {idx_source} {note}")

    # ---------------- Market Amount (TWSE/TPEx) ----------------
    st.subheader("市場成交金額（官方優先 / 免費 best-effort）")
    amt = fetch_amount_total_latest()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", "待更新" if amt.amount_twse is None else f"{amt.amount_twse/1e8:,.0f} 億")
    c2.metric("TPEx 上櫃", "待更新" if amt.amount_tpex is None else f"{amt.amount_tpex/1e8:,.0f} 億")
    c3.metric("Total 合計", "待更新" if amt.amount_total is None else f"{amt.amount_total/1e8:,.0f} 億")
    c4.metric("最新交易日", latest_trade_date)

    st.caption(f"來源：TWSE={amt.source_twse}｜TPEx={amt.source_tpex}")
    if amt.warnings:
        st.warning("；".join(amt.warnings))

    # ---------------- TopN (True market ranking) ----------------
    st.subheader(f"今日分析清單（Top{topn} + 持倉監控）— 以全市場日行情做真正排名")

    top_df = pd.DataFrame()
    dayall_source = ""
    top_build_err = None
    data_asof = None

    try:
        dayall = fetch_twse_stock_day_all(latest_dt)
        dayall_source = dayall.source
        top_df = build_topn_from_market_dayall(dayall, topn=topn, preselect_by_turnover=250)
        data_asof = latest_trade_date  # dayall 是最新交易日口徑
    except Exception as e:
        top_build_err = str(e)

    if top_build_err:
        st.error(f"TopN 建立失敗：{top_build_err}")
        st.stop()

    final_df = merge_with_positions(top_df, positions)
    final_df_display = final_df.copy()

    # 補上中文名：TopN 來自 TWSE OpenAPI 已是中文；positions 補入的 name 可能空
    # 這裡不強行從其他來源補，避免誤對映；若要補，可用 TWSE 公司基本資料表再擴充。
    cols = ["symbol","name","date","close","ret20_pct","vol_ratio","ma_bias_pct","volume","score","rank","tag"]
    for c in cols:
        if c not in final_df_display.columns:
            final_df_display[c] = None
    st.dataframe(final_df_display[cols], use_container_width=True)

    # ---------------- Institutional (TWSE T86) ----------------
    st.subheader("三大法人買賣超（TWSE T86 / 免費）")
    inst_status = "READY"
    inst_asof = latest_trade_date
    inst_err = None
    inst_map = {}

    try:
        inst_df = fetch_twse_institutional_all(latest_dt)
        inst_map = dict(zip(inst_df["symbol"], inst_df["inst_net"]))
        st.caption(f"法人資料日期：{inst_asof}｜來源：TWSE T86")
        # 展示：對 final_df 的前三大法人淨買賣
        show = final_df_display[["symbol","name"]].copy()
        show["inst_net_shares"] = show["symbol"].map(inst_map).fillna(0).astype(int)
        st.dataframe(show, use_container_width=True)
    except Exception as e:
        inst_status = "UNAVAILABLE"
        inst_err = str(e)
        st.warning(f"法人資料不可用：{inst_err}")

    # ---------------- Audit & Degraded Mode ----------------
    audit_flags = []
    # 指數日期稽核
    idx_stale = _days_stale(latest_trade_date, idx_asof)
    if idx_stale is None:
        audit_flags.append("INDEX_MISSING")
    elif idx_stale >= 1:
        audit_flags.append(f"DATA_STALE_INDEX_{idx_stale}D")

    # TopN 稽核：top_df date 應該等於 latest_trade_date（我們口徑固定）
    top_stale = _days_stale(latest_trade_date, data_asof)
    if top_stale is None:
        audit_flags.append("TOPN_DATE_MISSING")
    elif top_stale >= 1:
        audit_flags.append(f"DATA_STALE_TOPN_{top_stale}D")

    # 成交金額稽核：TWSE amount 必須有，TPEx 可缺但會警告
    if amt.amount_twse is None:
        audit_flags.append("AMOUNT_TWSE_MISSING")
    if amt.amount_total is None:
        audit_flags.append("AMOUNT_TOTAL_MISSING_PARTIAL")

    # 法人稽核
    if inst_status != "READY":
        audit_flags.append("INST_UNAVAILABLE")

    # 免費模擬版裁決：
    # - 若指數缺失 或 TopN 日期落後>=1 或 TWSE amount 缺失 => degraded_mode true
    degraded_mode = False
    hard_triggers = {"INDEX_MISSING", "TOPN_DATE_MISSING", "AMOUNT_TWSE_MISSING"}
    if any(f in hard_triggers for f in audit_flags):
        degraded_mode = True
    if any(f.startswith("DATA_STALE_INDEX_") for f in audit_flags):
        degraded_mode = True
    if any(f.startswith("DATA_STALE_TOPN_") for f in audit_flags):
        degraded_mode = True

    # ---------------- AI JSON (Arbiter Input) ----------------
    meta = {
        "system": "Predator V15.7 (SIM-FREE)",
        "market": "tw-share",
        "timestamp": now.strftime("%Y-%m-%d %H:%M"),
        "session": session,
        "topn": int(topn),
    }

    macro = {
        "overview": {
            "latest_trade_date": latest_trade_date,
            "data_mode": _session_to_data_mode(session),

            # 大盤
            "index_symbol": "^TWII",
            "index_level": index_level,
            "index_change": index_change,
            "index_chg_pct": index_chg_pct,
            "index_asof": idx_asof,
            "index_source": idx_source,

            # 成交金額（元）
            "amount_twse": amt.amount_twse,
            "amount_tpex": amt.amount_tpex,
            "amount_total": amt.amount_total,
            "amount_source_twse": amt.source_twse,
            "amount_source_tpex": amt.source_tpex,

            # 法人
            "inst_status": inst_status,
            "inst_asof": inst_asof if inst_status == "READY" else None,
            "inst_source": "TWSE T86" if inst_status == "READY" else f"ERR:{inst_err}",

            # 裁決/稽核
            "audit_flags": audit_flags,
            "degraded_mode": degraded_mode,
        }
    }

    stocks = []
    for _, r in final_df_display.iterrows():
        sym = str(r.get("symbol", "")).strip()
        if sym == "":
            continue
        stocks.append({
            "Symbol": sym,
            "Name": r.get("name", ""),
            "Date": r.get("date", None),
            "Close": r.get("close", None),
            "ret20_pct": r.get("ret20_pct", None),
            "vol_ratio": r.get("vol_ratio", None),
            "ma_bias_pct": r.get("ma_bias_pct", None),
            "volume": r.get("volume", None),
            "Score": r.get("score", None),
            "Rank": r.get("rank", None),
            "Tag": r.get("tag", None),
            # 法人（若可用）
            "Inst_Net_Shares": int(inst_map.get(sym, 0)) if inst_status == "READY" else None,
        })

    arbiter_input = {
        "meta": meta,
        "macro": macro,
        "stocks": stocks,
        "positions": positions,
        "positions_count": len(positions),
        "final_universe_count": len(stocks),
    }

    st.subheader("AI JSON（Arbiter Input）— 可回溯（模擬期免費）")
    json_text = json.dumps(arbiter_input, ensure_ascii=False, indent=2)

    _copy_button("複製 JSON", json_text, key="arbiter_json_blob")
    st.download_button("Download JSON", data=json_text, file_name=f"arbiter_input_{latest_trade_date}_{session}.json", mime="application/json")
    st.code(json_text, language="json")

    # ---------------- Human-readable judgement ----------------
    st.subheader("今日系統判斷（白話解釋）")
    bullets = []
    if degraded_mode:
        bullets.append(f"系統判定：**degraded_mode = true** → 強制禁止 BUY/TRIAL（原因：{', '.join(audit_flags) if audit_flags else 'N/A'}）")
    else:
        bullets.append(f"系統判定：**資料稽核通過**（audit_flags={audit_flags}）→ 可進入下一層 Arbiter 規則判定。")

    bullets.append(f"追蹤股票數量：Top{topn} + 持倉 {len(positions)} → 最終輸出 {len(stocks)} 檔。")
    bullets.append(f"Top{topn} 來源：{dayall_source}（全市場日行情→真正排名）")
    bullets.append(f"大盤指數：{index_level}（{index_change} / {index_chg_pct}%）｜日期 {idx_asof}｜來源 {idx_source}")
    bullets.append(f"成交金額：TWSE={amt.amount_twse}｜TPEx={amt.amount_tpex}｜Total={amt.amount_total}（元）")

    if inst_status == "READY":
        bullets.append("法人資料：TWSE T86 可用（免費官方）")
    else:
        bullets.append("法人資料：不可用（免費階段會保守降級，避免錯判）")

    for b in bullets:
        st.write("• " + b)

if __name__ == "__main__":
    app()
