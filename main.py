# -*- coding: utf-8 -*-
"""
Sunhero | 股市智能超盤中控台 (Data-Layer + Arbiter Orchestrator)
入口：Streamlit app（main.py）
統一裁決入口：arbiter_run(payload, run_mode)

設計目標：
- 資料層輸出為準（Data-Layer Snapshot）
- Arbiter 只相信 JSON（不補資料）
- L1 先做資料稽核 Gate，失敗直接 NO_TRADE
- UI 只做「展示 + 觸發」，不做推論
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Tuple, Optional

import streamlit as st

from downloader_tw import get_market_snapshot
from arbiter import arbiter_run


TZ_TPE = timezone(timedelta(hours=8))


def _now_tpe() -> datetime:
    return datetime.now(TZ_TPE)


def _safe_json_loads(s: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return None, "JSON 必須是物件（dict）"
        return obj, None
    except Exception as e:
        return None, f"JSON 解析錯誤: {e}"


def _build_payload_from_snapshot(snapshot: Dict[str, Any], topn: int = 20) -> Dict[str, Any]:
    """
    用資料層 snapshot 組一份 Arbiter payload 範本（供 UI 一鍵載入）
    注意：這是「範本」，真正裁決仍以 JSON 為唯一可信輸入。
    """
    meta = snapshot.get("meta", {}) or {}
    macro = snapshot.get("macro", {}) or {}
    stocks = snapshot.get("stocks", []) or []

    payload = {
        "meta": {
            "timestamp": meta.get("timestamp") or _now_tpe().strftime("%Y-%m-%d %H:%M:%S"),
            "session": meta.get("session") or "EOD",
            "market_status": meta.get("market_status") or "NORMAL",
            "confidence_level": meta.get("confidence_level") or "HIGH",
            "is_using_previous_day": bool(meta.get("is_using_previous_day", False)),
            "effective_trade_date": meta.get("effective_trade_date"),
            "war_time_override": bool(meta.get("war_time_override", False)),
            "audit_modules": meta.get("audit_modules") or [],
        },
        "macro": macro,
        "stocks": stocks[: int(topn)],
    }
    return payload


def run_ui():
    st.set_page_config(
        page_title="Sunhero | 股市智能超盤中控台",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Sunhero｜股市智能超盤中控台（Data-Layer + Arbiter Orchestrator）")

    # ===== Sidebar =====
    with st.sidebar:
        st.subheader("模式 / 交易日")

        run_mode = st.radio("RUN 模式", ["L1", "L2", "L3"], index=0)
        session = st.selectbox("Session", ["EOD", "INTRADAY"], index=0)

        # 台北時間日期選擇
        default_date = _now_tpe().date()
        d = st.date_input("目標日期（台北）", value=default_date)

        topn = st.slider("TopN（上市成交額排序）", min_value=5, max_value=50, value=20, step=1)

        if st.button("立即更新", type="primary"):
            st.cache_data.clear()
            st.rerun()

    target_dt = datetime(d.year, d.month, d.day, tzinfo=TZ_TPE)

    # ===== Data-Layer Snapshot (cached) =====
    @st.cache_data(ttl=60, show_spinner=False)
    def get_snapshot_cached(target_iso: str, session: str, topn: int) -> Dict[str, Any]:
        # target_iso: "YYYY-MM-DD"
        return get_market_snapshot(target_iso, session=session, topn=topn)

    with st.spinner("資料層抓取中（Snapshot）..."):
        t0 = time.time()
        snapshot = get_snapshot_cached(target_dt.strftime("%Y-%m-%d"), session, int(topn))
        latency_ms = int((time.time() - t0) * 1000)

    # ===== 上方市場狀態展示 =====
    st.subheader("📊 市場狀態（以資料層輸出為準）")

    macro = snapshot.get("macro", {}) or {}
    overview = macro.get("overview", {}) or {}
    ma = macro.get("market_amount", {}) or {}
    inst = macro.get("institutional", {}) or {}

    c1, c2, c3, c4 = st.columns(4)

    # 1) TWII
    with c1:
        twii_close = overview.get("twii_close")
        twii_chg = overview.get("twii_change")
        twii_pct = overview.get("twii_pct")

        st.metric(
            "加權指數 TWII（TWSE）",
            "—" if twii_close is None else f"{float(twii_close):,.2f}",
            None if twii_chg is None else f"{float(twii_chg):+,.2f} ({(float(twii_pct) * 100):+.2f}%)" if twii_pct is not None else f"{float(twii_chg):+,.2f}",
        )
        st.caption("TWII 讀取失敗（Arbiter 內部 L1 會直接擋下）" if twii_close is None else "")

    # 2) TWSE amount
    with c2:
        twse_amt = ma.get("amount_twse")
        st.metric("上市成交額（TWSE）", "—" if twse_amt is None else f"{int(twse_amt):,}")
        st.caption(f"來源：{ma.get('source_twse')}｜錯誤：{ma.get('error_twse')}")

    # 3) TPEX amount
    with c3:
        tpex_amt = ma.get("amount_tpex")
        st.metric("上櫃成交額（TPEX）", "—" if tpex_amt is None else f"{int(tpex_amt):,}")
        st.caption(f"Tier={ma.get('tier_tpex')}｜來源：{ma.get('source_tpex')}｜錯誤：{ma.get('error_tpex')}")

    # 4) Total amount
    with c4:
        total_amt = ma.get("amount_total")
        st.metric("總成交額", "—" if total_amt is None else f"{int(total_amt):,}")
        st.caption(
            f"EOD Guard：is_using_previous_day={snapshot.get('meta', {}).get('is_using_previous_day')}｜effective_trade_date={snapshot.get('meta', {}).get('effective_trade_date')}"
        )

    # ===== 法人區 =====
    st.subheader("🧾 三大法人（TWSE T86）")
    inst_ok = inst.get("ok", False)
    if not inst_ok:
        st.error(f"T86 讀取失敗：{inst.get('error')}")
    else:
        a, b, c, d = st.columns(4)
        a.metric("外資淨買賣超", f"{int(inst.get('foreign', 0)):,}")
        b.metric("投信淨買賣超", f"{int(inst.get('trust', 0)):,}")
        c.metric("自營商淨買賣超", f"{int(inst.get('dealer', 0)):,}")
        d.metric("三大法人合計", f"{int(inst.get('total', 0)):,}")

    st.caption(f"Snapshot latency: {latency_ms} ms")

    st.divider()

    # ===== JSON Payload 輸入 / 生成 =====
    left, right = st.columns([1.2, 1.0])

    with left:
        st.subheader("輸入 JSON Payload（可貼 Arbiter JSON 或用範本生成）")

        if st.button("載入標準範本（以 TopN + 市場資料層組裝）"):
            payload = _build_payload_from_snapshot(snapshot, topn=int(topn))
            st.session_state["json_input"] = json.dumps(payload, ensure_ascii=False, indent=2)

        json_input = st.text_area(
            "JSON 內容",
            height=520,
            key="json_input",
            value=st.session_state.get("json_input", "{}"),
        )

    with right:
        st.subheader("執行結果（統一入口：arbiter_run）")

        if st.button("🚀 執行（arbiter_run）", type="primary"):
            payload, err = _safe_json_loads(json_input)
            if err:
                st.error(err)
                st.stop()

            try:
                result = arbiter_run(payload, run_mode)
            except Exception as e:
                st.error(f"裁決引擎錯誤：{e}")
                st.stop()

            # 顯示摘要（先給人看重點）
            verdict = result.get("VERDICT") or result.get("verdict")
            risk_reason = result.get("RISK_REASON") or result.get("risk_reason")
            if verdict == "NO_TRADE":
                st.warning("NO_TRADE：已被 L1 Gate 阻擋（資料不可信）")
            elif verdict:
                st.success(f"VERDICT: {verdict}")

            if risk_reason:
                st.caption(f"RISK_REASON: {risk_reason}")

            st.markdown("### ① Arbiter 統一輸出")
            st.json(result)

    # ===== Debug / Raw Snapshot =====
    with st.expander("（Debug）查看資料層 Snapshot 原始輸出", expanded=False):
        st.json(snapshot)


if __name__ == "__main__":
    run_ui()
