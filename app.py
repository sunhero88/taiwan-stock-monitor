# app.py
# -*- coding: utf-8 -*-
# Streamlit Cloud Entry — STABLE UI (NO list index assumptions)
# Unified execution: arbiter_run(payload, run_mode)

import json
from datetime import datetime, timedelta, timezone
import streamlit as st

from downloader_tw import build_snapshot, build_v203_min_json
from arbiter import arbiter_run

TZ_TPE = timezone(timedelta(hours=8))


# -------------------------
# helpers
# -------------------------
def today_tpe_date():
    return datetime.now(TZ_TPE).date()


def fmt_money(n):
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except:
        return str(n)


def fmt_num(n):
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except:
        return str(n)


def fmt_pct(x):
    if x is None:
        return "—"
    try:
        return f"{float(x)*100:.2f}%"
    except:
        return str(x)


@st.cache_data(ttl=120)
def get_snapshot_cached(target_iso: str, session_name: str, top_n: int):
    dt = datetime.fromisoformat(target_iso).replace(tzinfo=TZ_TPE)
    return build_snapshot(dt, session_name=session_name, top_n=top_n)


def build_default_params():
    return {
        "k_regime": 1.2,
        "lambda_drawdown": 2.0,
        "max_loss_per_trade_pct": 0.02,
        "stress_drawdown_trigger": 0.10,
        "min_trades_for_trust": 8,
        "trust_default_when_insufficient": 0.49,
        "trust_attack_scale_low": 0.30,
        "l1_price_min": 1,
        "l1_price_max": 5000,
        "l1_price_median_mult_hi": 50,
        "prev_day_allocation_scale": 0.70,
        "kronos_enabled": False,
    }


def build_default_portfolio():
    return {
        "equity": 2_000_000,
        "drawdown_pct": -0.06,
        "loss_streak": 0,
        "alpha_prev": 0.45,
    }


def build_default_monitoring():
    return {
        "regime_predictive_score": 0.70,
        "regime_outcome_score": 0.70,
        "trade_count_20d": 10,
    }


def action_summary(ucc: dict) -> dict:
    # Safe summary counts; action lists always exist after arbiter normalization
    out = {}
    for k in ["OPEN", "ADD", "HOLD", "REDUCE", "CLOSE", "NO_TRADE"]:
        v = ucc.get(k, [])
        out[k] = len(v) if isinstance(v, list) else 0
    out["DECISION"] = ucc.get("DECISION", "—")
    out["CONFIDENCE"] = ucc.get("CONFIDENCE", "—")
    return out


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
st.title("Sunhero｜股市智能超盤中控台（STABLE / arbiter_run 單一入口）")

with st.sidebar:
    st.subheader("模式選擇")
    run_mode = st.radio("RUN", ["L1", "L2", "L3"], index=1)  # default L2
    session_name = st.selectbox("Session", ["EOD", "INTRADAY"], index=0)

    st.divider()
    d = st.date_input("目標日期（台北）", value=today_tpe_date())
    top_n = st.slider("TopN", min_value=10, max_value=50, value=20, step=5)

    if st.button("立即更新", type="primary"):
        st.cache_data.clear()
        st.rerun()

target_iso = d.isoformat()
snap = get_snapshot_cached(target_iso, session_name=session_name, top_n=top_n)

# -------------------------
# Market block
# -------------------------
twii_pack = snap.get("twii", {}) or {}
twii_ok = bool(twii_pack.get("ok"))
twii = (twii_pack.get("data") or {}) if twii_ok else {}

ma = snap.get("market_amount") or {}
t86 = snap.get("t86") or {}
rec = snap.get("recency") or {}

st.markdown("### 📊 市場即時狀態")

c1, c2, c3, c4 = st.columns(4)

with c1:
    if twii_ok and twii.get("close") is not None:
        st.metric("加權指數（TWII）", f"{twii.get('close'):,.2f}", f"{(twii.get('chg') or 0):+.2f}")
        st.caption(f"指數資料日：{twii.get('date', snap.get('trade_date_iso'))}")
    else:
        st.metric("加權指數（TWII）", "—")
        st.caption("TWII 讀取失敗（L1 可能擋下）")

with c2:
    st.metric("上市成交額（TWSE）", fmt_money(ma.get("amount_twse")))
    twse_meta = ma.get("twse_amount_meta") or {}
    st.caption(f"來源：{twse_meta.get('source_name','—')}｜錯誤：{twse_meta.get('error_code') or '—'}")

with c3:
    st.metric("上櫃成交額（TPEX）", fmt_money(ma.get("amount_tpex")))
    tpex_meta = ma.get("tpex_amount_meta") or {}
    st.caption(f"Tier={tpex_meta.get('tier','—')}｜來源：{tpex_meta.get('source_name','—')}｜錯誤：{tpex_meta.get('error_code') or '—'}")

with c4:
    st.metric("總成交額", fmt_money(ma.get("amount_total")))
    st.caption(f"effective_trade_date={rec.get('effective_trade_date')}｜is_prev_day={rec.get('is_using_previous_day')}")

st.divider()

# -------------------------
# Institutional T86
# -------------------------
st.markdown("### 🧾 三大法人（TWSE T86）")
if not t86.get("ok"):
    st.warning(f"T86 讀取失敗：{(t86.get('meta') or {}).get('error_code')}")
else:
    s = t86.get("summary") or {}
    a, b, c, d4 = st.columns(4)
    a.metric("外資淨買賣超", fmt_num(s.get("外資及陸資(不含外資自營商)")))
    b.metric("投信淨買賣超", fmt_num(s.get("投信")))
    c.metric("自營商淨買賣超", fmt_num(s.get("自營商")))
    d4.metric("三大法人合計", fmt_num(s.get("合計")))

    with st.expander("查看 T86 明細（最多 200 列）", expanded=False):
        st.json((t86.get("df") or [])[:200])

st.divider()

# -------------------------
# Payload editor
# -------------------------
st.markdown("### 🧩 輸入 JSON Payload")

if "payload_text" not in st.session_state:
    st.session_state["payload_text"] = "{}"

left, right = st.columns(2)

with left:
    if st.button("載入標準範本（含 TopN）"):
        payload = build_v203_min_json(
            snapshot=snap,
            system_params=build_default_params(),
            portfolio=build_default_portfolio(),
            monitoring=build_default_monitoring(),
            session=session_name,
        )
        st.session_state["payload_text"] = json.dumps(payload, ensure_ascii=False, indent=2)
        st.rerun()

    p_text = st.text_area("JSON 內容", value=st.session_state["payload_text"], height=560)
    st.session_state["payload_text"] = p_text

with right:
    st.markdown("### ✅ Arbiter 裁決結果（單一入口）")
    if st.button("🚀 執行裁決", type="primary"):
        try:
            payload = json.loads(st.session_state["payload_text"])
            result = arbiter_run(payload, run_mode=run_mode)

            # High level status
            if result.get("VERDICT") == "NO_TRADE":
                st.error("VERDICT=NO_TRADE：已阻擋（L1 FAIL 或策略 NO_TRADE）")
            else:
                st.success("VERDICT=EXECUTED：已完成裁決")

            # Show action summary safely (never indexes into lists)
            ucc = result.get("UCC") or {}
            st.markdown("#### ① 動作摘要（Counts）")
            st.json(action_summary(ucc))

            st.markdown("#### ② L1 稽核（AUDIT.L1）")
            st.json(((result.get("AUDIT") or {}).get("L1")) or {})

            st.markdown("#### ③ UCC 全輸出（Normalized）")
            st.json(ucc)

        except Exception as e:
            st.exception(e)

# -------------------------
# Situation summary (no fake data)
# -------------------------
st.divider()
st.markdown("### 🛡️ 戰情摘要（不造假：僅用已取得資料）")
chg_pct = twii.get("chg_pct") if twii_ok else None
total_amt = ma.get("amount_total")

if chg_pct is None:
    st.info("目前無法取得 TWII 漲跌幅（TWSE 指數資料缺漏）。")
else:
    if chg_pct <= -0.02:
        status = "🔵 大盤轉弱（單日跌幅 ≥ 2%）"
    elif chg_pct >= 0.015:
        status = "🟢 大盤偏強（單日漲幅 ≥ 1.5%）"
    else:
        status = "🟡 大盤中性"

    st.info(
        f"{status}\n\n"
        f"- 大盤日漲跌幅：{fmt_pct(chg_pct)}\n"
        f"- 總成交額：{fmt_money(total_amt)}\n"
    )
