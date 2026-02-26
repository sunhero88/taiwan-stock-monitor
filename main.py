# main.py
# Sunhero｜中控台 (Data-Layer + L1 Gate + UCC)
# - Market data from downloader_tw (TWSE endpoints + tiered fallback + audit)
# - Pre-run L1 integrity gate from verify_integrity (V20.x)
# - If L1 FAIL -> block execution (NO_TRADE)
# - No yfinance dependency

import json
from datetime import datetime, timedelta, timezone

import streamlit as st

from ucc_v19_1 import UCCv19_1
from downloader_tw import build_snapshot, build_v203_min_json
from verify_integrity import l1_gate

TZ_TPE = timezone(timedelta(hours=8))


# =========================
# UI config
# =========================
st.set_page_config(page_title="Sunhero｜中控台", layout="wide")
st.title("Sunhero｜股市智能超盤中控台（Data-Layer + L1 Gate）")


# =========================
# Caching wrappers
# =========================
@st.cache_data(ttl=120)
def get_snapshot_cached(target_iso: str, session_name: str, top_n: int = 20):
    # target_iso: "YYYY-MM-DD"
    dt = datetime.fromisoformat(target_iso).replace(tzinfo=TZ_TPE)
    return build_snapshot(dt, session_name=session_name, top_n=top_n)


def fmt_money(n):
    if n is None:
        return "—"
    return f"{int(n):,}"


def fmt_num(n):
    if n is None:
        return "—"
    return f"{int(n):,}"


def fmt_pct(x):
    if x is None:
        return "—"
    return f"{x*100:.2f}%"


# =========================
# Sidebar controls
# =========================
with st.sidebar:
    st.subheader("模式 / 交易日")
    run_mode = st.radio(
        "RUN 模式",
        ["L1", "L2", "L3"],
        format_func=lambda x: {"L1": "L1（審計）", "L2": "L2（裁決）", "L3": "L3（壓測）"}[x],
    )
    session_name = st.selectbox("Session", ["EOD", "INTRADAY"], index=0)

    today = datetime.now(TZ_TPE).date()
    target_date = st.date_input("目標日期（台北）", value=today)
    top_n = st.slider("TopN（上市成交額排序）", min_value=10, max_value=50, value=20, step=5)

    if st.button("立即更新", type="primary"):
        st.cache_data.clear()
        st.rerun()


# =========================
# Fetch snapshot
# =========================
target_iso = target_date.isoformat()
snap = get_snapshot_cached(target_iso, session_name=session_name, top_n=top_n)

twii_ok = bool(snap.get("twii", {}).get("ok"))
twii = (snap.get("twii", {}).get("data") or {}) if twii_ok else {}
ma = snap.get("market_amount") or {}
t86 = snap.get("t86") or {}
rec = snap.get("recency") or {}

# =========================
# Dashboard metrics
# =========================
st.markdown("### 📊 市場狀態（以資料層輸出為準）")

c1, c2, c3, c4 = st.columns(4)

with c1:
    if twii_ok and twii.get("close") is not None:
        delta_txt = f"{twii.get('chg'):+.2f}" if twii.get("chg") is not None else None
        st.metric("加權指數 TWII（TWSE）", f"{twii.get('close'):,.2f}", delta_txt)
        st.caption(f"資料日：{twii.get('date', snap.get('trade_date_iso'))}")
    else:
        st.metric("加權指數 TWII（TWSE）", "—")
        st.caption("TWII 讀取失敗（L1 會直接 FAIL）")

with c2:
    st.metric("上市成交額（TWSE）", fmt_money(ma.get("amount_twse")))
    src = (ma.get("twse_amount_meta") or {}).get("source_name")
    err = (ma.get("twse_amount_meta") or {}).get("error_code")
    st.caption(f"來源：{src or '—'}｜錯誤：{err or '—'}")

with c3:
    st.metric("上櫃成交額（TPEX）", fmt_money(ma.get("amount_tpex")))
    tpex_meta = ma.get("tpex_amount_meta") or {}
    st.caption(f"Tier={tpex_meta.get('tier')}｜來源：{tpex_meta.get('source_name')}｜錯誤：{tpex_meta.get('error_code') or '—'}")

with c4:
    st.metric("總成交額", fmt_money(ma.get("amount_total")))
    st.caption(f"EOD Guard：is_using_previous_day={rec.get('is_using_previous_day')}｜effective_trade_date={rec.get('effective_trade_date')}")


# =========================
# Institutional (T86)
# =========================
st.markdown("### 🧾 三大法人（TWSE T86）")
if not t86.get("ok"):
    st.error(f"T86 讀取失敗：{(t86.get('meta') or {}).get('error_code')}")
else:
    s = t86.get("summary") or {}
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("外資淨買賣超", fmt_num(s.get("外資及陸資(不含外資自營商)")))
    d2.metric("投信淨買賣超", fmt_num(s.get("投信")))
    d3.metric("自營商淨買賣超", fmt_num(s.get("自營商")))
    d4.metric("三大法人合計", fmt_num(s.get("合計")))

    with st.expander("查看 T86 明細（records）", expanded=False):
        st.json((t86.get("df") or [])[:200])  # 避免一次噴太多


# =========================
# Payload editor + L1 Gate + UCC run
# =========================
if "payload_text" not in st.session_state:
    st.session_state["payload_text"] = "{}"

left, right = st.columns(2)

with left:
    st.subheader("輸入 JSON Payload（可貼 Arbiter JSON 或用範本生成）")

    if st.button("載入標準範本（以 TopN + 市場資料層組裝）"):
        # 最小必要 system_params（包含 L1 價格 sanity 閾值）
        system_params = {
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
            # Kronos 預設關閉（你之後要上 V20.4 再打開）
            "kronos_enabled": False,
        }

        portfolio = {
            "equity": 2_000_000,
            "drawdown_pct": -0.06,
            "loss_streak": 0,
            "alpha_prev": 0.45,
        }

        monitoring = {
            "regime_predictive_score": 0.70,
            "regime_outcome_score": 0.70,
            "trade_count_20d": 10,
        }

        # 直接用 snapshot 組 V20.x 兼容 min-json
        payload = build_v203_min_json(
            snapshot=snap,
            system_params=system_params,
            portfolio=portfolio,
            monitoring=monitoring,
            session=session_name,
        )

        st.session_state["payload_text"] = json.dumps(payload, ensure_ascii=False, indent=2)
        st.rerun()

    p_input = st.text_area("JSON 內容", value=st.session_state["payload_text"], height=560)
    st.session_state["payload_text"] = p_input

with right:
    st.subheader("執行結果（先 L1 Gate，PASS 才允許 UCC）")

    if st.button("🚀 執行（L1 → UCC）", type="primary"):
        try:
            payload = json.loads(p_input)

            # 1) L1 Gate
            report = l1_gate(payload)

            st.markdown("#### ① L1 稽核結果")
            st.json(report)

            if report.get("VERDICT") != "PASS":
                st.error("L1 FAIL：資料完整性不合格 → 已阻止裁決（NO_TRADE）")
            else:
                # 2) UCC
                st.markdown("#### ② UCC 裁決結果")
                result = UCCv19_1().run(payload, run_mode=run_mode)
                st.json(result) if isinstance(result, dict) else st.code(result)

        except Exception as e:
            st.error(f"解析/執行錯誤：{type(e).__name__}: {str(e)}")


# =========================
# Plain-language situation card (NO fake VIX/SMR)
# =========================
st.markdown("---")
st.markdown("### 🛡️ 戰情摘要（不造假：僅用已取得的資料）")

chg_pct = twii.get("chg_pct") if twii_ok else None
total_amt = ma.get("amount_total")

if chg_pct is None:
    st.info("目前無法取得 TWII 漲跌幅（TWSE 指數資料失敗或缺漏）。")
else:
    # 只用可確定的訊號：大盤漲跌幅 + 成交額量級
    if chg_pct <= -0.02:
        status = "🔵 大盤明顯轉弱（單日跌幅 ≥ 2%）"
        advice = "策略重點：降低新增曝險、嚴格停損，避免在波動擴大時放大倉位。"
    elif chg_pct >= 0.015:
        status = "🟢 大盤偏強（單日漲幅 ≥ 1.5%）"
        advice = "策略重點：允許較積極的加碼，但仍以 L1/L2 的風控上限為硬約束。"
    else:
        status = "🟡 大盤中性（波動在常態區間）"
        advice = "策略重點：以個股訊號與法人/成交額結構為主，不追價、不硬猜。"

    amt_txt = f"總成交額約 {fmt_money(total_amt)}" if total_amt is not None else "總成交額未知"
    st.info(f"{status}\n\n- {amt_txt}\n- 大盤日漲跌幅：{fmt_pct(chg_pct)}\n\n{advice}")
