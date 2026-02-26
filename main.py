# main.py
# Sunhero｜中控台 (UI + CLI) — Unified Arbiter Entry (arbiter_run)
# - UI: Streamlit dashboard + payload editor
# - CLI: deterministic daily run that shares the same arbiter_run entrypoint
#        and produces artifacts (reports/, data/, macro.json)

import json
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

# --- Core imports (shared) ---
from downloader_tw import build_snapshot, build_v203_min_json
from arbiter import arbiter_run

TZ_TPE = timezone(timedelta(hours=8))


# =========================
# Shared helpers (UI/CLI)
# =========================
def now_tpe() -> datetime:
    return datetime.now(TZ_TPE)


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def dt_stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def dump_json(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)


def dump_text(s: str, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(s)


def build_default_system_params() -> Dict[str, Any]:
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


def build_default_portfolio(equity: int = 2_000_000) -> Dict[str, Any]:
    return {
        "equity": int(equity),
        "drawdown_pct": -0.06,
        "loss_streak": 0,
        "alpha_prev": 0.45,
    }


def build_default_monitoring() -> Dict[str, Any]:
    return {
        "regime_predictive_score": 0.70,
        "regime_outcome_score": 0.70,
        "trade_count_20d": 10,
    }


def to_report_text(result: Dict[str, Any]) -> str:
    lines = []
    lines.append("Predator Daily Report (main.py CLI)")
    lines.append("=" * 60)
    lines.append(f"MODE: {result.get('MODE')}")
    lines.append(f"RUN: {result.get('RUN')}")
    lines.append(f"VERDICT: {result.get('VERDICT')}")
    lines.append(f"NO_TRADE: {result.get('NO_TRADE')}")
    if result.get("RISK_REASON"):
        lines.append(f"RISK_REASON: {result.get('RISK_REASON')}")

    l1 = ((result.get("AUDIT") or {}).get("L1") or {})
    if l1:
        lines.append("")
        lines.append("[L1 AUDIT]")
        lines.append(f"VERDICT: {l1.get('VERDICT')}")
        fatals = l1.get("FATAL_ISSUES") or []
        warns = l1.get("WARNINGS") or []
        if fatals:
            lines.append("FATAL_ISSUES:")
            for x in fatals:
                lines.append(f"- {x}")
        if warns:
            lines.append("WARNINGS:")
            for x in warns:
                lines.append(f"- {x}")

    ucc = result.get("UCC") or {}
    if ucc:
        lines.append("")
        lines.append("[UCC OUTPUT]")
        for k in ("MODE", "ENGINE", "DECISION", "NO_TRADE", "RISK_REASON", "CONFIDENCE"):
            if k in ucc:
                lines.append(f"{k}: {ucc.get(k)}")

    lines.append("")
    return "\n".join(lines)


def build_macro_json(snapshot: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    twii_pack = snapshot.get("twii") or {}
    twii = (twii_pack.get("data") or {}) if twii_pack.get("ok") else {}
    ma = snapshot.get("market_amount") or {}
    rec = snapshot.get("recency") or {}
    integ = snapshot.get("integrity") or {}

    return {
        "meta": {
            "timestamp": now_tpe().strftime("%Y-%m-%d %H:%M:%S%z"),
            "session": (result.get("RUN") or ""),
            "effective_trade_date": rec.get("effective_trade_date") or snapshot.get("trade_date_iso"),
            "is_using_previous_day": rec.get("is_using_previous_day"),
        },
        "market": {
            "twii": {
                "date": twii.get("date"),
                "close": twii.get("close"),
                "chg": twii.get("chg"),
                "chg_pct": twii.get("chg_pct"),
            },
            "amount": {
                "amount_twse": ma.get("amount_twse"),
                "amount_tpex": ma.get("amount_tpex"),
                "amount_total": ma.get("amount_total"),
                "source_twse": ma.get("source_twse"),
                "source_tpex": ma.get("source_tpex"),
            },
        },
        "integrity": {
            "twii_ok": integ.get("twii_ok"),
            "twse_amount_ok": integ.get("twse_amount_ok"),
            "tpex_tier": integ.get("tpex_tier"),
            "top_ok": integ.get("top_ok"),
            "t86_ok": integ.get("t86_ok"),
        },
        "arbiter": {
            "verdict": result.get("VERDICT"),
            "no_trade": result.get("NO_TRADE"),
            "risk_reason": result.get("RISK_REASON"),
        },
    }


# =========================
# CLI mode (single entrypoint: arbiter_run)
# =========================
def run_cli(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cli", action="store_true", help="Run in CLI mode (no Streamlit UI)")
    ap.add_argument("--session", default="EOD", choices=["EOD", "INTRADAY"])
    ap.add_argument("--run", default="L2", choices=["L1", "L2", "L3"])
    ap.add_argument("--date", default="", help="YYYY-MM-DD Asia/Taipei. empty => today")
    ap.add_argument("--topn", default=20, type=int)
    ap.add_argument("--equity", default=2_000_000, type=int)

    args = ap.parse_args(argv)

    # Resolve date
    if args.date:
        target_dt = datetime.fromisoformat(args.date).replace(tzinfo=TZ_TPE)
    else:
        d = now_tpe().date()
        target_dt = datetime(d.year, d.month, d.day, tzinfo=TZ_TPE)

    # Build snapshot
    snapshot = build_snapshot(target_dt, session_name=args.session, top_n=int(args.topn))

    # Build payload
    payload = build_v203_min_json(
        snapshot=snapshot,
        system_params=build_default_system_params(),
        portfolio=build_default_portfolio(int(args.equity)),
        monitoring=build_default_monitoring(),
        session=args.session,
    )

    # Unified arbiter entry
    result = arbiter_run(payload, run_mode=args.run)

    # Artifacts
    ensure_dir("reports")
    ensure_dir("data")

    ts = now_tpe()
    tag = f"{yyyymmdd(ts)}_{dt_stamp(ts).split('_')[1]}"

    report_json_path = f"reports/report_{tag}.json"
    report_txt_path = f"reports/report_{tag}.txt"
    snapshot_path = f"data/snapshot_{yyyymmdd(target_dt)}.json"
    macro_path = "macro.json"

    dump_json(result, report_json_path)
    dump_text(to_report_text(result), report_txt_path)
    dump_json(snapshot, snapshot_path)
    dump_json(build_macro_json(snapshot, result), macro_path)

    print(f"[OK] wrote: {report_json_path}")
    print(f"[OK] wrote: {report_txt_path}")
    print(f"[OK] wrote: {snapshot_path}")
    print(f"[OK] wrote: {macro_path}")
    print(f"[RESULT] VERDICT={result.get('VERDICT')} NO_TRADE={result.get('NO_TRADE')}")

    # NO_TRADE => non-zero for schedulers
    if result.get("NO_TRADE") is True or result.get("VERDICT") == "NO_TRADE":
        print("[BLOCK] NO_TRADE triggered. exit(1)")
        return 1
    return 0


# =========================
# UI mode (Streamlit) — same as before, but execution uses arbiter_run
# =========================
def run_ui():
    import streamlit as st

    st.set_page_config(page_title="Sunhero｜中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台（Data-Layer + Arbiter Orchestrator）")

    @st.cache_data(ttl=120)
    def get_snapshot_cached(target_iso: str, session_name: str, top_n: int = 20):
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

    target_iso = target_date.isoformat()
    snap = get_snapshot_cached(target_iso, session_name=session_name, top_n=top_n)

    twii_ok = bool(snap.get("twii", {}).get("ok"))
    twii = (snap.get("twii", {}).get("data") or {}) if twii_ok else {}
    ma = snap.get("market_amount") or {}
    t86 = snap.get("t86") or {}
    rec = snap.get("recency") or {}

    st.markdown("### 📊 市場狀態（以資料層輸出為準）")
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        if twii_ok and twii.get("close") is not None:
            delta_txt = f"{twii.get('chg'):+.2f}" if twii.get("chg") is not None else None
            st.metric("加權指數 TWII（TWSE）", f"{twii.get('close'):,.2f}", delta_txt)
            st.caption(f"資料日：{twii.get('date', snap.get('trade_date_iso'))}")
        else:
            st.metric("加權指數 TWII（TWSE）", "—")
            st.caption("TWII 讀取失敗（Arbiter 內部 L1 會直接擋下）")

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

        with st.expander("查看 T86 明細（records，最多 200 列）", expanded=False):
            st.json((t86.get("df") or [])[:200])

    if "payload_text" not in st.session_state:
        st.session_state["payload_text"] = "{}"

    left, right = st.columns(2)

    with left:
        st.subheader("輸入 JSON Payload（可貼 Arbiter JSON 或用範本生成）")

        if st.button("載入標準範本（以 TopN + 市場資料層組裝）"):
            payload = build_v203_min_json(
                snapshot=snap,
                system_params=build_default_system_params(),
                portfolio=build_default_portfolio(),
                monitoring=build_default_monitoring(),
                session=session_name,
            )
            st.session_state["payload_text"] = json.dumps(payload, ensure_ascii=False, indent=2)
            st.rerun()

        p_input = st.text_area("JSON 內容", value=st.session_state["payload_text"], height=560)
        st.session_state["payload_text"] = p_input

    with right:
        st.subheader("執行結果（統一入口：arbiter_run）")

        if st.button("🚀 執行（arbiter_run）", type="primary"):
            try:
                payload = json.loads(p_input)
                result = arbiter_run(payload, run_mode=run_mode)

                verdict = result.get("VERDICT")
                if verdict == "NO_TRADE":
                    st.error("NO_TRADE：已被 L1 Gate 阻擋（資料不可信）")
                else:
                    st.success("EXECUTED：已完成裁決（L1 PASS → UCC）")

                st.markdown("#### ① Arbiter 統一輸出")
                st.json(result)

                st.markdown("#### ② L1 稽核報告（AUDIT.L1）")
                st.json(((result.get("AUDIT") or {}).get("L1") or {}))

                st.markdown("#### ③ UCC 裁決（若有）")
                st.json(result.get("UCC") or {})

            except Exception as e:
                st.error(f"解析/執行錯誤：{type(e).__name__}: {str(e)}")

    st.markdown("---")
    st.markdown("### 🛡️ 戰情摘要（不造假：僅用已取得的資料）")

    chg_pct = twii.get("chg_pct") if twii_ok else None
    total_amt = ma.get("amount_total")

    if chg_pct is None:
        st.info("目前無法取得 TWII 漲跌幅（TWSE 指數資料失敗或缺漏）。")
    else:
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


# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    # CLI fast path: allow running without streamlit
    if "--cli" in sys.argv:
        code = run_cli(sys.argv[1:])
        sys.exit(code)
    else:
        run_ui()
