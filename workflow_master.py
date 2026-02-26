# workflow_master.py
# -*- coding: utf-8 -*-
"""
Workflow Master (NO-DRIFT / Single Entry)
Data-Layer (downloader_tw) -> Payload Builder (build_v203_min_json) -> Arbiter Orchestrator (arbiter_run)

Design goals:
- Single execution entrypoint for GitHub Actions / CLI / future schedulers
- Deterministic audit chain: if L1 FAIL -> exit(1) to stop workflow (avoid committing bad artifacts)
- Produce artifacts:
  - macro.json (lightweight snapshot for downstream)
  - reports/report_YYYYMMDD_HHMMSS.json
  - reports/report_YYYYMMDD_HHMMSS.txt
  - data/snapshot_YYYYMMDD.json (optional but useful for debugging)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from downloader_tw import build_snapshot, build_v203_min_json
from arbiter import arbiter_run

TZ_TPE = timezone(timedelta(hours=8))


def now_tpe() -> datetime:
    return datetime.now(TZ_TPE)


def dt_stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def dump_json(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)


def dump_text(s: str, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(s)


def build_default_system_params() -> Dict[str, Any]:
    # 你可以把這些搬到 config json；此處先提供穩定預設，避免外推
    return {
        # L2 params
        "k_regime": 1.2,
        "lambda_drawdown": 2.0,
        "max_loss_per_trade_pct": 0.02,
        "stress_drawdown_trigger": 0.10,
        "min_trades_for_trust": 8,
        "trust_default_when_insufficient": 0.49,
        "trust_attack_scale_low": 0.30,
        # L1 price sanity (V20.1+)
        "l1_price_min": 1,
        "l1_price_max": 5000,
        "l1_price_median_mult_hi": 50,
        # recency scaling
        "prev_day_allocation_scale": 0.70,
        # Kronos pack default OFF (你要上 V20.4 再開)
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
    # Human-friendly summary for quick reading
    lines = []
    lines.append("Predator Daily Report (Workflow Master)")
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
        # 只摘要，避免 txt 爆炸
        for k in ("MODE", "ENGINE", "DECISION", "NO_TRADE", "RISK_REASON", "CONFIDENCE"):
            if k in ucc:
                lines.append(f"{k}: {ucc.get(k)}")

    lines.append("")
    return "\n".join(lines)


def build_macro_json(snapshot: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    # 給你 repo 內既有 macro.json 使用者（下游/報表/監控）
    twii_pack = snapshot.get("twii") or {}
    twii = (twii_pack.get("data") or {}) if twii_pack.get("ok") else {}
    ma = snapshot.get("market_amount") or {}
    rec = snapshot.get("recency") or {}
    integ = snapshot.get("integrity") or {}

    m = {
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
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", default="EOD", choices=["EOD", "INTRADAY"])
    ap.add_argument("--run", default="L2", choices=["L1", "L2", "L3"])
    ap.add_argument("--date", default="", help="YYYY-MM-DD in Asia/Taipei. empty => today")
    ap.add_argument("--topn", default=20, type=int)
    ap.add_argument("--equity", default=2_000_000, type=int)

    args = ap.parse_args()

    # Resolve date
    if args.date:
        target_dt = datetime.fromisoformat(args.date).replace(tzinfo=TZ_TPE)
    else:
        d = now_tpe().date()
        target_dt = datetime(d.year, d.month, d.day, tzinfo=TZ_TPE)

    # Build snapshot (data-layer)
    snapshot = build_snapshot(target_dt, session_name=args.session, top_n=int(args.topn))

    # Assemble payload (V20.3-compatible minimal JSON builder)
    system_params = build_default_system_params()
    portfolio = build_default_portfolio(equity=int(args.equity))
    monitoring = build_default_monitoring()

    payload = build_v203_min_json(
        snapshot=snapshot,
        system_params=system_params,
        portfolio=portfolio,
        monitoring=monitoring,
        session=args.session,
    )

    # Arbiter unified entrypoint (L1 -> UCC)
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

    # If NO_TRADE (i.e., blocked by L1), exit non-zero to stop workflow commit/push
    if result.get("NO_TRADE") is True or result.get("VERDICT") == "NO_TRADE":
        print("[BLOCK] NO_TRADE triggered. Exiting with code 1 to stop CI publish.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
