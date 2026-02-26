# arbiter.py
# -*- coding: utf-8 -*-
"""
Predator Arbiter Facade (V20.x compatible)
- Single entrypoint for decision execution with deterministic audit chain:
    Data-Layer -> L1 Integrity Gate -> UCC Engine
- NO-DRIFT: never补資料，僅依 payload 內容做裁決
- Backward compatible: keep legacy per-stock arbitrate() for old scripts (deprecated)

Key API:
    arbiter_run(payload: dict, run_mode: str = "L2") -> dict
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import json

from verify_integrity import l1_gate
from ucc_v19_1 import UCCv19_1


# =========================
# Core Orchestrator (NEW)
# =========================
def arbiter_run(payload: Dict[str, Any], run_mode: str = "L2") -> Dict[str, Any]:
    """
    Orchestrated arbiter run.

    Behavior:
    - Always runs L1 gate first (verify_integrity.l1_gate).
    - If L1 FAIL => block trading (NO_TRADE) and return audit report.
    - If PASS => call UCC engine and attach L1 audit.

    Output is deterministic and audit-friendly.
    """
    run_mode = (run_mode or "L2").upper()

    # ---- L1 Gate ----
    l1_report = l1_gate(payload)

    if l1_report.get("VERDICT") != "PASS":
        # Block execution. Keep output shape stable.
        return {
            "MODE": "ARBITER_ORCHESTRATOR",
            "RUN": run_mode,
            "VERDICT": "NO_TRADE",
            "NO_TRADE": True,
            "RISK_REASON": "L1_FAIL_DATA_INTEGRITY",
            "AUDIT": {
                "L1": l1_report,
            },
            # minimal compatibility fields (some of your reporters expect these keys)
            "DECISION": {"NO_TRADE": True},
            "ENGINE": {"name": "UCCv19_1", "executed": False},
        }

    # ---- Execute UCC (only if PASS) ----
    ucc = UCCv19_1()
    ucc_out = ucc.run(payload, run_mode=run_mode)

    # Normalize engine output to dict (some versions might return str)
    if not isinstance(ucc_out, dict):
        ucc_out = {"raw": str(ucc_out)}

    return {
        "MODE": "ARBITER_ORCHESTRATOR",
        "RUN": run_mode,
        "VERDICT": "EXECUTED",
        "NO_TRADE": False,
        "AUDIT": {
            "L1": l1_report,
        },
        "ENGINE": {
            "name": "UCCv19_1",
            "executed": True,
        },
        "UCC": ucc_out,
    }


# =========================
# Utility: stable JSON dump (for logging / reports)
# =========================
def dump_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)


# =========================
# LEGACY: per-stock arbiter (DEPRECATED)
# - kept to avoid breaking old scripts
# =========================
def _tech_signals_from_tag(tag: str) -> int:
    tag = (tag or "")
    s = 0
    if "起漲" in tag:
        s += 1
    if "主力" in tag:
        s += 1
    if "真突破" in tag:
        s += 1
    return s


def arbitrate(stock, macro_overview: dict, account="Conservative"):
    """
    DEPRECATED (V15.x legacy per-stock arbiter).
    Kept for backward compatibility only.

    NOTE:
    New pipeline should use arbiter_run(payload) which is:
        Data-Layer -> L1 Gate -> UCC -> decision
    """
    # ---- Original legacy code preserved (minimally edited) ----
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", inst_status != "READY"))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    data_mode = str(macro_overview.get("data_mode", "EOD") or "EOD").upper()
    lag_days = int(macro_overview.get("lag_days", 0) or 0)

    degraded_level2 = (kill_switch or v14_watch)
    inst_missing = (inst_status != "READY") or degraded_mode

    inst = stock.get("Institutional", {}) or {}
    tech = stock.get("Technical", {}) or {}
    struct = stock.get("Structure", {}) or {}
    ranking = stock.get("ranking", {}) or {}
    risk = stock.get("risk", {}) or {}
    weaken = stock.get("weaken_flags", {}) or {}
    orphan_holding = bool(stock.get("orphan_holding", False))

    tier = ranking.get("tier", "B")
    top20_flag = bool(ranking.get("top20_flag", False))

    score = float(tech.get("Score", 0) or 0)
    tag = str(tech.get("Tag", "") or "")
    tech_signals = _tech_signals_from_tag(tag)

    rev_growth = float(struct.get("Rev_Growth", -999) or -999)
    opm = float(struct.get("OPM", 0) or 0)

    position_pct_max = int(risk.get("position_pct_max", 12) or 12)
    trial_flag = bool(risk.get("trial_flag", True))

    technical_weaken = bool(weaken.get("technical_weaken", False))
    structure_weaken = bool(weaken.get("structure_weaken", False))

    def _cap(sz: int) -> int:
        return min(int(sz), int(position_pct_max))

    if data_mode == "STALE":
        if orphan_holding:
            if technical_weaken or structure_weaken:
                return {
                    "Decision": "REDUCE",
                    "action_size_pct": 5,
                    "exit_reason_code": "STALE_DATA_WEAK",
                    "degraded_note": "資料降級：是（STALE：只做持倉風控）",
                    "reason_technical": "資料落後且出現弱化訊號 → 減碼控風險。",
                    "reason_structure": "資料落後且/或結構弱化 → 減碼控風險。",
                    "reason_inst": f"data_mode=STALE, lag_days={lag_days}, inst_status={inst_status}",
                }
            return {
                "Decision": "HOLD",
                "action_size_pct": 0,
                "exit_reason_code": "STALE_DATA_HOLD",
                "degraded_note": "資料降級：是（STALE：只做持倉風控）",
                "reason_technical": "資料落後但未見弱化 → 維持持有，等待資料更新。",
                "reason_structure": "資料落後但未見結構弱化 → 維持持有。",
                "reason_inst": f"data_mode=STALE, lag_days={lag_days}, inst_status={inst_status}",
            }

        return {
            "Decision": "WATCH" if top20_flag else "IGNORE",
            "action_size_pct": 0,
            "exit_reason_code": "STALE_DATA_NO_ENTRY",
            "degraded_note": "資料降級：是（STALE：禁止新倉）",
            "reason_technical": "資料落後（非今日交易日）→ 禁止用舊資料進場。",
            "reason_structure": "資料落後 → 結構條件暫不作為進場依據。",
            "reason_inst": f"data_mode=STALE, lag_days={lag_days}, inst_status={inst_status}",
        }

    if degraded_level2:
        if orphan_holding and (technical_weaken or structure_weaken):
            decision = "REDUCE"
            action_size = 5
            exit_code = "DATA_DEGRADED"
        else:
            decision = "WATCH" if not orphan_holding else "HOLD"
            action_size = 0
            exit_code = "DATA_DEGRADED"

        return {
            "Decision": decision,
            "action_size_pct": action_size,
            "exit_reason_code": exit_code,
            "degraded_note": "資料降級：是（系統級：只賣不買）",
            "reason_technical": "v14_watch/kill_switch 觸發：禁止進場。",
            "reason_structure": "v14_watch/kill_switch 觸發：禁止進場。",
            "reason_inst": f"data_mode={data_mode} / inst_status={inst_status} / degraded_mode={degraded_mode}",
        }

    if orphan_holding:
        if technical_weaken or structure_weaken:
            return {
                "Decision": "REDUCE",
                "action_size_pct": 5,
                "exit_reason_code": "STRUCTURE_WEAK" if structure_weaken else "TECH_BREAK",
                "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
                "reason_technical": "跌出 Top20 且技術弱化，執行減碼。",
                "reason_structure": "跌出 Top20 且/或結構弱化，執行減碼。",
                "reason_inst": f"data_mode={data_mode} / inst_status={inst_status}（不作為砍倉唯一依據）",
            }
        return {
            "Decision": "HOLD",
            "action_size_pct": 0,
            "exit_reason_code": "None",
            "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
            "reason_technical": "跌出 Top20 但未見弱化訊號，維持持有。",
            "reason_structure": "未見結構弱化訊號，維持持有。",
            "reason_inst": f"data_mode={data_mode} / inst_status={inst_status}",
        }

    if not top20_flag:
        return {
            "Decision": "IGNORE",
            "action_size_pct": 0,
            "exit_reason_code": "OUT_OF_POOL",
            "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
            "reason_technical": "不在 Top20 池，且非持倉：忽略。",
            "reason_structure": "不在 Top20 池，且非持倉：忽略。",
            "reason_inst": f"data_mode={data_mode} / inst_status={inst_status}",
        }

    intraday_no_buy_conservative = (data_mode == "INTRADAY" and account == "Conservative")
    intraday_only_trial_aggressive = (data_mode == "INTRADAY" and account == "Aggressive")

    inst_ok = (
        inst.get("Inst_Status") == "READY"
        and int(inst.get("Inst_Streak3", 0) or 0) >= 3
        and inst.get("Inst_Dir3") == "POSITIVE"
    )

    decision = "WATCH"
    action_size = 0
    exit_code = "None"

    if account == "Conservative":
        if intraday_no_buy_conservative:
            if (
                trial_flag
                and tier == "A"
                and tech_signals >= 2
                and score >= 55
                and rev_growth >= 0
                and not (technical_weaken or structure_weaken)
            ):
                decision = "TRIAL"
                action_size = _cap(5)
                exit_code = "INTRADAY_TRIAL_ONLY"
            else:
                decision = "WATCH"
                action_size = 0
                exit_code = "INTRADAY_TRIAL_ONLY"
        else:
            if not inst_missing and inst_ok:
                if tier == "A" and score >= 50 and rev_growth >= 0:
                    decision = "BUY"
                    action_size = _cap(10)
                    exit_code = "None"
                else:
                    decision = "WATCH"
            else:
                if (
                    tier == "A"
                    and tech_signals >= 2
                    and score >= 55
                    and rev_growth >= 0
                    and not (technical_weaken or structure_weaken)
                ):
                    decision = "BUY"
                    action_size = _cap(5)
                    exit_code = "INST_MISSING_MODE"
                else:
                    decision = "WATCH"
                    action_size = 0
                    exit_code = "INST_MISSING_MODE"

    elif account == "Aggressive":
        if intraday_only_trial_aggressive:
            if (
                trial_flag
                and score >= 45
                and tech_signals >= 1
                and rev_growth >= 0
                and not (technical_weaken or structure_weaken)
            ):
                action_size = _cap(10 if (tier == "A" and tech_signals >= 2 and score >= 55) else 5)
                decision = "TRIAL"
                exit_code = "INTRADAY_TRIAL_ONLY"
            else:
                decision = "WATCH"
                action_size = 0
                exit_code = "INTRADAY_TRIAL_ONLY"
        else:
            if not inst_missing and inst_ok:
                if tier == "A" and score >= 50 and rev_growth >= 0:
                    decision = "BUY"
                    action_size = _cap(10)
                    exit_code = "None"
                elif score >= 45 and inst.get("Inst_Dir3") != "NEGATIVE":
                    decision = "TRIAL"
                    action_size = _cap(5)
                    exit_code = "None"
                else:
                    decision = "WATCH"
            else:
                if (
                    trial_flag
                    and score >= 45
                    and tech_signals >= 1
                    and rev_growth >= 0
                    and not (technical_weaken or structure_weaken)
                ):
                    decision = "TRIAL"
                    action_size = _cap(10 if (tier == "A" and tech_signals >= 2 and score >= 55) else 5)
                    exit_code = "INST_MISSING_MODE"
                else:
                    decision = "WATCH"
                    action_size = 0
                    exit_code = "INST_MISSING_MODE"

    if decision in ("BUY", "TRIAL") and (technical_weaken or structure_weaken):
        decision = "WATCH"
        action_size = 0
        exit_code = "TECH_BREAK" if technical_weaken else "STRUCTURE_WEAK"

    degraded_note = "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）"

    return {
        "Decision": decision,
        "action_size_pct": int(action_size),
        "exit_reason_code": exit_code,
        "degraded_note": degraded_note,
        "reason_technical": f"data_mode={data_mode}, tag_signals={tech_signals}, Score={score}, Tag='{tag}'",
        "reason_structure": f"Rev_Growth={rev_growth}%, OPM={opm}%",
        "reason_inst": f"inst_status={inst_status} / Inst_Status={inst.get('Inst_Status','PENDING')}（無法人時不作為進場唯一依據）",
    }
