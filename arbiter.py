# arbiter.py
# -*- coding: utf-8 -*-
"""
Predator Arbiter Facade (V20.x compatible) — STABLE OUTPUT EDITION

Goals:
- Single entrypoint for execution: arbiter_run(payload, run_mode)
- Always L1 gate first; L1 FAIL => NO_TRADE + audit
- Normalize output schema so UI/CLI/CI never crashes:
    - Always provide ACTION lists: OPEN/ADD/HOLD/REDUCE/CLOSE/NO_TRADE
    - Always provide AUDIT.L1
    - Always provide UCC (dict), even if blocked
"""

from __future__ import annotations

from typing import Any, Dict, List
import json

from verify_integrity import l1_gate
from ucc_v19_1 import UCCv19_1


ACTION_KEYS = ["OPEN", "ADD", "HOLD", "REDUCE", "CLOSE", "NO_TRADE"]


def _ensure_list(x) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    # sometimes engine returns dict/single object; wrap it
    return [x]


def normalize_ucc_output(ucc_out: Any) -> Dict[str, Any]:
    """
    Normalize UCC output into a stable dict that always contains ACTION_KEYS as lists.

    - If ucc_out is not dict -> {'raw': str(ucc_out)}
    - For each action key -> ensure list exists
    """
    if not isinstance(ucc_out, dict):
        ucc_out = {"raw": str(ucc_out)}

    # Some engines nest actions under 'DECISION' or similar; keep original as-is
    # but we still provide top-level action lists for UI safety.
    for k in ACTION_KEYS:
        ucc_out[k] = _ensure_list(ucc_out.get(k))

    # Provide a stable decision summary if missing
    if "DECISION" not in ucc_out:
        # infer from NO_TRADE / OPEN / HOLD presence
        if len(ucc_out["NO_TRADE"]) > 0:
            ucc_out["DECISION"] = "NO_TRADE"
        elif len(ucc_out["OPEN"]) > 0:
            ucc_out["DECISION"] = "OPEN"
        elif len(ucc_out["ADD"]) > 0:
            ucc_out["DECISION"] = "ADD"
        elif len(ucc_out["REDUCE"]) > 0:
            ucc_out["DECISION"] = "REDUCE"
        elif len(ucc_out["CLOSE"]) > 0:
            ucc_out["DECISION"] = "CLOSE"
        elif len(ucc_out["HOLD"]) > 0:
            ucc_out["DECISION"] = "HOLD"
        else:
            ucc_out["DECISION"] = "HOLD"

    # Ensure confidence exists
    if "CONFIDENCE" not in ucc_out:
        ucc_out["CONFIDENCE"] = "—"

    return ucc_out


def arbiter_run(payload: Dict[str, Any], run_mode: str = "L2") -> Dict[str, Any]:
    """
    Orchestrated arbiter run.

    Output contract (stable):
    {
      MODE, RUN, VERDICT, NO_TRADE, RISK_REASON,
      AUDIT: {L1: ...},
      ENGINE: {name, executed},
      UCC: normalized dict with OPEN/ADD/HOLD/REDUCE/CLOSE/NO_TRADE lists
    }
    """
    run_mode = (run_mode or "L2").upper()

    # ---- L1 Gate ----
    l1_report = l1_gate(payload)

    if l1_report.get("VERDICT") != "PASS":
        # Block execution. Still return normalized UCC skeleton.
        ucc_norm = normalize_ucc_output({
            "MODE": "BLOCKED_BY_L1",
            "NO_TRADE": [{"reason": "L1_FAIL_DATA_INTEGRITY"}],
            "RISK_REASON": "L1_FAIL_DATA_INTEGRITY",
            "CONFIDENCE": "LOW"
        })
        return {
            "MODE": "ARBITER_ORCHESTRATOR",
            "RUN": run_mode,
            "VERDICT": "NO_TRADE",
            "NO_TRADE": True,
            "RISK_REASON": "L1_FAIL_DATA_INTEGRITY",
            "AUDIT": {"L1": l1_report},
            "ENGINE": {"name": "UCCv19_1", "executed": False},
            "UCC": ucc_norm,
        }

    # ---- Execute UCC ----
    ucc = UCCv19_1()
    ucc_out = ucc.run(payload, run_mode=run_mode)
    ucc_norm = normalize_ucc_output(ucc_out)

    # If engine itself says no-trade, mirror at arbiter layer for CI safety
    engine_no_trade = False
    if isinstance(ucc_norm.get("NO_TRADE"), list) and len(ucc_norm["NO_TRADE"]) > 0:
        engine_no_trade = True
    if ucc_norm.get("DECISION") == "NO_TRADE":
        engine_no_trade = True

    return {
        "MODE": "ARBITER_ORCHESTRATOR",
        "RUN": run_mode,
        "VERDICT": "EXECUTED" if not engine_no_trade else "NO_TRADE",
        "NO_TRADE": bool(engine_no_trade),
        "RISK_REASON": ucc_norm.get("RISK_REASON", None),
        "AUDIT": {"L1": l1_report},
        "ENGINE": {"name": "UCCv19_1", "executed": True},
        "UCC": ucc_norm,
    }


def dump_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
