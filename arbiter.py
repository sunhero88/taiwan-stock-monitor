# arbiter.py
# -*- coding: utf-8 -*-
"""
Predator Arbiter — Single Entrypoint / Stable Output Schema

核心目標
1) arbiter_run(payload, run_mode) 作為唯一裁決入口（UI/CLI/CI 一致）
2) 永遠先跑 L1 Gate（verify_integrity.l1_gate）
3) 永遠輸出「不會炸」的標準化 schema
   - UCC 內永遠有 OPEN/ADD/HOLD/REDUCE/CLOSE/NO_TRADE（list）
   - 最上層也永遠有 AUDIT.L1 / ENGINE / VERDICT / NO_TRADE 等欄位

注意
- 此檔案不再依賴 ucc_v19_1.py（完全移除）
- 引擎改由 ucc_engine.UCCEngine 提供
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import json

from verify_integrity import l1_gate
from ucc_engine import UCCEngine


ACTION_KEYS = ["OPEN", "ADD", "HOLD", "REDUCE", "CLOSE", "NO_TRADE"]


def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _infer_decision(ucc: Dict[str, Any]) -> str:
    # 依 action 優先序推導決策（若引擎沒給 DECISION）
    if len(ucc.get("NO_TRADE", [])) > 0:
        return "NO_TRADE"
    if len(ucc.get("CLOSE", [])) > 0:
        return "CLOSE"
    if len(ucc.get("REDUCE", [])) > 0:
        return "REDUCE"
    if len(ucc.get("ADD", [])) > 0:
        return "ADD"
    if len(ucc.get("OPEN", [])) > 0:
        return "OPEN"
    if len(ucc.get("HOLD", [])) > 0:
        return "HOLD"
    return "HOLD"


def normalize_ucc_output(ucc_out: Any) -> Dict[str, Any]:
    """
    把任何形態的引擎輸出，轉成穩定 dict：
    - 保證 ACTION_KEYS 全存在且為 list
    - 若缺 DECISION / CONFIDENCE / RISK_REASON 會補預設值
    """
    if not isinstance(ucc_out, dict):
        ucc_out = {"raw": str(ucc_out)}

    # 強制 action keys 存在且為 list
    for k in ACTION_KEYS:
        ucc_out[k] = _ensure_list(ucc_out.get(k))

    # 補決策欄位
    if "DECISION" not in ucc_out or not ucc_out.get("DECISION"):
        ucc_out["DECISION"] = _infer_decision(ucc_out)

    # 補信心與風險原因
    if "CONFIDENCE" not in ucc_out:
        ucc_out["CONFIDENCE"] = "—"
    if "RISK_REASON" not in ucc_out:
        ucc_out["RISK_REASON"] = None

    # 統一 MODE（避免 UI 用舊 key 亂抓）
    if "MODE" not in ucc_out:
        ucc_out["MODE"] = "UCC_ENGINE"

    return ucc_out


def _blocked_ucc(reason: str) -> Dict[str, Any]:
    """
    L1 FAIL 時回傳一個標準化的 UCC（依然具備 action lists）
    """
    u = {
        "MODE": "BLOCKED_BY_L1",
        "DECISION": "NO_TRADE",
        "NO_TRADE": [{"reason": reason}],
        "RISK_REASON": reason,
        "CONFIDENCE": "LOW",
    }
    return normalize_ucc_output(u)


def arbiter_run(payload: Dict[str, Any], run_mode: str = "L2") -> Dict[str, Any]:
    """
    唯一裁決入口

    回傳格式（穩定）：
    {
      "MODE": "ARBITER_ORCHESTRATOR",
      "RUN": "L1|L2|L3",
      "VERDICT": "EXECUTED|NO_TRADE",
      "NO_TRADE": bool,
      "RISK_REASON": str|None,
      "AUDIT": {"L1": {...}},
      "ENGINE": {"name": "...", "executed": bool},
      "UCC": { ... normalized ... }
    }
    """
    run_mode = (run_mode or "L2").upper().strip()
    if run_mode not in ("L1", "L2", "L3"):
        run_mode = "L2"

    # -------- L1 Gate --------
    l1_report = l1_gate(payload) or {}
    l1_verdict = l1_report.get("VERDICT")

    if l1_verdict != "PASS":
        ucc_norm = _blocked_ucc("L1_FAIL_DATA_INTEGRITY")
        return {
            "MODE": "ARBITER_ORCHESTRATOR",
            "RUN": run_mode,
            "VERDICT": "NO_TRADE",
            "NO_TRADE": True,
            "RISK_REASON": "L1_FAIL_DATA_INTEGRITY",
            "AUDIT": {"L1": l1_report},
            "ENGINE": {"name": "UCCEngine", "executed": False},
            "UCC": ucc_norm,
        }

    # -------- Execute Engine --------
    engine = UCCEngine()
    ucc_out = engine.run(payload, run_mode=run_mode)
    ucc_norm = normalize_ucc_output(ucc_out)

    # 引擎層 NO_TRADE → 上層也要 NO_TRADE（CI/排程可依此決策）
    engine_no_trade = (ucc_norm.get("DECISION") == "NO_TRADE") or (len(ucc_norm.get("NO_TRADE", [])) > 0)

    return {
        "MODE": "ARBITER_ORCHESTRATOR",
        "RUN": run_mode,
        "VERDICT": "NO_TRADE" if engine_no_trade else "EXECUTED",
        "NO_TRADE": bool(engine_no_trade),
        "RISK_REASON": ucc_norm.get("RISK_REASON"),
        "AUDIT": {"L1": l1_report},
        "ENGINE": {"name": "UCCEngine", "executed": True},
        "UCC": ucc_norm,
    }


def dump_json(obj: Any) -> str:
    """
    Debug helper：印出穩定 JSON
    """
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
