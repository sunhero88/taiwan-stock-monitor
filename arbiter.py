# arbiter.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional


def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v
    except Exception:
        return float(default)


def _get_macro_overview(macro: dict) -> dict:
    # macro 可傳入整包或已是 overview；此處做相容
    if not isinstance(macro, dict):
        return {}
    if "overview" in macro and isinstance(macro["overview"], dict):
        return macro["overview"]
    return macro


def _parse_inst_net_ab(overview: dict) -> tuple[Optional[float], Optional[float]]:
    """
    inst_net 支援格式：
    1) {"A": <float>, "B": <float>}
    2) {"A_total": <float>, "B_foreign": <float>}
    3) 字串（舊版）→ 無法解析則回 None
    """
    inst_net = overview.get("inst_net")
    if isinstance(inst_net, dict):
        a = inst_net.get("A", inst_net.get("A_total"))
        b = inst_net.get("B", inst_net.get("B_foreign"))
        a = _safe_float(a, default=None) if a is not None else None
        b = _safe_float(b, default=None) if b is not None else None
        return a, b
    return None, None


def _macro_boost_factor(a_net: Optional[float], b_net: Optional[float]) -> float:
    """
    以「方向」做輕量加權（不做預測，只做風險/信心微調）：
    - A>0 且 B>0：順風（boost=1.2）
    - A<0 且 B<0：逆風（boost=0.7）
    - 其餘：中性（boost=1.0）

    這裡不設金額閾值，因為金額尺度會隨市場與資料源變動；
    若你後續要更精準，可在 JSON 端提供「inst_net_unit/threshold」。
    """
    if a_net is None or b_net is None:
        return 1.0
    if a_net > 0 and b_net > 0:
        return 1.2
    if a_net < 0 and b_net < 0:
        return 0.7
    return 1.0


def arbitrate(stock: dict, macro: dict, account: str = "Conservative") -> Dict[str, Any]:
    """
    回傳單一股票的最終裁決（V15.6.3）
    - 嚴格遵守 Data Health Gate：degraded_mode / inst_status != READY / kill_switch / v14_watch → 禁止 BUY/TRIAL
    - 加入 inst_net A/B（市場整體）作為「倉位微調與嚴格度微調」：不改核心門檻，只在允許範圍內調整 action_size_pct
    """

    overview = _get_macro_overview(macro)

    # ---------- Step 1：Data Health Gate ----------
    inst_status = overview.get("inst_status", "PENDING")
    degraded_mode = bool(overview.get("degraded_mode", inst_status != "READY"))

    degraded = (
        degraded_mode
        or inst_status != "READY"
        or bool(overview.get("kill_switch", False))
        or bool(overview.get("v14_watch", False))
    )

    inst = stock.get("Institutional", {}) or {}
    tech = stock.get("Technical", {}) or {}
    struct = stock.get("Structure", {}) or {}
    ranking = stock.get("ranking", {}) or {}
    risk = stock.get("risk", {}) or {}
    weaken_flags = stock.get("weaken_flags", {}) or {}
    orphan_holding = bool(stock.get("orphan_holding", False))

    position_pct_max = int(_safe_float(risk.get("position_pct_max", 12), default=12))
    risk_per_trade_max = _safe_float(risk.get("risk_per_trade_max", 1.0), default=1.0)
    trial_flag = bool(risk.get("trial_flag", True))

    # ---------- Default output ----------
    decision = "WATCH"
    action_size = 0
    exit_code = "None"

    # ---------- 降級模式（強制） ----------
    if degraded:
        return {
            "Decision": "WATCH",
            "action_size_pct": 0,
            "exit_reason_code": "DATA_DEGRADED",
            "degraded_note": "資料降級：是（禁止 BUY）",
            "reason_technical": "資料健康門觸發：禁止交易進場。",
            "reason_structure": "資料健康門觸發：禁止交易進場。",
            "reason_inst": f"inst_status={inst_status} / degraded_mode={degraded_mode}",
        }

    # ---------- Step 2：法人連續性硬規則 ----------
    inst_ok = (
        inst.get("Inst_Status") == "READY"
        and int(_safe_float(inst.get("Inst_Streak3", 0), default=0)) >= 3
        and inst.get("Inst_Dir3") == "POSITIVE"
    )

    # ---------- Step 3：Top20 池化裁決 ----------
    top20_flag = bool(ranking.get("top20_flag", False))
    tier = str(ranking.get("tier", "B"))

    # Top20 以外且非持倉 → 直接 IGNORE（明確化）
    if (not top20_flag) and (not orphan_holding):
        return {
            "Decision": "IGNORE",
            "action_size_pct": 0,
            "exit_reason_code": "OUT_OF_POOL",
            "degraded_note": "資料降級：否",
            "reason_technical": "不在 Top20 且非持倉，依規則忽略。",
            "reason_structure": "不在 Top20 且非持倉，依規則忽略。",
            "reason_inst": "不在 Top20 池化範圍。",
        }

    # Orphan 持倉處理：不自動賣出；若 weaken → REDUCE
    if orphan_holding:
        tech_weaken = bool(weaken_flags.get("technical_weaken", False))
        struct_weaken = bool(weaken_flags.get("structure_weaken", False))
        if tech_weaken or struct_weaken:
            return {
                "Decision": "REDUCE",
                "action_size_pct": min(5, position_pct_max),
                "exit_reason_code": "STRUCTURE_WEAK" if struct_weaken else "TECH_BREAK",
                "degraded_note": "資料降級：否",
                "reason_technical": "Orphan 持倉且 technical_weaken=True → 減碼。",
                "reason_structure": "Orphan 持倉且 structure_weaken=True → 減碼。" if struct_weaken else "Orphan 持倉：結構未弱化。",
                "reason_inst": "Orphan 持倉規則：不自動賣出，僅在弱化時減碼。",
            }
        return {
            "Decision": "HOLD",
            "action_size_pct": 0,
            "exit_reason_code": "None",
            "degraded_note": "資料降級：否",
            "reason_technical": "Orphan 持倉且無弱化 → HOLD。",
            "reason_structure": "Orphan 持倉且無弱化 → HOLD。",
            "reason_inst": "Orphan 持倉規則。",
        }

    # ---------- Step 4：雙帳戶決策引擎（核心門檻不改） ----------
    score = _safe_float(tech.get("Score", 0), default=0.0)
    rev_growth = _safe_float(struct.get("Rev_Growth", -999), default=-999.0)

    # 技術正向訊號（簡化：以 Score 門檻代表 ≥1 或 ≥2 訊號）
    # 若你已在 JSON 端提供明確訊號計數，可改用 signal_count。
    tech_ok_1 = score >= 45
    tech_ok_2 = score >= 50

    # ---------- Macro A/B 淨額（用於 action_size 微調） ----------
    a_net, b_net = _parse_inst_net_ab(overview)
    boost = _macro_boost_factor(a_net, b_net)

    # 標準單位（你在前面要求定義）
    UNIT_TRIAL = 5
    UNIT_BUY_HIGH = 10

    if account == "Conservative":
        # BUY 條件（保持你原邏輯：Score>=50 + Rev_Growth>=0 + inst_ok）
        if tier == "A" and tech_ok_2 and rev_growth >= 0 and inst_ok:
            decision = "BUY"
            # 先給高信心 10%，再乘 macro boost，最後受 position_pct_max 限制
            size = int(round(UNIT_BUY_HIGH * boost))
            action_size = min(max(size, 5), position_pct_max)  # 至少 5%，不超上限
        else:
            decision = "WATCH"
            action_size = 0

    elif account == "Aggressive":
        # TRIAL 條件（Top20 + Score>=45 + inst_dir3 != NEGATIVE + trial_flag）
        inst_dir3 = inst.get("Inst_Dir3", "PENDING")
        if top20_flag and tech_ok_1 and inst_dir3 != "NEGATIVE" and trial_flag:
            decision = "TRIAL"
            size = int(round(UNIT_TRIAL * boost))
            action_size = min(max(size, 3), position_pct_max)  # 允許縮到 3% 做試單
        else:
            decision = "WATCH"
            action_size = 0

    else:
        decision = "WATCH"
        action_size = 0

    # ---------- Step 5：風控不可突破 ----------
    # 已用 position_pct_max 截斷；risk_per_trade_max 需你在下單模組依停損距離換算，
    # Arbiter 僅輸出 cap 與建議 size，不直接推導金額風險。
    if action_size > position_pct_max:
        action_size = position_pct_max
        exit_code = "RISK_LIMIT"

    # 若 A/B 同為負，且本來要 BUY/TRIAL → 保守降一級（效益最大化＝避免逆風加碼）
    if (a_net is not None and b_net is not None) and (a_net < 0 and b_net < 0):
        if decision == "BUY":
            # 逆風時仍允許 BUY（因已滿足嚴格條件），但縮倉到 5%
            action_size = min(action_size, 5)
        elif decision == "TRIAL":
            action_size = min(action_size, 3)

    return {
        "Decision": decision,
        "action_size_pct": int(action_size),
        "exit_reason_code": exit_code,
        "degraded_note": "資料降級：否",
        "reason_technical": f"Score={score:.1f}，門檻：TRIAL>=45 / BUY>=50。",
        "reason_structure": f"Rev_Growth={rev_growth:.2f}（需 >=0 才允許 BUY）。",
        "reason_inst": f"inst_ok={inst_ok} / Inst_Streak3={inst.get('Inst_Streak3')} / Inst_Dir3={inst.get('Inst_Dir3')} / Macro(A,B)=({a_net},{b_net}) boost={boost}",
        "risk_caps": {
            "position_pct_max": position_pct_max,
            "risk_per_trade_max": risk_per_trade_max,
        },
    }
