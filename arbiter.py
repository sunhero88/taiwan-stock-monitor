# arbiter.py
# -*- coding: utf-8 -*-
from __future__ import annotations


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
    回傳單一股票的最終裁決（無法人資料亦可運作）
    規則版本：V15.6.3-NA + data_mode gate

    macro_overview：請傳 macro["overview"]
    新增欄位：
      - data_mode: "INTRADAY" | "EOD" | "STALE"
        * INTRADAY：禁止 BUY（最多 TRIAL）
        * STALE：禁止任何新進場（只做持倉管理）
        * EOD：照完整規則
    """

    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", inst_status != "READY"))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    # ✅ 新增：資料模式
    data_mode = (macro_overview.get("data_mode", "EOD") or "EOD").upper()

    # ---------- 兩階降級 ----------
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

    # ---------- Level-2：只賣不買 ----------
    if degraded_level2:
        if orphan_holding and (technical_weaken or structure_weaken):
            decision = "REDUCE"
            action_size = 5
        else:
            decision = "WATCH" if not orphan_holding else "HOLD"
            action_size = 0

        return {
            "Decision": decision,
            "action_size_pct": int(action_size),
            "exit_reason_code": "DATA_DEGRADED",
            "degraded_note": "資料降級：是（系統級：只賣不買）",
            "reason_technical": "v14_watch/kill_switch 觸發：禁止進場。",
            "reason_structure": "v14_watch/kill_switch 觸發：禁止進場。",
            "reason_inst": f"inst_status={inst_status} / degraded_mode={degraded_mode}",
            "reason_data_mode": f"data_mode={data_mode}",
        }

    # ---------- ✅ data_mode = STALE：禁止新進場，只做持倉管理 ----------
    if data_mode == "STALE":
        if orphan_holding:
            if technical_weaken or structure_weaken:
                return {
                    "Decision": "REDUCE",
                    "action_size_pct": 5,
                    "exit_reason_code": "DATA_STALE",
                    "degraded_note": "資料降級：是（資料落後：只做持倉管理）",
                    "reason_technical": "資料落後：不允許新進場；持倉且出現弱化 → 減碼。",
                    "reason_structure": "資料落後：不允許新進場；持倉且出現弱化 → 減碼。",
                    "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
                    "reason_data_mode": f"data_mode={data_mode}",
                }
            return {
                "Decision": "HOLD",
                "action_size_pct": 0,
                "exit_reason_code": "DATA_STALE",
                "degraded_note": "資料降級：是（資料落後：只做持倉管理）",
                "reason_technical": "資料落後：不允許新進場；持倉且未弱化 → 持有。",
                "reason_structure": "資料落後：不允許新進場；持倉且未弱化 → 持有。",
                "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
                "reason_data_mode": f"data_mode={data_mode}",
            }

        return {
            "Decision": "WATCH",
            "action_size_pct": 0,
            "exit_reason_code": "DATA_STALE",
            "degraded_note": "資料降級：是（資料落後：禁止新進場）",
            "reason_technical": "資料落後：禁止新進場。",
            "reason_structure": "資料落後：禁止新進場。",
            "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
            "reason_data_mode": f"data_mode={data_mode}",
        }

    # ---------- 持倉管理優先（跌出名單不自動砍） ----------
    if orphan_holding:
        if technical_weaken or structure_weaken:
            return {
                "Decision": "REDUCE",
                "action_size_pct": 5,
                "exit_reason_code": "STRUCTURE_WEAK" if structure_weaken else "TECH_BREAK",
                "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
                "reason_technical": "跌出 Top20 且技術弱化，執行減碼。",
                "reason_structure": "跌出 Top20 且/或結構弱化，執行減碼。",
                "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
                "reason_data_mode": f"data_mode={data_mode}",
            }
        return {
            "Decision": "HOLD",
            "action_size_pct": 0,
            "exit_reason_code": "None",
            "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
            "reason_technical": "跌出 Top20 但未見弱化訊號，維持持有。",
            "reason_structure": "未見結構弱化訊號，維持持有。",
            "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
            "reason_data_mode": f"data_mode={data_mode}",
        }

    # ---------- 非 Top20 且非持倉：忽略 ----------
    if not top20_flag:
        return {
            "Decision": "IGNORE",
            "action_size_pct": 0,
            "exit_reason_code": "OUT_OF_POOL",
            "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
            "reason_technical": "不在 Top20 池，且非持倉：忽略。",
            "reason_structure": "不在 Top20 池，且非持倉：忽略。",
            "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
            "reason_data_mode": f"data_mode={data_mode}",
        }

    # ---------- 法人可用時（保留原規則） ----------
    inst_ok = (
        inst.get("Inst_Status") == "READY"
        and int(inst.get("Inst_Streak3", 0) or 0) >= 3
        and inst.get("Inst_Dir3") == "POSITIVE"
    )

    decision = "WATCH"
    action_size = 0
    exit_code = "None"

    # ---------- Conservative ----------
    if account == "Conservative":
        if not inst_missing and inst_ok:
            if tier == "A" and score >= 50 and rev_growth >= 0:
                decision = "BUY"
                action_size = _cap(10)
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

    # ---------- Aggressive ----------
    elif account == "Aggressive":
        if not inst_missing and inst_ok:
            if tier == "A" and score >= 50 and rev_growth >= 0:
                decision = "BUY"
                action_size = _cap(10)
            elif score >= 45 and inst.get("Inst_Dir3") != "NEGATIVE":
                decision = "TRIAL"
                action_size = _cap(5)
            else:
                decision = "WATCH"
        else:
            if (
                trial_flag
                and score >= 45
                and tech_signals >= 1
                and not (technical_weaken or structure_weaken)
            ):
                decision = "TRIAL"
                action_size = _cap(10 if (tier == "A" and tech_signals >= 2 and score >= 55) else 5)
                exit_code = "INST_MISSING_MODE"
            else:
                decision = "WATCH"
                action_size = 0
                exit_code = "INST_MISSING_MODE"

    # ---------- ✅ data_mode = INTRADAY：禁止 BUY（最多 TRIAL） ----------
    if data_mode == "INTRADAY" and decision == "BUY":
        decision = "TRIAL" if trial_flag else "WATCH"
        action_size = _cap(5) if decision == "TRIAL" else 0
        exit_code = "INTRADAY_NO_BUY"

    # ---------- weaken 強制不買 ----------
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
        "reason_technical": f"tag_signals={tech_signals}, Score={score}, Tag='{tag}'",
        "reason_structure": f"Rev_Growth={rev_growth}%, OPM={opm}%",
        "reason_inst": f"inst_status={inst_status} / Inst_Status={inst.get('Inst_Status','PENDING')}（無法人時不作為進場依據）",
        "reason_data_mode": f"data_mode={data_mode}",
    }
