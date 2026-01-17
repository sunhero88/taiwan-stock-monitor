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
    規則版本：V15.6.3-NA (No-Inst Optimized)

    macro_overview：請傳 macro["overview"]（你 main.py / analyzer.py 的 macro_data["overview"]）
    """

    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", inst_status != "READY"))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    # ---------- 兩階降級 ----------
    # Level-2：系統級禁止買（最嚴格）
    degraded_level2 = (kill_switch or v14_watch)
    # Level-1：法人缺失降級（允許小倉位/試單）
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

    # ---------- 預設輸出 ----------
    decision = "WATCH"
    action_size = 0
    exit_code = "None"

    # ---------- Level-2：只賣不買 ----------
    if degraded_level2:
        # 持倉管理：弱化就減碼，否則持有/觀察
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
            "reason_inst": f"inst_status={inst_status} / degraded_mode={degraded_mode}",
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
            }
        return {
            "Decision": "HOLD",
            "action_size_pct": 0,
            "exit_reason_code": "None",
            "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
            "reason_technical": "跌出 Top20 但未見弱化訊號，維持持有。",
            "reason_structure": "未見結構弱化訊號，維持持有。",
            "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
        }

    # ---------- 非 Top20 且非持倉：直接忽略（避免干擾） ----------
    if not top20_flag:
        return {
            "Decision": "IGNORE",
            "action_size_pct": 0,
            "exit_reason_code": "OUT_OF_POOL",
            "degraded_note": "資料降級：否" if not inst_missing else "資料降級：是（法人缺失模式）",
            "reason_technical": "不在 Top20 池，且非持倉：忽略。",
            "reason_structure": "不在 Top20 池，且非持倉：忽略。",
            "reason_inst": f"inst_status={inst_status}（不作為判斷依據）",
        }

    # ---------- 法人可用時（保留你原本規則） ----------
    inst_ok = (
        inst.get("Inst_Status") == "READY"
        and int(inst.get("Inst_Streak3", 0) or 0) >= 3
        and inst.get("Inst_Dir3") == "POSITIVE"
    )

    # ---------- 無法人模式：用替代規則 ----------
    # 標準單位：5%；信心高：10%；不得超過 position_pct_max
    def _cap(sz: int) -> int:
        return min(int(sz), int(position_pct_max))

    if account == "Conservative":
        # 優先使用法人規則；若法人缺失則用替代 gating
        if not inst_missing and inst_ok:
            # 有法人且符合 → 允許較大倉位
            if tier == "A" and score >= 50 and rev_growth >= 0:
                decision = "BUY"
                action_size = _cap(10)
                exit_code = "None"
            else:
                decision = "WATCH"
        else:
            # 無法人（或法人未READY）→ 以技術+結構決定小倉位 BUY
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
        # 有法人且 inst_ok → 可 BUY/加碼；無法人 → TRIAL 為主
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
            # 無法人：TRIAL gating
            if (
                trial_flag
                and score >= 45
                and tech_signals >= 1
                and not (technical_weaken or structure_weaken)
            ):
                decision = "TRIAL"
                # Tier A 且信號強 → 10% 試單；否則 5%
                action_size = _cap(10 if (tier == "A" and tech_signals >= 2 and score >= 55) else 5)
                exit_code = "INST_MISSING_MODE"
            else:
                decision = "WATCH"
                action_size = 0
                exit_code = "INST_MISSING_MODE"

    # ---------- 補強：若出現 weaken，強制不買（避免逆勢加碼） ----------
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
    }
