# arbiter.py
# -*- coding: utf-8 -*-

def arbitrate(stock: dict, macro_overview: dict, account: str = "Conservative") -> dict:
    """
    Predator V15.6.3 Frozen - Arbiter
    Input:
      - stock: single stock dict from JSON
      - macro_overview: macro.overview dict
      - account: Conservative / Aggressive
    Output (per-account):
      - Decision, action_size_pct, exit_reason_code, degraded_note
      - plus 3 reasons for audit
    """

    # -------------------------------
    # Step 1: Data Health Gate
    # -------------------------------
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", inst_status != "READY"))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    if degraded_mode or inst_status != "READY" or kill_switch or v14_watch:
        return {
            "Decision": "WATCH",
            "action_size_pct": 0,
            "exit_reason_code": "DATA_DEGRADED",
            "degraded_note": "資料降級：是（禁止 BUY）",
            "reason_technical": "資料健康門觸發：禁止交易進場。",
            "reason_structure": "資料健康門觸發：禁止交易進場。",
            "reason_inst": f"inst_status={inst_status} / degraded_mode={degraded_mode}",
        }

    # -------------------------------
    # Read blocks
    # -------------------------------
    ranking = stock.get("ranking", {}) or {}
    tech = stock.get("Technical", {}) or {}
    struct = stock.get("Structure", {}) or {}
    inst = stock.get("Institutional", {}) or {}
    risk = stock.get("risk", {}) or {}

    rank = int(ranking.get("rank", 999))
    tier = ranking.get("tier", "B")
    top20_flag = bool(ranking.get("top20_flag", rank <= 20))

    orphan = bool(stock.get("orphan_holding", False))
    weaken = stock.get("weaken_flags", {}) or {}
    tech_weaken = bool(weaken.get("technical_weaken", False))
    struct_weaken = bool(weaken.get("structure_weaken", False))

    # Risk caps
    position_pct_max = int(risk.get("position_pct_max", 12))
    risk_per_trade_max = float(risk.get("risk_per_trade_max", 1))
    trial_flag = bool(risk.get("trial_flag", True))

    # -------------------------------
    # Step 2: Institutional 3-day streak hard rule
    # -------------------------------
    inst_ok = (
        inst.get("Inst_Status") == "READY"
        and int(inst.get("Inst_Streak3", 0)) >= 3
        and inst.get("Inst_Dir3") == "POSITIVE"
    )

    # -------------------------------
    # Step 3: Universe handling
    # -------------------------------
    # Non-Top20 and non-holding => IGNORE/WATCH
    if (not top20_flag) and (not orphan):
        return {
            "Decision": "WATCH",
            "action_size_pct": 0,
            "exit_reason_code": "OUT_OF_UNIVERSE",
            "degraded_note": "資料降級：否",
            "reason_technical": "不在 Top20 且非持倉：不進行交易裁決。",
            "reason_structure": "不在 Top20 且非持倉：不進行交易裁決。",
            "reason_inst": "不在 Top20 且非持倉：不進行交易裁決。",
        }

    # Orphan holding handling (跌出名單持倉)
    if orphan:
        if tech_weaken or struct_weaken:
            return {
                "Decision": "REDUCE",
                "action_size_pct": min(5, position_pct_max),
                "exit_reason_code": "ORPHAN_WEAKEN",
                "degraded_note": "資料降級：否",
                "reason_technical": f"孤立持倉且弱化：technical_weaken={tech_weaken}",
                "reason_structure": f"孤立持倉且弱化：structure_weaken={struct_weaken}",
                "reason_inst": "孤立持倉：不新增部位，優先控風險。",
            }
        return {
            "Decision": "HOLD",
            "action_size_pct": 0,
            "exit_reason_code": "ORPHAN_HOLD",
            "degraded_note": "資料降級：否",
            "reason_technical": "孤立持倉：未觸發弱化訊號，維持持有。",
            "reason_structure": "孤立持倉：未觸發弱化訊號，維持持有。",
            "reason_inst": "孤立持倉：未觸發弱化訊號，維持持有。",
        }

    # -------------------------------
    # Step 4: Account engines
    # -------------------------------
    score = float(tech.get("Score", 0))
    tag = str(tech.get("Tag", ""))

    rev_g = float(struct.get("Rev_Growth", -999))
    opm = float(struct.get("OPM", -999))

    # Minimal "technical positive signals"
    tech_positive = 0
    if "🟢" in tag or "起漲" in tag:
        tech_positive += 1
    if "🔥" in tag or "主力" in tag:
        tech_positive += 1
    if "⚡" in tag or "真突破" in tag:
        tech_positive += 1

    # Standard unit sizing rule (V15.6.2 suggestion integrated)
    # - default unit = 5%
    # - high confidence (all conservative conditions) = 10%
    # - cannot exceed position_pct_max
    def size_unit(high_conf: bool) -> int:
        base = 10 if high_conf else 5
        return min(base, position_pct_max)

    # Conservative rules
    if account == "Conservative":
        # Buy only from Tier A (Top10)
        if tier != "A":
            return {
                "Decision": "WATCH",
                "action_size_pct": 0,
                "exit_reason_code": "TIER_B_BLOCK",
                "degraded_note": "資料降級：否",
                "reason_technical": "保守帳戶：僅允許 Tier A（Top10）進場。",
                "reason_structure": f"Rev_Growth={rev_g}%, OPM={opm}%",
                "reason_inst": f"inst_ok={inst_ok}",
            }

        # Require >=2 positive technical signals, Rev_Growth>=0, and Inst streak ok
        if (tech_positive >= 2) and (rev_g >= 0) and inst_ok:
            high_conf = True  # because all strict conditions satisfied
            return {
                "Decision": "BUY",
                "action_size_pct": size_unit(high_conf),
                "exit_reason_code": "None",
                "degraded_note": "資料降級：否",
                "reason_technical": f"技術滿足：正向訊號={tech_positive}（Tag={tag}）",
                "reason_structure": f"結構滿足：Rev_Growth={rev_g}%, OPM={opm}%",
                "reason_inst": f"法人滿足：Streak3>=3 且 POSITIVE（Inst_Streak3={inst.get('Inst_Streak3')})",
            }

        # Reduce rule example (optional): if Tier A but clear weaken flags
        if tech_weaken or struct_weaken:
            return {
                "Decision": "REDUCE",
                "action_size_pct": min(5, position_pct_max),
                "exit_reason_code": "WEAKEN_FLAGS",
                "degraded_note": "資料降級：否",
                "reason_technical": f"技術弱化：{tech_weaken}",
                "reason_structure": f"結構弱化：{struct_weaken}",
                "reason_inst": f"inst_ok={inst_ok}",
            }

        return {
            "Decision": "WATCH",
            "action_size_pct": 0,
            "exit_reason_code": "CONSERVATIVE_BLOCK",
            "degraded_note": "資料降級：否",
            "reason_technical": f"未滿足保守進場：tech_positive={tech_positive}, Tag={tag}",
            "reason_structure": f"Rev_Growth={rev_g}%, OPM={opm}%",
            "reason_inst": f"inst_ok={inst_ok}（需 3 日同向）",
        }

    # Aggressive rules
    if account == "Aggressive":
        # TRIAL only in Top20 and trial_flag
        if (not top20_flag) or (not trial_flag):
            return {
                "Decision": "WATCH",
                "action_size_pct": 0,
                "exit_reason_code": "TRIAL_BLOCK",
                "degraded_note": "資料降級：否",
                "reason_technical": "積極帳戶：TRIAL 需 Top20 且 trial_flag=true。",
                "reason_structure": f"Rev_Growth={rev_g}%, OPM={opm}%",
                "reason_inst": f"Inst_Dir3={inst.get('Inst_Dir3')}",
            }

        # TRIAL condition: >=1 positive technical signal, and Inst not NEGATIVE
        if (tech_positive >= 1) and (inst.get("Inst_Dir3") != "NEGATIVE"):
            high_conf = False
            return {
                "Decision": "TRIAL",
                "action_size_pct": size_unit(high_conf),
                "exit_reason_code": "None",
                "degraded_note": "資料降級：否",
                "reason_technical": f"TRIAL：tech_positive={tech_positive}（Tag={tag}）",
                "reason_structure": f"Rev_Growth={rev_g}%, OPM={opm}%",
                "reason_inst": f"Inst_Dir3={inst.get('Inst_Dir3')}（不得 NEGATIVE）",
            }

        return {
            "Decision": "WATCH",
            "action_size_pct": 0,
            "exit_reason_code": "AGGRESSIVE_BLOCK",
            "degraded_note": "資料降級：否",
            "reason_technical": f"未滿足 TRIAL：tech_positive={tech_positive}",
            "reason_structure": f"Rev_Growth={rev_g}%, OPM={opm}%",
            "reason_inst": f"Inst_Dir3={inst.get('Inst_Dir3')}",
        }

    # Fallback
    return {
        "Decision": "WATCH",
        "action_size_pct": 0,
        "exit_reason_code": "UNKNOWN_ACCOUNT",
        "degraded_note": "資料降級：否",
        "reason_technical": "未知帳戶類型",
        "reason_structure": "未知帳戶類型",
        "reason_inst": "未知帳戶類型",
    }
