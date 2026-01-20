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
    回傳單一股票的最終裁決（支援無法人 / 盤中 / 盤後 / 舊資料 STALE）
    規則版本：V15.6.3 + data_mode gate

    macro_overview：請傳 macro["overview"]
      - data_mode: "INTRADAY" | "EOD" | "STALE"
      - lag_days: int (可選；STALE 時用於說明)
      - inst_status: "READY" | "PENDING" | "UNAVAILABLE" | ...
      - degraded_mode / kill_switch / v14_watch: bool
    """

    # ---------- Macro 狀態 ----------
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", inst_status != "READY"))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    data_mode = str(macro_overview.get("data_mode", "EOD") or "EOD").upper()
    lag_days = int(macro_overview.get("lag_days", 0) or 0)

    # ---------- 兩階降級（原本精神保留） ----------
    # Level-2：系統級禁止買（最嚴格）
    degraded_level2 = (kill_switch or v14_watch)

    # Level-1：法人缺失降級（允許小倉位/試單；但仍受 data_mode 約束）
    inst_missing = (inst_status != "READY") or degraded_mode

    # ---------- Stock blocks ----------
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

    # ---------- 小工具 ----------
    def _cap(sz: int) -> int:
        return min(int(sz), int(position_pct_max))

    # ================
    # 0) data_mode Gate（新增，最高優先於「是否有法人」）
    # ================
    # 原則：
    # - STALE：資料落後 >= 1 天 → 禁止新倉（BUY/TRIAL），只做持倉風控（HOLD/REDUCE）
    # - INTRADAY：盤中噪音較大 → Conservative 禁止 BUY；Aggressive 只能 TRIAL（≤5%或≤10%）
    # - EOD：盤後資料 → 允許完整策略（含 BUY/TRIAL），但仍受法人/弱化/池化等規則約束

    if data_mode == "STALE":
        # STALE 一律不開新倉，避免用舊資料做進場判斷
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

        # 非持倉：不允許進場
        return {
            "Decision": "WATCH" if top20_flag else "IGNORE",
            "action_size_pct": 0,
            "exit_reason_code": "STALE_DATA_NO_ENTRY",
            "degraded_note": "資料降級：是（STALE：禁止新倉）",
            "reason_technical": "資料落後（非今日交易日）→ 禁止用舊資料進場。",
            "reason_structure": "資料落後 → 結構條件暫不作為進場依據。",
            "reason_inst": f"data_mode=STALE, lag_days={lag_days}, inst_status={inst_status}",
        }

    # ================
    # 1) Level-2：系統級禁止買（你原本規則）
    # ================
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
            "reason_inst": f"data_mode={data_mode} / inst_status={inst_status} / degraded_mode={degraded_mode}",
        }

    # ================
    # 2) 持倉管理優先（原本規則）
    # ================
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

    # ================
    # 3) 非 Top20 且非持倉：忽略（原本規則）
    # ================
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

    # ================
    # 4) 盤中模式 Gate（新增）
    # ================
    # 盤中不把「最大化」用在「加槓桿/重倉」，而是用在「降低誤判率、保留彈性」
    # 因此：
    # - Conservative：INTRADAY 不 BUY，只 WATCH/TRIAL（≤5%）
    # - Aggressive：INTRADAY 允許 TRIAL（≤5%，條件強可到 10%），EOD 才允許 BUY 擴大
    intraday_no_buy_conservative = (data_mode == "INTRADAY" and account == "Conservative")
    intraday_only_trial_aggressive = (data_mode == "INTRADAY" and account == "Aggressive")

    # ================
    # 5) 法人可用判斷（保留你原本規則）
    # ================
    inst_ok = (
        inst.get("Inst_Status") == "READY"
        and int(inst.get("Inst_Streak3", 0) or 0) >= 3
        and inst.get("Inst_Dir3") == "POSITIVE"
    )

    # ================
    # 6) 進場裁決
    # ================
    decision = "WATCH"
    action_size = 0
    exit_code = "None"

    # --------- Conservative ----------
    if account == "Conservative":
        # INTRADAY：禁止 BUY（只 TRIAL 或 WATCH）
        if intraday_no_buy_conservative:
            # 盤中僅允許「明確信號」的 5% 試單
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
            # EOD：優先法人；法人缺失就用替代規則（你原本 NA 精神）
            if not inst_missing and inst_ok:
                if tier == "A" and score >= 50 and rev_growth >= 0:
                    decision = "BUY"
                    action_size = _cap(10)
                    exit_code = "None"
                else:
                    decision = "WATCH"
            else:
                # 無法人 / 法人未READY：只允許小倉位 BUY（5%）且條件必須更強
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

    # --------- Aggressive ----------
    elif account == "Aggressive":
        # INTRADAY：只允許 TRIAL（不 BUY）
        if intraday_only_trial_aggressive:
            if (
                trial_flag
                and score >= 45
                and tech_signals >= 1
                and rev_growth >= 0
                and not (technical_weaken or structure_weaken)
            ):
                # Tier A + 信號強 + 分數高 → 10% 試單；否則 5%
                action_size = _cap(10 if (tier == "A" and tech_signals >= 2 and score >= 55) else 5)
                decision = "TRIAL"
                exit_code = "INTRADAY_TRIAL_ONLY"
            else:
                decision = "WATCH"
                action_size = 0
                exit_code = "INTRADAY_TRIAL_ONLY"

        else:
            # EOD：有法人且 inst_ok → 允許 BUY/加碼；無法人 → TRIAL 為主或小 BUY
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

    # ================
    # 7) 最終硬保護：出現 weaken → 強制不買/不試（你原本規則）
    # ================
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
