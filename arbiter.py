# arbiter.py
# -*- coding: utf-8 -*-

def _has_positive_tech_signals(tech: dict) -> int:
    """
    以 Tag 判斷正向技術訊號數量：
    🟢起漲 / 🔥主力 / ⚡真突破
    """
    tag = str(tech.get("Tag", "") or "")
    cnt = 0
    for k in ["🟢起漲", "🔥主力", "⚡真突破"]:
        if k in tag:
            cnt += 1
    return cnt


def _has_tech_breakdown(tech: dict) -> bool:
    """
    預留：若你後端未來加入技術破位 alert，可在 Tag 放 '技術破位'
    """
    tag = str(tech.get("Tag", "") or "")
    return ("技術破位" in tag) or ("破位" in tag)


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def arbitrate(stock: dict, macro: dict, account: str = "Conservative"):
    """
    回傳單一股票的最終裁決（V15.6.3）
    僅使用輸入 JSON 欄位，不推測、不補資料。
    """

    # ==============================
    # Step 0｜讀取必要欄位
    # ==============================
    ranking = stock.get("ranking", {}) or stock.get("Ranking", {}) or {}
    inst = stock.get("Institutional", {}) or {}
    tech = stock.get("Technical", {}) or {}
    struct = stock.get("Structure", {}) or {}

    # ranking key 兼容
    tier = str(ranking.get("tier", stock.get("tier", "B")) or "B")
    top20_flag = bool(ranking.get("top20_flag", stock.get("top20_flag", False)))

    # orphan / weaken flags（若生成端尚未提供，視為 False）
    orphan_holding = bool(stock.get("orphan_holding", False))
    weaken_flags = stock.get("weaken_flags", {}) or {}
    technical_weaken = bool(weaken_flags.get("technical_weaken", False))
    structure_weaken = bool(weaken_flags.get("structure_weaken", False))

    # 風控參數（如 JSON 未提供，使用預設）
    risk = stock.get("risk", {}) or {}
    position_pct_max = int(risk.get("position_pct_max", 12))
    risk_per_trade_max = int(risk.get("risk_per_trade_max", 1))
    trial_flag = bool(risk.get("trial_flag", True))  # Aggressive 才用

    # ==============================
    # Step 1｜Data Health Gate
    # ==============================
    # 明確降級狀態來源（macro 端）
    inst_status_macro = str(macro.get("inst_status", "PENDING") or "PENDING")
    degraded_mode_macro = bool(macro.get("degraded_mode", False))
    kill_switch = bool(macro.get("kill_switch", False))
    v14_watch = bool(macro.get("v14_watch", False))

    # trade_date / inst_dates_3d 對齊檢核（避免你之前日期錯誤）
    trade_date = str(macro.get("trade_date", "") or "")
    inst_dates_3d_raw = macro.get("inst_dates_3d", "[]")
    inst_dates_3d = str(inst_dates_3d_raw)

    # 簡化：只要 inst_status != READY 或 declared degraded_mode 或 kill/v14，就視為降級
    degraded = (
        degraded_mode_macro
        or inst_status_macro != "READY"
        or kill_switch
        or v14_watch
    )

    # ==============================
    # Step 2｜法人連續性硬規則（V15.5.7）
    # ==============================
    inst_ok = (
        str(inst.get("Inst_Status", "PENDING")) == "READY"
        and int(inst.get("Inst_Streak3", 0) or 0) >= 3
        and str(inst.get("Inst_Dir3", "PENDING")) == "POSITIVE"
    )

    inst_dir3 = str(inst.get("Inst_Dir3", "PENDING") or "PENDING")

    # ==============================
    # Step 3｜Top20 池化 + Orphan 規則
    # ==============================
    # 不在 Top20 且非持倉：直接 IGNORE（避免測試資料干擾）
    if (not top20_flag) and (not orphan_holding):
        return {
            "symbol": stock.get("Symbol", "Unknown"),
            "tier": tier,
            "Decision": "IGNORE",
            "action_size_pct": 0,
            "reason_technical": "不在 Top20 且非持倉：不進入裁決池。",
            "reason_structure": "不在裁決池，結構理由不啟用。",
            "reason_inst": "不在裁決池，法人理由不啟用。",
            "degraded_note": "資料降級：是（禁止 BUY）" if degraded else "資料降級：否",
            "exit_reason_code": "OUT_OF_POOL",
        }

    # Orphan（持倉但跌出名單）：不自動賣出
    if orphan_holding:
        if technical_weaken or structure_weaken:
            return {
                "symbol": stock.get("Symbol", "Unknown"),
                "tier": tier,
                "Decision": "REDUCE",
                "action_size_pct": 5,  # 降碼以 5% 為單位（可在後端再依倉位換算）
                "reason_technical": "孤立持倉且出現技術/結構轉弱旗標：啟動降碼。",
                "reason_structure": "structure_weaken=true（或 technical_weaken=true）觸發。",
                "reason_inst": "法人不作為孤立持倉降碼的必要條件。",
                "degraded_note": "資料降級：是（禁止 BUY）" if degraded else "資料降級：否",
                "exit_reason_code": "ORPHAN_WEAKEN",
            }
        else:
            return {
                "symbol": stock.get("Symbol", "Unknown"),
                "tier": tier,
                "Decision": "HOLD",
                "action_size_pct": 0,
                "reason_technical": "孤立持倉但未出現轉弱旗標：不自動賣出。",
                "reason_structure": "未觸發 structure_weaken / technical_weaken。",
                "reason_inst": "法人資料不構成孤立持倉強制賣出理由。",
                "degraded_note": "資料降級：是（禁止 BUY）" if degraded else "資料降級：否",
                "exit_reason_code": "ORPHAN_HOLD",
            }

    # ==============================
    # Step 4｜降級模式（禁止 BUY/TRIAL）
    # ==============================
    if degraded:
        return {
            "symbol": stock.get("Symbol", "Unknown"),
            "tier": tier,
            "Decision": "WATCH",
            "action_size_pct": 0,
            "reason_technical": "資料降級：不允許進場動作（BUY/TRIAL）。",
            "reason_structure": "資料降級：結構資料僅供參考，不形成進場。",
            "reason_inst": f"inst_status={inst_status_macro} / inst_dates_3d={inst_dates_3d}",
            "degraded_note": "資料降級：是（禁止 BUY）",
            "exit_reason_code": "DATA_DEGRADED",
        }

    # ==============================
    # Step 5｜雙帳戶決策引擎（V15.6）
    # ==============================
    pos_signals = _has_positive_tech_signals(tech)
    tech_break = _has_tech_breakdown(tech)

    rev_growth = _safe_float(struct.get("Rev_Growth", None), default=-999.0)
    opm = _safe_float(struct.get("OPM", None), default=-999.0)

    # 你目前 JSON 尚未提供 opm_industry_level，先採「OPM >= 0」作最小可行門檻
    # 後續你若補上 struct["OPM_Industry_Level"]，即可在此替換成 opm >= industry_level
    opm_industry_level = _safe_float(struct.get("OPM_Industry_Level", 0.0), default=0.0)

    score = _safe_float(tech.get("Score", 0), default=0.0)

    # 標準單位：5%，高信心 10%，不得超過 position_pct_max
    def _unit_size(high_conf: bool) -> int:
        base = 10 if high_conf else 5
        return min(base, position_pct_max)

    # ---- Conservative ----
    if account == "Conservative":
        # BUY 條件（全部滿足）
        # - Tier A（Top10）
        # - >=2 正向技術訊號
        # - 無破位
        # - Rev_Growth >= 0
        # - OPM >= 產業水準（暫用 opm >= max(0, opm_industry_level)）
        # - 法人三日同向成立
        if (
            tier == "A"
            and pos_signals >= 2
            and (not tech_break)
            and rev_growth >= 0
            and opm >= max(0.0, opm_industry_level)
            and inst_ok
        ):
            return {
                "symbol": stock.get("Symbol", "Unknown"),
                "tier": tier,
                "Decision": "BUY",
                "action_size_pct": _unit_size(high_conf=True),
                "reason_technical": f"正向訊號={pos_signals}（Tag={tech.get('Tag','')}），無破位。",
                "reason_structure": f"Rev_Growth={rev_growth:.1f}%，OPM={opm:.2f}%（門檻≥{max(0.0, opm_industry_level):.2f}%）。",
                "reason_inst": f"READY 且 Inst_Streak3>=3 且 POSITIVE（Streak3={inst.get('Inst_Streak3',0)}）。",
                "degraded_note": "資料降級：否",
                "exit_reason_code": "None",
            }

        # 若技術破位：保守帳戶偏向 REDUCE（但你目前尚無持倉欄位，先用 WATCH）
        if tech_break:
            return {
                "symbol": stock.get("Symbol", "Unknown"),
                "tier": tier,
                "Decision": "WATCH",
                "action_size_pct": 0,
                "reason_technical": "技術破位標記存在：保守帳戶不進場。",
                "reason_structure": "結構條件不覆蓋技術破位。",
                "reason_inst": "法人不作為破位下的進場理由。",
                "degraded_note": "資料降級：否",
                "exit_reason_code": "TECH_BREAK",
            }

        # 其餘情境：WATCH
        miss = []
        if tier != "A":
            miss.append("非 Tier A")
        if pos_signals < 2:
            miss.append(f"正向訊號不足({pos_signals}/2)")
        if rev_growth < 0:
            miss.append("Rev_Growth<0")
        if opm < max(0.0, opm_industry_level):
            miss.append("OPM<門檻")
        if not inst_ok:
            miss.append("法人未達三日同向")

        return {
            "symbol": stock.get("Symbol", "Unknown"),
            "tier": tier,
            "Decision": "WATCH",
            "action_size_pct": 0,
            "reason_technical": f"Tag={tech.get('Tag','')} / Score={score:.1f}",
            "reason_structure": f"Rev_Growth={rev_growth:.1f}% / OPM={opm:.2f}%",
            "reason_inst": f"Inst_Status={inst.get('Inst_Status')} / Dir3={inst_dir3} / Streak3={inst.get('Inst_Streak3',0)}；未滿足：{', '.join(miss)}",
            "degraded_note": "資料降級：否",
            "exit_reason_code": "RULE_BLOCK",
        }

    # ---- Aggressive ----
    if account == "Aggressive":
        # TRIAL 條件：
        # - Top20
        # - >=1 正向技術訊號
        # - 無重大破位
        # - inst_dir3 != NEGATIVE（NEUTRAL/POSITIVE 可）
        # - trial_flag = true
        if (
            top20_flag
            and pos_signals >= 1
            and (not tech_break)
            and inst_dir3 != "NEGATIVE"
            and trial_flag
        ):
            return {
                "symbol": stock.get("Symbol", "Unknown"),
                "tier": tier,
                "Decision": "TRIAL",
                "action_size_pct": _unit_size(high_conf=False),
                "reason_technical": f"正向訊號={pos_signals}（Tag={tech.get('Tag','')}），無破位。",
                "reason_structure": f"Rev_Growth={rev_growth:.1f}% / OPM={opm:.2f}%（積極帳戶不做硬性門檻）。",
                "reason_inst": f"Dir3={inst_dir3}（不得為 NEGATIVE）。",
                "degraded_note": "資料降級：否",
                "exit_reason_code": "None",
            }

        miss = []
        if pos_signals < 1:
            miss.append("無正向訊號")
        if tech_break:
            miss.append("技術破位")
        if inst_dir3 == "NEGATIVE":
            miss.append("法人為負向")
        if not trial_flag:
            miss.append("trial_flag=false")

        return {
            "symbol": stock.get("Symbol", "Unknown"),
            "tier": tier,
            "Decision": "WATCH",
            "action_size_pct": 0,
            "reason_technical": f"Tag={tech.get('Tag','')} / Score={score:.1f}",
            "reason_structure": f"Rev_Growth={rev_growth:.1f}% / OPM={opm:.2f}%",
            "reason_inst": f"Inst_Status={inst.get('Inst_Status')} / Dir3={inst_dir3} / Streak3={inst.get('Inst_Streak3',0)}；未滿足：{', '.join(miss)}",
            "degraded_note": "資料降級：否",
            "exit_reason_code": "RULE_BLOCK",
        }

    # 未知帳戶：保守處理
    return {
        "symbol": stock.get("Symbol", "Unknown"),
        "tier": tier,
        "Decision": "WATCH",
        "action_size_pct": 0,
        "reason_technical": "未知帳戶類型：保守處理。",
        "reason_structure": "未知帳戶類型：保守處理。",
        "reason_inst": "未知帳戶類型：保守處理。",
        "degraded_note": "資料降級：否",
        "exit_reason_code": "UNKNOWN_ACCOUNT",
    }
