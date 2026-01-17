# arbiter.py
# -*- coding: utf-8 -*-
"""
Predator V15.6.4 (Hotfix in Arbiter)
- inst_net uses dual-layer: A=三大法人合計, B=外資
- A decides "can buy" (Hard Gate), B decides "position sizing" (Soft Governor)
- Keep V15.6.3 core principles: JSON only, deterministic, risk-first
"""

from __future__ import annotations
from typing import Any, Dict, Tuple


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v == v:  # not NaN
            return v
        return default
    except Exception:
        return default


def _extract_inst_net_ab(macro_overview: Dict[str, Any]) -> Tuple[float, float]:
    """
    解析 inst_net A/B（單位建議：元或新台幣）
    支援多種輸入型態（避免你上游還在調整 schema）：

    1) overview["inst_net_A"], overview["inst_net_B"]
    2) overview["inst_net"] = {"A":..., "B":...} 或 {"inst_net_A":..., "inst_net_B":...}
    3) overview["inst_net"] 是數字 → 視為 A，B=0（兼容舊版）
    4) overview["inst_net"] 是字串 "待更新" → 回傳 (0,0)
    """
    a = 0.0
    b = 0.0

    if not isinstance(macro_overview, dict):
        return a, b

    # Direct keys
    if "inst_net_A" in macro_overview or "inst_net_B" in macro_overview:
        a = _safe_float(macro_overview.get("inst_net_A", 0.0))
        b = _safe_float(macro_overview.get("inst_net_B", 0.0))
        return a, b

    inst_net = macro_overview.get("inst_net", 0.0)

    # Dict style
    if isinstance(inst_net, dict):
        # Common patterns
        a = _safe_float(inst_net.get("A", inst_net.get("inst_net_A", inst_net.get("total", 0.0))), 0.0)
        b = _safe_float(inst_net.get("B", inst_net.get("inst_net_B", inst_net.get("foreign", 0.0))), 0.0)
        return a, b

    # Numeric style (legacy)
    if isinstance(inst_net, (int, float)):
        a = float(inst_net)
        b = 0.0
        return a, b

    # String / others (e.g., "待更新")
    return 0.0, 0.0


def arbitrate(stock: Dict[str, Any], macro: Dict[str, Any], account: str = "Conservative") -> Dict[str, Any]:
    """
    回傳單一股票的最終裁決（可直接塞進 stocks[i]["FinalDecision"][account]）

    account:
      - "Conservative"
      - "Aggressive"
    """

    # ------------------------------------------------------------
    # Step 0: Read macro.overview (兼容你的 analyzer.py 結構)
    # ------------------------------------------------------------
    overview = {}
    if isinstance(macro, dict):
        # macro 可能直接傳 overview，或傳整包 macro
        if "overview" in macro and isinstance(macro["overview"], dict):
            overview = macro["overview"]
        else:
            overview = macro

    # ------------------------------------------------------------
    # Step 1: Data Health Gate (Hard)
    # ------------------------------------------------------------
    # 只要資料降級（法人未 READY / kill_switch / v14_watch / degraded_mode）→ 禁止 BUY/TRIAL
    inst_status = str(overview.get("inst_status", "PENDING")).upper()
    degraded_mode = bool(overview.get("degraded_mode", inst_status != "READY"))
    kill_switch = bool(overview.get("kill_switch", False))
    v14_watch = bool(overview.get("v14_watch", False))

    degraded = (inst_status != "READY") or degraded_mode or kill_switch or v14_watch

    inst = stock.get("Institutional", {}) or {}
    tech = stock.get("Technical", {}) or {}
    struct = stock.get("Structure", {}) or {}
    ranking = stock.get("ranking", {}) or {}
    risk = stock.get("risk", {}) or {}
    weaken_flags = stock.get("weaken_flags", {}) or {}
    orphan_holding = bool(stock.get("orphan_holding", False))

    # 風控上限
    position_pct_max = int(_safe_float(risk.get("position_pct_max", 12), 12))
    risk_per_trade_max = _safe_float(risk.get("risk_per_trade_max", 1.0), 1.0)
    trial_flag = bool(risk.get("trial_flag", True))

    # Default output
    out = {
        "Decision": "WATCH",
        "action_size_pct": 0,
        "exit_reason_code": "None",
        "degraded_note": "資料降級：否",
        "reason_technical": "",
        "reason_structure": "",
        "reason_inst": "",
    }

    if degraded:
        out.update(
            {
                "Decision": "WATCH",
                "action_size_pct": 0,
                "exit_reason_code": "DATA_DEGRADED",
                "degraded_note": "資料降級：是（禁止 BUY）",
                "reason_technical": "資料健康門觸發：禁止交易進場。",
                "reason_structure": "資料健康門觸發：禁止交易進場。",
                "reason_inst": f"inst_status={inst_status} / degraded_mode={degraded_mode} / kill_switch={kill_switch} / v14_watch={v14_watch}",
            }
        )
        return out

    # ------------------------------------------------------------
    # Step 2: 取得 inst_net A/B（A=三大法人合計, B=外資）
    # ------------------------------------------------------------
    inst_net_a, inst_net_b = _extract_inst_net_ab(overview)

    # Hard Gate (市場總開關)：A < -10億 → 禁止 BUY（TRIAL 也只能 <=5%）
    HARD_GATE_A_NTD = -1_000_000_000  # -10億
    SOFT_STRONG_A_NTD = 1_000_000_000  # +10億
    SOFT_STRONG_B_NTD = 500_000_000    # +5億

    hard_gate = inst_net_a < HARD_GATE_A_NTD

    # ------------------------------------------------------------
    # Step 3: 法人連續性（單股）— 只有 READY + streak>=3 + POSITIVE 才可作為進場理由
    # ------------------------------------------------------------
    inst_ok = (
        str(inst.get("Inst_Status", "PENDING")).upper() == "READY"
        and int(_safe_float(inst.get("Inst_Streak3", 0), 0)) >= 3
        and str(inst.get("Inst_Dir3", "PENDING")).upper() == "POSITIVE"
    )

    # ------------------------------------------------------------
    # Step 4: Top20 池化 + orphan 規則
    # ------------------------------------------------------------
    rank = int(_safe_float(ranking.get("rank", 999), 999))
    top20_flag = bool(ranking.get("top20_flag", rank <= 20))
    tier = str(ranking.get("tier", "A" if rank <= 10 else "B"))

    # 非 Top20 且非持倉 → 直接忽略/觀望（避免測試資料混入）
    if (not top20_flag) and (not orphan_holding):
        out.update(
            {
                "Decision": "WATCH",
                "action_size_pct": 0,
                "exit_reason_code": "IGNORE",
                "reason_technical": "不在 Top20 且非持倉：不納入操作池。",
                "reason_structure": "不在 Top20 且非持倉：不納入操作池。",
                "reason_inst": "不在操作池：忽略。",
            }
        )
        return out

    # 持倉跌出名單（orphan_holding=true）不可自動賣出：除非 weaken
    if orphan_holding:
        tech_weaken = bool(weaken_flags.get("technical_weaken", False))
        struct_weaken = bool(weaken_flags.get("structure_weaken", False))
        if tech_weaken or struct_weaken:
            out.update(
                {
                    "Decision": "REDUCE",
                    "action_size_pct": min(5, position_pct_max),
                    "exit_reason_code": "STRUCTURE_WEAK" if struct_weaken else "TECH_BREAK",
                    "reason_technical": "孤立持倉：技術轉弱，依規則減碼。",
                    "reason_structure": "孤立持倉：結構轉弱，依規則減碼。" if struct_weaken else "孤立持倉：結構未轉弱。",
                    "reason_inst": "孤立持倉：不因跌出名單自動清倉，僅在轉弱時減碼。",
                }
            )
        else:
            out.update(
                {
                    "Decision": "HOLD",
                    "action_size_pct": 0,
                    "exit_reason_code": "None",
                    "reason_technical": "孤立持倉：未見轉弱旗標，維持持有。",
                    "reason_structure": "孤立持倉：未見轉弱旗標，維持持有。",
                    "reason_inst": "孤立持倉：不自動賣出。",
                }
            )
        return out

    # ------------------------------------------------------------
    # Step 5: 基礎信號（以你現有 JSON 欄位為主，避免 AI 自行推估）
    # ------------------------------------------------------------
    score = _safe_float(tech.get("Score", 0), 0.0)
    tag = str(tech.get("Tag", ""))

    # 以 tag 文字作為「至少 1~2 個正向技術訊號」的代理（避免你還沒加 RSI/MA 進 JSON）
    # 你後續若補 ma10/20/60, rsi14 等，可把這段替換成更嚴謹的計數器。
    tech_pos_1 = ("起漲" in tag) or ("主力" in tag) or ("真突破" in tag)
    tech_pos_2 = sum([("起漲" in tag), ("主力" in tag), ("真突破" in tag)]) >= 2
    tech_break = ("破位" in tag)  # 若你未來有「技術破位」標記，可直接生效

    rev_growth = _safe_float(struct.get("Rev_Growth", -999), -999)
    opm = _safe_float(struct.get("OPM", -999), -999)
    # 產業水準若未提供，先用 0 作保守替代（避免誤判為達標）
    opm_industry = _safe_float(struct.get("opm_industry_level", 0), 0)

    # ------------------------------------------------------------
    # Step 6: 標準買入單位（你要求 action_size_pct 可落地）
    # ------------------------------------------------------------
    # 基準：5%
    # 高信心（滿足 Conservative 全部條件）→ 10%
    UNIT_LOW = 5
    UNIT_HIGH = 10

    # ------------------------------------------------------------
    # Step 7: 帳戶決策
    # ------------------------------------------------------------
    # A) Conservative：必須更嚴格（風控優先）
    if account == "Conservative":
        # Conservative BUY 必須是 Tier A (Top10) 且滿足多條件
        is_tier_a = (tier.upper() == "A") and (rank <= 10)

        can_buy = (
            is_tier_a
            and tech_pos_2
            and (not tech_break)
            and rev_growth >= 0
            and opm >= opm_industry
            and inst_ok
            and (not hard_gate)
        )

        if can_buy:
            out["Decision"] = "BUY"
            out["action_size_pct"] = min(UNIT_HIGH, position_pct_max)
            out["exit_reason_code"] = "None"
            out["reason_technical"] = f"Tier A，技術≥2正向訊號（Tag={tag}），無破位。"
            out["reason_structure"] = f"Rev_Growth={rev_growth:.2f}%，OPM={opm:.2f}% ≥ 產業水準={opm_industry:.2f}%。"
            out["reason_inst"] = "單股法人連續成立：Inst_Streak3≥3 且 Dir3=POSITIVE。"
        else:
            # 不買：判斷是否需要 REDUCE（此處僅對非 orphan 的操作池股票做保守處理）
            out["Decision"] = "WATCH"
            out["action_size_pct"] = 0
            out["exit_reason_code"] = "None"
            out["reason_technical"] = f"不滿足 Conservative 進場條件（Tag={tag} / Score={score:.1f}）。"
            out["reason_structure"] = f"Rev_Growth={rev_growth:.2f}%，OPM={opm:.2f}%（產業={opm_industry:.2f}%）。"
            out["reason_inst"] = f"inst_ok={inst_ok}（需 READY+3日同向才可進場）。"

    # B) Aggressive：允許 TRIAL，但仍受 A Hard Gate 控制
    elif account == "Aggressive":
        # Aggressive TRIAL 條件：Top20、至少 1 技術正向、無重大破位、外資不為 NEGATIVE、且 trial_flag=true
        inst_dir3 = str(inst.get("Inst_Dir3", "PENDING")).upper()

        can_trial = (
            top20_flag
            and tech_pos_1
            and (not tech_break)
            and inst_dir3 != "NEGATIVE"
            and trial_flag
            and (not hard_gate)
        )

        if can_trial:
            out["Decision"] = "TRIAL"
            out["action_size_pct"] = min(UNIT_LOW, position_pct_max)
            out["exit_reason_code"] = "None"
            out["reason_technical"] = f"Top20，具≥1正向技術訊號（Tag={tag}），無破位。"
            out["reason_structure"] = f"結構面不作為 TRIAL 必要門檻（Rev_Growth={rev_growth:.2f}%，OPM={opm:.2f}%）。"
            out["reason_inst"] = f"Inst_Dir3={inst_dir3}（不得為 NEGATIVE），trial_flag={trial_flag}。"
        else:
            out["Decision"] = "WATCH"
            out["action_size_pct"] = 0
            out["exit_reason_code"] = "None"
            out["reason_technical"] = f"不滿足 Aggressive TRIAL 條件（Tag={tag} / Score={score:.1f}）。"
            out["reason_structure"] = f"Rev_Growth={rev_growth:.2f}%，OPM={opm:.2f}%（僅供參考）。"
            out["reason_inst"] = f"Inst_Dir3={inst_dir3} / trial_flag={trial_flag} / hard_gate={hard_gate}。"

    else:
        out.update(
            {
                "Decision": "WATCH",
                "action_size_pct": 0,
                "exit_reason_code": "BAD_ACCOUNT",
                "reason_technical": f"未知帳戶類型：{account}",
                "reason_structure": "未知帳戶類型：不裁決。",
                "reason_inst": "未知帳戶類型：不裁決。",
            }
        )
        return out

    # ------------------------------------------------------------
    # Step 8: A/B 市場層倉位調節（Soft Governor / Accelerator）
    # ------------------------------------------------------------
    # Soft Governor：A>0 但 B<0 → 降倉（即使符合 BUY 也壓到 5%）
    if out["Decision"] in ("BUY", "TRIAL"):
        if (inst_net_a > 0) and (inst_net_b < 0):
            out["action_size_pct"] = min(UNIT_LOW, position_pct_max)
            out["exit_reason_code"] = "INST_REVERSAL"
            out["reason_inst"] += f"｜市場A>0但外資B<0（A={inst_net_a:.0f}, B={inst_net_b:.0f}）：依規則降倉至5%。"

        # Accelerator：A>+10億 且 B>+5億 → 允許放大（Aggressive 可到 position_pct_max）
        if (inst_net_a > SOFT_STRONG_A_NTD) and (inst_net_b > SOFT_STRONG_B_NTD):
            if account == "Aggressive" and out["Decision"] == "TRIAL":
                # 強順風盤：積極帳戶 TRIAL 可直接提升到 position_pct_max
                out["action_size_pct"] = max(min(position_pct_max, position_pct_max), UNIT_LOW)
                out["reason_inst"] += f"｜強順風加速器（A>{SOFT_STRONG_A_NTD:.0f}, B>{SOFT_STRONG_B_NTD:.0f}）：TRIAL 倉位放大至上限{position_pct_max}%。"
            elif account == "Conservative" and out["Decision"] == "BUY":
                # 保守帳戶仍維持 10%（不超上限）
                out["action_size_pct"] = min(UNIT_HIGH, position_pct_max)
                out["reason_inst"] += f"｜強順風加速器：維持保守標準倉位{out['action_size_pct']}%。"

    # Hard Gate：A < -10億 → 禁止 BUY；若已產生 BUY/TRIAL，強制降級處理
    if hard_gate and out["Decision"] in ("BUY", "TRIAL"):
        out["Decision"] = "WATCH"
        out["action_size_pct"] = 0
        out["exit_reason_code"] = "RISK_LIMIT"
        out["reason_inst"] += f"｜Hard Gate：A<{HARD_GATE_A_NTD:.0f}（A={inst_net_a:.0f}）：禁止進場。"

    # ------------------------------------------------------------
    # Step 9: 風控不可突破（最後一道）
    # ------------------------------------------------------------
    if out["action_size_pct"] > position_pct_max:
        out["action_size_pct"] = position_pct_max
        out["exit_reason_code"] = "RISK_LIMIT"
        out["reason_inst"] += f"｜倉位上限保護：position_pct_max={position_pct_max}%。"

    # 風險/單筆（目前你未給 stop/ATR 之類數據，先保留欄位以便日後擴展）
    _ = risk_per_trade_max  # placeholder (kept for forward compatibility)

    return out
