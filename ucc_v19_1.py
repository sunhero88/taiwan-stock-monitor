# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator + UCC V19.1）
# SINGLE-FILE FINAL HARDENED BUILD (避免循環 import / 反覆改)
# Date: 2026-02-24
#
# ✅ 功能
# - Streamlit UI：RUN L1/L2/L3 切換
# - UI 開關：「盤中是否允許當日法人資料」
# - Payload 補齊：max_equity_allowed_pct、market_status_reason、confidence 多維欄位、policy 欄位
# - UCC 引擎：L1 / L2 / L3 單次只輸出一個模式結果
#
# ✅ 設計原則（對齊你的鐵律）
# - JSON 是唯一資料來源（UCC 只讀 payload，不猜、不補）
# - path=value 證據輸出
# - 不確定就降級（NO TRADE / 高風險）
# =========================================================

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

Json = Dict[str, Any]


# -----------------------------
# 小工具：安全取值 + path=value
# -----------------------------
def jget(d: Any, path: str, default=None):
    """
    支援 dot path：a.b.c
    不支援 [*] 語法（迭代請在外層做）
    """
    cur = d
    for k in path.split("."):
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def jhas(d: Any, path: str) -> bool:
    sentinel = object()
    return jget(d, path, sentinel) is not sentinel


def pv(d: Any, path: str) -> str:
    """path=value 字串（value 以 json 風格顯示）"""
    val = jget(d, path, None)
    return f"{path}={json.dumps(val, ensure_ascii=False)}"


def safe_parse_date(s: Any) -> Optional[datetime]:
    if not isinstance(s, str) or not s.strip():
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


# -----------------------------
# Payload Patch（UI用：明示補齊）
# -----------------------------
def ensure_list(x) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def patch_payload_for_ui(payload: Json, allow_same_day_inst: bool, enforce_token_when_same_day: bool) -> Tuple[Json, List[str]]:
    """
    這個 patch 是「UI 幫你補齊欄位/治理欄位」用。
    注意：這不是 UCC 引擎補資料；是你在產生/整理 payload 的前處理。
    """
    p = json.loads(json.dumps(payload, ensure_ascii=False))  # deep copy
    notes: List[str] = []

    # meta 基礎欄位
    p.setdefault("meta", {})
    p["meta"].setdefault("market_status_reason", [])
    p["meta"]["market_status_reason"] = ensure_list(p["meta"].get("market_status_reason"))

    p["meta"].setdefault("confidence", {"price": "LOW", "volume": "LOW", "institutional": "LOW"})
    if not isinstance(p["meta"]["confidence"], dict):
        p["meta"]["confidence"] = {"price": "LOW", "volume": "LOW", "institutional": "LOW"}
        notes.append("meta.confidence 非 dict → 已重置為預設 dict")

    p["meta"].setdefault("max_equity_lock_reason", [])
    p["meta"]["max_equity_lock_reason"] = ensure_list(p["meta"].get("max_equity_lock_reason"))

    # macro.overview.max_equity_allowed_pct
    p.setdefault("macro", {})
    p["macro"].setdefault("overview", {})
    if not jhas(p, "macro.overview.max_equity_allowed_pct"):
        # 保守：缺就補 0.0（等同 NO TRADE）
        p["macro"]["overview"]["max_equity_allowed_pct"] = 0.0
        p["meta"]["max_equity_lock_reason"].append("MAX_EQUITY_MISSING_DEFAULT_0")
        notes.append("缺少 macro.overview.max_equity_allowed_pct → UI 保守補 0.0 並鎖倉")

    # policy（盤中法人資料）
    p["meta"].setdefault("intraday_institutional_policy", {})
    pol = p["meta"]["intraday_institutional_policy"]
    if not isinstance(pol, dict):
        pol = {}
        p["meta"]["intraday_institutional_policy"] = pol

    # 以 UI 開關覆寫治理欄位（明確可稽核）
    pol["allow_same_day"] = bool(allow_same_day_inst)
    pol["enforce_token_when_same_day"] = bool(enforce_token_when_same_day)

    eff_trade = p["meta"].get("effective_trade_date")
    eff_dt = safe_parse_date(eff_trade)

    # resolved_use_same_day：只有當 session=INTRADAY 且 allow_same_day 才可能 true
    session = p["meta"].get("session", "")
    if session == "INTRADAY" and allow_same_day_inst:
        # 若要求 token 且偵測到「空 token」來源標記，則強制 resolved false
        token_problem = False
        if enforce_token_when_same_day:
            for i, s in enumerate(p.get("stocks", []) or []):
                src = jget(s, "Institutional.inst_source", "")
                if isinstance(src, str) and "EMPTY_TOKEN" in src:
                    token_problem = True
                    notes.append(f"stocks[{i}].Institutional.inst_source={src} → SAME_DAY 需要 token，resolved_use_same_day 將被關閉")
                    break
        pol["resolved_use_same_day"] = False if token_problem else True
    else:
        pol["resolved_use_same_day"] = False

    # inst_effective_date：僅做治理欄位補齊（不改 stocks 內容）
    # 若 is_using_previous_day=true → 預設 inst_effective_date = effective_trade_date（你本來就設為前一日）
    if isinstance(eff_dt, datetime):
        pol.setdefault("inst_effective_date", eff_dt.strftime("%Y-%m-%d"))
    else:
        pol.setdefault("inst_effective_date", None)

    return p, notes


# -----------------------------
# UCC V19.1 引擎（單檔內嵌）
# -----------------------------
@dataclass
class L1Result:
    mode: str
    verdict: str
    risk_level: str
    fatal_issues: List[str]
    structural_warnings: List[str]
    audit_confidence: str


class UCCv19_1:
    """
    Predator UCC V19.1 Hardened Final Lockdown
    - JSON-only
    - path=value evidence
    - single-mode output
    """

    def __init__(self) -> None:
        pass

    # --------------- V17 / V18 / V19 ---------------
    def _v17_wartime_triggered(self, payload: Json) -> Tuple[bool, List[str]]:
        reasons = []
        if jget(payload, "meta.war_time_override", False) is True:
            reasons.append(pv(payload, "meta.war_time_override"))
        regime = str(jget(payload, "meta.current_regime", "") or "")
        if re.search(r"\b(WAR|CRISIS)\b", regime, re.IGNORECASE):
            reasons.append(pv(payload, "meta.current_regime"))
        return (len(reasons) > 0), reasons

    def _v19_constitution_guard(self, requested_mode: str, l1_verdict: Optional[str]) -> Tuple[bool, List[str]]:
        """
        只做「流程違憲」檢查（不能跳過 L1）
        """
        issues = []
        if requested_mode in ("L2", "L3") and l1_verdict != "PASS":
            issues.append(f"流程違憲：requested={requested_mode} 但 L1.verdict={l1_verdict}（不得跳過 L1）")
        return (len(issues) > 0), issues

    # --------------- L1：資料審計 ---------------
    def run_l1(self, payload: Json) -> L1Result:
        fatal: List[str] = []
        warn: List[str] = []

        # 3.1 致命條件
        if not jhas(payload, "macro.overview.twii_close") or jget(payload, "macro.overview.twii_close") is None:
            fatal.append(f"致命：大盤收盤價缺失/為 null → {pv(payload, 'macro.overview.twii_close')}")
        if jget(payload, "macro.integrity.kill", False) is True:
            fatal.append(f"致命：kill=true → {pv(payload, 'macro.integrity.kill')}")

        # NO_UPDATE_TODAY 但 Inst_Net_3d 仍有數值（0.0 也算）
        stocks = payload.get("stocks") or []
        for i, s in enumerate(stocks):
            inst_status = jget(s, "Institutional.Inst_Status", None)
            inst_net_3d = jget(s, "Institutional.Inst_Net_3d", None)
            if inst_status == "NO_UPDATE_TODAY" and inst_net_3d is not None:
                fatal.append(
                    f"致命：法人狀態 NO_UPDATE_TODAY 但仍有 Inst_Net_3d → "
                    f"stocks[{i}].Institutional.Inst_Status={json.dumps(inst_status, ensure_ascii=False)}; "
                    f"stocks[{i}].Institutional.Inst_Net_3d={json.dumps(inst_net_3d, ensure_ascii=False)}"
                )

        # meta.confidence_level=LOW 但 meta.market_status=NORMAL
        if jget(payload, "meta.confidence_level") == "LOW" and jget(payload, "meta.market_status") == "NORMAL":
            fatal.append(
                "致命：confidence_level=LOW 但 market_status=NORMAL（矛盾） → "
                f"{pv(payload, 'meta.confidence_level')}, {pv(payload, 'meta.market_status')}"
            )

        # amount_total_blended < amount_total_raw
        raw_amt = jget(payload, "macro.market_amount.amount_total_raw", None)
        blend_amt = jget(payload, "macro.market_amount.amount_total_blended", None)
        if isinstance(raw_amt, (int, float)) and isinstance(blend_amt, (int, float)) and blend_amt < raw_amt:
            fatal.append(
                f"致命：blended < raw（不可能） → {pv(payload, 'macro.market_amount.amount_total_blended')}, {pv(payload, 'macro.market_amount.amount_total_raw')}"
            )

        # is_using_previous_day=true 但缺 effective_trade_date
        if jget(payload, "meta.is_using_previous_day", False) is True and not jhas(payload, "meta.effective_trade_date"):
            fatal.append(f"致命：is_using_previous_day=true 但缺 effective_trade_date → {pv(payload, 'meta.is_using_previous_day')}")

        # Symbol–Price 跨量級錯位（簡化版：2330 <200 視為錯）
        for i, s in enumerate(stocks):
            sym = str(s.get("Symbol", "") or "")
            price = s.get("Price", None)
            if sym == "2330.TW" and isinstance(price, (int, float)) and price < 200:
                fatal.append(f"致命：2330.TW 價格 <200（跨量級） → stocks[{i}].Symbol={json.dumps(sym)}, stocks[{i}].Price={json.dumps(price)}")

        # --------------- 結構性警告（不直接 FAIL，但會拉高風險/降低信心） ---------------

        # 核心指標缺失（會導致 L2 不可判定）
        for k in ["SMR", "Slope5", "Acceleration"]:
            if not jhas(payload, f"macro.overview.{k}") or jget(payload, f"macro.overview.{k}") is None:
                warn.append(f"警告：核心指標缺失 → {pv(payload, f'macro.overview.{k}')}（L2 可能 NO TRADE）")

        # 股票價格缺失比例
        missing_price = 0
        for s in stocks:
            if s.get("Price", None) is None:
                missing_price += 1
        if len(stocks) > 0 and missing_price > 0:
            ratio = missing_price / max(1, len(stocks))
            warn.append(f"警告：stocks Price 缺失 {missing_price}/{len(stocks)} = {ratio:.1%}")

        # 盤中法人政策一致性警告（不做外推，只做一致性）
        session = jget(payload, "meta.session", "")
        pol = jget(payload, "meta.intraday_institutional_policy", {}) if isinstance(jget(payload, "meta.intraday_institutional_policy", {}), dict) else {}
        allow_same_day = pol.get("allow_same_day", None)
        resolved_use_same_day = pol.get("resolved_use_same_day", None)
        if session == "INTRADAY" and allow_same_day is False and resolved_use_same_day is True:
            warn.append(
                "警告：INTRADAY 且 allow_same_day=false 但 resolved_use_same_day=true（治理矛盾） → "
                f"{pv(payload, 'meta.intraday_institutional_policy.allow_same_day')}, {pv(payload, 'meta.intraday_institutional_policy.resolved_use_same_day')}"
            )

        # L1 verdict / risk / confidence
        if fatal:
            verdict = "FAIL"
            risk = "CRITICAL"
            conf = "HIGH" if len(fatal) >= 2 else "MEDIUM"
        else:
            # 有警告 → PARTIAL_PASS
            verdict = "PASS" if not warn else "PARTIAL_PASS"

            # 風險以市場狀態 & 缺失程度保守估
            market_status = str(jget(payload, "meta.market_status", "") or "")
            conf_lvl = str(jget(payload, "meta.confidence_level", "") or "")
            if verdict == "PASS" and market_status == "NORMAL" and conf_lvl != "LOW":
                risk = "LOW"
                conf = "HIGH"
            else:
                risk = "HIGH" if conf_lvl == "LOW" or market_status == "DEGRADED" else "MEDIUM"
                conf = "LOW" if conf_lvl == "LOW" else "MEDIUM"

        return L1Result(
            mode="L1_AUDIT",
            verdict=verdict,
            risk_level=risk,
            fatal_issues=fatal,
            structural_warnings=warn,
            audit_confidence=conf,
        )

    # --------------- L2：交易裁決 ---------------
    def run_l2(self, payload: Json, l1: L1Result) -> str:
        # V19：不能跳過 L1
        violated, issues = self._v19_constitution_guard("L2", l1.verdict)
        if violated:
            # 立即回退 L1 FAIL（規格：違憲 → L1 FAIL）
            return json.dumps(
                {
                    "mode": "L1_AUDIT",
                    "verdict": "FAIL",
                    "risk_level": "CRITICAL",
                    "fatal_issues": issues,
                    "structural_warnings": [],
                    "audit_confidence": "HIGH",
                },
                ensure_ascii=False,
                indent=2,
            )

        # 啟動條件
        if l1.verdict != "PASS" or jget(payload, "macro.integrity.kill", False) is True:
            return "\n".join(
                [
                    "MODE: L2_EXECUTE",
                    "MARKET_STATE: NORMAL",
                    "MAX_EQUITY_ALLOWED: 0.0%",
                    "DECISION:",
                    "NO TRADE",
                    "",
                    f"RISK_REASON: L1.verdict={l1.verdict} OR macro.integrity.kill={json.dumps(jget(payload,'macro.integrity.kill',None), ensure_ascii=False)} → rule_triggered",
                    "CONFIDENCE: LOW",
                ]
            )

        # MAX_EQUITY 唯一合法來源
        max_eq = jget(payload, "macro.overview.max_equity_allowed_pct", None)
        if not isinstance(max_eq, (int, float)):
            return "\n".join(
                [
                    "MODE: L2_EXECUTE",
                    "MARKET_STATE: NORMAL",
                    "MAX_EQUITY_ALLOWED: 0.0%",
                    "DECISION:",
                    "NO TRADE",
                    "",
                    f"RISK_REASON: {pv(payload,'macro.overview.max_equity_allowed_pct')} → missing/invalid",
                    "CONFIDENCE: LOW",
                ]
            )

        # V17 戰時模式（若觸發，強制 max<=5% 且禁 OPEN/ADD）
        wartime, wt_reasons = self._v17_wartime_triggered(payload)
        if wartime:
            max_eq = min(float(max_eq), 0.05)

        # MARKET_STATE 判定（只用 JSON）
        smr = jget(payload, "macro.overview.SMR", None)
        blow = jget(payload, "macro.overview.Blow_Off_Phase", None)

        if not isinstance(smr, (int, float)) and not isinstance(blow, bool):
            # 核心判定資料不足 → NO TRADE
            return "\n".join(
                [
                    "MODE: L2_EXECUTE",
                    "MARKET_STATE: NORMAL",
                    f"MAX_EQUITY_ALLOWED: {max_eq*100:.1f}%",
                    "DECISION:",
                    "NO TRADE",
                    "",
                    f"RISK_REASON: {pv(payload,'macro.overview.SMR')} & {pv(payload,'macro.overview.Blow_Off_Phase')} → core metric missing",
                    "CONFIDENCE: LOW",
                ]
            )

        market_state = "NORMAL"
        if isinstance(smr, (int, float)) and (smr >= 0.33):
            market_state = "OVERHEAT"
        elif isinstance(blow, bool) and blow is True:
            market_state = "OVERHEAT"
        elif isinstance(smr, (int, float)) and (smr >= 0.30):
            market_state = "OVERHEAT"
        elif isinstance(smr, (int, float)) and (smr < 0):
            market_state = "DEFENSIVE"

        # LOW 信心規則：allocation ×0.5（以 0.5% 階梯）
        conf_lvl = str(jget(payload, "meta.confidence_level", "") or "")
        alloc_multiplier = 0.5 if conf_lvl == "LOW" else 1.0
        rounding_rule = "0.5% step"

        # 這版 L2 不做選股（你規格寫：你不選股/不預測；此處只示範裁決框架）
        # 因此：不開倉，僅輸出 NO TRADE / 或戰時限制
        lines = [
            "MODE: L2_EXECUTE",
            f"MARKET_STATE: {market_state}",
            f"MAX_EQUITY_ALLOWED: {max_eq*100:.1f}%",
            "DECISION:",
        ]

        if wartime:
            lines += [
                "NO TRADE",
                "",
                f"RISK_REASON: {'; '.join(wt_reasons)} → V17_WAR_TIME_OVERRIDE (禁止 OPEN/ADD)",
                "CONFIDENCE: LOW",
            ]
            return "\n".join(lines)

        # 若 max_eq=0 → 明確 NO TRADE
        if float(max_eq) <= 0.0:
            lines += [
                "NO TRADE",
                "",
                f"RISK_REASON: {pv(payload,'macro.overview.max_equity_allowed_pct')} → MAX_EQUITY=0",
                "CONFIDENCE: LOW" if conf_lvl == "LOW" else "MEDIUM",
            ]
            return "\n".join(lines)

        # 保守：若 OVERHEAT 且 LOW → 不開倉
        if market_state == "OVERHEAT" and conf_lvl == "LOW":
            lines += [
                "NO TRADE",
                "",
                f"RISK_REASON: {pv(payload,'macro.overview.SMR')} / {pv(payload,'meta.confidence_level')} → OVERHEAT + LOW",
                "CONFIDENCE: LOW",
            ]
            return "\n".join(lines)

        # 否則仍不主動選股，僅框架輸出
        lines += [
            "NO TRADE",
            "",
            f"RISK_REASON: policy(no stock selection) + alloc_multiplier={alloc_multiplier} ({rounding_rule})",
            "CONFIDENCE: LOW" if conf_lvl == "LOW" else "MEDIUM",
        ]
        return "\n".join(lines)

    # --------------- L3：回撤壓測 ---------------
    def run_l3(self, payload: Json, l1: L1Result) -> str:
        violated, issues = self._v19_constitution_guard("L3", l1.verdict)
        if violated:
            return json.dumps(
                {
                    "mode": "L1_AUDIT",
                    "verdict": "FAIL",
                    "risk_level": "CRITICAL",
                    "fatal_issues": issues,
                    "structural_warnings": [],
                    "audit_confidence": "HIGH",
                },
                ensure_ascii=False,
                indent=2,
            )

        smr = jget(payload, "macro.overview.SMR", None)
        # 觸發條件（依你規格：SMR<0 或 MARKET_STATE=DEFENSIVE 或連敗>=3 或回撤>=0.08）
        # portfolio.* 若不存在不得推算
        portfolio = payload.get("portfolio", {}) if isinstance(payload.get("portfolio", {}), dict) else {}
        perf = portfolio.get("performance", {}) if isinstance(portfolio.get("performance", {}), dict) else {}

        consecutive_losses = perf.get("consecutive_losses", None)
        drawdown_pct = perf.get("drawdown_pct", None)

        activated = False
        triggers = []

        if isinstance(smr, (int, float)) and smr < 0:
            activated = True
            triggers.append(pv(payload, "macro.overview.SMR") + " → SMR<0")

        # 若沒有 portfolio，就不推算
        if isinstance(consecutive_losses, int) and consecutive_losses >= 3:
            activated = True
            triggers.append(f"portfolio.performance.consecutive_losses={consecutive_losses} → >=3")
        if isinstance(drawdown_pct, (int, float)) and drawdown_pct >= 0.08:
            activated = True
            triggers.append(f"portfolio.performance.drawdown_pct={drawdown_pct} → >=0.08")

        if not activated:
            return "\n".join(
                [
                    "MODE: L3_STRESS",
                    "STRESS_TEST: NOT_ACTIVATED",
                    "STRUCTURAL_BREACH: FALSE",
                    "5%_SCENARIO: SAFE",
                    "10%_SCENARIO: SAFE",
                    "15%_SCENARIO: SAFE",
                    "PSYCHOLOGICAL_RISK: LOW",
                    "SURVIVAL_SCORE: 100",
                    "SYSTEM_STATUS: STABLE",
                    "FINAL_VERDICT: SYSTEM_SURVIVES",
                    "",
                    "RISK_REASON: no_trigger → " + ("; ".join(triggers) if triggers else "none"),
                ]
            )

        # 這裡給「系統層級」壓測（不推算倉位，只做保守等級）
        # 若資料 degraded/low → 分數降低
        conf_lvl = str(jget(payload, "meta.confidence_level", "") or "")
        market_status = str(jget(payload, "meta.market_status", "") or "")

        base = 80
        if conf_lvl == "LOW":
            base -= 20
        if market_status == "DEGRADED":
            base -= 15

        score = int(max(0, min(100, base)))
        system_status = "STABLE" if score >= 75 else ("FRAGILE" if score >= 50 else "CRITICAL")
        final = "SYSTEM_SURVIVES" if score >= 75 else ("SYSTEM_AT_RISK" if score >= 50 else "SYSTEM_FAILURE")

        # 情境等級（保守）
        s5 = "WARNING" if system_status != "STABLE" else "SAFE"
        s10 = "FAILURE" if system_status == "CRITICAL" else ("WARNING" if system_status == "FRAGILE" else "SAFE")
        s15 = "FAILURE" if system_status != "STABLE" else "WARNING"

        psycho = "HIGH" if system_status == "CRITICAL" else ("MEDIUM" if system_status == "FRAGILE" else "LOW")
        breach = "TRUE" if system_status == "CRITICAL" else "FALSE"

        return "\n".join(
            [
                "MODE: L3_STRESS",
                "STRESS_TEST: ACTIVATED",
                f"STRUCTURAL_BREACH: {breach}",
                f"5%_SCENARIO: {s5}",
                f"10%_SCENARIO: {s10}",
                f"15%_SCENARIO: {s15}",
                f"PSYCHOLOGICAL_RISK: {psycho}",
                f"SURVIVAL_SCORE: {score}",
                f"SYSTEM_STATUS: {system_status}",
                f"FINAL_VERDICT: {final}",
                "",
                "RISK_REASON: " + "; ".join(triggers),
            ]
        )

    # --------------- 入口：只輸出一個模式 ---------------
    def run(self, payload: Json, run_mode: str) -> str:
        l1 = self.run_l1(payload)

        if run_mode == "L1":
            return json.dumps(
                {
                    "mode": l1.mode,
                    "verdict": l1.verdict,
                    "risk_level": l1.risk_level,
                    "fatal_issues": l1.fatal_issues,
                    "structural_warnings": l1.structural_warnings,
                    "audit_confidence": l1.audit_confidence,
                },
                ensure_ascii=False,
                indent=2,
            )

        if run_mode == "L2":
            return self.run_l2(payload, l1)

        if run_mode == "L3":
            return self.run_l3(payload, l1)

        # fallback
        return json.dumps(
            {
                "mode": "L1_AUDIT",
                "verdict": "FAIL",
                "risk_level": "CRITICAL",
                "fatal_issues": [f"未知 run_mode={run_mode}"],
                "structural_warnings": [],
                "audit_confidence": "HIGH",
            },
            ensure_ascii=False,
            indent=2,
        )


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Sunhero｜股市智能超盤（Predator + UCC V19.1）", layout="wide")

st.title("Sunhero｜股市智能超盤（Predator + UCC V19.1）")

with st.sidebar:
    st.header("控制面板")

    run_mode = st.selectbox("RUN 模式（只輸出一個結果）", ["L1", "L2", "L3"], index=0)

    st.subheader("盤中法人資料政策（UI 開關）")
    allow_same_day_inst = st.checkbox("盤中是否允許當日法人資料", value=False)
    enforce_token_when_same_day = st.checkbox("若允許當日法人資料，是否強制 Token", value=True)

    st.caption("說明：此開關只會寫入 meta.intraday_institutional_policy，並讓 L1/L2 依一致性做審計/降級。")

st.subheader("輸入 Payload（JSON）")
default_payload = {
    "meta": {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "session": "INTRADAY", "market_status": "DEGRADED", "confidence_level": "LOW"},
    "macro": {"overview": {"twii_close": None, "SMR": None, "max_equity_allowed_pct": 0.0}, "market_amount": {}, "integrity": {"kill": False, "reason": "OK"}},
    "stocks": [],
}
payload_text = st.text_area("貼上你的 JSON（可直接貼你上面那包）", value=json.dumps(default_payload, ensure_ascii=False, indent=2), height=380)

colA, colB = st.columns([1, 1])

with colA:
    st.subheader("Patch（補齊欄位 / 治理欄位）")
    if st.button("✅ 套用 Patch 並顯示修補後 JSON", use_container_width=True):
        try:
            raw = json.loads(payload_text)
            patched, notes = patch_payload_for_ui(raw, allow_same_day_inst, enforce_token_when_same_day)
            st.success("Patch 完成")
            if notes:
                st.info("Patch 註記（可稽核）：" + "；".join(notes))
            st.code(json.dumps(patched, ensure_ascii=False, indent=2), language="json")
        except Exception as e:
            st.error(f"JSON 解析失敗：{e}")

with colB:
    st.subheader("執行 UCC")
    if st.button("🚀 RUN（依左側模式）", use_container_width=True):
        try:
            raw = json.loads(payload_text)
            # 先 patch（你要求：payload 補齊 + UI 政策）
            patched, notes = patch_payload_for_ui(raw, allow_same_day_inst, enforce_token_when_same_day)

            ucc = UCCv19_1()
            out = ucc.run(patched, run_mode)

            st.success(f"UCC 輸出完成（MODE={run_mode}）")
            if notes:
                st.info("Patch 註記（可稽核）：" + "；".join(notes))

            # 顯示輸出（可能是 JSON 或多行文字）
            if out.strip().startswith("{"):
                st.code(out, language="json")
            else:
                st.code(out, language="text")

            # 額外顯示：L1 審計摘要（不算第二個模式輸出；只是 UI 顯示）
            l1 = ucc.run_l1(patched)
            st.caption("（UI 附加顯示）L1 審計摘要
