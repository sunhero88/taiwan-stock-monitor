# ucc_v19_1.py
# =========================================================
# Predator UCC V19.1 Hardened Final Lockdown (Audit-Hardened)
# - JSON-only
# - path=value evidence
# - single-mode output (L1 or L2 or L3)
# =========================================================

from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple, Union

Json = Dict[str, Any]


def _split_path(path: str) -> List[str]:
    return re.split(r"\.(?![^\[]*\])", path)


def jget(d: Any, path: str, default: Any = None) -> Any:
    cur = d
    for tok in _split_path(path):
        if not tok:
            continue
        m = re.fullmatch(r"([^\[]+)(\[(\-?\d+)\])?", tok)
        if not m:
            return default
        key = m.group(1)
        idx = m.group(3)

        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]

        if idx is not None:
            if not isinstance(cur, list):
                return default
            i = int(idx)
            if i < 0 or i >= len(cur):
                return default
            cur = cur[i]
    return cur


def exists(d: Any, path: str) -> bool:
    sentinel = object()
    return jget(d, path, sentinel) is not sentinel


def to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            s = x.strip().replace(",", "")
            if s == "":
                return None
            return float(s)
        return None
    except Exception:
        return None


def contains_any(s: Any, needles: List[str]) -> bool:
    if not isinstance(s, str):
        return False
    up = s.upper()
    return any(n.upper() in up for n in needles)


class UCCv19_1:
    SMR_OVERHEAT_1 = 0.30
    SMR_OVERHEAT_2 = 0.33

    # -------------------------
    # V17 War-time override
    # -------------------------
    def v17_triggered(self, j: Json) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        if jget(j, "meta.war_time_override") is True:
            reasons.append("meta.war_time_override=true")
        reg = jget(j, "meta.current_regime")
        if contains_any(reg, ["WAR", "CRISIS"]):
            reasons.append(f"meta.current_regime={reg}")
        return (len(reasons) > 0, reasons)

    # -------------------------
    # Market state (JSON-only)
    # -------------------------
    def market_state(self, j: Json) -> Tuple[str, List[str]]:
        reasons: List[str] = []
        smr = to_float(jget(j, "macro.overview.SMR"))
        bop = jget(j, "macro.overview.Blow_Off_Phase", None)
        if smr is not None:
            reasons.append(f"macro.overview.SMR={smr}")
        if bop is not None:
            reasons.append(f"macro.overview.Blow_Off_Phase={bop}")

        if (smr is not None and smr >= self.SMR_OVERHEAT_2) or (bop is True):
            return "OVERHEAT", reasons
        if smr is not None and smr >= self.SMR_OVERHEAT_1:
            return "OVERHEAT", reasons
        if smr is not None and smr < 0:
            return "DEFENSIVE", reasons
        return "NORMAL", reasons

    # -------------------------
    # L3 trigger gate
    # -------------------------
    def l3_triggered(self, j: Json, market_state: Optional[str] = None) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        smr = to_float(jget(j, "macro.overview.SMR"))
        if smr is not None and smr < 0:
            reasons.append(f"macro.overview.SMR={smr} -> SMR<0")
        if market_state == "DEFENSIVE":
            reasons.append("MARKET_STATE=DEFENSIVE")

        cons = jget(j, "portfolio.performance.consecutive_losses", None)
        dd = to_float(jget(j, "portfolio.performance.drawdown_pct", None))
        if cons is not None:
            try:
                if int(cons) >= 3:
                    reasons.append(f"portfolio.performance.consecutive_losses={cons} >= 3")
            except Exception:
                pass
        if dd is not None and dd >= 0.08:
            reasons.append(f"portfolio.performance.drawdown_pct={dd} >= 0.08")

        return (len(reasons) > 0, reasons)

    # -------------------------
    # L1 audit rules
    # -------------------------
    def run_l1(self, j: Json) -> Json:
        fatal: List[str] = []
        warn: List[str] = []

        # 3.1 fatal conditions
        twii = jget(j, "macro.overview.twii_close", None)
        if twii is None:
            fatal.append('macro.overview.twii_close is null/missing -> FAIL (path=$.macro.overview.twii_close)')

        if jget(j, "macro.integrity.kill") is True:
            fatal.append('macro.integrity.kill=true -> FAIL (path=$.macro.integrity.kill=true)')

        conf = jget(j, "meta.confidence_level")
        ms = jget(j, "meta.market_status")
        if conf == "LOW" and ms == "NORMAL":
            fatal.append('meta.confidence_level="LOW" but meta.market_status="NORMAL" -> FAIL (path=$.meta.confidence_level, $.meta.market_status)')

        amt_raw = to_float(jget(j, "macro.market_amount.amount_total_raw"))
        amt_blend = to_float(jget(j, "macro.market_amount.amount_total_blended"))
        if amt_raw is not None and amt_blend is not None and amt_blend < amt_raw:
            fatal.append(f"macro.market_amount.amount_total_blended < amount_total_raw -> FAIL (path=$.macro.market_amount.amount_total_blended={amt_blend}, $.macro.market_amount.amount_total_raw={amt_raw})")

        if jget(j, "meta.is_using_previous_day") is True and not exists(j, "meta.effective_trade_date"):
            fatal.append("meta.is_using_previous_day=true but missing meta.effective_trade_date -> FAIL (path=$.meta.is_using_previous_day, $.meta.effective_trade_date)")

        # Institutional stale contradiction
        stocks = jget(j, "stocks", [])
        if isinstance(stocks, list):
            for i, s in enumerate(stocks):
                st = jget(j, f"stocks[{i}].Institutional.Inst_Status")
                net3 = jget(j, f"stocks[{i}].Institutional.Inst_Net_3d", None)
                if st == "NO_UPDATE_TODAY" and net3 is not None:
                    fatal.append(f'stocks[{i}].Institutional.Inst_Status="NO_UPDATE_TODAY" but Inst_Net_3d has value -> FAIL (path=$.stocks[{i}].Institutional.Inst_Status, $.stocks[{i}].Institutional.Inst_Net_3d={net3})')

        # Symbol–Price cross-scale sanity (only apply to 2330.TW per your spec example)
        if isinstance(stocks, list):
            for i, s in enumerate(stocks):
                sym = jget(j, f"stocks[{i}].Symbol")
                px = to_float(jget(j, f"stocks[{i}].Price"))
                if sym == "2330.TW" and px is not None and px < 200:
                    fatal.append(f"Symbol–Price cross-scale mismatch -> FAIL (path=$.stocks[{i}].Symbol={sym}, $.stocks[{i}].Price={px})")

        verdict = "PASS" if len(fatal) == 0 else "FAIL"
        risk = "LOW" if verdict == "PASS" else "CRITICAL"

        return {
            "mode": "L1_AUDIT",
            "verdict": verdict,
            "risk_level": risk,
            "fatal_issues": fatal,
            "structural_warnings": warn,
            "audit_confidence": "HIGH" if verdict == "PASS" else "HIGH",
        }

    # -------------------------
    # L2 execute
    # -------------------------
    def run_l2(self, j: Json, l1_verdict: str = "PASS") -> str:
        # Guard: must have max_equity_allowed_pct
        maxeq = to_float(jget(j, "macro.overview.max_equity_allowed_pct"))
        if maxeq is None:
            return "MODE: L2_EXECUTE\nDECISION: NO TRADE\nRISK_REASON: macro.overview.max_equity_allowed_pct missing -> NO_TRADE\nCONFIDENCE: LOW"

        # V17 war-time override (禁止 OPEN/ADD)
        war, war_reason = self.v17_triggered(j)

        ms, ms_reason = self.market_state(j)
        conf_level = jget(j, "meta.confidence_level", "MEDIUM")

        # LOW confidence scaling rule
        scale = 1.0
        scale_note = ""
        if conf_level == "LOW":
            scale = 0.5
            scale_note = "LOW_CONF_SCALE: allocation x0.5 (rounded to 0.5%)"

        # War-time clamp
        if war:
            maxeq = min(maxeq, 0.05)

        lines: List[str] = []
        lines.append("MODE: L2_EXECUTE")
        lines.append(f"MARKET_STATE: {ms}")
        lines.append(f"MAX_EQUITY_ALLOWED: {maxeq*100:.1f}%")
        if scale_note:
            lines.append(scale_note)

        # Decision policy (minimal, JSON-only, conservative)
        if war:
            lines.append("DECISION:")
            lines.append("NO TRADE")
            lines.append("RISK_REASON:" + " | ".join([f"{r} -> V17_WAR_TIME" for r in war_reason]))
            lines.append("CONFIDENCE: LOW")
            return "\n".join(lines)

        # Overheat -> default no open
        if ms == "OVERHEAT":
            lines.append("DECISION:")
            lines.append("NO TRADE")
            lines.append("RISK_REASON:" + " | ".join([f"{r} -> MARKET_OVERHEAT" for r in ms_reason]))
            lines.append(f"CONFIDENCE: {conf_level}")
            return "\n".join(lines)

        lines.append("DECISION:")
        lines.append("NO TRADE")
        lines.append("RISK_REASON: JSON_ONLY_POLICY -> NO_MODEL_SIGNAL")
        lines.append(f"CONFIDENCE: {conf_level}")
        return "\n".join(lines)

    # -------------------------
    # L3 stress
    # -------------------------
    def run_l3(self, j: Json) -> str:
        ms, _ = self.market_state(j)
        trig, reasons = self.l3_triggered(j, market_state=ms)
        if not trig:
            return "MODE: L3_STRESS\nSTRESS_TEST: NOT_ACTIVATED\nFINAL_VERDICT: SYSTEM_SURVIVES"

        # Minimal stress model (rule-based)
        dd = to_float(jget(j, "portfolio.performance.drawdown_pct"))
        cons = jget(j, "portfolio.performance.consecutive_losses")

        breach = False
        if dd is not None and dd >= 0.15:
            breach = True

        def scen(x: float) -> str:
            if dd is None:
                return "WARNING"
            if dd + x >= 0.15:
                return "FAILURE"
            if dd + x >= 0.10:
                return "WARNING"
            return "SAFE"

        surv = 80
        if ms == "DEFENSIVE":
            surv -= 15
        if dd is not None:
            surv -= int(dd * 100)
        if cons is not None:
            try:
                surv -= int(cons) * 3
            except Exception:
                pass
        surv = max(0, min(100, surv))

        sys_status = "STABLE"
        final = "SYSTEM_SURVIVES"
        if breach or surv < 40:
            sys_status = "CRITICAL"
            final = "SYSTEM_FAILURE"
        elif surv < 60:
            sys_status = "FRAGILE"
            final = "SYSTEM_AT_RISK"

        lines: List[str] = []
        lines.append("MODE: L3_STRESS")
        lines.append("STRESS_TEST: ACTIVATED")
        lines.append(f"STRUCTURAL_BREACH: {'TRUE' if breach else 'FALSE'}")
        lines.append(f"5%_SCENARIO: {scen(0.05)}")
        lines.append(f"10%_SCENARIO: {scen(0.10)}")
        lines.append(f"15%_SCENARIO: {scen(0.15)}")
        lines.append("PSYCHOLOGICAL_RISK: " + ("HIGH" if surv < 50 else "MEDIUM" if surv < 70 else "LOW"))
        lines.append(f"SURVIVAL_SCORE: {surv}")
        lines.append(f"SYSTEM_STATUS: {sys_status}")
        lines.append(f"FINAL_VERDICT: {final}")
        lines.append("RISK_REASON: " + " | ".join([f"{r} -> L3_TRIGGER" for r in reasons]))
        return "\n".join(lines)

    # ------------------
    # Top-level run (V19 enforcement)
    # -------------------------
    def run(self, j: Json, run_mode: str = "L1") -> Union[Json, str]:
        run_mode = (run_mode or "L1").strip().upper()
        if run_mode not in ("L1", "L2", "L3"):
            run_mode = "L1"

        # always L1 first（不可跳過）
        l1 = self.run_l1(j)
        l1_verdict = l1.get("verdict")

        if run_mode == "L1":
            return l1

        # V19：防跳關（但保留 L1 原始 fatal_issues，避免只看到「不能跑 L2」卻看不到根因）
        if run_mode == "L2" and l1_verdict != "PASS":
            fatal = list(l1.get("fatal_issues", []))
            fatal.insert(0, f"VIOLATION: RUN:L2 requested but L1.verdict != PASS")
            fatal.insert(1, f'EVIDENCE: $.mode="L1_AUDIT", $.verdict="{l1_verdict}"')
            l1["fatal_issues"] = fatal
            l1["verdict"] = "FAIL"
            l1["risk_level"] = "CRITICAL"
            return l1

        if run_mode == "L2":
            return self.run_l2(j, l1_verdict=l1_verdict)

        if run_mode == "L3":
            ms, _ = self.market_state(j)
            trig, _ = self.l3_triggered(j, market_state=ms)
            if not trig:
                fatal = list(l1.get("fatal_issues", []))
                fatal.insert(0, "VIOLATION: RUN:L3 requested but L3 not triggered")
                fatal.insert(1, f"EVIDENCE: MARKET_STATE={ms}")
                l1["fatal_issues"] = fatal
                l1["verdict"] = "FAIL"
                l1["risk_level"] = "CRITICAL"
                l1.setdefault("structural_warnings", []).append(f"macro.overview.SMR={jget(j,'macro.overview.SMR')} -> L3_NOT_TRIGGERED")
                l1.setdefault("structural_warnings", []).append(f"portfolio.performance.drawdown_pct={jget(j,'portfolio.performance.drawdown_pct')} -> L3_NOT_TRIGGERED")
                l1.setdefault("structural_warnings", []).append(f"portfolio.performance.consecutive_losses={jget(j,'portfolio.performance.consecutive_losses')} -> L3_NOT_TRIGGERED")
                return l1
            return self.run_l3(j)

        return l1
