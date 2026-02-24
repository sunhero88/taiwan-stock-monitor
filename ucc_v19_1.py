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
            reasons.append(f"meta.war_time_override={jget(j,'meta.war_time_override')}")
        reg = jget(j, "meta.current_regime")
        if contains_any(reg, ["WAR", "CRISIS"]):
            reasons.append(f"meta.current_regime={reg}")
        return (len(reasons) > 0), reasons

    # -------------------------
    # L1 Fatal checks (per spec)
    # -------------------------
    def l1_fatal_checks(self, j: Json) -> List[str]:
        issues: List[str] = []

        # (1) twii_close null/missing
        if (not exists(j, "macro.overview.twii_close")) or (jget(j, "macro.overview.twii_close") is None):
            issues.append(f"macro.overview.twii_close={jget(j,'macro.overview.twii_close')} -> FATAL(null_or_missing)")

        # (2) kill switch
        if jget(j, "macro.integrity.kill") is True:
            issues.append(f"macro.integrity.kill={jget(j,'macro.integrity.kill')} -> FATAL(kill_switch)")

        # (3) zombie inst data
        stocks = jget(j, "stocks", [])
        if isinstance(stocks, list):
            for i, s in enumerate(stocks):
                st = jget(s, "Institutional.Inst_Status")
                net3 = jget(s, "Institutional.Inst_Net_3d")
                if st == "NO_UPDATE_TODAY" and net3 is not None:
                    issues.append(
                        f"stocks[{i}].Institutional.Inst_Status={st}, "
                        f"stocks[{i}].Institutional.Inst_Net_3d={net3} -> FATAL(zombie_inst_data)"
                    )

        # (4) confidence LOW but market_status NORMAL
        if jget(j, "meta.confidence_level") == "LOW" and jget(j, "meta.market_status") == "NORMAL":
            issues.append(
                f"meta.confidence_level={jget(j,'meta.confidence_level')}, "
                f"meta.market_status={jget(j,'meta.market_status')} -> FATAL(conflict_low_vs_normal)"
            )

        # (5) symbol–price mismatch example
        if isinstance(stocks, list):
            for i, s in enumerate(stocks):
                sym = jget(s, "Symbol")
                px = to_float(jget(s, "Price"))
                if sym == "2330.TW" and px is not None and px < 200:
                    issues.append(f"stocks[{i}].Symbol={sym}, stocks[{i}].Price={px} -> FATAL(symbol_price_mismatch)")

        # (6) blended < raw
        raw = to_float(jget(j, "macro.market_amount.amount_total_raw"))
        blended = to_float(jget(j, "macro.market_amount.amount_total_blended"))
        if raw is not None and blended is not None and blended < raw:
            issues.append(
                f"macro.market_amount.amount_total_blended={blended}, "
                f"macro.market_amount.amount_total_raw={raw} -> FATAL(blended_lt_raw)"
            )

        # (7) using previous day but missing effective_trade_date
        if jget(j, "meta.is_using_previous_day") is True and (not exists(j, "meta.effective_trade_date")):
            issues.append(
                f"meta.is_using_previous_day={jget(j,'meta.is_using_previous_day')}, "
                f"meta.effective_trade_date={jget(j,'meta.effective_trade_date')} -> FATAL(missing_effective_trade_date)"
            )

        return issues

    def run_l1(self, j: Json) -> Json:
        fatal = self.l1_fatal_checks(j)
        warnings: List[str] = []

        ms = jget(j, "meta.market_status")
        if ms == "DEGRADED":
            warnings.append(f"meta.market_status={ms} -> WARNING(data_feed_degraded)")

        conf = jget(j, "meta.confidence_level")
        if conf == "LOW":
            warnings.append(f"meta.confidence_level={conf} -> WARNING(low_confidence)")

        if jget(j, "meta.is_using_previous_day") is True:
            warnings.append(
                f"meta.is_using_previous_day={jget(j,'meta.is_using_previous_day')}, "
                f"meta.effective_trade_date={jget(j,'meta.effective_trade_date')} -> WARNING(using_t_minus_1)"
            )

        # Extra (non-fatal) consistency checks: forensics-friendly
        regime = jget(j, "meta.current_regime")
        smr = to_float(jget(j, "macro.overview.SMR"))
        bop = jget(j, "macro.overview.Blow_Off_Phase")
        if regime == "DATA_FAILURE" and (smr is not None or bop is True):
            warnings.append(
                f"meta.current_regime={regime}, macro.overview.SMR={smr}, macro.overview.Blow_Off_Phase={bop} "
                f"-> WARNING(regime_vs_macro_inconsistent)"
            )

        # Exposure lock forensic hint
        max_eq = to_float(jget(j, "macro.overview.max_equity_allowed_pct"))
        if max_eq is not None and max_eq == 0.0:
            lock_reason = jget(j, "meta.max_equity_lock_reason")
            warnings.append(
                f"macro.overview.max_equity_allowed_pct={max_eq}, meta.max_equity_lock_reason={lock_reason} "
                f"-> WARNING(exposure_locked_need_reason)"
            )

        stocks = jget(j, "stocks", [])
        if isinstance(stocks, list) and stocks:
            stale = any(
                (jget(s, "Institutional.Inst_Status") in ("USING_T_MINUS_1", "NO_UPDATE_TODAY"))
                or (jget(s, "Institutional.inst_data_fresh") is False)
                for s in stocks
            )
            if stale:
                warnings.append("stocks[*].Institutional.(Inst_Status/inst_data_fresh) -> WARNING(institutional_not_fresh)")

        if fatal:
            verdict = "FAIL"
            risk_level = "CRITICAL"
            audit_conf = "HIGH"
        else:
            verdict = "PASS"
            if ms == "DEGRADED" or conf == "LOW":
                risk_level = "HIGH"
                audit_conf = "MEDIUM"
            else:
                risk_level = "LOW"
                audit_conf = "HIGH"

        return {
            "mode": "L1_AUDIT",
            "verdict": verdict,
            "risk_level": risk_level,
            "fatal_issues": fatal,
            "structural_warnings": warnings,
            "audit_confidence": audit_conf
        }

    # -------------------------
    # MARKET_STATE (JSON-only)
    # -------------------------
    def market_state(self, j: Json) -> Tuple[str, List[str]]:
        reasons: List[str] = []
        smr = to_float(jget(j, "macro.overview.SMR"))
        bop_exists = exists(j, "macro.overview.Blow_Off_Phase")
        bop = jget(j, "macro.overview.Blow_Off_Phase") if bop_exists else None

        if smr is not None and smr >= self.SMR_OVERHEAT_2:
            reasons.append(f"macro.overview.SMR={smr} -> MARKET_STATE=OVERHEAT(SMR>=0.33)")
            return "OVERHEAT", reasons
        if bop_exists and bop is True:
            reasons.append(f"macro.overview.Blow_Off_Phase={bop} -> MARKET_STATE=OVERHEAT(Blow_Off_Phase=true)")
            return "OVERHEAT", reasons
        if smr is not None and smr >= self.SMR_OVERHEAT_1:
            reasons.append(f"macro.overview.SMR={smr} -> MARKET_STATE=OVERHEAT(SMR>=0.30)")
            return "OVERHEAT", reasons
        if smr is not None and smr < 0:
            reasons.append(f"macro.overview.SMR={smr} -> MARKET_STATE=DEFENSIVE(SMR<0)")
            return "DEFENSIVE", reasons

        reasons.append(f"macro.overview.SMR={smr} -> MARKET_STATE=NORMAL")
        return "NORMAL", reasons

    # -------------------------
    # L2 (strict gating)
    # -------------------------
    def run_l2(self, j: Json, l1_verdict: str) -> str:
        if l1_verdict != "PASS":
            return "\n".join([
                "MODE: L2_EXECUTE",
                "MARKET_STATE: N/A",
                "MAX_EQUITY_ALLOWED: N/A",
                "",
                "DECISION:",
                "NO TRADE",
                "",
                "RISK_REASON:mode_gate=L1_NOT_PASS -> rule_triggered",
                "",
                "CONFIDENCE: LOW"
            ])

        if jget(j, "macro.integrity.kill") is True:
            return "\n".join([
                "MODE: L2_EXECUTE",
                "MARKET_STATE: N/A",
                "MAX_EQUITY_ALLOWED: N/A",
                "",
                "DECISION:",
                "NO TRADE",
                "",
                f"RISK_REASON:macro.integrity.kill={jget(j,'macro.integrity.kill')} -> rule_triggered",
                "",
                "CONFIDENCE: LOW"
            ])

        ms, ms_reasons = self.market_state(j)

        # MAX_EQUITY唯一合法來源
        max_eq = to_float(jget(j, "macro.overview.max_equity_allowed_pct"))
        if max_eq is None:
            rr = ms_reasons + [f"macro.overview.max_equity_allowed_pct={jget(j,'macro.overview.max_equity_allowed_pct')} -> NO_TRADE(missing_MAX_EQUITY_ALLOWED)"]
            return "\n".join([
                "MODE: L2_EXECUTE",
                f"MARKET_STATE: {ms}",
                "MAX_EQUITY_ALLOWED: N/A",
                "",
                "DECISION:",
                "NO TRADE",
                "",
                "RISK_REASON:" + ("; ".join(rr)),
                "",
                f"CONFIDENCE: {jget(j,'meta.confidence_level') or 'LOW'}"
            ])

        # V17
        v17_on, v17_reasons = self.v17_triggered(j)
        rr = []
        rr.extend(ms_reasons)
        if v17_on:
            max_eq = min(max_eq, 0.05)
            rr.extend([f"{r} -> V17_TRIGGERED" for r in v17_reasons])
            rr.append("V17_RULE: MAX_EQUITY_ALLOWED<=5% AND FORBID_OPEN_ADD")

        # UCC不選股：若 JSON 沒提供可執行 signal/action，維持 NO TRADE（合憲保守）
        return "\n".join([
            "MODE: L2_EXECUTE",
            f"MARKET_STATE: {ms}",
            f"MAX_EQUITY_ALLOWED: {max_eq*100:.1f}%",
            "",
            "DECISION:",
            "NO TRADE",
            "",
            "RISK_REASON:" + ("; ".join(rr)),
            "",
            f"CONFIDENCE: {jget(j,'meta.confidence_level') or 'LOW'}"
        ])

    # -------------------------
    # L3
    # -------------------------
    def l3_triggered(self, j: Json, market_state: Optional[str]) -> Tuple[bool, List[str]]:
        reasons: List[str] = []

        smr = to_float(jget(j, "macro.overview.SMR"))
        if smr is not None and smr < 0:
            reasons.append(f"macro.overview.SMR={smr} -> L3_TRIGGER(SMR<0)")

        if market_state == "DEFENSIVE":
            reasons.append(f"MARKET_STATE={market_state} -> L3_TRIGGER(DEFENSIVE)")

        if exists(j, "portfolio.performance.consecutive_losses"):
            try:
                cl = float(jget(j, "portfolio.performance.consecutive_losses"))
            except:
                cl = None
            if cl is not None and cl >= 3:
                reasons.append(f"portfolio.performance.consecutive_losses={cl} -> L3_TRIGGER(>=3)")

        if exists(j, "portfolio.performance.drawdown_pct"):
            dd = to_float(jget(j, "portfolio.performance.drawdown_pct"))
            if dd is not None and dd >= 0.08:
                reasons.append(f"portfolio.performance.drawdown_pct={dd} -> L3_TRIGGER(>=0.08)")

        return (len(reasons) > 0), reasons

    def run_l3(self, j: Json) -> str:
        ms, ms_reasons = self.market_state(j)
        trig, reasons = self.l3_triggered(j, market_state=ms)
        if not trig:
            return "UNREACHABLE"

        dd = to_float(jget(j, "portfolio.performance.drawdown_pct"))
        dd = dd if dd is not None else 0.0

        def scenario(th: float) -> str:
            if dd >= th:
                return "FAILURE"
            if dd >= th * 0.6:
                return "WARNING"
            return "SAFE"

        s5, s10, s15 = scenario(0.05), scenario(0.10), scenario(0.15)

        breach = (jget(j, "meta.market_status") == "DEGRADED" and jget(j, "meta.confidence_level") == "LOW" and ms == "DEFENSIVE")

        psycho = "LOW"
        if ms == "DEFENSIVE":
            psycho = "HIGH"
        elif jget(j, "meta.confidence_level") == "LOW":
            psycho = "MEDIUM"

        score = 100
        if s5 != "SAFE":
            score -= 20
        if s10 != "SAFE":
            score -= 25
        if s15 != "SAFE":
            score -= 30
        if breach:
            score -= 15
        score = max(0, min(100, score))

        if score >= 70:
            status, final = "STABLE", "SYSTEM_SURVIVES"
        elif score >= 40:
            status, final = "FRAGILE", "SYSTEM_AT_RISK"
        else:
            status, final = "CRITICAL", "SYSTEM_FAILURE"

        rr = ms_reasons + reasons

        return "\n".join([
            "MODE: L3_STRESS",
            "STRESS_TEST: ACTIVATED",
            "",
            f"STRUCTURAL_BREACH: {'TRUE' if breach else 'FALSE'}",
            f"5%_SCENARIO: {s5}",
            f"10%_SCENARIO: {s10}",
            f"15%_SCENARIO: {s15}",
            "",
            f"PSYCHOLOGICAL_RISK: {psycho}",
            f"SURVIVAL_SCORE: {score}",
            f"SYSTEM_STATUS: {status}",
            f"FINAL_VERDICT: {final}",
            "",
            "RISK_REASON:" + ("; ".join(rr))
        ])

    # -------------------------
    # Top-level run (V19 enforcement)
    # -------------------------
    def run(self, j: Json, run_mode: str = "L1") -> Union[Json, str]:
        run_mode = (run_mode or "L1").upper()

        # always L1 first (不可跳過)
        l1 = self.run_l1(j)
        l1_verdict = l1["verdict"]

        if run_mode == "L1":
            return l1

        if run_mode == "L2":
            if l1_verdict != "PASS":
                l1["verdict"] = "FAIL"
                l1["risk_level"] = "CRITICAL"
                l1["fatal_issues"].append(f"V19: RUN:L2 requested but L1.verdict={l1_verdict} -> VIOLATION(skip_gate)")
                return l1
            return self.run_l2(j, l1_verdict=l1_verdict)

        if run_mode == "L3":
            ms, _ = self.market_state(j)
            trig, _ = self.l3_triggered(j, market_state=ms)
            if not trig:
                l1["verdict"] = "FAIL"
                l1["risk_level"] = "CRITICAL"
                l1["fatal_issues"].append("V19: RUN:L3 requested but L3 not triggered -> VIOLATION(no_trigger)")
                l1["structural_warnings"].append(f"macro.overview.SMR={jget(j,'macro.overview.SMR')} -> L3_NOT_TRIGGERED")
                l1["structural_warnings"].append(f"portfolio.performance.drawdown_pct={jget(j,'portfolio.performance.drawdown_pct')} -> L3_NOT_TRIGGERED")
                l1["structural_warnings"].append(f"portfolio.performance.consecutive_losses={jget(j,'portfolio.performance.consecutive_losses')} -> L3_NOT_TRIGGERED")
                return l1
            return self.run_l3(j)

        return l1
