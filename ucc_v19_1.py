# ucc_v19_1.py
# =========================================================
# Predator UCC V19.1 Hardened Final Lockdown (Standalone)
# - 禁止 self-import（最重要）
# - JSON-only evidence: path=value
# - 一次只輸出 L1 或 L2 或 L3
# =========================================================

from __future__ import annotations
from typing import Any, Dict, List, Tuple


class UCCv19_1:
    VERSION = "UCC_V19.1_HARDENED_FINAL_LOCKDOWN"

    # -------------------------
    # Public entry
    # -------------------------
    def run(self, payload: Dict[str, Any], run_mode: str = "L1") -> Any:
        """
        run_mode: "L1" | "L2" | "L3"
        憲法：不可跳過 L1。若 L1 != PASS 仍要求 L2/L3 -> 直接回 L1 FAIL（違憲）
        """
        run_mode = (run_mode or "L1").strip().upper()
        if run_mode not in ("L1", "L2", "L3"):
            run_mode = "L1"

        l1 = self._l1_audit(payload)

        # V19：防跳關
        if run_mode in ("L2", "L3") and l1["verdict"] != "PASS":
            return self._l1_fail_constitution(
                fatal=[
                    f'VIOLATION: RUN:{run_mode} requested but L1.verdict != PASS',
                    f'EVIDENCE: $.mode="L1_AUDIT", $.verdict="{l1["verdict"]}"'
                ],
                structural=[]
            )

        if run_mode == "L1":
            return l1
        if run_mode == "L2":
            return self._l2_execute(payload, l1)
        return self._l3_stress(payload, l1)

    # -------------------------
    # Helpers
    # -------------------------
    def _get(self, obj: Any, path: List[str]) -> Any:
        cur = obj
        for k in path:
            if not isinstance(cur, dict) or k not in cur:
                return None
            cur = cur[k]
        return cur

    def _evi(self, jpath: str, value: Any) -> str:
        return f"{jpath}={value}"

    def _contains(self, s: Any, token: str) -> bool:
        if s is None:
            return False
        return token.upper() in str(s).upper()

    # =========================================================
    # L1: Data Integrity Arbiter
    # =========================================================
    def _l1_audit(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        fatal: List[str] = []
        warn: List[str] = []

        meta = payload.get("meta", {}) or {}
        overview = self._get(payload, ["macro", "overview"]) or {}
        market_amount = self._get(payload, ["macro", "market_amount"]) or {}
        integrity = self._get(payload, ["macro", "integrity"]) or {}
        stocks = payload.get("stocks", []) or []

        # 3.1 Fatal conditions

        # (1) twii_close null/missing
        twii_close = overview.get("twii_close", None)
        if twii_close is None:
            fatal.append("FATAL: twii_close missing/null -> FAIL")
            fatal.append("EVIDENCE: " + self._evi("$.macro.overview.twii_close", twii_close))

        # (2) integrity.kill=true
        kill = integrity.get("kill", False)
        if kill is True:
            fatal.append("FATAL: integrity.kill=true -> FAIL")
            fatal.append("EVIDENCE: " + self._evi("$.macro.integrity.kill", kill))

        # (3) NO_UPDATE_TODAY but Inst_Net_3d present
        for i, s in enumerate(stocks):
            inst = (s or {}).get("Institutional", {}) or {}
            st = inst.get("Inst_Status", None)
            net3d = inst.get("Inst_Net_3d", None)
            if st == "NO_UPDATE_TODAY" and net3d is not None:
                fatal.append("FATAL: NO_UPDATE_TODAY but Inst_Net_3d present -> ZOMBIE_DATA")
                fatal.append("EVIDENCE: " + self._evi(f"$.stocks[{i}].Institutional.Inst_Status", st))
                fatal.append("EVIDENCE: " + self._evi(f"$.stocks[{i}].Institutional.Inst_Net_3d", net3d))
                break

        # (4) confidence_level=LOW but market_status=NORMAL
        conf_level = meta.get("confidence_level", None)
        market_status = meta.get("market_status", None)
        if conf_level == "LOW" and market_status == "NORMAL":
            fatal.append("FATAL: confidence_level=LOW but market_status=NORMAL -> INCONSISTENT")
            fatal.append("EVIDENCE: " + self._evi("$.meta.confidence_level", conf_level))
            fatal.append("EVIDENCE: " + self._evi("$.meta.market_status", market_status))

        # (5) 2330.TW price < 200
        for i, s in enumerate(stocks):
            sym = (s or {}).get("Symbol", "")
            price = (s or {}).get("Price", None)
            if sym == "2330.TW" and price is not None:
                try:
                    if float(price) < 200:
                        fatal.append("FATAL: 2330.TW price < 200 -> CROSS_SCALE_MISMATCH")
                        fatal.append("EVIDENCE: " + self._evi(f"$.stocks[{i}].Symbol", sym))
                        fatal.append("EVIDENCE: " + self._evi(f"$.stocks[{i}].Price", price))
                        break
                except Exception:
                    fatal.append("FATAL: 2330.TW price not numeric -> INVALID")
                    fatal.append("EVIDENCE: " + self._evi(f"$.stocks[{i}].Price", price))
                    break

        # (6) amount_total_blended < amount_total_raw
        raw_total = market_amount.get("amount_total_raw", None)
        blended = market_amount.get("amount_total_blended", None)
        if raw_total is not None and blended is not None:
            try:
                if int(blended) < int(raw_total):
                    fatal.append("FATAL: amount_total_blended < amount_total_raw -> BROKEN_AMOUNT")
                    fatal.append("EVIDENCE: " + self._evi("$.macro.market_amount.amount_total_raw", raw_total))
                    fatal.append("EVIDENCE: " + self._evi("$.macro.market_amount.amount_total_blended", blended))
            except Exception:
                warn.append("WARN: amount_total_raw/blended not comparable (non-int)")

        # (7) is_using_previous_day=true but effective_trade_date missing
        is_prev = meta.get("is_using_previous_day", False)
        eff_date = meta.get("effective_trade_date", None)
        if is_prev is True and not eff_date:
            fatal.append("FATAL: is_using_previous_day=true but effective_trade_date missing")
            fatal.append("EVIDENCE: " + self._evi("$.meta.is_using_previous_day", is_prev))
            fatal.append("EVIDENCE: " + self._evi("$.meta.effective_trade_date", eff_date))

        # Structural warnings
        conf_obj = meta.get("confidence", {}) or {}
        if market_status == "DEGRADED":
            if conf_obj.get("price") == "HIGH" and conf_obj.get("volume") == "HIGH" and conf_obj.get("institutional") == "HIGH":
                warn.append("WARN: market_status=DEGRADED but confidence all HIGH (possible mismatch)")
                warn.append("EVIDENCE: " + self._evi("$.meta.market_status", market_status))
                warn.append("EVIDENCE: " + self._evi("$.meta.confidence", conf_obj))

        vix_invalid = integrity.get("vix_invalid", None)
        if vix_invalid is True:
            warn.append("WARN: vix_invalid=true -> macro risk metrics may be compromised")
            warn.append("EVIDENCE: " + self._evi("$.macro.integrity.vix_invalid", vix_invalid))

        if fatal:
            return {
                "mode": "L1_AUDIT",
                "verdict": "FAIL",
                "risk_level": "CRITICAL",
                "fatal_issues": fatal,
                "structural_warnings": warn,
                "audit_confidence": "HIGH"
            }

        verdict = "PASS" if not warn else "PARTIAL_PASS"
        risk = "LOW" if verdict == "PASS" else "MEDIUM"
        audit_conf = "HIGH" if verdict == "PASS" else "MEDIUM"

        return {
            "mode": "L1_AUDIT",
            "verdict": verdict,
            "risk_level": risk,
            "fatal_issues": [],
            "structural_warnings": warn,
            "audit_confidence": audit_conf
        }

    def _l1_fail_constitution(self, fatal: List[str], structural: List[str]) -> Dict[str, Any]:
        return {
            "mode": "L1_AUDIT",
            "verdict": "FAIL",
            "risk_level": "CRITICAL",
            "fatal_issues": fatal,
            "structural_warnings": structural,
            "audit_confidence": "HIGH"
        }

    # =========================================================
    # Market State + V17 War-Time (enforced in L2)
    # =========================================================
    def _market_state(self, payload: Dict[str, Any]) -> Tuple[str, List[str]]:
        ov = self._get(payload, ["macro", "overview"]) or {}
        smr = ov.get("SMR", None)
        bop = ov.get("Blow_Off_Phase", None)

        if smr is None:
            return "UNKNOWN", ["RISK_REASON: $.macro.overview.SMR=None → MARKET_STATE_UNKNOWN"]

        try:
            smr_f = float(smr)
        except Exception:
            return "UNKNOWN", [f"RISK_REASON: $.macro.overview.SMR={smr} → not numeric"]

        if bop is True and smr_f >= 0.33:
            return "OVERHEAT", [
                f"RISK_REASON: {self._evi('$.macro.overview.Blow_Off_Phase', bop)} → OVERHEAT",
                f"RISK_REASON: {self._evi('$.macro.overview.SMR', smr_f)} → OVERHEAT"
            ]
        if smr_f >= 0.30:
            return "OVERHEAT", [f"RISK_REASON: {self._evi('$.macro.overview.SMR', smr_f)} → OVERHEAT"]
        if smr_f < 0:
            return "DEFENSIVE", [f"RISK_REASON: {self._evi('$.macro.overview.SMR', smr_f)} → DEFENSIVE"]
        return "NORMAL", [f"RISK_REASON: {self._evi('$.macro.overview.SMR', smr_f)} → NORMAL"]

    def _v17_war_time(self, payload: Dict[str, Any]) -> Tuple[bool, List[str]]:
        meta = payload.get("meta", {}) or {}
        reasons: List[str] = []
        if meta.get("war_time_override", False) is True:
            reasons.append("RISK_REASON: " + self._evi("$.meta.war_time_override", True))
            return True, reasons
        if self._contains(meta.get("current_regime"), "WAR") or self._contains(meta.get("current_regime"), "CRISIS"):
            reasons.append("RISK_REASON: " + self._evi("$.meta.current_regime", meta.get("current_regime")))
            return True, reasons
        return False, reasons

    # =========================================================
    # L2: Execution Arbiter (no signal invention → default NO TRADE)
    # =========================================================
    def _l2_execute(self, payload: Dict[str, Any], l1: Dict[str, Any]) -> str:
        integrity = self._get(payload, ["macro", "integrity"]) or {}
        if integrity.get("kill", False) is True:
            return "\n".join([
                "MODE: L2_EXECUTE",
                "MARKET_STATE: UNKNOWN",
                "MAX_EQUITY_ALLOWED: 0%",
                "DECISION:",
                "NO TRADE",
                "",
                "RISK_REASON: $.macro.integrity.kill=true → rule_triggered",
                "CONFIDENCE: LOW"
            ])

        ov = self._get(payload, ["macro", "overview"]) or {}
        max_eq = ov.get("max_equity_allowed_pct", None)
        if max_eq is None:
            return "\n".join([
                "MODE: L2_EXECUTE",
                "MARKET_STATE: UNKNOWN",
                "MAX_EQUITY_ALLOWED: N/A",
                "DECISION:",
                "NO TRADE",
                "",
                "RISK_REASON: $.macro.overview.max_equity_allowed_pct missing → NO TRADE",
                "CONFIDENCE: LOW"
            ])

        try:
            max_eq_f = float(max_eq)
        except Exception:
            max_eq_f = 0.0

        market_state, ms_reasons = self._market_state(payload)

        # V17 War-Time override
        war, war_reasons = self._v17_war_time(payload)
        if war:
            cap = min(max_eq_f, 0.05)
            lines = [
                "MODE: L2_EXECUTE",
                f"MARKET_STATE: {market_state}",
                f"MAX_EQUITY_ALLOWED: {cap*100:.1f}%",
                "DECISION:",
                "HOLD: []",
                "REDUCE: []",
                "CLOSE: []",
                "NO TRADE",
                "",
            ]
            lines.extend(war_reasons)
            lines.extend(ms_reasons)
            lines.append("CONFIDENCE: LOW")
            return "\n".join(lines)

        meta = payload.get("meta", {}) or {}
        conf_level = meta.get("confidence_level", None)

        low_conf_note = []
        if conf_level == "LOW":
            low_conf_note.append("RISK_REASON: $.meta.confidence_level=LOW → OPEN/ADD allocation ×0.5 (0.5% step)")

        lines = [
            "MODE: L2_EXECUTE",
            f"MARKET_STATE: {market_state}",
            f"MAX_EQUITY_ALLOWED: {max_eq_f*100:.1f}%",
            "DECISION:",
            "NO TRADE",
            "",
        ]
        lines.extend(ms_reasons)
        lines.extend(low_conf_note)
        lines.append("CONFIDENCE: " + ("LOW" if conf_level == "LOW" else "MEDIUM"))
        return "\n".join(lines)

    # =========================================================
    # L3: Stress Arbiter
    # =========================================================
    def _l3_stress(self, payload: Dict[str, Any], l1: Dict[str, Any]) -> Any:
        ov = self._get(payload, ["macro", "overview"]) or {}
        smr = ov.get("SMR", None)
        market_state, ms_reasons = self._market_state(payload)

        activated = False
        triggers: List[str] = []

        if smr is not None:
            try:
                if float(smr) < 0:
                    activated = True
                    triggers.append("TRIGGER: $.macro.overview.SMR<0")
            except Exception:
                pass

        if market_state == "DEFENSIVE":
            activated = True
            triggers.append("TRIGGER: MARKET_STATE=DEFENSIVE")

        perf = self._get(payload, ["portfolio", "performance"])
        if isinstance(perf, dict):
            cl = perf.get("consecutive_losses", None)
            dd = perf.get("drawdown_pct", None)
            if cl is not None and int(cl) >= 3:
                activated = True
                triggers.append("TRIGGER: $.portfolio.performance.consecutive_losses>=3")
            if dd is not None and float(dd) >= 0.08:
                activated = True
                triggers.append("TRIGGER: $.portfolio.performance.drawdown_pct>=0.08")

        if not activated:
            return self._l1_fail_constitution(
                fatal=[
                    "VIOLATION: RUN:L3 but no activation trigger satisfied",
                    "EVIDENCE: " + self._evi("$.macro.overview.SMR", smr),
                    "EVIDENCE: " + self._evi("MARKET_STATE", market_state)
                ],
                structural=ms_reasons
            )

        max_eq = ov.get("max_equity_allowed_pct", 0.0)
        try:
            max_eq_f = float(max_eq)
        except Exception:
            max_eq_f = 0.0

        structural_breach = (l1.get("verdict") != "PASS")

        def grade(loss: float) -> str:
            if max_eq_f <= 0.05:
                return "SAFE" if loss <= 0.10 else "WARNING"
            if max_eq_f <= 0.20:
                return "WARNING" if loss <= 0.10 else "FAILURE"
            return "FAILURE"

        s5 = grade(0.05)
        s10 = grade(0.10)
        s15 = grade(0.15)

        base = 80 if max_eq_f <= 0.05 else (55 if max_eq_f <= 0.20 else 30)
        if structural_breach:
            base -= 20
        base = max(0, min(100, base))

        psycho = "LOW" if base >= 70 else ("MEDIUM" if base >= 45 else "HIGH")
        status = "STABLE" if base >= 70 else ("FRAGILE" if base >= 45 else "CRITICAL")
        final = "SYSTEM_SURVIVES" if base >= 70 else ("SYSTEM_AT_RISK" if base >= 45 else "SYSTEM_FAILURE")

        lines = [
            "MODE: L3_STRESS",
            "STRESS_TEST: ACTIVATED",
            f"STRUCTURAL_BREACH: {'TRUE' if structural_breach else 'FALSE'}",
            f"5%_SCENARIO: {s5}",
            f"10%_SCENARIO: {s10}",
            f"15%_SCENARIO: {s15}",
            f"PSYCHOLOGICAL_RISK: {psycho}",
            f"SURVIVAL_SCORE: {base}",
            f"SYSTEM_STATUS: {status}",
            f"FINAL_VERDICT: {final}",
            "",
        ]
        lines.extend(triggers)
        lines.extend(ms_reasons)
        return "\n".join(lines)
