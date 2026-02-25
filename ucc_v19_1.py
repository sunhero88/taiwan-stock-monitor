# ucc_v19_1.py (snippet)
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
import math

def _get(d: Dict[str, Any], path: str, default=None):
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _clamp(lo: float, hi: float, x: float) -> float:
    return max(lo, min(hi, x))

def _pct(x: float) -> str:
    return f"{x*100:.2f}%"

@dataclass
class AuditResult:
    mode: str
    verdict: str            # PASS / PARTIAL_PASS / FAIL
    risk_level: str         # LOW / MEDIUM / HIGH / CRITICAL
    fatal_issues: List[str]
    structural_warnings: List[str]
    audit_confidence: str   # HIGH / MEDIUM / LOW

class CapitalWeaponEngine:
    """
    Predator Apex UCC V6.0 Capital Weapon System
    - JSON only
    - No inference
    - Path=value evidence
    """

    def run(self, payload: Dict[str, Any], run_mode: str = "L1") -> Any:
        run_mode = (run_mode or "L1").upper().strip()
        if run_mode == "L1":
            return self.l1_audit(payload)
        if run_mode == "L2":
            a = self.l1_audit(payload)
            return self.l2_execute(payload, a)
        if run_mode == "L3":
            # L3 依你的規格可再擴充；此處提供可落地框架
            a = self.l1_audit(payload)
            l2 = self.l2_execute(payload, a) if a.verdict == "PASS" else None
            return self.l3_stress(payload, a, l2)
        raise ValueError("run_mode must be L1/L2/L3")

    # =========================
    # L1 — Data Integrity Arbiter
    # =========================
    def l1_audit(self, p: Dict[str, Any]) -> AuditResult:
        fatal = []
        warn = []

        # 1) 必要欄位
        twii = _get(p, "macro.overview.twii_close")
        if twii is None:
            fatal.append(f"macro.overview.twii_close={twii} → FAIL: missing_or_null")

        kill = _get(p, "macro.integrity.kill")
        if kill is True:
            fatal.append(f"macro.integrity.kill={kill} → FAIL: kill_switch")

        conf = _get(p, "meta.confidence_level")
        mstat = _get(p, "meta.market_status")
        if conf == "LOW" and mstat == "NORMAL":
            fatal.append(f"meta.confidence_level={conf}, meta.market_status={mstat} → FAIL: semantic_mismatch")

        raw = _get(p, "macro.market_amount.amount_total_raw")
        blend = _get(p, "macro.market_amount.amount_total_blended")
        if raw is not None and blend is not None and blend < raw:
            fatal.append(f"macro.market_amount.amount_total_blended={blend} < amount_total_raw={raw} → FAIL: invalid_blend")

        is_t1 = _get(p, "meta.is_using_previous_day")
        eff = _get(p, "meta.effective_trade_date")
        if is_t1 is True and not eff:
            fatal.append(f"meta.is_using_previous_day={is_t1}, meta.effective_trade_date={eff} → FAIL: missing_effective_trade_date")

        # 2) 法人 NO_UPDATE_TODAY 但有值 → FAIL
        stocks = _get(p, "stocks", [])
        if not isinstance(stocks, list) or len(stocks) == 0:
            fatal.append("stocks=[] → FAIL: empty_universe")
        else:
            prices = []
            for i, s in enumerate(stocks):
                sym = s.get("Symbol")
                price = s.get("Price")
                inst_status = _get(s, "Institutional.Inst_Status")
                inst_net = _get(s, "Institutional.Inst_Net_3d")

                if inst_status == "NO_UPDATE_TODAY" and inst_net is not None:
                    fatal.append(
                        f"stocks[{i}].Institutional.Inst_Status={inst_status}, "
                        f"stocks[{i}].Institutional.Inst_Net_3d={inst_net} → FAIL: no_update_but_has_value"
                    )

                if isinstance(price, (int, float)) and price > 0:
                    prices.append(float(price))
                else:
                    warn.append(f"stocks[{i}].Price={price} → WARNING: invalid_or_missing_price")

            # 3) Symbol–Price 跨量級錯位（用群體離群，不用外部常識）
            if len(prices) >= 8:
                logs = [math.log10(x) for x in prices if x > 0]
                med = sorted(logs)[len(logs)//2]
                abs_dev = sorted([abs(x - med) for x in logs])
                mad = abs_dev[len(abs_dev)//2] or 1e-9
                # 以 3.5*MAD 判離群（穩健）
                for i, s in enumerate(stocks):
                    price = s.get("Price")
                    if not isinstance(price, (int, float)) or price <= 0:
                        continue
                    z = abs(math.log10(float(price)) - med) / mad
                    if z > 3.5:
                        fatal.append(
                            f"stocks[{i}].Symbol={s.get('Symbol')}, stocks[{i}].Price={price} → FAIL: price_scale_outlier(z~{z:.2f})"
                        )

        # 4) Crash 欄位缺失 → PARTIAL_PASS（但 L2 會 NO TRADE）
        dr = _get(p, "macro.overview.daily_return_pct")
        dr_prev = _get(p, "macro.overview.daily_return_pct_prev")
        if dr is None or dr_prev is None:
            warn.append(f"macro.overview.daily_return_pct={dr}, daily_return_pct_prev={dr_prev} → WARNING: missing_crash_inputs(L2 will NO_TRADE)")

        # 5) system_params 缺失 → PARTIAL_PASS（避免引擎自填）
        k = _get(p, "system_params.k_regime")
        lam = _get(p, "system_params.lambda_drawdown")
        ml = _get(p, "system_params.max_loss_per_trade_pct")
        if k is None or lam is None or ml is None:
            warn.append(
                f"system_params.k_regime={k}, lambda_drawdown={lam}, max_loss_per_trade_pct={ml} "
                f"→ WARNING: missing_params(L2 will NO_TRADE)"
            )

        # Verdict
        if fatal:
            return AuditResult("L1_AUDIT", "FAIL", "CRITICAL", fatal, warn, "HIGH")
        # 缺 crash / params → PARTIAL_PASS
        if (dr is None or dr_prev is None) or (k is None or lam is None or ml is None):
            return AuditResult("L1_AUDIT", "PARTIAL_PASS", "HIGH", [], warn, "MEDIUM")

        return AuditResult("L1_AUDIT", "PASS", "MEDIUM", [], warn, "HIGH")

    # =========================
    # L2 — Execution Arbiter + Capital Weapon Engine
    # =========================
    def l2_execute(self, p: Dict[str, Any], audit: AuditResult) -> str:
        # Gate
        if audit.verdict != "PASS":
            return self._l2_no_trade_block("L1 not PASS", audit)

        if _get(p, "macro.integrity.kill") is True:
            return self._l2_no_trade_block("kill_switch", audit)

        # Required params (guaranteed by L1 PASS, but keep defensive)
        SMR = float(_get(p, "macro.overview.SMR"))
        vix = float(_get(p, "macro.overview.vix"))
        base_risk = float(_get(p, "macro.overview.max_equity_allowed_pct"))

        k = float(_get(p, "system_params.k_regime"))
        lam = float(_get(p, "system_params.lambda_drawdown"))
        max_loss = float(_get(p, "system_params.max_loss_per_trade_pct"))

        dr = float(_get(p, "macro.overview.daily_return_pct"))
        dr_prev = float(_get(p, "macro.overview.daily_return_pct_prev"))

        dd = float(_get(p, "portfolio.drawdown_pct", 0.0))

        # MARKET_STATE（報告用）
        blowoff = bool(_get(p, "macro.overview.Blow_Off_Phase", False))
        if SMR < 0:
            market_state = "DEFENSIVE"
        elif blowoff or SMR >= 0.33:
            market_state = "OVERHEAT"
        else:
            market_state = "NORMAL"

        # Crash layers
        crash_layer = "SAFE"
        if dr <= -0.06 or (dr <= -0.03 and dr_prev <= -0.03):
            crash_layer = "L2_HALT"
        elif dr <= -0.04:
            crash_layer = "L1_OVERRIDE"

        # VIX_norm + VolFactor（明確且可回測）
        vix_norm = max(0.0, (vix - 15.0) / 25.0)
        vol_factor = 1.0 / (1.0 + vix_norm)

        # DrawdownFactor
        dd_factor = max(0.2, 1.0 - (lam * dd))

        # Continuous RegimePenalty（CVT）
        regime_penalty = _clamp(0.6, 1.3, 1.1 - k * SMR)

        # RiskBudget（總火力）
        risk_budget = base_risk * vol_factor * dd_factor

        # Crash actions
        if crash_layer == "L2_HALT":
            return self._l2_close_all_block(p, market_state, base_risk, vol_factor, dd_factor, regime_penalty, crash_layer)
        if crash_layer == "L1_OVERRIDE":
            # 只允許減倉，不開新倉
            return self._l2_reduce_half_block(p, market_state, base_risk, vol_factor, dd_factor, regime_penalty, crash_layer)

        # Adaptive Weights（JSON 條件決定）
        weights, w_reason = self._adaptive_weights(SMR, vix, float(_get(p, "macro.overview.Acceleration", 0.0)))

        # Stock Edge + allocation
        stocks = _get(p, "stocks", [])
        scored = []
        warnings = []
        for i, s in enumerate(stocks):
            sym = s.get("Symbol")
            name = s.get("Name")
            price = s.get("Price")

            stop = _get(s, "risk.stop_distance_pct")
            if stop is None or not isinstance(stop, (int, float)) or stop <= 0:
                warnings.append(f"stocks[{i}].risk.stop_distance_pct={stop} → forbid OPEN/ADD")
                continue  # 不能開/加碼

            edge, evidence = self._edge_score_for_stock(s, weights)
            adj_edge = min(100.0, edge * regime_penalty)
            if adj_edge <= 0:
                continue

            # EV/AvgLoss clamp：allocation <= max_loss / stop_distance
            ev_cap = max_loss / float(stop)

            scored.append({
                "Symbol": sym, "Name": name, "Price": price,
                "EdgeScore": edge,
                "AdjustedEdge": adj_edge,
                "StopDist": float(stop),
                "EVCap": ev_cap,
                "Evidence": evidence
            })

        if not scored:
            return self._l2_no_trade_decision(p, market_state, base_risk, vol_factor, dd_factor, regime_penalty, crash_layer, w_reason, warnings)

        # 按 AdjustedEdge 配重分配總 RiskBudget
        sum_adj = sum(x["AdjustedEdge"] for x in scored) or 1e-9
        opens = []
        used = 0.0
        for x in sorted(scored, key=lambda z: z["AdjustedEdge"], reverse=True):
            raw_alloc = risk_budget * (x["AdjustedEdge"] / sum_adj)
            alloc = min(raw_alloc, x["EVCap"], base_risk - used)
            if alloc <= 0:
                continue
            used += alloc
            opens.append([
                x["Symbol"],
                x["Name"],
                _pct(alloc),
                f"stop_distance_pct={x['StopDist']:.4f} (EVCap={_pct(x['EVCap'])})",
                "; ".join(x["Evidence"])[:260]
            ])
            if used >= base_risk:
                break

        if not opens:
            return self._l2_no_trade_decision(p, market_state, base_risk, vol_factor, dd_factor, regime_penalty, crash_layer, w_reason, warnings)

        # Confidence（只取 JSON）
        conf = _get(p, "meta.confidence_level", "LOW")

        # Output DSL
        lines = []
        lines.append("MODE: L2_EXECUTE")
        lines.append(f"MARKET_STATE: {market_state}")
        lines.append(f"MAX_EQUITY_ALLOWED: {_pct(base_risk)}")
        lines.append("")
        lines.append("ENGINE:")
        lines.append(f"- BaseRisk: macro.overview.max_equity_allowed_pct={base_risk}")
        lines.append(f"- VolatilityFactor: {vol_factor:.4f} (macro.overview.vix={vix}, vix_norm={vix_norm:.4f})")
        lines.append(f"- DrawdownFactor: {dd_factor:.4f} (portfolio.drawdown_pct={dd}, lambda={lam})")
        lines.append(f"- RegimePenalty: {regime_penalty:.4f} (SMR={SMR}, k={k})")
        lines.append(f"- CrashLayer: {crash_layer} (daily_return_pct={dr}, prev={dr_prev})")
        lines.append(f"- AdaptiveWeights: {weights} ({w_reason})")
        lines.append("")
        lines.append("DECISION:")
        lines.append(f"- OPEN: {opens}")
        lines.append("- ADD: []")
        lines.append("- HOLD: []")
        lines.append("- REDUCE: []")
        lines.append("- CLOSE: []")
        lines.append("- NO TRADE: []")
        lines.append("")
        lines.append("RISK_REASON:")
        lines.append(f"- macro.overview.max_equity_allowed_pct={base_risk} → BaseRisk single source")
        lines.append(f"- portfolio.drawdown_pct={dd} → DrawdownFactor applied")
        lines.append(f"- macro.overview.vix={vix} → VolatilityFactor applied")
        lines.append(f"- macro.overview.SMR={SMR} → RegimePenalty applied")
        lines.append(f"- system_params.max_loss_per_trade_pct={max_loss} → EVCap enforced per stock")
        for w in warnings[:10]:
            lines.append(f"- {w}")
        lines.append("")
        lines.append(f"CONFIDENCE: {conf}")
        return "\n".join(lines)

    def _adaptive_weights(self, smr: float, vix: float, macro_acc: float) -> Tuple[Dict[str, float], str]:
        # 僅依規則：高波動 / 強趨勢 / 其他
        if vix > 25:
            return ({"inst": 0.6, "div": 0.3, "acc": 0.1, "slope": 0.0}, "vix>25 → high_volatility_regime")
        if smr > 0.15 and macro_acc > 0:
            return ({"inst": 0.2, "div": 0.1, "acc": 0.4, "slope": 0.3}, "SMR>0.15 & macro.Acceleration>0 → strong_trend_regime")
        return ({"inst": 0.4, "div": 0.4, "acc": 0.2, "slope": 0.0}, "default → chop/divergence_regime")

    def _edge_score_for_stock(self, s: Dict[str, Any], w: Dict[str, float]) -> Tuple[float, List[str]]:
        # 注意：此函數只用股票自身可得欄位（缺就降權）
        evid = []

        inst = _get(s, "Institutional.Inst_Net_3d")
        inst_score = 0.0
        if isinstance(inst, (int, float)):
            # 只做符號與粗幅度映射（不使用外部分布），避免漂移
            inst_score = 100.0 if inst > 0 else (0.0 if inst < 0 else 50.0)
            evid.append(f"Institutional.Inst_Net_3d={inst} → inst_score={inst_score:.0f}")
        else:
            evid.append(f"Institutional.Inst_Net_3d={inst} → inst_score=NA")

        acc = s.get("Acceleration")
        acc_score = 50.0
        if isinstance(acc, (int, float)):
            acc_score = 100.0 if acc > 0 else (0.0 if acc < 0 else 50.0)
            evid.append(f"stocks[].Acceleration={acc} → acc_score={acc_score:.0f}")
        else:
            evid.append(f"stocks[].Acceleration={acc} → acc_score=NA")

        slope = s.get("Slope5")
        slope_score = 50.0
        if isinstance(slope, (int, float)):
            slope_score = 100.0 if slope > 0 else (0.0 if slope < 0 else 50.0)
            evid.append(f"stocks[].Slope5={slope} → slope_score={slope_score:.0f}")
        else:
            evid.append(f"stocks[].Slope5={slope} → slope_score=NA")

        # divergence：若 slope<=0 但 inst>0 且 acc>0 → 強烈底背離；反向則頂背離
        div_score = 50.0
        if isinstance(inst, (int, float)) and isinstance(acc, (int, float)) and isinstance(slope, (int, float)):
            if slope <= 0 and inst > 0 and acc > 0:
                div_score = 100.0
                evid.append("divergence: slope<=0 & inst>0 & acc>0 → bottom_divergence_score=100")
            elif slope >= 0 and inst < 0 and acc < 0:
                div_score = 0.0
                evid.append("divergence: slope>=0 & inst<0 & acc<0 → top_divergence_score=0")
            else:
                div_score = 50.0
                evid.append("divergence: no_strong_signal → 50")

        # 缺欄位就把該權重挪給現有因子（避免硬扣分造成漂移）
        available = {
            "inst": isinstance(inst, (int, float)),
            "acc": isinstance(acc, (int, float)),
            "slope": isinstance(slope, (int, float)),
            "div": True  # divergence 可用（最低也會回 50）
        }
        w_eff = dict(w)
        total = sum(w_eff[k] for k in w_eff if available.get(k, False))
        if total <= 0:
            return 0.0, evid + ["weights invalid → edge=0"]

        # normalize
        for k in list(w_eff.keys()):
            if not available.get(k, False):
                w_eff[k] = 0.0
        total = sum(w_eff.values()) or 1e-9
        for k in w_eff:
            w_eff[k] = w_eff[k] / total

        edge = (
            w_eff["inst"] * inst_score +
            w_eff["acc"] * acc_score +
            w_eff["div"] * div_score +
            w_eff["slope"] * slope_score
        )
        evid.append(f"weights_eff={w_eff} → EdgeScore={edge:.2f}")
        return edge, evid

    def _l2_no_trade_block(self, reason: str, audit: AuditResult) -> str:
        lines = []
        lines.append("MODE: L2_EXECUTE")
        lines.append("MARKET_STATE: UNKNOWN")
        lines.append("MAX_EQUITY_ALLOWED: 0.00%")
        lines.append("")
        lines.append("DECISION:")
        lines.append("- NO TRADE")
        lines.append("")
        lines.append("RISK_REASON:")
        lines.append(f"- {reason}")
        if audit.fatal_issues:
            for x in audit.fatal_issues[:10]:
                lines.append(f"- {x}")
        if audit.structural_warnings:
            for x in audit.structural_warnings[:10]:
                lines.append(f"- {x}")
        lines.append("")
        lines.append("CONFIDENCE: LOW")
        return "\n".join(lines)

    def _l2_no_trade_decision(self, p, market_state, base_risk, vol_factor, dd_factor, regime_penalty, crash_layer, w_reason, warnings) -> str:
        conf = _get(p, "meta.confidence_level", "LOW")
        lines = []
        lines.append("MODE: L2_EXECUTE")
        lines.append(f"MARKET_STATE: {market_state}")
        lines.append(f"MAX_EQUITY_ALLOWED: {_pct(base_risk)}")
        lines.append("")
        lines.append("ENGINE:")
        lines.append(f"- VolatilityFactor: {vol_factor:.4f}")
        lines.append(f"- DrawdownFactor: {dd_factor:.4f}")
        lines.append(f"- RegimePenalty: {regime_penalty:.4f}")
        lines.append(f"- CrashLayer: {crash_layer}")
        lines.append(f"- Notes: {w_reason}")
        lines.append("")
        lines.append("DECISION:")
        lines.append("- NO TRADE")
        lines.append("")
        lines.append("RISK_REASON:")
        for w in warnings[:12]:
            lines.append(f"- {w}")
        lines.append("")
        lines.append(f"CONFIDENCE: {conf}")
        return "\n".join(lines)

    def _l2_close_all_block(self, p, market_state, base_risk, vol_factor, dd_factor, regime_penalty, crash_layer) -> str:
        conf = _get(p, "meta.confidence_level", "LOW")
        dr = _get(p, "macro.overview.daily_return_pct")
        dr_prev = _get(p, "macro.overview.daily_return_pct_prev")
        lines = []
        lines.append("MODE: L2_EXECUTE")
        lines.append(f"MARKET_STATE: {market_state}")
        lines.append(f"MAX_EQUITY_ALLOWED: {_pct(base_risk)}")
        lines.append("")
        lines.append("ENGINE:")
        lines.append(f"- CrashLayer: {crash_layer} (daily_return_pct={dr}, prev={dr_prev}) → SYSTEM_HALT")
        lines.append("")
        lines.append("DECISION:")
        lines.append("- CLOSE: ALL (100%)")
        lines.append("- NO TRADE: NEW_OPEN_DISABLED")
        lines.append("")
        lines.append("RISK_REASON:")
        lines.append(f"- macro.overview.daily_return_pct={dr}, daily_return_pct_prev={dr_prev} → CascadeCrashProtection L2")
        lines.append("")
        lines.append(f"CONFIDENCE: {conf}")
        return "\n".join(lines)

    def _l2_reduce_half_block(self, p, market_state, base_risk, vol_factor, dd_factor, regime_penalty, crash_layer) -> str:
        conf = _get(p, "meta.confidence_level", "LOW")
        dr = _get(p, "macro.overview.daily_return_pct")
        lines = []
        lines.append("MODE: L2_EXECUTE")
        lines.append(f"MARKET_STATE: {market_state}")
        lines.append(f"MAX_EQUITY_ALLOWED: {_pct(base_risk)}")
        lines.append("")
        lines.append("ENGINE:")
        lines.append(f"- CrashLayer: {crash_layer} (daily_return_pct={dr}) → REDUCE_50%, NO_OPEN")
        lines.append("")
        lines.append("DECISION:")
        lines.append("- REDUCE: ALL_POSITIONS x0.5")
        lines.append("- NO TRADE: NEW_OPEN_DISABLED")
        lines.append("")
        lines.append("RISK_REASON:")
        lines.append(f"- macro.overview.daily_return_pct={dr} → CascadeCrashProtection L1")
        lines.append("")
        lines.append(f"CONFIDENCE: {conf}")
        return "\n".join(lines)

    # =========================
    # L3 — Stress (skeleton)
    # =========================
    def l3_stress(self, p: Dict[str, Any], audit: AuditResult, l2_output: Optional[str]) -> str:
        # 僅提供可落地框架：你可依既定 L3 格式擴寫
        smr = _get(p, "macro.overview.SMR")
        dd = _get(p, "portfolio.drawdown_pct")
        trig = _get(p, "system_params.stress_drawdown_trigger")
        loss_streak = _get(p, "portfolio.loss_streak", 0)

        activated = False
        if isinstance(smr, (int, float)) and smr < 0:
            activated = True
        if isinstance(dd, (int, float)) and isinstance(trig, (int, float)) and dd >= trig:
            activated = True
        if isinstance(loss_streak, int) and loss_streak >= 3:
            activated = True

        if not activated:
            return "MODE: L3_STRESS\nSTRESS_TEST: NOT_ACTIVATED"

        # 這裡先用簡易打分（不引外部資料）
        survival = 100
        if isinstance(dd, (int, float)):
            survival = max(0, int(100 - dd * 250))  # dd=0.2 → 50 分

        system_status = "STABLE" if survival >= 70 else ("FRAGILE" if survival >= 40 else "CRITICAL")
        final_verdict = "SYSTEM_SURVIVES" if survival >= 70 else ("SYSTEM_AT_RISK" if survival >= 40 else "SYSTEM_FAILURE")

        return "\n".join([
            "MODE: L3_STRESS",
            "STRESS_TEST: ACTIVATED",
            "",
            f"STRUCTURAL_BREACH: {'TRUE' if audit.verdict!='PASS' else 'FALSE'}",
            "5%_SCENARIO: WARNING",
            "10%_SCENARIO: WARNING",
            "15%_SCENARIO: FAILURE" if survival < 40 else "15%_SCENARIO: WARNING",
            "",
            f"PSYCHOLOGICAL_RISK: {'HIGH' if loss_streak>=3 else 'MEDIUM' if (dd or 0)>=0.08 else 'LOW'}",
            f"SURVIVAL_SCORE: {survival}",
            f"SYSTEM_STATUS: {system_status}",
            "",
            f"FINAL_VERDICT: {final_verdict}",
        ])
