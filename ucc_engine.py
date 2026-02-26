# ucc_engine.py
# -*- coding: utf-8 -*-
"""
UCC Engine (Stable / Versionless)
- 永遠回傳結構化 dict（避免 UI / workflow 因 DSL 字串或空 list 炸裂）
- 僅使用輸入 payload（JSON only），不外推不補資料
- 保留舊版 ucc_v19_1 核心規則精神：L1 Audit Gate + L2 Capital Weapon sizing + CrashLayer
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math


def _get(d: Dict[str, Any], path: str, default=None):
    cur: Any = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _clamp(lo: float, hi: float, x: float) -> float:
    return max(lo, min(hi, x))


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


@dataclass
class AuditResult:
    mode: str
    verdict: str              # PASS / PARTIAL_PASS / FAIL
    risk_level: str           # LOW / MEDIUM / HIGH / CRITICAL
    fatal_issues: List[str]
    structural_warnings: List[str]
    audit_confidence: str     # HIGH / MEDIUM / LOW


class UCCEngine:
    """
    Stable UCC Engine
    run_mode: "L1" | "L2" | "L3"
    回傳 dict（結構固定）
    """

    def run(self, payload: Dict[str, Any], run_mode: str = "L2") -> Dict[str, Any]:
        run_mode = (run_mode or "L2").upper().strip()

        audit = self.l1_audit(payload)

        if run_mode == "L1":
            return self._pack_l1(audit)

        if run_mode == "L2":
            return self.l2_execute(payload, audit)

        if run_mode == "L3":
            l2 = self.l2_execute(payload, audit) if audit.verdict == "PASS" else self._pack_no_trade("L1 not PASS", audit)
            return self.l3_stress(payload, audit, l2)

        return self._pack_error(f"invalid run_mode={run_mode}", audit)

    # -------------------------
    # L1 — Data Integrity Gate
    # -------------------------
    def l1_audit(self, p: Dict[str, Any]) -> AuditResult:
        fatal: List[str] = []
        warn: List[str] = []

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
        if raw is not None and blend is not None:
            try:
                if float(blend) < float(raw):
                    fatal.append(f"macro.market_amount.amount_total_blended={blend} < amount_total_raw={raw} → FAIL: invalid_blend")
            except Exception:
                warn.append(f"macro.market_amount.amount_total_raw={raw}, amount_total_blended={blend} → WARNING: non_numeric_amount")

        is_prev = _get(p, "meta.is_using_previous_day")
        eff = _get(p, "meta.effective_trade_date")
        if is_prev is True and not eff:
            fatal.append(f"meta.is_using_previous_day={is_prev}, meta.effective_trade_date={eff} → FAIL: missing_effective_trade_date")

        stocks = _get(p, "stocks", [])
        if not isinstance(stocks, list) or len(stocks) == 0:
            fatal.append("stocks=[] → FAIL: empty_universe")
        else:
            prices: List[float] = []
            for i, s in enumerate(stocks):
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

            # 舊版：>=8 檔才啟動 MAD outlier（避免小樣本亂殺）
            if len(prices) >= 8:
                logs = [math.log10(x) for x in prices if x > 0]
                med = sorted(logs)[len(logs) // 2]
                abs_dev = sorted([abs(x - med) for x in logs])
                mad = abs_dev[len(abs_dev) // 2] or 1e-9

                for i, s in enumerate(stocks):
                    price = s.get("Price")
                    if not isinstance(price, (int, float)) or price <= 0:
                        continue
                    z = abs(math.log10(float(price)) - med) / mad
                    if z > 3.5:
                        fatal.append(
                            f"stocks[{i}].Symbol={s.get('Symbol')}, stocks[{i}].Price={price} → FAIL: price_scale_outlier(z~{z:.2f})"
                        )

        dr = _get(p, "macro.overview.daily_return_pct")
        dr_prev = _get(p, "macro.overview.daily_return_pct_prev")
        if dr is None or dr_prev is None:
            warn.append(f"macro.overview.daily_return_pct={dr}, daily_return_pct_prev={dr_prev} → WARNING: missing_crash_inputs(L2 will NO_TRADE)")

        k = _get(p, "system_params.k_regime")
        lam = _get(p, "system_params.lambda_drawdown")
        ml = _get(p, "system_params.max_loss_per_trade_pct")
        if k is None or lam is None or ml is None:
            warn.append(
                f"system_params.k_regime={k}, lambda_drawdown={lam}, max_loss_per_trade_pct={ml} "
                f"→ WARNING: missing_params(L2 will NO_TRADE)"
            )

        if fatal:
            return AuditResult("L1_AUDIT", "FAIL", "CRITICAL", fatal, warn, "HIGH")

        if (dr is None or dr_prev is None) or (k is None or lam is None or ml is None):
            return AuditResult("L1_AUDIT", "PARTIAL_PASS", "HIGH", [], warn, "MEDIUM")

        return AuditResult("L1_AUDIT", "PASS", "MEDIUM", [], warn, "HIGH")

    def _pack_l1(self, audit: AuditResult) -> Dict[str, Any]:
        return {
            "MODE": "L1_AUDIT",
            "VERDICT": audit.verdict,
            "RISK_LEVEL": audit.risk_level,
            "FATAL_ISSUES": list(audit.fatal_issues),
            "WARNINGS": list(audit.structural_warnings),
            "AUDIT_CONFIDENCE": audit.audit_confidence,
            "OPEN": [],
            "ADD": [],
            "HOLD": [],
            "REDUCE": [],
            "CLOSE": [],
            "NO_TRADE": [] if audit.verdict == "PASS" else [{"reason": "L1_NOT_PASS"}],
            "RISK_REASON": "L1_AUDIT_ONLY",
            "CALC_TRACE": {"L1": {"verdict": audit.verdict}},
        }

    # -------------------------
    # L2 — Execute + Sizing
    # -------------------------
    def l2_execute(self, p: Dict[str, Any], audit: AuditResult) -> Dict[str, Any]:
        if audit.verdict != "PASS":
            return self._pack_no_trade("L1 not PASS", audit)

        if _get(p, "macro.integrity.kill") is True:
            return self._pack_no_trade("kill_switch", audit)

        # required (L1 PASS 應該已具備，但仍防禦)
        try:
            smr = float(_get(p, "macro.overview.SMR"))
            vix = float(_get(p, "macro.overview.vix"))
            base_risk = float(_get(p, "macro.overview.max_equity_allowed_pct"))
            k = float(_get(p, "system_params.k_regime"))
            lam = float(_get(p, "system_params.lambda_drawdown"))
            max_loss = float(_get(p, "system_params.max_loss_per_trade_pct"))
            dr = float(_get(p, "macro.overview.daily_return_pct"))
            dr_prev = float(_get(p, "macro.overview.daily_return_pct_prev"))
        except Exception:
            return self._pack_no_trade("missing_required_numeric_fields", audit)

        dd = float(_get(p, "portfolio.drawdown_pct", 0.0))

        # market_state（報告用）
        blowoff = bool(_get(p, "macro.overview.Blow_Off_Phase", False))
        if smr < 0:
            market_state = "DEFENSIVE"
        elif blowoff or smr >= 0.33:
            market_state = "OVERHEAT"
        else:
            market_state = "NORMAL"

        # CrashLayer（沿用舊門檻）
        crash_layer = "SAFE"
        if dr <= -0.06 or (dr <= -0.03 and dr_prev <= -0.03):
            crash_layer = "L2_HALT"
        elif dr <= -0.04:
            crash_layer = "L1_OVERRIDE"

        if crash_layer == "L2_HALT":
            return self._pack_close_all(p, market_state, audit, dr, dr_prev)

        if crash_layer == "L1_OVERRIDE":
            return self._pack_reduce_half(p, market_state, audit, dr)

        # VIX_norm + VolFactor（vix_norm = max(0,(vix-15)/25)）
        vix_norm = max(0.0, (vix - 15.0) / 25.0)
        vol_factor = 1.0 / (1.0 + vix_norm)

        # DrawdownFactor（dd_factor = max(0.2, 1 - lam * dd)）
        dd_factor = max(0.2, 1.0 - (lam * dd))

        # RegimePenalty（clamp(0.6,1.3, 1.1 - k*SMR)）
        regime_penalty = _clamp(0.6, 1.3, 1.1 - k * smr)

        # 總火力（risk_budget = base_risk * vol_factor * dd_factor）
        risk_budget = base_risk * vol_factor * dd_factor

        # weights（沿用舊 adaptive）
        macro_acc = float(_get(p, "macro.overview.Acceleration", 0.0))
        weights, w_reason = self._adaptive_weights(smr, vix, macro_acc)

        stocks = _get(p, "stocks", [])
        if not isinstance(stocks, list) or not stocks:
            return self._pack_no_trade("empty_universe", audit)

        scored: List[Dict[str, Any]] = []
        warnings: List[str] = []

        for i, s in enumerate(stocks):
            sym = s.get("Symbol") or s.get("symbol")
            name = s.get("Name") or s.get("name")
            price = s.get("Price") if "Price" in s else s.get("price")

            stop = _get(s, "risk.stop_distance_pct")
            if stop is None and isinstance(s.get("risk"), dict):
                stop = s["risk"].get("stop_distance_pct")
            if stop is None:
                stop = _get(s, "Risk.stop_distance_pct")  # 容錯

            if stop is None or not isinstance(stop, (int, float)) or stop <= 0:
                warnings.append(f"stocks[{i}].risk.stop_distance_pct={stop} → forbid OPEN/ADD")
                continue

            edge, evidence = self._edge_score_for_stock(s, weights)
            adj_edge = min(100.0, edge * regime_penalty)
            if adj_edge <= 0:
                continue

            ev_cap = max_loss / float(stop)

            scored.append({
                "symbol": sym,
                "name": name,
                "price": price,
                "edge": float(edge),
                "adj_edge": float(adj_edge),
                "stop_distance_pct": float(stop),
                "ev_cap": float(ev_cap),
                "evidence": evidence,
            })

        if not scored:
            return self._pack_no_trade("no_eligible_stocks(stop_distance_missing_or_edge<=0)", audit, extra_warnings=warnings)

        sum_adj = sum(x["adj_edge"] for x in scored) or 1e-9
        opens: List[Dict[str, Any]] = []
        used = 0.0

        for x in sorted(scored, key=lambda z: z["adj_edge"], reverse=True):
            raw_alloc = risk_budget * (x["adj_edge"] / sum_adj)
            alloc = min(raw_alloc, x["ev_cap"], max(0.0, base_risk - used))
            if alloc <= 0:
                continue

            used += alloc
            opens.append({
                "symbol": x["symbol"],
                "name": x["name"],
                "allocation": alloc,
                "allocation_pct": _pct(alloc),
                "stop_distance_pct": x["stop_distance_pct"],
                "ev_cap": x["ev_cap"],
                "price": x["price"],
                "evidence": x["evidence"][:8],  # 避免 UI 太長
            })

            if used >= base_risk:
                break

        if not opens:
            return self._pack_no_trade("allocation_all_zero", audit, extra_warnings=warnings)

        conf = _get(p, "meta.confidence_level", "LOW")

        # 統一結構輸出（避免 UI index error）
        return {
            "MODE": "L2_EXECUTE",
            "MARKET_STATE": market_state,
            "DECISION": "OPEN" if opens else "HOLD",
            "OPEN": opens,
            "ADD": [],
            "HOLD": [],
            "REDUCE": [],
            "CLOSE": [],
            "NO_TRADE": [],
            "RISK_REASON": [
                f"macro.overview.max_equity_allowed_pct={base_risk} → BaseRisk",
                f"macro.overview.vix={vix} → vix_norm={vix_norm:.4f} → VolatilityFactor={vol_factor:.4f}",
                f"portfolio.drawdown_pct={dd} → lambda_drawdown={lam} → DrawdownFactor={dd_factor:.4f}",
                f"macro.overview.SMR={smr} → k_regime={k} → RegimePenalty={regime_penalty:.4f}",
                f"system_params.max_loss_per_trade_pct={max_loss} → EVCap=max_loss/stop_distance enforced",
                f"AdaptiveWeights={weights} ({w_reason})",
            ] + [f"WARNING: {w}" for w in warnings[:10]],
            "CALC_TRACE": {
                "BaseRisk": base_risk,
                "vix": vix,
                "vix_norm": vix_norm,
                "VolatilityFactor": vol_factor,
                "drawdown_pct": dd,
                "lambda_drawdown": lam,
                "DrawdownFactor": dd_factor,
                "SMR": smr,
                "k_regime": k,
                "RegimePenalty": regime_penalty,
                "CrashLayer": crash_layer,
                "risk_budget": risk_budget,
                "weights": weights,
                "weights_reason": w_reason,
                "used_allocation": used,
            },
            "CONFIDENCE": conf,
            "AUDIT": {
                "verdict": audit.verdict,
                "fatal_issues": audit.fatal_issues[:20],
                "warnings": audit.structural_warnings[:20],
            },
        }

    def _adaptive_weights(self, smr: float, vix: float, macro_acc: float) -> Tuple[Dict[str, float], str]:
        if vix > 25:
            return ({"inst": 0.6, "div": 0.3, "acc": 0.1, "slope": 0.0}, "vix>25 → high_volatility_regime")
        if smr > 0.15 and macro_acc > 0:
            return ({"inst": 0.2, "div": 0.1, "acc": 0.4, "slope": 0.3}, "SMR>0.15 & macro.Acceleration>0 → strong_trend_regime")
        return ({"inst": 0.4, "div": 0.4, "acc": 0.2, "slope": 0.0}, "default → chop/divergence_regime")

    def _edge_score_for_stock(self, s: Dict[str, Any], w: Dict[str, float]) -> Tuple[float, List[str]]:
        evid: List[str] = []

        inst = _get(s, "Institutional.Inst_Net_3d")
        if inst is None and isinstance(s.get("institutional"), dict):
            inst = s["institutional"].get("inst_net_3d")
        inst_score = 0.0
        if isinstance(inst, (int, float)):
            inst_score = 100.0 if inst > 0 else (0.0 if inst < 0 else 50.0)
            evid.append(f"Institutional.Inst_Net_3d={inst} → inst_score={inst_score:.0f}")
        else:
            evid.append(f"Institutional.Inst_Net_3d={inst} → inst_score=NA")

        acc = s.get("Acceleration")
        if acc is None and isinstance(s.get("signals"), dict):
            acc = s["signals"].get("acceleration")
        acc_score = 50.0
        if isinstance(acc, (int, float)):
            acc_score = 100.0 if acc > 0 else (0.0 if acc < 0 else 50.0)
            evid.append(f"Acceleration={acc} → acc_score={acc_score:.0f}")
        else:
            evid.append(f"Acceleration={acc} → acc_score=NA")

        slope = s.get("Slope5")
        if slope is None and isinstance(s.get("signals"), dict):
            slope = s["signals"].get("slope5")
        slope_score = 50.0
        if isinstance(slope, (int, float)):
            slope_score = 100.0 if slope > 0 else (0.0 if slope < 0 else 50.0)
            evid.append(f"Slope5={slope} → slope_score={slope_score:.0f}")
        else:
            evid.append(f"Slope5={slope} → slope_score=NA")

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

        available = {
            "inst": isinstance(inst, (int, float)),
            "acc": isinstance(acc, (int, float)),
            "slope": isinstance(slope, (int, float)),
            "div": True,
        }

        w_eff = dict(w)
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
        return float(edge), evid

    # -------------------------
    # L2 helpers
    # -------------------------
    def _pack_no_trade(self, reason: str, audit: AuditResult, extra_warnings: Optional[List[str]] = None) -> Dict[str, Any]:
        extra_warnings = extra_warnings or []
        return {
            "MODE": "L2_EXECUTE",
            "MARKET_STATE": "UNKNOWN",
            "DECISION": "NO_TRADE",
            "OPEN": [],
            "ADD": [],
            "HOLD": [],
            "REDUCE": [],
            "CLOSE": [],
            "NO_TRADE": [{"reason": reason}],
            "RISK_REASON": [reason] + audit.fatal_issues[:10] + audit.structural_warnings[:10] + extra_warnings[:10],
            "CALC_TRACE": {"gate": reason, "L1_verdict": audit.verdict},
            "CONFIDENCE": "LOW",
            "AUDIT": {
                "verdict": audit.verdict,
                "fatal_issues": audit.fatal_issues[:20],
                "warnings": audit.structural_warnings[:20],
            },
        }

    def _pack_close_all(self, p: Dict[str, Any], market_state: str, audit: AuditResult, dr: float, dr_prev: float) -> Dict[str, Any]:
        conf = _get(p, "meta.confidence_level", "LOW")
        return {
            "MODE": "L2_EXECUTE",
            "MARKET_STATE": market_state,
            "DECISION": "CLOSE",
            "OPEN": [],
            "ADD": [],
            "HOLD": [],
            "REDUCE": [],
            "CLOSE": [{"scope": "ALL", "ratio": 1.0, "reason": "CascadeCrashProtection L2_HALT"}],
            "NO_TRADE": [{"reason": "NEW_OPEN_DISABLED"}],
            "RISK_REASON": [f"daily_return_pct={dr}, daily_return_pct_prev={dr_prev} → CrashLayer=L2_HALT"],
            "CALC_TRACE": {"CrashLayer": "L2_HALT", "daily_return_pct": dr, "daily_return_pct_prev": dr_prev},
            "CONFIDENCE": conf,
            "AUDIT": {
                "verdict": audit.verdict,
                "fatal_issues": audit.fatal_issues[:20],
                "warnings": audit.structural_warnings[:20],
            },
        }

    def _pack_reduce_half(self, p: Dict[str, Any], market_state: str, audit: AuditResult, dr: float) -> Dict[str, Any]:
        conf = _get(p, "meta.confidence_level", "LOW")
        return {
            "MODE": "L2_EXECUTE",
            "MARKET_STATE": market_state,
            "DECISION": "REDUCE",
            "OPEN": [],
            "ADD": [],
            "HOLD": [],
            "REDUCE": [{"scope": "ALL", "ratio": 0.5, "reason": "CascadeCrashProtection L1_OVERRIDE"}],
            "CLOSE": [],
            "NO_TRADE": [{"reason": "NEW_OPEN_DISABLED"}],
            "RISK_REASON": [f"daily_return_pct={dr} → CrashLayer=L1_OVERRIDE → REDUCE_50%, NO_OPEN"],
            "CALC_TRACE": {"CrashLayer": "L1_OVERRIDE", "daily_return_pct": dr},
            "CONFIDENCE": conf,
            "AUDIT": {
                "verdict": audit.verdict,
                "fatal_issues": audit.fatal_issues[:20],
                "warnings": audit.structural_warnings[:20],
            },
        }

    # -------------------------
    # L3 (skeleton): 先給穩定輸出
    # -------------------------
    def l3_stress(self, p: Dict[str, Any], audit: AuditResult, l2_output: Dict[str, Any]) -> Dict[str, Any]:
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
            return {
                "MODE": "L3_STRESS",
                "STRESS_TEST": "NOT_ACTIVATED",
                "DECISION": l2_output.get("DECISION", "NO_TRADE"),
                "DETAIL": {},
            }

        survival = 100
        if isinstance(dd, (int, float)):
            survival = max(0, int(100 - dd * 250))

        system_status = "STABLE" if survival >= 70 else ("FRAGILE" if survival >= 40 else "CRITICAL")
        final_verdict = "SYSTEM_SURVIVES" if survival >= 70 else ("SYSTEM_AT_RISK" if survival >= 40 else "SYSTEM_FAILURE")

        return {
            "MODE": "L3_STRESS",
            "STRESS_TEST": "ACTIVATED",
            "SURVIVAL_SCORE": survival,
            "SYSTEM_STATUS": system_status,
            "FINAL_VERDICT": final_verdict,
            "INPUTS": {
                "SMR": smr,
                "drawdown_pct": dd,
                "stress_drawdown_trigger": trig,
                "loss_streak": loss_streak,
                "L1_verdict": audit.verdict,
            },
        }

    def _pack_error(self, msg: str, audit: AuditResult) -> Dict[str, Any]:
        return {
            "MODE": "ENGINE_ERROR",
            "DECISION": "NO_TRADE",
            "OPEN": [],
            "ADD": [],
            "HOLD": [],
            "REDUCE": [],
            "CLOSE": [],
            "NO_TRADE": [{"reason": msg}],
            "RISK_REASON": [msg],
            "CALC_TRACE": {"error": msg},
            "CONFIDENCE": "LOW",
            "AUDIT": {
                "verdict": audit.verdict,
                "fatal_issues": audit.fatal_issues[:20],
                "warnings": audit.structural_warnings[:20],
            },
        }
