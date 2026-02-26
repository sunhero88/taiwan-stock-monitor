# verify_integrity.py
# Predator Apex - V20.x Data Integrity Gate (L1)
# - NO-DRIFT friendly
# - Deterministic / Auditable output
# - Optional: Kronos Exogenous Gate (V20.4)
#
# Usage:
#   python verify_integrity.py --json macro.json
#   python verify_integrity.py --json snapshot_tw.json --snapshot
#
# Exit code:
#   0 = PASS
#   2 = FAIL

import json
import os
import sys
import argparse
from typing import Any, Dict, List, Optional, Tuple


# =========================
# Helpers (safe get + format)
# =========================
def jget(d: Dict[str, Any], path: str, default=None):
    """
    path like: "macro.overview.twii_close"
    """
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def is_null(x) -> bool:
    return x is None


def to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(",", "").strip()
        if s in ("", "None", "null", "nan", "--"):
            return None
        return float(s)
    except:
        return None


def to_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = str(x).replace(",", "").strip()
        if s in ("", "None", "null", "nan", "--"):
            return None
        return int(float(s))
    except:
        return None


def median(nums: List[float]) -> Optional[float]:
    if not nums:
        return None
    a = sorted(nums)
    n = len(a)
    mid = n // 2
    if n % 2 == 1:
        return a[mid]
    return (a[mid - 1] + a[mid]) / 2.0


def ensure_list(x) -> List[Any]:
    return x if isinstance(x, list) else []


def path_kv(path: str, value: Any) -> str:
    return f"{path}={value}"


# =========================
# Kronos Gate (V20.4)
# - If kronos_enabled=true but missing audit/exogenous -> force disable and warn
# =========================
def _find_kronos_audit_module(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # support both: payload.audit.modules[] OR payload.meta.audit_modules[]
    mods = jget(payload, "audit.modules", None)
    if mods is None:
        mods = jget(payload, "meta.audit_modules", None)
    for m in ensure_list(mods):
        if isinstance(m, dict):
            # accept either module_name or name field
            if m.get("module_name") == "KRONOS_EXOGENOUS" or m.get("name") == "KRONOS_EXOGENOUS":
                return m
    return None


def kronos_gate(payload: Dict[str, Any]) -> Tuple[bool, bool, List[str]]:
    """
    Returns:
      ok (bool): gate executed successfully
      forced_disabled (bool): whether we forced kronos_enabled=false
      warnings (list[str])
    """
    warnings: List[str] = []

    sp = jget(payload, "system_params", {}) or {}
    enabled = bool(sp.get("kronos_enabled", False))
    if not enabled:
        return True, False, warnings

    km = _find_kronos_audit_module(payload)
    exo = jget(payload, "exogenous.kronos", None)

    missing: List[str] = []

    # audit module required
    if km is None:
        missing.append("audit.modules[KRONOS_EXOGENOUS]")
    else:
        # required audit fields (V20.4 lock)
        # accept both variants: module_name/name, input_window/window, bar_freq
        status = km.get("status")
        model_id = km.get("model_id")
        tokenizer_id = km.get("tokenizer_id")
        input_window = km.get("input_window", km.get("window"))
        bar_freq = km.get("bar_freq")
        feature_hash = km.get("feature_hash")

        if status != "OK":
            missing.append("audit.modules[KRONOS_EXOGENOUS].status!=OK")
        if not model_id:
            missing.append("audit.modules[KRONOS_EXOGENOUS].model_id")
        if not tokenizer_id:
            missing.append("audit.modules[KRONOS_EXOGENOUS].tokenizer_id")
        if input_window != 256:
            missing.append("audit.modules[KRONOS_EXOGENOUS].input_window!=256")
        if bar_freq != "1D":
            missing.append("audit.modules[KRONOS_EXOGENOUS].bar_freq!=1D")
        if not feature_hash:
            missing.append("audit.modules[KRONOS_EXOGENOUS].feature_hash")

    # exogenous required
    if not isinstance(exo, dict):
        missing.append("exogenous.kronos")
    else:
        for k in ["sri_0_1", "vol_ratio_3d", "consistency_0_1"]:
            if exo.get(k) is None:
                missing.append(f"exogenous.kronos.{k}")

    if missing:
        # force disable to avoid drift / non-reproducible backtest
        warnings.append("KRONOS_DISABLED_MISSING_AUDIT_OR_EXOGENOUS:" + ";".join(missing))
        payload.setdefault("system_params", {})
        payload["system_params"]["kronos_enabled"] = False
        return True, True, warnings

    return True, False, warnings


# =========================
# L1 Gate (V20.1) - F1~F6
# =========================
def l1_gate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Output fixed format:
      MODE: L1_AUDIT
      VERDICT: PASS/FAIL
      FATAL_ISSUES: [...]
      WARNINGS: [...]
      AUDIT_TRAIL: [...]
    """
    fatal: List[str] = []
    warn: List[str] = []
    trail: List[str] = []

    # ---- F1: macro.overview.twii_close missing/null ----
    twii_close = jget(payload, "macro.overview.twii_close", None)
    if twii_close is None:
        fatal.append("F1_TWII_CLOSE_MISSING")
        trail.append(path_kv("macro.overview.twii_close", twii_close))
    else:
        trail.append(path_kv("macro.overview.twii_close", twii_close))

    # ---- F2: macro.integrity.kill == true ----
    kill = bool(jget(payload, "macro.integrity.kill", False))
    trail.append(path_kv("macro.integrity.kill", kill))
    if kill:
        fatal.append("F2_INTEGRITY_KILL_TRUE")

    # ---- F3: meta.confidence_level LOW while meta.market_status NORMAL ----
    m_conf = str(jget(payload, "meta.confidence_level", "") or "")
    m_status = str(jget(payload, "meta.market_status", "") or "")
    trail.append(path_kv("meta.confidence_level", m_conf))
    trail.append(path_kv("meta.market_status", m_status))
    if m_conf.upper() == "LOW" and m_status.upper() == "NORMAL":
        fatal.append("F3_CONF_LOW_BUT_STATUS_NORMAL")

    # ---- F4: PRICE_SANITY_FAIL (Hard Range + Median Scale Gate) ----
    sp = jget(payload, "system_params", {}) or {}
    pmin = to_float(sp.get("l1_price_min"))
    pmax = to_float(sp.get("l1_price_max"))
    pmult = to_float(sp.get("l1_price_median_mult_hi"))

    trail.append(path_kv("system_params.l1_price_min", pmin))
    trail.append(path_kv("system_params.l1_price_max", pmax))
    trail.append(path_kv("system_params.l1_price_median_mult_hi", pmult))

    stocks = ensure_list(payload.get("stocks"))
    prices: List[float] = []
    price_cells: List[Tuple[int, str, Optional[float]]] = []

    for i, s in enumerate(stocks):
        if not isinstance(s, dict):
            continue
        # support both key variants: price/Price
        sym = s.get("symbol", s.get("Symbol", f"idx{i}"))
        pr = to_float(s.get("price", s.get("Price")))
        price_cells.append((i, str(sym), pr))
        if pr is not None:
            prices.append(pr)

    # Hard range gate (requires params)
    if pmin is None or pmax is None or pmult is None:
        warn.append("W_SYS_PARAMS_L1_PRICE_THRESHOLDS_MISSING")
    else:
        for i, sym, pr in price_cells:
            if pr is None:
                # price missing itself is a L1 issue? (你的 V20.1 沒列為 FATAL，先做 warning)
                warn.append(f"W_STOCK_PRICE_MISSING:stocks[{i}].symbol={sym}")
                continue
            if pr < pmin or pr > pmax:
                fatal.append("F4_PRICE_SANITY_FAIL:HARD_RANGE")
                trail.append(path_kv(f"stocks[{i}].symbol", sym))
                trail.append(path_kv(f"stocks[{i}].price", pr))
                trail.append(path_kv("PRICE_SANITY_RULE", f"{pmin}<=price<={pmax}"))
                break

        # Same-payload scale gate (stocks >= 3)
        if len(prices) >= 3 and not fatal:
            med = median(prices)
            trail.append(path_kv("PRICE_SANITY.median_price", med))
            if med is not None and med > 0:
                for i, sym, pr in price_cells:
                    if pr is None:
                        continue
                    if pr > med * pmult or pr < med / pmult:
                        fatal.append("F4_PRICE_SANITY_FAIL:MEDIAN_SCALE")
                        trail.append(path_kv(f"stocks[{i}].symbol", sym))
                        trail.append(path_kv(f"stocks[{i}].price", pr))
                        trail.append(path_kv("PRICE_SANITY_RULE", f"median/{pmult}<=price<=median*{pmult}"))
                        break

    # ---- F5: meta.is_using_previous_day=true but missing effective_trade_date ----
    is_prev = bool(jget(payload, "meta.is_using_previous_day", False))
    eff_date = jget(payload, "meta.effective_trade_date", None)
    trail.append(path_kv("meta.is_using_previous_day", is_prev))
    trail.append(path_kv("meta.effective_trade_date", eff_date))
    if is_prev and not eff_date:
        fatal.append("F5_PREV_DAY_TRUE_BUT_EFFECTIVE_TRADE_DATE_MISSING")

    # ---- F6: institutional.inst_status == NO_UPDATE_TODAY but inst_net_3d is not null ----
    # supports: stocks[i].institutional.inst_status / inst_net_3d
    for i, s in enumerate(stocks):
        if not isinstance(s, dict):
            continue
        inst = s.get("institutional", {}) or {}
        st = str(inst.get("inst_status", "") or "")
        net3d = inst.get("inst_net_3d", None)
        if st == "NO_UPDATE_TODAY" and net3d is not None:
            fatal.append("F6_INSTITUTIONAL_ZOMBIE_DATA:inst_status=NO_UPDATE_TODAY_BUT_inst_net_3d_NONNULL")
            trail.append(path_kv(f"stocks[{i}].institutional.inst_status", st))
            trail.append(path_kv(f"stocks[{i}].institutional.inst_net_3d", net3d))
            break

    # ---- Extra: Market amount warnings (not fatal by V20.1, but audit-visible) ----
    amt_twse = jget(payload, "macro.market_amount.amount_twse", None)
    amt_tpex = jget(payload, "macro.market_amount.amount_tpex", None)
    if amt_twse is None:
        warn.append("W_MARKET_AMOUNT_TWSE_MISSING")
    if amt_tpex is None:
        warn.append("W_MARKET_AMOUNT_TPEX_MISSING")

    # ---- Kronos gate (V20.4 optional) ----
    _, forced_disabled, k_warn = kronos_gate(payload)
    warn.extend(k_warn)
    if forced_disabled:
        trail.append(path_kv("system_params.kronos_enabled", False))

    verdict = "PASS" if len(fatal) == 0 else "FAIL"

    return {
        "MODE": "L1_AUDIT",
        "VERDICT": verdict,
        "FATAL_ISSUES": fatal,
        "WARNINGS": warn,
        "AUDIT_TRAIL": trail,
    }


# =========================
# Snapshot input compatibility (optional)
# - If user passes snapshot file, try to extract min-json like structure
# =========================
def extract_payload_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    If the file is a "snapshot_tw.json" (from downloader_tw.py), it isn't the Arbiter payload directly.
    This helper attempts to convert it to a minimal payload for L1 checks.
    """
    # If it already looks like arbiter payload, return as-is
    if "macro" in snapshot and "stocks" in snapshot and "meta" in snapshot:
        return snapshot

    out: Dict[str, Any] = {
        "meta": {
            "confidence_level": "HIGH",
            "market_status": "NORMAL",
            "is_using_previous_day": bool(snapshot.get("recency", {}).get("is_using_previous_day", False)),
            "effective_trade_date": snapshot.get("trade_date_iso"),
        },
        "macro": {
            "integrity": {"kill": False},
            "overview": {
                "twii_close": (snapshot.get("twii", {}).get("data", {}) or {}).get("close"),
            },
            "market_amount": {
                "amount_twse": (snapshot.get("market_amount", {}) or {}).get("amount_twse"),
                "amount_tpex": (snapshot.get("market_amount", {}) or {}).get("amount_tpex"),
            }
        },
        "system_params": snapshot.get("system_params", {}) or {},
        "stocks": [],
    }

    # top rows -> stocks
    top_rows = snapshot.get("top", {}).get("rows", []) or []
    for r in top_rows:
        code = r.get("code")
        close = r.get("close")
        if not code or close is None:
            continue
        out["stocks"].append({
            "symbol": f"{code}.TW",
            "price": close,
            "institutional": {"inst_status": "OK", "inst_net_3d": None},
            "risk": {"stop_distance_pct": None},
            "signals": {}
        })
    return out


# =========================
# CLI
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="Arbiter payload json path (e.g., macro.json)")
    ap.add_argument("--snapshot", action="store_true", help="Treat input as snapshot_tw.json and extract payload")
    args = ap.parse_args()

    if not os.path.exists(args.json):
        print(f"❌ 找不到檔案: {args.json}")
        sys.exit(2)

    with open(args.json, "r", encoding="utf-8") as f:
        obj = json.load(f)

    payload = extract_payload_from_snapshot(obj) if args.snapshot else obj

    report = l1_gate(payload)

    print("MODE:", report["MODE"])
    print("VERDICT:", report["VERDICT"])
    print("FATAL_ISSUES:")
    for x in report["FATAL_ISSUES"]:
        print(" -", x)
    print("WARNINGS:")
    for x in report["WARNINGS"]:
        print(" -", x)
    print("AUDIT_TRAIL:")
    for x in report["AUDIT_TRAIL"]:
        print(" -", x)

    sys.exit(0 if report["VERDICT"] == "PASS" else 2)


if __name__ == "__main__":
    main()
