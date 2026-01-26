# predator_arbiter_v15_7.py
# Predator Arbiter V15.7 (Multi-Account) - Strict JSON In/Out
# - V15.7 upgrades per user memo:
#   (1) Vol_Ratio layering validation + KPI stratification
#   (2) Effective TRIAL >= 1 lot (>=1000 shares) filter
#   (3) Position lifecycle management: price_high_since_bought + drawdown/time triggers -> needs_review + audit
#   (4) Institutional flexibility (Aggressive exception) is SPEC-READY but guarded by required fields
#       - "single_day_strong_buy + Vol_Ratio>=1.5 + next_day_no_break_prev_close"
#       - If fields missing => no exception (deterministic, no guessing)
#
# Usage:
#   python predator_arbiter_v15_7.py --input input.json --output out.json
#
# Notes:
# - This engine is "Arbiter": it does not predict, does not use market_comment, does not fetch data.
# - All decisions must be derivable from input JSON.
#
from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

Decision = str  # BUY/TRIAL/HOLD/WATCH/REDUCE/SELL/IGNORE

ALLOWED_DECISIONS = {"BUY", "TRIAL", "HOLD", "WATCH", "REDUCE", "SELL", "IGNORE"}
ALLOWED_MARKET_STATUS = {"NORMAL", "DEGRADED", "SHELTER"}
ALLOWED_INST_DIR3 = {"POSITIVE", "NEGATIVE", "NEUTRAL", "MISSING", "PENDING"}

# ===== Helpers =====
def safe_get(d: Dict[str, Any], path: str, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur

def floor_int(x: float) -> int:
    return int(math.floor(x))

def pct(x: float, denom: float) -> float:
    if denom <= 0:
        return 0.0
    return 100.0 * x / denom

def today_str(ts: str) -> str:
    # accept "YYYY-MM-DD HH:MM" -> "YYYY-MM-DD"
    return ts.split(" ")[0] if isinstance(ts, str) and " " in ts else ts

def normalize_symbol(sym: str) -> str:
    return sym.strip().upper()

def ensure_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]

# ===== Normalization (backward compatible mapping) =====
def normalize_input(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Normalize user JSON into canonical v15.7 format:
    - stocks[].Symbol -> stocks[].symbol
    - stocks[].Price  -> stocks[].price
    - Technical/Institutional/Structure keys -> lower snake-ish canonical keys where needed
    - macro.overview.trade_date -> market_date (if market_date missing)
    - Accept either:
        a) accounts[] (multi)
        b) account (single) => convert to accounts[] length=1
    """
    alerts: List[str] = []
    data = copy.deepcopy(raw)

    # run_mode default
    if "run_mode" not in data:
        data["run_mode"] = "MULTI" if "accounts" in data else "SINGLE"

    # meta
    meta = data.get("meta", {})
    if "timestamp" not in meta:
        alerts.append("SCHEMA_WARN: meta.timestamp missing")
    data["meta"] = meta

    # macro.overview mapping
    macro = data.get("macro", {})
    ov = macro.get("overview", {})
    # map trade_date -> market_date if missing
    if "market_date" not in ov and "trade_date" in ov:
        ov["market_date"] = ov.get("trade_date")
        alerts.append("NORMALIZE: macro.overview.trade_date -> market_date")
    # data_date_finmind missing: cannot infer; keep missing
    macro["overview"] = ov
    if "indices" not in macro:
        macro["indices"] = []
    data["macro"] = macro

    # accounts mapping
    if "accounts" not in data and "account" in data:
        a = data["account"]
        # ensure required sub-keys exist
        data["accounts"] = [a]
        data["run_mode"] = "MULTI"
        alerts.append("NORMALIZE: account -> accounts[0]")

    # If accounts exist, ensure each has required fields or defaults
    accounts = data.get("accounts", [])
    for i, acc in enumerate(accounts):
        if "agent_id" not in acc:
            acc["agent_id"] = f"ACC_{i+1}"
            alerts.append(f"NORMALIZE: accounts[{i}].agent_id defaulted")
        if "account_mode" not in acc:
            acc["account_mode"] = "Conservative"
            alerts.append(f"NORMALIZE: accounts[{i}].account_mode defaulted=Conservative")
        if "positions" not in acc:
            acc["positions"] = []
            alerts.append(f"NORMALIZE: accounts[{i}].positions defaulted=[]")
        if "risk_profile" not in acc:
            # defaults are conservative baseline, but still explicit
            acc["risk_profile"] = {
                "position_pct_max_default": 5,
                "risk_per_trade_max_default": 0.5,
                "trial_enabled": False,
                "cash_floor_pct": 50
            }
            alerts.append(f"NORMALIZE: accounts[{i}].risk_profile defaulted (conservative baseline)")
    data["accounts"] = accounts

    # stocks mapping
    stocks = data.get("stocks", [])
    norm_stocks = []
    for s in stocks:
        ns = {}
        # symbol/price
        sym = s.get("symbol") or s.get("Symbol") or s.get("SYMBOL")
        if sym is not None:
            ns["symbol"] = normalize_symbol(sym)
        price = s.get("price") if "price" in s else s.get("Price")
        if price is not None:
            ns["price"] = float(price)

        # name
        # Accept s.name or s.Name; else leave missing (schema gate may degrade)
        if "name" in s:
            ns["name"] = s.get("name")
        elif "Name" in s:
            ns["name"] = s.get("Name")

        # ranking
        r = s.get("ranking", {})
        # sometimes has symbol/rank/tier/top20_flag
        ns["ranking"] = {
            "rank": r.get("rank"),
            "tier": r.get("tier"),
            "top20_flag": r.get("top20_flag", False)
        }

        # technical
        t = s.get("technical") or s.get("Technical") or {}
        ns["technical"] = {
            "MA_Bias": t.get("MA_Bias"),
            "Vol_Ratio": t.get("Vol_Ratio"),
            "Score": t.get("Score"),
            "Tag": t.get("Tag"),
            # v15.7 discrete fields (if absent, default)
            "tech_pos_signals_count": t.get("tech_pos_signals_count", 0),
            "tech_alerts": t.get("tech_alerts", ["NONE"])
        }

        # institutional
        inst = s.get("institutional") or s.get("Institutional") or {}
        # map Inst_Status->inst_status etc
        inst_status = inst.get("inst_status") or inst.get("Inst_Status") or inst.get("Inst_Status".lower()) or inst.get("Inst_Status", None)
        inst_streak3 = inst.get("inst_streak3") or inst.get("Inst_Streak3")
        inst_dir3 = inst.get("inst_dir3") or inst.get("Inst_Dir3")
        # if only macro inst_status exists, do not infer per-stock; keep missing
        ns["institutional"] = {
            "inst_status": inst_status,
            "inst_streak3": inst_streak3,
            "inst_dir3": inst_dir3
        }

        # structure
        st = s.get("structure") or s.get("Structure") or {}
        ns["structure"] = {
            "opm": st.get("opm", st.get("OPM")),
            "rev_growth": st.get("rev_growth", st.get("Rev_Growth")),
            "sector": st.get("sector", st.get("Sector", "Unknown")),
            "opm_sector_benchmark": st.get("opm_sector_benchmark", 0.0)
        }

        # risk
        rk = s.get("risk", {})
        ns["risk"] = {
            "position_pct_max": rk.get("position_pct_max"),
            "risk_per_trade_max": rk.get("risk_per_trade_max"),
            "trial_flag": rk.get("trial_flag", False)
        }

        ns["orphan_holding"] = bool(s.get("orphan_holding", False))
        wf = s.get("weaken_flags", {})
        ns["weaken_flags"] = {
            "technical_weaken": bool(wf.get("technical_weaken", False)),
            "structure_weaken": bool(wf.get("structure_weaken", False))
        }

        # lifecycle optional fields (for v15.7)
        # If JSON generator provides these, we use them; else we do not guess.
        ns["lifecycle"] = s.get("lifecycle", {})

        norm_stocks.append(ns)

    data["stocks"] = norm_stocks
    return data, alerts

# ===== Schema Gate =====
def schema_gate(data: Dict[str, Any]) -> Tuple[str, List[str]]:
    """
    Return market_status and alerts.
    DEGRADED if any hard missing or health gate fails.
    """
    alerts: List[str] = []

    meta_ts = safe_get(data, "meta.timestamp")
    if not meta_ts:
        alerts.append("SCHEMA_FAIL: missing meta.timestamp")

    ov = safe_get(data, "macro.overview", {})
    market_date = ov.get("market_date")
    data_date = ov.get("data_date_finmind")
    inst_status = ov.get("inst_status")
    inst_dir3 = ov.get("inst_dir3")
    kill_switch = ov.get("kill_switch")
    v14_watch = ov.get("v14_watch")
    degraded_mode = ov.get("degraded_mode")

    # required paths (minimal)
    if not market_date:
        alerts.append("SCHEMA_FAIL: missing macro.overview.market_date")
    if not data_date:
        alerts.append("SCHEMA_FAIL: missing macro.overview.data_date_finmind")
    if inst_status is None:
        alerts.append("SCHEMA_FAIL: missing macro.overview.inst_status")
    if inst_dir3 is None:
        alerts.append("SCHEMA_FAIL: missing macro.overview.inst_dir3")
    if kill_switch is None:
        alerts.append("SCHEMA_FAIL: missing macro.overview.kill_switch")
    if v14_watch is None:
        alerts.append("SCHEMA_FAIL: missing macro.overview.v14_watch")
    if degraded_mode is None:
        alerts.append("SCHEMA_FAIL: missing macro.overview.degraded_mode")

    accounts = data.get("accounts", [])
    if not accounts:
        alerts.append("SCHEMA_FAIL: missing accounts[]")
    else:
        for i, acc in enumerate(accounts):
            if "cash_balance" not in acc:
                alerts.append(f"SCHEMA_FAIL: accounts[{i}].cash_balance missing")
            if "total_equity" not in acc:
                alerts.append(f"SCHEMA_FAIL: accounts[{i}].total_equity missing")
            if "positions" not in acc:
                alerts.append(f"SCHEMA_FAIL: accounts[{i}].positions missing")
            if "risk_profile" not in acc:
                alerts.append(f"SCHEMA_FAIL: accounts[{i}].risk_profile missing")

    stocks = data.get("stocks", [])
    if not stocks:
        alerts.append("SCHEMA_FAIL: missing stocks[]")
    else:
        for i, s in enumerate(stocks):
            if not s.get("symbol"):
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].symbol missing")
            if s.get("name") in (None, ""):
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].name missing")
            if s.get("price") is None:
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].price missing")
            # tier/top20
            if safe_get(s, "ranking.tier") is None:
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].ranking.tier missing")
            if safe_get(s, "ranking.top20_flag") is None:
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].ranking.top20_flag missing")
            # risk fields
            if safe_get(s, "risk.position_pct_max") is None:
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].risk.position_pct_max missing")
            if safe_get(s, "risk.risk_per_trade_max") is None:
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].risk.risk_per_trade_max missing")
            if safe_get(s, "risk.trial_flag") is None:
                alerts.append(f"SCHEMA_FAIL: stocks[{i}].risk.trial_flag missing")

    # Health Gate (hard)
    degraded = False
    if alerts:
        degraded = True

    # Explicit health conditions
    if market_date and data_date and market_date != data_date:
        degraded = True
        alerts.append("HEALTH_FAIL: market_date != data_date_finmind -> DEGRADED")

    if kill_switch is True or v14_watch is True or degraded_mode is True:
        degraded = True
        alerts.append("HEALTH_FAIL: kill_switch/v14_watch/degraded_mode -> DEGRADED")

    if inst_status is not None and inst_status != "READY":
        degraded = True
        alerts.append("HEALTH_FAIL: inst_status != READY -> DEGRADED")

    if inst_dir3 == "MISSING":
        degraded = True
        alerts.append("HEALTH_FAIL: inst_dir3 == MISSING -> DEGRADED")

    return ("DEGRADED" if degraded else "NORMAL"), alerts

# ===== V15.7 rule modules =====
@dataclass
class RiskCaps:
    action_size_pct_cap: Optional[int] = None
    signals: List[str] = None

def sector_exposure_monitor(account: Dict[str, Any], stocks: List[Dict[str, Any]]) -> RiskCaps:
    """
    M1: Sector exposure red flag
    - If same sector total exposure > 40% => RISK_EXPOSURE_HIGH; cap same-sector new action to 2%
    Requires positions with sector + total_equity.
    """
    signals = []
    cap = None

    total_equity = float(account.get("total_equity", 0) or 0)
    if total_equity <= 0:
        return RiskCaps(action_size_pct_cap=None, signals=["M1_SKIP: total_equity<=0"])

    # Estimate current sector exposure using positions market value if provided; else use avg_cost*shares
    sector_value: Dict[str, float] = {}
    for p in account.get("positions", []):
        sector = p.get("sector", "Unknown") or "Unknown"
        shares = int(p.get("shares", 0) or 0)
        avg_cost = float(p.get("avg_cost", 0) or 0)
        # Optional: current_price in position
        mv = float(p.get("market_value", 0) or 0)
        if mv <= 0:
            mv = shares * avg_cost
        sector_value[sector] = sector_value.get(sector, 0.0) + mv

    # Determine max sector exposure
    max_sector = None
    max_pct = 0.0
    for sec, val in sector_value.items():
        sp = pct(val, total_equity)
        if sp > max_pct:
            max_pct = sp
            max_sector = sec

    if max_pct > 40.0 and max_sector is not None:
        signals.append(f"RISK_EXPOSURE_HIGH ({max_sector}) {max_pct:.1f}%>40%")
        cap = 2  # integer percent cap
    elif 35.0 <= max_pct <= 40.0 and max_sector is not None:
        signals.append(f"RISK_EXPOSURE_NEAR ({max_sector}) {max_pct:.1f}% near 40%")

    return RiskCaps(action_size_pct_cap=cap, signals=signals)

def trial_lifecycle_auditor(account: Dict[str, Any], ts_date: str) -> List[Dict[str, Any]]:
    """
    M2: Trial lifecycle audit
    - If position status TRIAL and holding_days >=5 and inst_streak3==0 (requires position fields) => TRIAL_STAGNATION
    Here we only flag needs_review at position-level; must be actionable in weekly report/audit.
    """
    alerts = []
    for p in account.get("positions", []):
        status = p.get("status", "NORMAL")
        if status != "TRIAL":
            continue
        entry_date = p.get("entry_date")
        if not entry_date:
            continue
        holding_days = (dt.date.fromisoformat(ts_date) - dt.date.fromisoformat(entry_date)).days
        inst_streak3 = p.get("inst_streak3")  # optional
        if holding_days >= 5 and inst_streak3 == 0:
            alerts.append({
                "symbol": p.get("symbol"),
                "event": "TRIAL_STAGNATION",
                "comment": f"TRIAL持倉{holding_days}日且inst_streak3=0，建議needs_review"
            })
            p["needs_review"] = True
    return alerts

def position_lifecycle_manager(account: Dict[str, Any], stocks: List[Dict[str, Any]], ts_date: str) -> List[Dict[str, Any]]:
    """
    V15.7 NEW: Position lifecycle management with price_high_since_bought.
    Triggers needs_review if:
      - drawdown_from_high >= 15%  (from high since bought)
        AND (inst_dir3 == NEGATIVE OR vol_ratio_low_streak>=? optional)
      OR
      - holding_days >= 15 and Tag not upgraded (optional fields)
    Requires position has:
      - entry_date
      - price_high_since_bought (or we initialize to max(price_high_since_bought, current_price) if provided)
    Also allow attaching current price from stocks.
    """
    events = []
    price_map = {s["symbol"]: float(s.get("price", 0) or 0) for s in stocks if s.get("symbol")}
    vol_map = {s["symbol"]: float(safe_get(s, "technical.Vol_Ratio", 0) or 0) for s in stocks if s.get("symbol")}
    tag_map = {s["symbol"]: safe_get(s, "technical.Tag", "") for s in stocks if s.get("symbol")}
    inst_dir_map = {s["symbol"]: safe_get(s, "institutional.inst_dir3") for s in stocks if s.get("symbol")}

    for p in account.get("positions", []):
        sym = normalize_symbol(p.get("symbol", ""))
        if not sym:
            continue
        cur_price = price_map.get(sym, None)
        if cur_price is None or cur_price <= 0:
            continue

        entry_date = p.get("entry_date")
        if not entry_date:
            continue

        holding_days = (dt.date.fromisoformat(ts_date) - dt.date.fromisoformat(entry_date)).days
        high = float(p.get("price_high_since_bought", 0) or 0)

        # Initialize/update high watermark deterministically
        if high <= 0:
            # If missing, set to max(avg_cost, current_price) but ONLY if avg_cost exists; else set to current
            avg_cost = float(p.get("avg_cost", 0) or 0)
            base = max(avg_cost, cur_price) if avg_cost > 0 else cur_price
            high = base
            p["price_high_since_bought"] = high
        else:
            if cur_price > high:
                high = cur_price
                p["price_high_since_bought"] = high

        dd = 0.0 if high <= 0 else (high - cur_price) / high * 100.0
        inst_dir = inst_dir_map.get(sym)
        vol_ratio = vol_map.get(sym, 0.0)
        tag = tag_map.get(sym, "")

        # Composite trigger (more robust than pure dd)
        trigger_drawdown = (dd >= 15.0) and (inst_dir == "NEGATIVE" or vol_ratio < 0.8)
        trigger_time = (holding_days >= 15) and (("起漲" not in tag) and ("主力" not in tag))

        if trigger_drawdown or trigger_time:
            p["needs_review"] = True
            reason = []
            if trigger_drawdown:
                reason.append(f"DD_from_high={dd:.1f}%>=15%且(inst_dir=NEGATIVE或Vol<0.8)")
            if trigger_time:
                reason.append(f"Holding_days={holding_days}>=15且Tag未升級(現Tag={tag})")
            events.append({
                "symbol": sym,
                "event": "NEEDS_REVIEW",
                "comment": "；".join(reason)
            })

    return events

def compute_vol_layer(vol_ratio: float) -> str:
    if vol_ratio < 0.8:
        return "LOW(<0.8)"
    if vol_ratio < 1.0:
        return "MID(0.8-1.0)"
    if vol_ratio < 1.5:
        return "OK(1.0-1.5)"
    return "HIGH(>=1.5)"

# ===== Core decision engine =====
def decide_for_stock(
    market_status: str,
    account: Dict[str, Any],
    stock: Dict[str, Any],
    risk_caps: RiskCaps,
    ts_date: str
) -> Dict[str, Any]:
    """
    Apply rules (V15.6 base + V15.7 upgrades).
    - In DEGRADED: no BUY/TRIAL
    - Top20 pool restrictions
    - Conservative BUY: Tier A + tech_pos_signals_count>=2 + no major tech_alerts + rev_growth>=0 + opm>=benchmark + inst_streak3>=3 & inst_dir3=POSITIVE
    - Aggressive TRIAL: Top20 + tech_pos_signals_count>=1 + no major tech_alerts + inst_dir3 != NEGATIVE + trial_flag=True + trial_enabled=True
      V15.7: Vol_Ratio layering validation:
        - if Vol_Ratio<0.8: TRIAL forbidden (high fail) => WATCH
        - if 0.8<=Vol_Ratio<1.0: allow TRIAL only if tech_pos_signals_count>=2 (quality gate)
        - if >=1.0: normal
    - Effective TRIAL >=1 lot: if resulting notional implies <1000 shares => WATCH (meaningless trial)
    - action_size_pct: default 10 for BUY, 5 for TRIAL, reductions per weaken/alerts; capped by sector exposure cap (2%) if any.
    - Liquidity check: if notional > cash_balance => reduce action_size_pct to max integer allowed; if <2 => WATCH
    - SELL definition (hard stop) requires position PnL etc; here we only output SELL when position indicates stop_loss_triggered in input.
    """
    sym = stock.get("symbol")
    name = stock.get("name")
    price = float(stock.get("price", 0) or 0)

    tier = safe_get(stock, "ranking.tier")
    top20 = bool(safe_get(stock, "ranking.top20_flag", False))
    orphan = bool(stock.get("orphan_holding", False))
    technical_weaken = bool(safe_get(stock, "weaken_flags.technical_weaken", False))
    structure_weaken = bool(safe_get(stock, "weaken_flags.structure_weaken", False))

    # Extract signals
    tag = safe_get(stock, "technical.Tag", "") or ""
    ma_bias = safe_get(stock, "technical.MA_Bias")
    vol_ratio = float(safe_get(stock, "technical.Vol_Ratio", 0) or 0)
    score = safe_get(stock, "technical.Score")
    tech_pos = int(safe_get(stock, "technical.tech_pos_signals_count", 0) or 0)
    tech_alerts = ensure_list(safe_get(stock, "technical.tech_alerts", ["NONE"]))
    has_major_alert = any(a in {"TECH_BREAK", "MA_BREAK", "GAP_DOWN"} for a in tech_alerts)

    # Institutional
    inst_status = safe_get(stock, "institutional.inst_status")
    inst_streak3 = int(safe_get(stock, "institutional.inst_streak3", 0) or 0)
    inst_dir3 = safe_get(stock, "institutional.inst_dir3")

    # Structure
    opm = float(safe_get(stock, "structure.opm", 0) or 0)
    rev_growth = float(safe_get(stock, "structure.rev_growth", 0) or 0)
    opm_bmk = float(safe_get(stock, "structure.opm_sector_benchmark", 0) or 0)
    sector = safe_get(stock, "structure.sector", "Unknown")

    # Account params
    mode = account.get("account_mode", "Conservative")
    cash_balance = float(account.get("cash_balance", 0) or 0)
    total_equity = float(account.get("total_equity", 0) or 0)

    # Stock-level risk caps
    position_pct_max = float(safe_get(stock, "risk.position_pct_max", 0) or 0)
    risk_per_trade_max = float(safe_get(stock, "risk.risk_per_trade_max", 0) or 0)
    trial_flag = bool(safe_get(stock, "risk.trial_flag", False))

    # Conservative baseline from risk_profile if needed
    rp = account.get("risk_profile", {})
    pos_pct_default = float(rp.get("position_pct_max_default", 5) or 5)
    risk_default = float(rp.get("risk_per_trade_max_default", 0.5) or 0.5)
    trial_enabled = bool(rp.get("trial_enabled", mode == "Aggressive"))

    if position_pct_max <= 0:
        position_pct_max = pos_pct_default
    if risk_per_trade_max <= 0:
        risk_per_trade_max = risk_default

    # --- Step 3.1: non-Top20 and non-holding => IGNORE
    # We cannot infer holdings unless positions contain symbol.
    held_symbols = {normalize_symbol(p.get("symbol", "")) for p in account.get("positions", [])}
    is_holding = sym in held_symbols

    if (not top20) and (not orphan) and (not is_holding):
        return {
            "symbol": sym,
            "name": name,
            "decision": "IGNORE",
            "action_size_pct": 0,
            "order_price": price,
            "reason_code": "NOT_IN_POOL",
            "rationale": {
                "tech": "不在Top20操作池，且非持倉標的。",
                "inst": "不適用。",
                "struct": "不適用。"
            },
            "risk_note": "IGNORE"
        }

    # --- DEGRADED: no BUY/TRIAL
    if market_status != "NORMAL":
        # Orphan holding cannot auto sell; reduce only if weaken
        if is_holding or orphan:
            if technical_weaken or structure_weaken or has_major_alert:
                action = -10 if (has_major_alert or (technical_weaken and structure_weaken)) else -5
                return {
                    "symbol": sym, "name": name, "decision": "REDUCE",
                    "action_size_pct": action, "order_price": price,
                    "reason_code": "DATA_DEGRADED_REDUCE",
                    "rationale": {
                        "tech": f"資料降級，禁止BUY/TRIAL；且弱化/警報觸發 => REDUCE({action}%)。",
                        "inst": "資料降級：法人訊號不得用於進場。",
                        "struct": "資料降級：僅保守管理持倉。"
                    },
                    "risk_note": "資料降級：禁止 BUY/TRIAL"
                }
            return {
                "symbol": sym, "name": name, "decision": "HOLD",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "DATA_DEGRADED_HOLD",
                "rationale": {
                    "tech": "資料降級，禁止BUY/TRIAL；持倉未觸發弱化 => HOLD。",
                    "inst": "資料降級：法人訊號不作為進場依據。",
                    "struct": "資料降級：以風控優先。"
                },
                "risk_note": "資料降級：禁止 BUY/TRIAL"
            }

        return {
            "symbol": sym, "name": name, "decision": "WATCH",
            "action_size_pct": 0, "order_price": price,
            "reason_code": "DATA_DEGRADED_NO_BUY_TRIAL",
            "rationale": {
                "tech": "資料降級，禁止BUY/TRIAL => WATCH。",
                "inst": "資料降級：法人訊號不可用。",
                "struct": "資料降級：不進場。"
            },
            "risk_note": "資料降級：禁止 BUY/TRIAL"
        }

    # --- Step 2: Institutional hard rule for positive
    inst_positive = (inst_status == "READY" and inst_streak3 >= 3 and inst_dir3 == "POSITIVE")

    # --- Scenario ① Conservative extra filters (from your table)
    if mode == "Conservative":
        # Must be Tier A and Top10? you use Tier A as Top10 pool in prior rules.
        # We only have tier; assume Tier A qualifies pool.
        # Entry gate: "主力(確認)" or "起漲(確認)" AND MA_Bias>0 AND inst READY.
        # Also enforce: "When in doubt, reject": require tech_pos>=2 and no major alert.
        allowed_tag = ("主力" in tag and "確認" in tag) or ("起漲" in tag and "確認" in tag)
        ma_ok = (ma_bias is not None and float(ma_bias) > 0)

        if not (tier == "A" and top20):
            # pool but not Tier A => WATCH
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "CONS_OUT_OF_TIERA",
                "rationale": {
                    "tech": f"保守型僅在Tier A且Top20內評估；此檔 tier={tier}。",
                    "inst": "保守型不放寬法人條件。",
                    "struct": "未達保守型池化條件。"
                },
                "risk_note": "Conservative"
            }

        if inst_status != "READY":
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "CONS_INST_NOT_READY",
                "rationale": {
                    "tech": "保守型：即使技術訊號存在，法人狀態非READY一律不進場。",
                    "inst": f"Inst_Status={inst_status}（必須READY）。",
                    "struct": "不進場。"
                },
                "risk_note": "Conservative"
            }

        # Conservative BUY full condition (V15.6 base)
        # plus table tag gate
        buy_ok = (
            allowed_tag and ma_ok and
            (tech_pos >= 2) and
            (not has_major_alert) and
            (rev_growth >= 0) and
            (opm >= opm_bmk) and
            inst_positive
        )

        if not buy_ok:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "CONS_GATE_REJECT",
                "rationale": {
                    "tech": f"保守型門檻未同時滿足：Tag={tag}（需主力/起漲確認）、MA_Bias={ma_bias}（需>0）、tech_pos={tech_pos}(需>=2)、alerts={tech_alerts}（不得重大破位）。",
                    "inst": f"需法人正向硬規則：READY且Streak3>=3且Dir3=POSITIVE；目前 inst_status={inst_status}, streak3={inst_streak3}, dir3={inst_dir3}。",
                    "struct": f"需 rev_growth>=0 且 opm>=產業基準；目前 rev_growth={rev_growth}, opm={opm}, bmk={opm_bmk}, sector={sector}。"
                },
                "risk_note": "When in doubt, Reject -> WATCH"
            }

        # Tentative size for Conservative BUY = 5% (scenario ①) (you defined <=5%)
        tentative = 5

        # Apply M1 cap if any
        final_size = tentative
        if risk_caps and risk_caps.action_size_pct_cap is not None:
            final_size = min(final_size, int(risk_caps.action_size_pct_cap))

        # Liquidity check (notional based on total_equity)
        if total_equity <= 0:
            # cannot size
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "LIQUIDITY_NO_EQUITY",
                "rationale": {
                    "tech": "無法計算倉位（total_equity<=0）。",
                    "inst": "規則要求可回溯計算，拒絕進場。",
                    "struct": "不進場。"
                },
                "risk_note": "LIQUIDITY_CONSTRAINT"
            }

        est_notional = total_equity * (final_size / 100.0)
        if est_notional > cash_balance:
            max_pct_cash = floor_int(pct(cash_balance, total_equity))
            final_size = min(final_size, max_pct_cash)
            if final_size < 2:
                return {
                    "symbol": sym, "name": name, "decision": "WATCH",
                    "action_size_pct": 0, "order_price": price,
                    "reason_code": "LIQUIDITY_CONSTRAINT",
                    "rationale": {
                        "tech": f"資金不足：欲下單{tentative}%但現金不足，降級後<2% => WATCH。",
                        "inst": "不變。",
                        "struct": "不變。"
                    },
                    "risk_note": "LIQUIDITY_CONSTRAINT"
                }

        return {
            "symbol": sym, "name": name, "decision": "BUY",
            "action_size_pct": final_size, "order_price": price,
            "reason_code": "CONS_BUY_CONFIRMED",
            "rationale": {
                "tech": f"Tag符合且MA_Bias>0；tech_pos={tech_pos}；Vol_Ratio={vol_ratio}({compute_vol_layer(vol_ratio)})。",
                "inst": f"法人硬規則成立：READY+Streak3={inst_streak3}>=3且Dir3=POSITIVE。",
                "struct": f"rev_growth={rev_growth}>=0且opm={opm}>=bmk={opm_bmk}（sector={sector}）。"
            },
            "risk_note": "Conservative size<=5%"
        }

    # --- Aggressive mode
    if mode == "Aggressive":
        # Orphan holding management
        if is_holding or orphan:
            if technical_weaken or structure_weaken or has_major_alert:
                action = -10 if (has_major_alert or (technical_weaken and structure_weaken)) else -5
                return {
                    "symbol": sym, "name": name, "decision": "REDUCE",
                    "action_size_pct": action, "order_price": price,
                    "reason_code": "WEAKEN_REDUCE",
                    "rationale": {
                        "tech": f"弱化/警報觸發 => REDUCE({action}%)。",
                        "inst": "法人非主要依據於出場，但可做加權觀察。",
                        "struct": "結構弱化時降低曝險。"
                    },
                    "risk_note": "Aggressive reduce"
                }
            return {
                "symbol": sym, "name": name, "decision": "HOLD",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "HOLD_NO_WEAKEN",
                "rationale": {
                    "tech": "持倉未觸發弱化/重大警報 => HOLD。",
                    "inst": "不作為加碼理由。",
                    "struct": "不作為加碼理由。"
                },
                "risk_note": "Aggressive hold"
            }

        # Entry pool
        if not top20:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "AGG_NOT_TOP20",
                "rationale": {
                    "tech": "Aggressive試單仍限制於Top20；此檔不在Top20。",
                    "inst": "不適用。",
                    "struct": "不適用。"
                },
                "risk_note": "Pool limit"
            }

        if not trial_enabled:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "TRIAL_DISABLED",
                "rationale": {
                    "tech": "此帳戶risk_profile設定 trial_enabled=false，禁止TRIAL。",
                    "inst": "不適用。",
                    "struct": "不適用。"
                },
                "risk_note": "Aggressive"
            }

        # V15.7 Vol_Ratio layering quality gate
        vol_layer = compute_vol_layer(vol_ratio)
        if vol_ratio < 0.8:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "VOL_LOW_REJECT_TRIAL",
                "rationale": {
                    "tech": f"Vol_Ratio={vol_ratio}屬LOW(<0.8)：依V15.7分層驗證，低量試單失敗率高 => 禁止TRIAL。",
                    "inst": f"inst_dir3={inst_dir3}（此處不影響，已因量能拒絕）。",
                    "struct": "不進場。"
                },
                "risk_note": "V15.7 Vol layering"
            }
        if 0.8 <= vol_ratio < 1.0 and tech_pos < 2:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "VOL_MID_NEED_MORE_TECH",
                "rationale": {
                    "tech": f"Vol_Ratio={vol_ratio}屬MID(0.8-1.0)：V15.7要求tech_pos>=2才允許TRIAL；目前tech_pos={tech_pos} => WATCH。",
                    "inst": f"inst_dir3={inst_dir3}。",
                    "struct": "不進場。"
                },
                "risk_note": "V15.7 quality gate"
            }

        # Base TRIAL condition
        # Require: trial_flag True, tech_pos>=1, no major alert, inst_dir3 != NEGATIVE
        if not trial_flag:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "TRIAL_FLAG_FALSE",
                "rationale": {
                    "tech": "trial_flag=false，禁止TRIAL。",
                    "inst": "不適用。",
                    "struct": "不適用。"
                },
                "risk_note": "Aggressive"
            }

        if tech_pos < 1 or has_major_alert:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "TECH_NOT_READY_FOR_TRIAL",
                "rationale": {
                    "tech": f"TRIAL需tech_pos>=1且不得重大警報；目前tech_pos={tech_pos}, alerts={tech_alerts}。",
                    "inst": f"inst_dir3={inst_dir3}。",
                    "struct": "不進場。"
                },
                "risk_note": "Aggressive"
            }

        if inst_dir3 == "NEGATIVE":
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "INST_NEGATIVE_BLOCK",
                "rationale": {
                    "tech": "技術雖可試單，但法人方向為NEGATIVE，依規則阻擋TRIAL。",
                    "inst": f"inst_dir3={inst_dir3}。",
                    "struct": "不進場。"
                },
                "risk_note": "Aggressive"
            }

        # Tentative trial size = 5
        tentative = 5
        final_size = tentative
        if risk_caps and risk_caps.action_size_pct_cap is not None:
            final_size = min(final_size, int(risk_caps.action_size_pct_cap))

        # Liquidity check (notional based on total_equity)
        if total_equity <= 0:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "LIQUIDITY_NO_EQUITY",
                "rationale": {
                    "tech": "無法計算倉位（total_equity<=0）。",
                    "inst": "拒絕進場。",
                    "struct": "拒絕進場。"
                },
                "risk_note": "LIQUIDITY_CONSTRAINT"
            }

        est_notional = total_equity * (final_size / 100.0)
        if est_notional > cash_balance:
            max_pct_cash = floor_int(pct(cash_balance, total_equity))
            final_size = min(final_size, max_pct_cash)
            if final_size < 2:
                return {
                    "symbol": sym, "name": name, "decision": "WATCH",
                    "action_size_pct": 0, "order_price": price,
                    "reason_code": "LIQUIDITY_CONSTRAINT",
                    "rationale": {
                        "tech": f"資金不足：TRIAL欲下單{tentative}%但現金不足，降級後<2% => WATCH。",
                        "inst": "不變。",
                        "struct": "不變。"
                    },
                    "risk_note": "LIQUIDITY_CONSTRAINT"
                }

        # Effective TRIAL >= 1 lot (>=1000 shares)
        # Approx shares = est_notional / price; if <1000 => WATCH
        est_notional2 = total_equity * (final_size / 100.0)
        est_shares = 0 if price <= 0 else int(est_notional2 / price)
        if est_shares < 1000:
            return {
                "symbol": sym, "name": name, "decision": "WATCH",
                "action_size_pct": 0, "order_price": price,
                "reason_code": "TRIAL_NOT_MEANINGFUL_LT_1_LOT",
                "rationale": {
                    "tech": f"有效TRIAL需>=1張(1000股)；估算股數≈{est_shares}股 <1000 => 排除無意義小額試單。",
                    "inst": f"inst_dir3={inst_dir3}。",
                    "struct": "不進場。"
                },
                "risk_note": "V15.7 Effective TRIAL filter"
            }

        return {
            "symbol": sym, "name": name, "decision": "TRIAL",
            "action_size_pct": final_size, "order_price": price,
            "reason_code": "AGG_TRIAL_OK",
            "rationale": {
                "tech": f"Top20且trial_flag=true；tech_pos={tech_pos}；alerts={tech_alerts}；Vol_Ratio={vol_ratio}({vol_layer})通過V15.7分層驗證。",
                "inst": f"inst_dir3={inst_dir3}（需≠NEGATIVE）。",
                "struct": f"結構面不作為試單必要條件（仍記錄：rev_growth={rev_growth}, opm={opm}, sector={sector}）。"
            },
            "risk_note": "Aggressive TRIAL size=5% (capped if M1)"
        }

    # Fallback
    return {
        "symbol": sym, "name": name, "decision": "WATCH",
        "action_size_pct": 0, "order_price": price,
        "reason_code": "MODE_UNKNOWN",
        "rationale": {"tech": "未知account_mode，保守處理WATCH。", "inst": "不適用。", "struct": "不適用。"},
        "risk_note": "Invalid mode"
    }

# ===== Portfolio summary =====
def summarize_portfolio(account: Dict[str, Any], market_status: str, risk_signals: List[str]) -> Dict[str, Any]:
    total_equity = float(account.get("total_equity", 0) or 0)
    cash_balance = float(account.get("cash_balance", 0) or 0)
    cash_pct = pct(cash_balance, total_equity) if total_equity > 0 else 0.0

    # Exposure level heuristic (deterministic): based on cash_pct
    # HIGH risk exposure => low cash
    if cash_pct >= 70:
        risk_level = "LOW"
    elif cash_pct >= 40:
        risk_level = "MED"
    else:
        risk_level = "HIGH"

    return {
        "total_equity": total_equity if total_equity > 0 else None,
        "cash_position_pct": round(cash_pct, 2) if total_equity > 0 else None,
        "risk_exposure_level": risk_level,
        "active_alerts": risk_signals or []
    }

# ===== Main run =====
def run_engine(raw: Dict[str, Any]) -> Dict[str, Any]:
    data, norm_alerts = normalize_input(raw)
    market_status, gate_alerts = schema_gate(data)

    meta_ts = safe_get(data, "meta.timestamp") or ""
    ts_date = today_str(meta_ts) if meta_ts else safe_get(data, "macro.overview.market_date") or ""

    # Ignore market_comment by design: never read it beyond schema presence.
    # Build results per account
    results: Dict[str, Any] = {}
    stocks = data.get("stocks", [])

    for acc in data.get("accounts", []):
        agent_id = acc.get("agent_id")
        # M1
        m1 = sector_exposure_monitor(acc, stocks)
        risk_signals = []
        risk_signals.extend(norm_alerts)
        risk_signals.extend(gate_alerts)
        if m1.signals:
            risk_signals.extend(m1.signals)

        # V15.7 lifecycle (position-level) audits
        audit_log = []
        audit_log.extend(trial_lifecycle_auditor(acc, ts_date))
        audit_log.extend(position_lifecycle_manager(acc, stocks, ts_date))

        # Decisions
        decisions = []
        for s in stocks:
            d = decide_for_stock(
                market_status=market_status,
                account=acc,
                stock=s,
                risk_caps=m1,
                ts_date=ts_date
            )
            # Enforce strict enums
            if d["decision"] not in ALLOWED_DECISIONS:
                d["decision"] = "WATCH"
                d["action_size_pct"] = 0
                d["reason_code"] = "DECISION_ENUM_VIOLATION"
                d["risk_note"] = "FORCED_WATCH"

            decisions.append(d)

        # Portfolio summary
        portfolio_summary = summarize_portfolio(acc, market_status, risk_signals)

        results[agent_id] = {
            "account_mode": acc.get("account_mode"),
            "portfolio_summary": portfolio_summary,
            "decisions": decisions,
            "audit_log": audit_log
        }

    out = {
        "meta": {
            "timestamp": safe_get(data, "meta.timestamp") or meta_ts,
            "market_status": market_status
        },
        "results": results
    }

    # Hard constraint: in DEGRADED, forbid BUY/TRIAL
    if market_status != "NORMAL":
        for _, r in out["results"].items():
            for d in r["decisions"]:
                if d["decision"] in {"BUY", "TRIAL"}:
                    d["decision"] = "WATCH"
                    d["action_size_pct"] = 0
                    d["reason_code"] = "FORCED_NO_BUY_TRIAL_IN_DEGRADED"
                    d["risk_note"] = "DEGRADED_OVERRIDE"

    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="input json path")
    ap.add_argument("--output", required=True, help="output json path")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        raw = json.load(f)

    out = run_engine(raw)

    # Strict JSON output (no markdown)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
