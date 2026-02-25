# ucc_v19_1.py
# =========================================================
# Predator UCC V19.1 Hardened Final Lockdown (全中文優化版)
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
    # V17 戰時模式 (War-time override)
    # -------------------------
    def v17_triggered(self, j: Json) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        if jget(j, "meta.war_time_override") is True:
            reasons.append("meta.war_time_override=true (手動觸發戰時模式)")
        reg = jget(j, "meta.current_regime")
        if contains_any(reg, ["WAR", "CRISIS"]):
            reasons.append(f"meta.current_regime={reg} (偵測到危機環境)")
        return (len(reasons) > 0, reasons)

    # -------------------------
    # 市場狀態判定
    # -------------------------
    def market_state(self, j: Json) -> Tuple[str, List[str]]:
        reasons: List[str] = []
        smr = to_float(jget(j, "macro.overview.SMR"))
        bop = jget(j, "macro.overview.Blow_Off_Phase", None)
        
        if smr is not None:
            reasons.append(f"macro.overview.SMR={smr:.4f}")
        if bop is not None:
            reasons.append(f"macro.overview.Blow_Off_Phase={bop}")
            
        if (smr is not None and smr >= self.SMR_OVERHEAT_2) or (bop is True):
            return "極度過熱 (CRITICAL_OVERHEAT)", reasons
        if smr is not None and smr >= self.SMR_OVERHEAT_1:
            return "過熱 (OVERHEAT)", reasons
        if smr is not None and smr < 0:
            return "防禦 (DEFENSIVE)", reasons
        return "正常 (NORMAL)", reasons

    # -------------------------
    # L3 壓測觸發條件
    # -------------------------
    def l3_triggered(self, j: Json, market_state: Optional[str] = None) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        smr = to_float(jget(j, "macro.overview.SMR"))
        if smr is not None and smr < 0:
            reasons.append(f"macro.overview.SMR={smr:.4f} -> SMR小於0")
        if market_state and "防禦" in market_state:
            reasons.append("市場狀態 = 防禦模式")
            
        cons = jget(j, "portfolio.performance.consecutive_losses", None)
        dd = to_float(jget(j, "portfolio.performance.drawdown_pct", None))
        
        if cons is not None:
            try:
                if int(cons) >= 3:
                    reasons.append(f"連續虧損次數={cons} >= 3次")
            except Exception:
                pass
        if dd is not None and dd >= 0.08:
            reasons.append(f"資金回撤比例={dd*100:.1f}% >= 8%")
            
        return (len(reasons) > 0, reasons)

    # -------------------------
    # L1 數據審計
    # -------------------------
    def run_l1(self, j: Json) -> Json:
        fatal: List[str] = []
        warn: List[str] = []

        twii = jget(j, "macro.overview.twii_close", None)
        if twii is None:
            fatal.append('缺失大盤收盤價 -> 拒絕執行 (path=$.macro.overview.twii_close)')
            
        if jget(j, "macro.integrity.kill") is True:
            fatal.append('偵測到系統中斷指令 -> 拒絕執行 (path=$.macro.integrity.kill=true)')
            
        conf = jget(j, "meta.confidence_level")
        ms = jget(j, "meta.market_status")
        if conf == "LOW" and ms == "NORMAL":
            fatal.append('數據信心低落卻判定為正常市場 -> 邏輯矛盾 (path=$.meta.confidence_level)')
            
        amt_raw = to_float(jget(j, "macro.market_amount.amount_total_raw"))
        amt_blend = to_float(jget(j, "macro.market_amount.amount_total_blended"))
        if amt_raw is not None and amt_blend is not None and amt_blend < amt_raw:
            fatal.append(f"成交量估算異常：總量 < 原始量 -> 數據污染 (path=$.macro.market_amount)")
            
        if jget(j, "meta.is_using_previous_day") is True and not exists(j, "meta.effective_trade_date"):
            fatal.append("使用前日資料卻未提供生效日期 -> 拒絕執行")

        stocks = jget(j, "stocks", [])
        if isinstance(stocks, list):
            for i, s in enumerate(stocks):
                st = jget(j, f"stocks[{i}].Institutional.Inst_Status")
                net3 = jget(j, f"stocks[{i}].Institutional.Inst_Net_3d", None)
                if st == "NO_UPDATE_TODAY" and net3 is not None:
                    fatal.append(f'股票 {jget(j, f"stocks[{i}].Symbol")} 法人籌碼未更新卻有數值 -> 數據污染')
                
                sym = jget(j, f"stocks[{i}].Symbol")
                px = to_float(jget(j, f"stocks[{i}].Price"))
                if sym == "2330.TW" and px is not None and px < 200:
                    fatal.append(f"價格錯位：台積電價格小於200 -> 拒絕執行")

        verdict = "通過 (PASS)" if len(fatal) == 0 else "失敗 (FAIL)"
        risk = "低危險 (LOW)" if verdict == "通過 (PASS)" else "極高危險 (CRITICAL)"

        return {
            "模式": "L1_數據審計 (L1_AUDIT)",
            "裁決結果": verdict,
            "風險等級": risk,
            "致命缺失": fatal,
            "結構性警告": warn,
            "審計信心": "高 (HIGH)"
        }

    # -------------------------
    # L2 交易裁決
    # -------------------------
    def run_l2(self, j: Json, l1_verdict: str = "PASS") -> str:
        maxeq = to_float(jget(j, "macro.overview.max_equity_allowed_pct"))
        if maxeq is None:
            return "模式: L2_交易裁決\n決策: 不進行交易 (NO TRADE)\n風險原因: 缺失最大資金曝險上限 (max_equity_allowed_pct)\n信心水準: 低 (LOW)"

        war, war_reason = self.v17_triggered(j)
        ms, ms_reason = self.market_state(j)
        conf_level = jget(j, "meta.confidence_level", "MEDIUM")

        scale = 1.0
        scale_note = ""
        if conf_level == "LOW":
            scale = 0.5
            scale_note = "【低信心降級防護】: 所有資金配置強制減半 (x0.5)"

        if war:
            maxeq = min(maxeq, 0.05)

        lines: List[str] = []
        lines.append("模式: L2_交易裁決 (L2_EXECUTE)")
        lines.append(f"市場狀態: {ms}")
        lines.append(f"最大合法曝險: {maxeq*100:.1f}%")
        if scale_note:
            lines.append(scale_note)

        if war:
            lines.append("決策:")
            lines.append("- 不進行交易 (NO TRADE)")
            lines.append("風險原因: " + " | ".join([f"{r} -> 觸發戰時防禦" for r in war_reason]))
            lines.append("信心水準: 低 (LOW)")
            return "\n".join(lines)

        if "過熱" in ms:
            lines.append("決策:")
            lines.append("- 不進行交易 (NO TRADE) - 禁止開倉")
            lines.append("風險原因: " + " | ".join([f"{r} -> 市場過熱保護" for r in ms_reason]))
            lines.append(f"信心水準: {conf_level}")
            return "\n".join(lines)

        lines.append("決策:")
        lines.append("- 不進行交易 (NO TRADE)")
        lines.append("風險原因: 依據嚴格 JSON 策略 -> 當前無明確交易訊號")
        lines.append(f"信心水準: {conf_level}")
        return "\n".join(lines)

    # -------------------------
    # L3 回撤壓測
    # -------------------------
    def run_l3(self, j: Json) -> str:
        ms, _ = self.market_state(j)
        trig, reasons = self.l3_triggered(j, market_state=ms)
        
        if not trig:
            return "模式: L3_回撤壓測\n壓測狀態: 未啟動 (NOT ACTIVATED)\n最終判定: 系統安全存活 (SYSTEM SURVIVES)"

        dd = to_float(jget(j, "portfolio.performance.drawdown_pct"))
        cons = jget(j, "portfolio.performance.consecutive_losses")
        
        breach = False
        if dd is not None and dd >= 0.15:
            breach = True

        def scen(x: float) -> str:
            if dd is None: return "警告 (WARNING)"
            if dd + x >= 0.15: return "防線崩潰 (FAILURE)"
            if dd + x >= 0.10: return "警告 (WARNING)"
            return "安全 (SAFE)"

        surv = 80
        if "防禦" in ms: surv -= 15
        if dd is not None: surv -= int(dd * 100)
        if cons is not None:
            try: surv -= int(cons) * 3
            except Exception: pass
            
        surv = max(0, min(100, surv))
        
        sys_status = "穩定 (STABLE)"
        final = "系統存活 (SYSTEM_SURVIVES)"
        if breach or surv < 40:
            sys_status = "極度危險 (CRITICAL)"
            final = "系統崩潰 (SYSTEM_FAILURE)"
        elif surv < 60:
            sys_status = "脆弱 (FRAGILE)"
            final = "系統處於風險中 (SYSTEM_AT_RISK)"

        lines: List[str] = []
        lines.append("模式: L3_回撤壓測 (L3_STRESS)")
        lines.append("壓測狀態: 已啟動 (ACTIVATED)")
        lines.append(f"結構性崩潰: {'是 (TRUE)' if breach else '否 (FALSE)'}")
        lines.append(f"面臨 5% 跌幅情境: {scen(0.05)}")
        lines.append(f"面臨 10% 跌幅情境: {scen(0.10)}")
        lines.append(f"面臨 15% 跌幅情境: {scen(0.15)}")
        lines.append("心理層面風險: " + ("高 (HIGH)" if surv < 50 else "中 (MEDIUM)" if surv < 70 else "低 (LOW)"))
        lines.append(f"系統存活分數: {surv} / 100")
        lines.append(f"系統穩定度: {sys_status}")
        lines.append(f"最終判定: {final}")
        lines.append("觸發原因: " + " | ".join([f"{r} -> L3_TRIGGER" for r in reasons]))
        return "\n".join(lines)

    # -------------------------
    # 頂層執行
    # -------------------------
    def run(self, j: Json, run_mode: str = "L1") -> Union[Json, str]:
        run_mode = (run_mode or "L1").strip().upper()
        if run_mode not in ("L1", "L2", "L3"):
            run_mode = "L1"

        l1 = self.run_l1(j)
        l1_verdict = l1.get("裁決結果")

        if run_mode == "L1":
            return l1

        if run_mode == "L2" and "失敗" in l1_verdict:
            fatal = list(l1.get("致命缺失", []))
            fatal.insert(0, f"違規操作: 要求進入 L2，但 L1 數據審計未通過！")
            l1["致命缺失"] = fatal
            l1["裁決結果"] = "失敗 (FAIL) - 阻擋越權"
            l1["風險等級"] = "極高危險 (CRITICAL)"
            return l1

        if run_mode == "L2":
            return self.run_l2(j, l1_verdict=l1_verdict)

        if run_mode == "L3":
            ms, _ = self.market_state(j)
            trig, _ = self.l3_triggered(j, market_state=ms)
            if not trig:
                fatal = list(l1.get("致命缺失", []))
                fatal.insert(0, "違規操作: 請求 L3 壓測，但未滿足市場觸發條件！")
                l1["致命缺失"] = fatal
                l1["裁決結果"] = "失敗 (FAIL) - 阻擋越權"
                return l1
            return self.run_l3(j)

        return l1
