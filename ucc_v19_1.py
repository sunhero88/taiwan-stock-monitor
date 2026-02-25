# ucc_v19_1.py
# =========================================================
# Predator UCC V19.1 Hardened Final Lockdown (全中文裁決版)
# =========================================================
from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple, Union

Json = Dict[str, Any]

def jget(d: Any, path: str, default: Any = None) -> Any:
    cur = d
    for tok in re.split(r"\.(?![^\[]*\])", path):
        if not tok: continue
        m = re.fullmatch(r"([^\[]+)(\[(\-?\d+)\])?", tok)
        if not m: return default
        key, idx = m.group(1), m.group(3)
        if not isinstance(cur, dict) or key not in cur: return default
        cur = cur[key]
        if idx is not None:
            if not isinstance(cur, list): return default
            i = int(idx)
            if i < 0 or i >= len(cur): return default
            cur = cur[i]
    return cur

class UCCv19_1:
    def run_l1(self, j: Json) -> Json:
        fatal = []
        twii = jget(j, "macro.overview.twii_close")
        if twii is None:
            fatal.append("缺失大盤收盤價 (path=$.macro.overview.twii_close) ")
        if jget(j, "macro.integrity.kill") is True:
            fatal.append("偵測到系統中斷指令 (kill=true) [cite: 671]")
            
        verdict = "通過 (PASS)" if not fatal else "失敗 (FAIL)"
        risk = "低 (LOW)" if not fatal else "極高 (CRITICAL)"
        
        return {
            "模式": "L1_數據審計官 [cite: 700]",
            "裁決結果": verdict,
            "風險等級": risk,
            "致命缺失": fatal,
            "審計信心": "高 (HIGH)"
        }

    def run_l2(self, j: Json) -> str:
        maxeq = jget(j, "macro.overview.max_equity_allowed_pct")
        if maxeq is None: return "模式: L2_交易裁決官\n決策: 不進行交易\n原因: 缺失最大曝險上限 [cite: 714]"
        
        smr = jget(j, "macro.overview.SMR")
        state = "過熱 (OVERHEAT)" if smr and smr >= 0.33 else "正常 (NORMAL) [cite: 633, 638]"
        
        res = [
            "模式: L2_交易裁決官 ",
            f"市場狀態: {state}",
            f"最大合法曝險: {float(maxeq)*100:.1f}% [cite: 731]",
            "\n決策:",
            "- 觀察中 (HOLD): 等待趨勢確認",
            f"\n風險理由: [SMR={smr:.4f} → 過熱保護邏輯] [cite: 745]"
        ]
        return "\n".join(res)

    def run(self, j: Json, run_mode: str = "L1") -> Union[Json, str]:
        if run_mode == "L2": return self.run_l2(j)
        return self.run_l1(j)
