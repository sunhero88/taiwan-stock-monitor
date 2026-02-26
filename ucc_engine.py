# ucc_engine.py
# -*- coding: utf-8 -*-
"""
UCC Engine (Stable / Versionless)

這裡放你原本 ucc_v19_1.py 的核心裁決邏輯，但不綁版本號檔名，避免主流程耦合。
"""

from __future__ import annotations
from typing import Any, Dict


class UCCEngine:
    def run(self, payload: Dict[str, Any], run_mode: str = "L2") -> Dict[str, Any]:
        """
        TODO:
        1) 把 ucc_v19_1.py 裡面的核心 run() 內容搬過來
        2) 保持輸出欄位一致（OPEN/ADD/HOLD/REDUCE/CLOSE/NO_TRADE 等）
        """
        # === 先給一個不會炸的最小實作（你搬完邏輯後刪掉這段） ===
        return {
            "MODE": f"{run_mode}_ENGINE",
            "DECISION": "HOLD",
            "OPEN": [],
            "ADD": [],
            "HOLD": [{"reason": "ENGINE_STUB"}],
            "REDUCE": [],
            "CLOSE": [],
            "NO_TRADE": [],
            "RISK_REASON": "ENGINE_STUB",
            "CONFIDENCE": "—",
        }
