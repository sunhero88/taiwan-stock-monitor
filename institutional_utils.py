# institutional_utils.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd

NEUTRAL_THRESHOLD = 5_000_000  # 5,000 萬（你指定的閾值）


def normalize_inst_direction(net: float) -> str:
    net = float(net or 0.0)
    if abs(net) < NEUTRAL_THRESHOLD:
        return "NEUTRAL"
    return "POSITIVE" if net > 0 else "NEGATIVE"


def calc_inst_3d(inst_df: pd.DataFrame, symbol: str, trade_date: str):
    """
    inst_df 欄位需求：
    - date (YYYY-MM-DD)
    - symbol
    - net_amount
    回傳欄位：
    - Inst_Status: READY/PENDING
    - Inst_Streak3: 3 或 0（符合你 V15.6.x 硬規則：需連3日同向）
    - Inst_Dir3: POSITIVE/NEGATIVE/NEUTRAL/PENDING
    - Inst_Net_3d: 三日加總
    """
    if inst_df is None or inst_df.empty:
        return {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
        }

    df = inst_df[inst_df["symbol"] == symbol].copy()
    if df.empty:
        return {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
        }

    df = df.sort_values("date").tail(3)
    if len(df) < 3:
        return {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
        }

    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0)
    dirs = [normalize_inst_direction(x) for x in df["net_amount"]]
    net_sum = float(df["net_amount"].sum())

    # 連3日同向才給 streak=3；中性或混合一律 streak=0（符合你的硬規則）
    if all(d == "POSITIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "POSITIVE", "Inst_Net_3d": net_sum}
    if all(d == "NEGATIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "NEGATIVE", "Inst_Net_3d": net_sum}

    # 有 NEUTRAL 或混合
    return {"Inst_Status": "READY", "Inst_Streak3": 0, "Inst_Dir3": "NEUTRAL", "Inst_Net_3d": net_sum}
