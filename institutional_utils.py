# institutional_utils.py
# -*- coding: utf-8 -*-

import pandas as pd

# 小幅買賣視為中性（5000 萬）
NEUTRAL_THRESHOLD = 5_000_000


def normalize_inst_direction(net: float) -> str:
    """
    方向定義（生成端硬規則）：
    - |net| < 5000萬 => NEUTRAL
    - net > 0 => POSITIVE
    - net < 0 => NEGATIVE
    """
    try:
        net = float(net)
    except Exception:
        return "NEUTRAL"

    if abs(net) < NEUTRAL_THRESHOLD:
        return "NEUTRAL"
    return "POSITIVE" if net > 0 else "NEGATIVE"


def calc_inst_3d(inst_df: pd.DataFrame, symbol: str, trade_date: str):
    """
    inst_df 欄位需求：
      - date (YYYY-MM-DD)
      - symbol
      - net_amount

    回傳：
      Inst_Status: READY/PENDING
      Inst_Streak3: 3 or 0
      Inst_Dir3: POSITIVE/NEGATIVE/NEUTRAL/PENDING
      Inst_Net_3d: 三日合計
      Inst_Net_Last3: [d1, d2, d3]（依日期排序）
      Inst_Dates_Last3: [date1, date2, date3]
    """
    if inst_df is None or inst_df.empty:
        return {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
            "Inst_Net_Last3": [],
            "Inst_Dates_Last3": [],
        }

    df = inst_df[inst_df["symbol"] == symbol].copy()
    if df.empty:
        return {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
            "Inst_Net_Last3": [],
            "Inst_Dates_Last3": [],
        }

    df = df.sort_values("date").tail(3)

    if len(df) < 3:
        return {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
            "Inst_Net_Last3": df["net_amount"].tolist() if "net_amount" in df.columns else [],
            "Inst_Dates_Last3": df["date"].tolist() if "date" in df.columns else [],
        }

    net_last3 = [float(x) for x in df["net_amount"].tolist()]
    dates_last3 = [str(x) for x in df["date"].tolist()]
    dirs = [normalize_inst_direction(x) for x in net_last3]
    net_sum = float(sum(net_last3))

    # 三日同向才亮燈：streak=3，否則 streak=0
    if all(d == "POSITIVE" for d in dirs):
        streak = 3
        direction = "POSITIVE"
    elif all(d == "NEGATIVE" for d in dirs):
        streak = 3
        direction = "NEGATIVE"
    else:
        streak = 0
        direction = "NEUTRAL"

    return {
        "Inst_Status": "READY",
        "Inst_Streak3": streak,
        "Inst_Dir3": direction,
        "Inst_Net_3d": net_sum,
        "Inst_Net_Last3": net_last3,
        "Inst_Dates_Last3": dates_last3,
    }
