# institutional_utils.py
import pandas as pd
from datetime import datetime, timedelta

NEUTRAL_THRESHOLD = 5_000_000  # 5000 萬

def normalize_inst_direction(net):
    if abs(net) < NEUTRAL_THRESHOLD:
        return "NEUTRAL"
    return "POSITIVE" if net > 0 else "NEGATIVE"


def calc_inst_3d(inst_df: pd.DataFrame, symbol: str, trade_date: str):
    """
    inst_df 欄位需求：
    - date (YYYY-MM-DD)
    - symbol
    - net_amount
    """
    df = inst_df[inst_df["symbol"] == symbol].sort_values("date").tail(3)

    if len(df) < 3:
        return {
            "Inst_Status": "PENDING",
            "Inst_Streak3": 0,
            "Inst_Dir3": "PENDING",
            "Inst_Net_3d": 0.0,
        }

    dirs = [normalize_inst_direction(x) for x in df["net_amount"]]
    net_sum = df["net_amount"].sum()

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
    }
