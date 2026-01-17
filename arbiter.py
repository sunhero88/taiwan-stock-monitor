# arbiter.py
def arbitrate(stock, macro, account="Conservative"):
    """
    回傳單一股票的最終裁決
    """

    # ---------- Step 1：Data Health Gate ----------
    degraded = (
        macro.get("inst_status") != "READY"
        or macro.get("kill_switch", False)
        or macro.get("v14_watch", False)
    )

    inst = stock.get("Institutional", {})
    tech = stock.get("Technical", {})
    struct = stock.get("Structure", {})

    decision = "WATCH"
    action_size = 0
    exit_code = "None"

    # ---------- 降級模式 ----------
    if degraded:
        return {
            "Decision": "WATCH",
            "action_size_pct": 0,
            "exit_reason_code": "DATA_DEGRADED",
            "degraded_note": "資料降級：是（禁止 BUY）",
        }

    # ---------- Step 2：法人連續性 ----------
    inst_ok = (
        inst.get("Inst_Status") == "READY"
        and inst.get("Inst_Streak3", 0) >= 3
        and inst.get("Inst_Dir3") == "POSITIVE"
    )

    # ---------- Step 3：帳戶分流 ----------
    if account == "Conservative":
        if (
            tech.get("Score", 0) >= 50
            and struct.get("Rev_Growth", -999) >= 0
            and inst_ok
        ):
            decision = "BUY"
            action_size = 10
        else:
            decision = "WATCH"

    elif account == "Aggressive":
        if tech.get("Score", 0) >= 45 and inst.get("Inst_Dir3") != "NEGATIVE":
            decision = "TRIAL"
            action_size = 5

    return {
        "Decision": decision,
        "action_size_pct": action_size,
        "exit_reason_code": exit_code,
        "degraded_note": "資料降級：否",
    }
