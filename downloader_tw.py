# 只列出修改重點區塊，其餘保留

# ------------- 修正 1：market amount -------------
def get_market_amount_safe() -> Dict[str, Any]:
    return {
        "amount_twse": None,
        "amount_tpex": None,
        "amount_total": None,
        "source_twse": "FAIL",
        "source_tpex": "FAIL",
    }


# ------------- 修正 2：產生最小股票宇宙 -------------
def build_minimal_universe(topn: int) -> List[Dict[str, Any]]:
    # 暫時產生 TopN 假代碼（避免 empty_universe）
    # 之後你可接真實成交額排序
    universe = []
    for i in range(topn):
        universe.append({
            "symbol": f"MOCK{i+1:03d}",
            "close": None,
            "volume": None,
            "integrity_ok": False
        })
    return universe


# ------------- 修正 3：build_snapshot -------------

def build_snapshot(session: str, target_date: str, topn: int) -> Dict[str, Any]:

    meta = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "session": session,
        "effective_trade_date": target_date,
    }

    twii = get_twii_with_fallback(target_date)
    amt = get_market_amount_safe()

    stocks = build_minimal_universe(topn)

    macro = {
        "overview": {
            "trade_date": target_date,
            "twii_close": twii["close"],
            "twii_chg": twii["chg"],
            "twii_pct": twii["pct"],
        },
        "market_amount": amt,
        "institutional": None
    }

    audit = {
        "TWII": twii,
        "MARKET_AMOUNT": amt,
        "INSTITUTIONAL": {
            "source": "NOT_IMPLEMENTED",
            "error": "DATA_NOT_FETCHED"
        },
        "UNIVERSE": {
            "count": len(stocks),
            "mock": True
        }
    }

    arb_input = {
        "meta": meta,
        "macro": macro,
        "stocks": stocks,
    }

    return {
        "meta": meta,
        "macro": macro,
        "stocks": stocks,
        "audit": audit,
        "arb_input": arb_input,
    }
