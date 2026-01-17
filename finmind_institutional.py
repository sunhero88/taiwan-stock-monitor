# finmind_institutional.py
# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from datetime import datetime, timedelta

FINMIND_API = "https://api.finmindtrade.com/api/v4/data"


def _to_stock_id(symbol: str) -> str:
    """
    '2330.TW' -> '2330'
    '2603.TW' -> '2603'
    """
    return str(symbol).split(".")[0]


def fetch_finmind_institutional(
    symbols,
    start_date: str,
    end_date: str,
    token: str | None = None,
    timeout: int = 20,
) -> pd.DataFrame:
    """
    下載 FinMind 三大法人資料，回傳欄位：
    - date: YYYY-MM-DD
    - symbol: e.g. 2330.TW
    - net_amount: float (元)

    dataset 使用：
    InstitutionalInvestorsBuySell

    注意：
    1) 你必須提供 token（建議放環境變數 FINMIND_TOKEN）
    2) 若 API 沒資料或被限流，回傳會是空 DF
    """
    token = token or os.getenv("FINMIND_TOKEN", "")
    if not token:
        raise RuntimeError("FINMIND_TOKEN is missing. Please set env var FINMIND_TOKEN.")

    rows = []
    for sym in symbols:
        stock_id = _to_stock_id(sym)

        params = {
            "dataset": "InstitutionalInvestorsBuySell",
            "data_id": stock_id,
            "start_date": start_date,
            "end_date": end_date,
            "token": token,
        }

        r = requests.get(FINMIND_API, params=params, timeout=timeout)
        j = r.json()

        # FinMind 成功通常 status=200，data 是 list
        data = j.get("data", [])
        if not data:
            continue

        df = pd.DataFrame(data)

        # 常見欄位（實際欄位依 FinMind dataset 定義）
        # date, stock_id, name, buy, sell, investor 等
        # 我們先做「分組加總」：每一天把所有 investor 的 (buy - sell) 加總成 net_amount
        if "date" not in df.columns:
            continue

        # buy/sell 欄位若不存在就跳過
        if "buy" not in df.columns or "sell" not in df.columns:
            continue

        df["net_amount"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0) - pd.to_numeric(df["sell"], errors="coerce").fillna(0)

        g = (
            df.groupby("date", as_index=False)["net_amount"]
            .sum()
            .sort_values("date")
        )

        g["symbol"] = f"{stock_id}.TW"
        rows.append(g[["date", "symbol", "net_amount"]])

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "net_amount"])

    out = pd.concat(rows, ignore_index=True)
    out["date"] = out["date"].astype(str)
    out["symbol"] = out["symbol"].astype(str)
    out["net_amount"] = pd.to_numeric(out["net_amount"], errors="coerce").fillna(0.0)
    return out


def infer_trade_date_range(days_back: int = 10):
    """
    給你一個方便用的日期範圍：今天往回 days_back 天
    """
    end = datetime.now().date()
    start = end - timedelta(days=days_back)
    return str(start), str(end)
