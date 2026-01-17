# finmind_institutional.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

# 以「三大法人」常用組成：外資 + 投信 + 自營商（含避險）
# 你若要把 Foreign_Dealer_Self 視為外資的一部分，可加入此清單或另計。
A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}
B_FOREIGN_NAME = "Foreign_Investor"


def _headers(token: Optional[str]) -> dict:
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


def _get(dataset: str, params: dict, token: Optional[str]) -> dict:
    p = {"dataset": dataset, **params}
    r = requests.get(FINMIND_URL, headers=_headers(token), params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_finmind_institutional(
    symbols: List[str],
    start_date: str,
    end_date: str,
    token: Optional[str] = None,
) -> pd.DataFrame:
    """
    下載「個股法人買賣表 TaiwanStockInstitutionalInvestorsBuySell」
    回傳欄位：date, symbol, net_amount
    - net_amount = Σ(buy - sell) for name in A_NAMES
    參考文件：籌碼面 / TaiwanStockInstitutionalInvestorsBuySell（含 Schema） :contentReference[oaicite:3]{index=3}
    """
    rows = []
    for sym in symbols:
        stock_id = sym.replace(".TW", "").strip()

        js = _get(
            dataset="TaiwanStockInstitutionalInvestorsBuySell",
            params={"data_id": stock_id, "start_date": start_date, "end_date": end_date},
            token=token,
        )
        data = js.get("data", []) or []
        if not data:
            continue

        df = pd.DataFrame(data)
        # expected columns: date, stock_id, buy, name, sell :contentReference[oaicite:4]{index=4}
        need = {"date", "stock_id", "buy", "name", "sell"}
        if not need.issubset(set(df.columns)):
            continue

        df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
        df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)

        df = df[df["name"].isin(A_NAMES)].copy()
        if df.empty:
            continue

        df["net"] = df["buy"] - df["sell"]
        g = df.groupby("date", as_index=False)["net"].sum()
        for _, r in g.iterrows():
            rows.append(
                {
                    "date": str(r["date"]),
                    "symbol": sym,
                    "net_amount": float(r["net"]),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "net_amount"])

    out = pd.DataFrame(rows).sort_values(["symbol", "date"])
    return out


def fetch_finmind_market_inst_net_ab(
    trade_date: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, float]:
    """
    下載「台灣市場整體法人買賣表 TaiwanStockTotalInstitutionalInvestors」
    回傳：
      {
        "A": 三大法人合計淨額 (外資+投信+自營商(自營+避險)),
        "B": 外資淨額 (Foreign_Investor),
      }
    參考文件：TaiwanStockTotalInstitutionalInvestors（含 Schema） :contentReference[oaicite:5]{index=5}
    """
    # FinMind 允許 start/end 區間；這裡最穩定做法：抓一小段再取 trade_date
    start_date = start_date or trade_date
    end_date = end_date or trade_date

    js = _get(
        dataset="TaiwanStockTotalInstitutionalInvestors",
        params={"start_date": start_date, "end_date": end_date},
        token=token,
    )
    data = js.get("data", []) or []
    if not data:
        return {"A": 0.0, "B": 0.0}

    df = pd.DataFrame(data)
    # expected columns: buy, date, name, sell :contentReference[oaicite:6]{index=6}
    need = {"buy", "sell", "date", "name"}
    if not need.issubset(set(df.columns)):
        return {"A": 0.0, "B": 0.0}

    df = df[df["date"].astype(str) == trade_date].copy()
    if df.empty:
        return {"A": 0.0, "B": 0.0}

    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
    df["net"] = df["buy"] - df["sell"]

    a = float(df[df["name"].isin(A_NAMES)]["net"].sum())
    b = float(df[df["name"] == B_FOREIGN_NAME]["net"].sum())

    return {"A": a, "B": b}
