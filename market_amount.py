# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Tuple, Dict

import requests
import pandas as pd

TZ_TAIPEI = timezone(timedelta(hours=8))

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() == "nan":
            return default
        return int(float(s))
    except Exception:
        return default

@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
    warnings: list

def fetch_twse_amount_from_openapi(timeout: int = 20) -> Tuple[int, str]:
    """
    TWSE 成交金額：用 OpenAPI STOCK_DAY_ALL 的 TradeValue 合計（元）
    優點：免費、穩定、免 date 參數、適合 Streamlit Cloud。
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    r = requests.get(url, headers=USER_AGENT, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        raise RuntimeError("TWSE OpenAPI STOCK_DAY_ALL 回傳空資料")

    df = pd.DataFrame(data)
    if "TradeValue" not in df.columns:
        raise RuntimeError(f"TWSE STOCK_DAY_ALL 缺 TradeValue 欄位，現有欄位={list(df.columns)}")

    amt = int(df["TradeValue"].apply(safe_int).sum())
    return amt, "TWSE OpenAPI STOCK_DAY_ALL.TradeValue 合計(元)"

def fetch_tpex_amount_best_effort(timeout: int = 20) -> Tuple[Optional[int], str]:
    """
    TPEx 成交金額：免費來源較不穩（頁面常改版/JS渲染）。
    這裡採 best-effort；抓不到就回 None，但不致使主流程崩潰。
    """
    # 你原本用的 pricing.html 可能改版/JS；這裡改抓「上櫃股價指數收盤行情」頁面不一定含總成交金額。
    # 若未來你找到穩定的 TPEx JSON 端點，直接在這裡替換即可。
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    try:
        r = requests.get(url, headers=USER_AGENT, timeout=timeout)
        r.raise_for_status()
        # 嘗試抓「總成交金額: 1,234,567,890 元」
        m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
        if not m:
            return None, "TPEx pricing.html 未找到『總成交金額』(best-effort)"
        return safe_int(m.group(1)), "TPEx pricing.html 總成交金額(best-effort)"
    except Exception as e:
        return None, f"TPEx amount ERR(best-effort): {e}"

def fetch_amount_total_latest() -> MarketAmount:
    warnings = []
    twse_amt, twse_src = None, ""
    tpex_amt, tpex_src = None, ""

    try:
        twse_amt, twse_src = fetch_twse_amount_from_openapi()
    except Exception as e:
        warnings.append(f"TWSE amount 取得失敗: {e}")
        twse_src = f"ERR:{e}"

    tpex_amt, tpex_src = fetch_tpex_amount_best_effort()
    if tpex_amt is None:
        warnings.append("TPEx amount 不可用（免費 best-effort 失敗）")

    total = None
    if twse_amt is not None and tpex_amt is not None:
        total = twse_amt + tpex_amt

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        warnings=warnings,
    )
