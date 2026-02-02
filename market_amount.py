# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

import requests
import pandas as pd
import certifi

TZ_TAIPEI = timezone(timedelta(hours=8))

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)
TRADING_MINUTES = 270  # 09:00~13:30

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://streamlit.app/)"
}

# -----------------------
# Utils
# -----------------------
def _to_int_amount(x) -> int:
    if x is None:
        return 0
    s = str(x)
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else 0

def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def trading_progress(now: Optional[datetime] = None) -> float:
    now = now or _now_taipei()
    start_dt = now.replace(hour=TRADING_START.hour, minute=TRADING_START.minute, second=0, microsecond=0)
    end_dt = now.replace(hour=TRADING_END.hour, minute=TRADING_END.minute, second=0, microsecond=0)

    if now <= start_dt:
        return 0.0
    if now >= end_dt:
        return 1.0
    elapsed = (now - start_dt).total_seconds() / 60.0
    return max(0.0, min(1.0, elapsed / TRADING_MINUTES))

def progress_curve(p: float, alpha: float = 0.65) -> float:
    p = max(0.0, min(1.0, p))
    return p ** alpha

@dataclass
class MarketAmount:
    amount_twse: int
    amount_tpex: int
    amount_total: int
    source_twse: str
    source_tpex: str
    ssl_mode: str
    errors: Dict[str, str]

def http_get(url: str, timeout: int = 20, verify_ssl: bool = True) -> requests.Response:
    """
    verify_ssl=True: 使用 certifi CA bundle（建議）
    verify_ssl=False: 不驗證 SSL（最後防線，需 Gate 降級）
    """
    verify = certifi.where() if verify_ssl else False
    return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=verify)

# -----------------------
# TWSE: use OpenAPI (avoid www.twse.com.tw SSL issues)
# -----------------------
def fetch_twse_amount_openapi(verify_ssl: bool = True) -> Tuple[int, str, Optional[str]]:
    """
    上市成交金額（元）：TWSE OpenAPI STOCK_DAY_ALL，把 TradeValue 加總。
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    try:
        r = http_get(url, timeout=25, verify_ssl=verify_ssl)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data)
        if df.empty or "TradeValue" not in df.columns:
            return 0, "TWSE OpenAPI STOCK_DAY_ALL (schema/empty)", "TWSE_EMPTY_OR_SCHEMA_CHANGED"

        # TradeValue 多為字串數字（元）
        tv = df["TradeValue"].apply(_to_int_amount)
        amount = int(tv.sum())
        return amount, "TWSE OpenAPI STOCK_DAY_ALL TradeValue sum", None
    except Exception as e:
        return 0, "TWSE OpenAPI STOCK_DAY_ALL", f"TWSE_ERR:{type(e).__name__}"

# -----------------------
# TPEx: best-effort (HTML summary)
# -----------------------
def fetch_tpex_amount_html(verify_ssl: bool = True) -> Tuple[int, str, Optional[str]]:
    """
    上櫃成交金額（元）：TPEx pricing.html 內文字抓「總成交金額」。
    若抓不到，回傳 error（不 raise）。
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    try:
        r = http_get(url, timeout=25, verify_ssl=verify_ssl)
        r.raise_for_status()

        # 常見：總成交金額: 175,152,956,339元
        m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
        if not m:
            m2 = re.search(r"總成交金額.*?([\d,]+)\s*元", r.text)
            if not m2:
                return 0, "TPEx pricing.html", "TPEX_TOTAL_AMOUNT_NOT_FOUND"
            amount = _to_int_amount(m2.group(1))
        else:
            amount = _to_int_amount(m.group(1))

        return int(amount), "TPEx pricing.html 總成交金額", None
    except Exception as e:
        return 0, "TPEx pricing.html", f"TPEX_ERR:{type(e).__name__}"

# -----------------------
# Total
# -----------------------
def fetch_amount_total(verify_ssl: bool = True) -> MarketAmount:
    errors: Dict[str, str] = {}
    ssl_mode = "VERIFY_CERTIFI" if verify_ssl else "INSECURE_NO_VERIFY"

    twse, s1, e1 = fetch_twse_amount_openapi(verify_ssl=verify_ssl)
    if e1:
        errors["twse"] = e1

    tpex, s2, e2 = fetch_tpex_amount_html(verify_ssl=verify_ssl)
    if e2:
        errors["tpex"] = e2

    return MarketAmount(
        amount_twse=twse,
        amount_tpex=tpex,
        amount_total=twse + tpex,
        source_twse=s1,
        source_tpex=s2,
        ssl_mode=ssl_mode,
        errors=errors,
    )

def classify_ratio(r: float) -> str:
    if r < 0.8:
        return "LOW"
    if r > 1.2:
        return "HIGH"
    return "NORMAL"

def intraday_norm(
    amount_total_now: int,
    amount_total_prev: Optional[int],
    avg20_amount_total: Optional[int],
    now: Optional[datetime] = None,
    alpha: float = 0.65,
) -> dict:
    now = now or _now_taipei()
    p_now = trading_progress(now)
    p_prev = max(0.0, p_now - (5 / TRADING_MINUTES))

    out = {
        "progress": round(p_now, 4),
        "amount_norm_cum_ratio": None,
        "amount_norm_slice_ratio": None,
        "amount_norm_label": "UNKNOWN",
    }

    if not avg20_amount_total or avg20_amount_total <= 0:
        return out

    expected_cum = avg20_amount_total * progress_curve(p_now, alpha=alpha)
    cum_ratio = (amount_total_now / expected_cum) if expected_cum > 0 else None
    out["amount_norm_cum_ratio"] = None if cum_ratio is None else round(float(cum_ratio), 4)
    if cum_ratio is not None:
        out["amount_norm_label"] = classify_ratio(float(cum_ratio))

    if amount_total_prev is not None:
        slice_amount = max(0, amount_total_now - amount_total_prev)
        expected_slice = avg20_amount_total * (progress_curve(p_now, alpha=alpha) - progress_curve(p_prev, alpha=alpha))
        slice_ratio = (slice_amount / expected_slice) if expected_slice > 0 else None
        out["amount_norm_slice_ratio"] = None if slice_ratio is None else round(float(slice_ratio), 4)

    return out
