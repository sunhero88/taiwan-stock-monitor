# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple

import certifi
import requests
import pandas as pd


TZ_TAIPEI = timezone(timedelta(hours=8))

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)
TRADING_MINUTES = 270  # 09:00~13:30

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

# 安全開關：預設不關 SSL。只有你明確設環境變數才會關。
# Streamlit Cloud -> Settings -> Secrets / Env
ALLOW_INSECURE_SSL = str(os.getenv("ALLOW_INSECURE_SSL", "0")).strip() in ("1", "true", "TRUE", "yes", "YES")


def _requests_get(url: str, timeout: int = 15) -> requests.Response:
    """
    先用正常 SSL（certifi）連；
    若遇到 SSL 錯誤且 ALLOW_INSECURE_SSL=1 才退到 verify=False。
    """
    try:
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not ALLOW_INSECURE_SSL:
            raise
        # 退而求其次：僅在你允許時使用
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=False)


def _to_int_amount(x) -> int:
    """把 '775,402,495,419' 之類字串轉 int"""
    if x is None:
        return 0
    s = str(x)
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else 0


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def trading_progress(now: Optional[datetime] = None) -> float:
    """回傳盤中進度 0~1。盤外 clamp 到 0 或 1。"""
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
    """用冪次曲線做『盤中累積量能』預期，避免早盤被誤判 LOW。"""
    p = max(0.0, min(1.0, p))
    return p ** alpha


@dataclass
class MarketAmount:
    amount_twse: int
    amount_tpex: int
    amount_total: int
    source_twse: str
    source_tpex: str


def fetch_twse_amount() -> Tuple[int, str]:
    """
    上市成交金額（元）：TWSE MI_INDEX(HTML) 成交統計表，各類別成交金額加總。
    """
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?date=&response=html"
    r = _requests_get(url, timeout=15)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("TWSE MI_INDEX 找不到可解析表格")

    target = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("成交金額" in c for c in cols) and any("成交統計" in c for c in cols):
            target = t
            break
    if target is None:
        target = tables[0]

    amt_col = None
    for c in target.columns:
        if "成交金額" in str(c):
            amt_col = c
            break
    if amt_col is None:
        raise RuntimeError("TWSE 成交統計表找不到『成交金額』欄")

    amount = int(target[amt_col].apply(_to_int_amount).sum())
    return amount, "TWSE MI_INDEX(HTML) 成交統計各類別加總"


def fetch_tpex_amount() -> Tuple[int, str]:
    """
    上櫃成交金額（元）：TPEx 行情頁的「總成交金額」。
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    r = _requests_get(url, timeout=15)
    r.raise_for_status()

    m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
    if not m:
        m2 = re.search(r"總成交金額.*?([\d,]+)\s*元", r.text)
        if not m2:
            raise RuntimeError("TPEx pricing.html 找不到『總成交金額』")
        amount = _to_int_amount(m2.group(1))
    else:
        amount = _to_int_amount(m.group(1))

    return int(amount), "TPEx pricing.html 總成交金額"


def fetch_amount_total() -> MarketAmount:
    twse, s1 = fetch_twse_amount()
    tpex, s2 = fetch_tpex_amount()
    return MarketAmount(
        amount_twse=twse,
        amount_tpex=tpex,
        amount_total=twse + tpex,
        source_twse=s1,
        source_tpex=s2,
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
    """
    - amount_norm_cum_ratio：累積正規化（穩健型）
    - amount_norm_slice_ratio：切片正規化（保守型，需 prev）
    """
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
