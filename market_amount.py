# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple

import requests
import pandas as pd


TZ_TAIPEI = timezone(timedelta(hours=8))

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)
TRADING_MINUTES = 270  # 09:00~13:30

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}


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
    """回傳盤中進度 0~1。盤外會 clamp 到 0 或 1。"""
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
    上市成交金額（元）：用 TWSE MI_INDEX(HTML) 的成交統計表，把各類別成交金額加總。
    來源：TWSE 報表 MI_INDEX（成交統計/成交金額）。(turn7search10)
    """
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?date=&response=html"
    r = requests.get(url, headers=USER_AGENT, timeout=15)
    r.raise_for_status()

    # MI_INDEX HTML 內通常有多個表格，第一個常見就是成交統計
    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("TWSE MI_INDEX 找不到可解析表格")

    # 找出含有「成交統計」與「成交金額(元)」欄位的表
    target = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("成交金額" in c for c in cols) and any("成交統計" in c for c in cols):
            target = t
            break
    if target is None:
        # fallback：用第一張表
        target = tables[0]

    # 取出成交金額欄位（可能叫 成交金額(元) 或類似）
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
    上櫃成交金額（元）：用 TPEx 行情頁的「總成交金額」字樣抓取。
    來源：TPEx mainboard pricing/info/statistics 頁。(turn7search5/turn7search3/turn7search12)
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    r = requests.get(url, headers=USER_AGENT, timeout=15)
    r.raise_for_status()

    # 網頁上會出現：總成交金額: 175,152,956,339元
    m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
    if not m:
        # fallback：用「總成交金額」周邊抓數字
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
    回傳：
    - amount_norm_cum_ratio：累積正規化比率（穩健型使用）
    - amount_norm_slice_ratio：切片正規化比率（保守型使用，需 prev）
    - amount_norm_label：NORMAL/LOW/HIGH（以 cum_ratio 判定）
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

    # slice（需要前值）
    if amount_total_prev is not None:
        slice_amount = max(0, amount_total_now - amount_total_prev)
        expected_slice = avg20_amount_total * (progress_curve(p_now, alpha=alpha) - progress_curve(p_prev, alpha=alpha))
        slice_ratio = (slice_amount / expected_slice) if expected_slice > 0 else None
        out["amount_norm_slice_ratio"] = None if slice_ratio is None else round(float(slice_ratio), 4)

    return out
