# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

import certifi
import pandas as pd
import requests


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


def _today_yyyymmdd(now: Optional[datetime] = None) -> str:
    now = now or _now_taipei()
    return now.strftime("%Y%m%d")


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


def _http_get(url: str, timeout: int = 15) -> requests.Response:
    """
    Streamlit Cloud 常見 SSL 錯誤的修正點：
    - verify=certifi.where()：使用 certifi 的 CA bundle
    - headers：帶 UA
    """
    r = requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    r.raise_for_status()
    return r


def fetch_twse_amount(date_yyyymmdd: Optional[str] = None) -> Tuple[int, str]:
    """
    上市成交金額（元）：
    - 使用 TWSE MI_INDEX HTML（成交統計表）加總成交金額欄
    - 注意：TWSE 參數 date 不能空，空值有時會回到非預期頁面或被擋

    回傳：(amount_twse, source_desc)
    """
    date_yyyymmdd = date_yyyymmdd or _today_yyyymmdd()
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date={date_yyyymmdd}"

    r = _http_get(url)

    # MI_INDEX HTML 內通常有多個表格
    try:
        tables = pd.read_html(r.text)
    except Exception as e:
        raise RuntimeError(f"TWSE MI_INDEX HTML 解析失敗：{type(e).__name__}: {e}")

    if not tables:
        raise RuntimeError("TWSE MI_INDEX 找不到可解析表格")

    # 容錯策略：
    # 1) 優先找欄位含「成交金額」且表內有數字金額
    # 2) 找不到就用第一張表，但仍要確認有成交金額欄
    target = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("成交金額" in c for c in cols):
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

    # 若解析出來是 0，通常代表抓到錯表或非交易日/頁面變動
    if amount <= 0:
        raise RuntimeError("TWSE 成交金額解析結果為 0（可能非交易日或表格結構變動）")

    return amount, f"TWSE MI_INDEX(HTML) date={date_yyyymmdd} 成交統計成交金額加總"


def fetch_tpex_amount(date_yyyymmdd: Optional[str] = None) -> Tuple[int, str]:
    """
    上櫃成交金額（元）：
    - 先嘗試 pricing.html 直接抓「總成交金額」
    - 若頁面改版導致抓不到，直接丟錯誤（由上層標示待更新）

    回傳：(amount_tpex, source_desc)
    """
    # TPEx 這頁通常顯示當日資訊，不一定需要 date 參數；保留 date 只是為了記錄來源
    date_yyyymmdd = date_yyyymmdd or _today_yyyymmdd()
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"

    r = _http_get(url)

    m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
    if not m:
        # fallback：容錯抓取
        m2 = re.search(r"總成交金額.*?([\d,]+)\s*元", r.text)
        if not m2:
            raise RuntimeError("TPEx pricing.html 找不到『總成交金額』字樣（可能頁面改版）")
        amount = _to_int_amount(m2.group(1))
    else:
        amount = _to_int_amount(m.group(1))

    if amount <= 0:
        raise RuntimeError("TPEx 成交金額解析結果為 0（可能非交易日或頁面變動）")

    return int(amount), f"TPEx pricing.html（當日）date={date_yyyymmdd} 總成交金額"


def fetch_amount_total(date_yyyymmdd: Optional[str] = None) -> MarketAmount:
    """
    ✅ 核心：上市 + 上櫃合計
    """
    date_yyyymmdd = date_yyyymmdd or _today_yyyymmdd()
    twse, s1 = fetch_twse_amount(date_yyyymmdd=date_yyyymmdd)
    tpex, s2 = fetch_tpex_amount(date_yyyymmdd=date_yyyymmdd)
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
) -> Dict[str, Any]:
    """
    回傳：
    - progress：盤中進度 0~1
    - amount_norm_cum_ratio：累積正規化比率（穩健型）
      = amount_total_now / (avg20 * progress_curve(progress))
    - amount_norm_slice_ratio：切片正規化比率（保守型，需 prev）
      = (now-prev)/(預期 now-prev)
    - amount_norm_label：NORMAL/LOW/HIGH（以 cum_ratio 判定）
    """
    now = now or _now_taipei()
    p_now = trading_progress(now)
    p_prev = max(0.0, p_now - (5 / TRADING_MINUTES))  # 以 5 分鐘當切片

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
