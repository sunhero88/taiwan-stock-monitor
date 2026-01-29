# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd
import requests
import yfinance as yf

TZ_TAIPEI = timezone(timedelta(hours=8))

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)
TRADING_MINUTES = 270  # 09:00~13:30

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

# -------------------------
# Utilities
# -------------------------
def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def _yyyymmdd(d: str) -> str:
    # '2026-01-28' -> '20260128'
    return str(d).replace("-", "")

def _to_int_amount(x) -> int:
    if x is None:
        return 0
    s = re.sub(r"[^\d]", "", str(x))
    return int(s) if s else 0

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

def classify_ratio(r: float) -> str:
    if r < 0.8:
        return "LOW"
    if r > 1.2:
        return "HIGH"
    return "NORMAL"

@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: Optional[str]
    source_tpex: Optional[str]
    error: Optional[str] = None

# -------------------------
# Primary: TWSE / TPEx (best effort)
# -------------------------
def fetch_twse_amount(trade_date: str, allow_insecure_ssl: bool = True) -> Tuple[int, str]:
    """
    盡量抓「上市」成交金額（元）。
    - 先走 TWSE rwd JSON（較穩）
    - 不行再走 HTML（需要 lxml/html5lib）
    """
    ymd = _yyyymmdd(trade_date)

    # (A) TWSE rwd JSON（推薦）
    # MI_INDEX rwd：有時欄位會變動，這裡用「成交金額」做模糊找值
    url_json = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=ALL&response=json"
    try:
        r = requests.get(url_json, headers=USER_AGENT, timeout=15, verify=not allow_insecure_ssl)
        r.raise_for_status()
        js = r.json()
        # js["tables"] 可能有多張表，找含「成交金額」的欄位
        tables = js.get("tables", [])
        for t in tables:
            fields = t.get("fields", [])
            data = t.get("data", [])
            if not fields or not data:
                continue
            # 找到欄位名含「成交金額」
            amt_idx = None
            for i, f in enumerate(fields):
                if "成交金額" in str(f):
                    amt_idx = i
                    break
            if amt_idx is None:
                continue
            # 把該表的成交金額欄位加總（有些表是分分類）
            total = 0
            for row in data:
                if amt_idx < len(row):
                    total += _to_int_amount(row[amt_idx])
            if total > 0:
                return int(total), f"TWSE rwd MI_INDEX(JSON) 加總（date={ymd}）"
    except Exception as e:
        # 先吞掉，往下 fallback
        last_err = f"{type(e).__name__}: {e}"

    # (B) HTML fallback（你原本那套）
    url_html = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?date={ymd}&response=html"
    r = requests.get(url_html, headers=USER_AGENT, timeout=15, verify=not allow_insecure_ssl)
    r.raise_for_status()
    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError(f"TWSE MI_INDEX 找不到表格。前一段錯誤：{last_err}")

    # 找含「成交金額」的表，取第一個符合
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
    return amount, f"TWSE MI_INDEX(HTML) 成交金額欄加總（date={ymd}，allow_insecure_ssl={allow_insecure_ssl}）"

def fetch_tpex_amount(trade_date: str, allow_insecure_ssl: bool = False) -> Tuple[int, str]:
    """
    盡量抓「上櫃」成交金額（元）。
    TPEx 網頁常改版，所以用多種 regex 兜底。
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    r = requests.get(url, headers=USER_AGENT, timeout=15, verify=not allow_insecure_ssl)
    r.raise_for_status()

    text = r.text

    # 常見型態：總成交金額: 175,152,956,339元
    patterns = [
        r"總成交金額[:：]\s*([\d,]+)\s*元",
        r"總成交金額.*?([\d,]+)\s*元",
        r"成交金額.*?([\d,]+)\s*元",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return int(_to_int_amount(m.group(1))), f"TPEx pricing.html regex({p})"

    raise RuntimeError("TPEx pricing.html 找不到可解析的『總成交金額/成交金額』字樣")

def fetch_amount_total(trade_date: str) -> MarketAmount:
    """
    回傳：
    - amount_twse / amount_tpex / amount_total
    - source_twse / source_tpex
    - error（若任一失敗）
    """
    twse = tpex = None
    s1 = s2 = None
    err = None

    # TWSE：你遇過 SSL 問題 → 模擬期允許 insecure
    try:
        twse, s1 = fetch_twse_amount(trade_date, allow_insecure_ssl=True)
    except Exception as e:
        err = f"TWSE:{type(e).__name__}: {e}"

    try:
        tpex, s2 = fetch_tpex_amount(trade_date, allow_insecure_ssl=False)
    except Exception as e:
        err2 = f"TPEx:{type(e).__name__}: {e}"
        err = (err + " | " + err2) if err else err2

    total = (twse or 0) + (tpex or 0)
    if twse is None or tpex is None:
        # 部分缺失就視為 total 不可靠
        total_out: Optional[int] = None
    else:
        total_out = total

    return MarketAmount(
        amount_twse=twse,
        amount_tpex=tpex,
        amount_total=total_out,
        source_twse=s1,
        source_tpex=s2,
        error=err,
    )

# -------------------------
# Free fallback: yfinance proxy (yesterday / pre-open must-have)
# -------------------------
def yfinance_amount_proxy(symbols: List[str], trade_date: str) -> Tuple[Optional[int], str]:
    """
    用追蹤清單的「Close * Volume」加總當作 amount_total 代理（免費/模擬期保底）。
    注意：這不是全市場成交金額，只是你監控池的 proxy，但至少「昨日一定有值」。
    """
    try:
        # 抓近 10 天，避免遇到假日
        data = yf.download(symbols, period="15d", interval="1d", progress=False, auto_adjust=False, threads=True)
        if data is None or data.empty:
            return None, "yfinance proxy: empty"

        close = data["Close"].copy()
        vol = data["Volume"].copy()

        # 取目標日（若沒該日，取最近一個 <= trade_date）
        close.index = pd.to_datetime(close.index).tz_localize(None)
        vol.index = pd.to_datetime(vol.index).tz_localize(None)
        d0 = pd.to_datetime(trade_date)

        avail = close.index[close.index <= d0]
        if len(avail) == 0:
            return None, "yfinance proxy: no available date <= trade_date"
        d = avail.max()

        amt = float((close.loc[d] * vol.loc[d]).fillna(0).sum())
        return int(amt), f"yfinance proxy(監控池): sum(Close*Volume) date={d.strftime('%Y-%m-%d')}"
    except Exception as e:
        return None, f"yfinance proxy error: {type(e).__name__}: {e}"

# -------------------------
# Intraday normalization
# -------------------------
def intraday_norm(
    amount_total_now: int,
    amount_total_prev: Optional[int],
    avg20_amount_total: Optional[int],
    now: Optional[datetime] = None,
    alpha: float = 0.65,
) -> Dict[str, Any]:
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
