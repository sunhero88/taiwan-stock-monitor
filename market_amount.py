# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

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
    if x is None:
        return 0
    s = str(x)
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else 0


def _fmt_int(n: Optional[int]) -> str:
    if n is None:
        return "待更新"
    try:
        return f"{int(n):,}"
    except Exception:
        return "待更新"


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
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: Optional[str]
    source_tpex: Optional[str]
    error: Optional[str]


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


# -------------------------
# 官方抓取：多策略（免費）
# -------------------------
def fetch_twse_amount_openapi(trade_date: str) -> Tuple[int, str]:
    """
    嘗試 openapi.twse.com.tw（通常比 www.twse.com.tw 更不容易 SSL 炸裂）
    trade_date: 'YYYY-MM-DD' 或 None（openapi 有時只回最新）
    """
    # 多數 openapi 不吃 YYYY-MM-DD，保守做法：先打最新，再用欄位去加總成交金額
    url = "https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX"
    r = requests.get(url, headers=USER_AGENT, timeout=15)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, list) or not data:
        raise RuntimeError("TWSE openapi 回傳非預期格式")

    # 可能欄位名稱（中英混雜），用『包含 成交金額 或 TradeValue』的欄位加總
    keys = set()
    for row in data[:10]:
        if isinstance(row, dict):
            keys |= set(row.keys())

    cand = None
    for k in keys:
        ks = str(k)
        if "成交金額" in ks or "TradeValue" in ks or "trade_value" in ks or "Trade Value" in ks:
            cand = k
            break
    if cand is None:
        raise RuntimeError(f"TWSE openapi 找不到成交金額欄位，keys={list(keys)[:20]}")

    total = 0
    for row in data:
        if not isinstance(row, dict):
            continue
        total += _to_int_amount(row.get(cand))

    if total <= 0:
        raise RuntimeError("TWSE openapi 成交金額加總為 0（可能欄位不對或資料型態變更）")

    return total, "TWSE openapi /v1/exchangeReport/MI_INDEX（成交金額欄位加總）"


def fetch_tpex_amount_candidates(trade_date: str) -> Tuple[int, str]:
    """
    TPEx：用多 pattern 從頁面抓『總成交金額』，避免單點失效
    """
    urls = [
        "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html",
        "https://www.tpex.org.tw/en-us/mainboard/trading/info/pricing.html",
    ]

    patterns = [
        r"總成交金額[:：]\s*([\d,]+)\s*元",
        r"總成交金額.*?([\d,]+)\s*元",
        r"Total\s*Value\s*[:：]\s*([\d,]+)",
        r"Total\s*Trading\s*Value\s*[:：]\s*([\d,]+)",
    ]

    last_err = None
    for url in urls:
        try:
            r = requests.get(url, headers=USER_AGENT, timeout=15)
            r.raise_for_status()
            text = r.text

            for p in patterns:
                m = re.search(p, text, re.IGNORECASE | re.DOTALL)
                if m:
                    amt = _to_int_amount(m.group(1))
                    if amt > 0:
                        return amt, f"TPEx pricing（pattern={p}）"
            last_err = f"TPEx pricing 抓不到（url={url}）"
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)}"

    raise RuntimeError(last_err or "TPEx pricing 無法取得總成交金額")


def fetch_amount_total_safe(trade_date: str) -> Dict[str, Any]:
    """
    回傳給 main.py 使用的統一格式（即使失敗也要回傳原因，避免整頁掛掉）
    """
    sources: Dict[str, Any] = {"twse": None, "tpex": None, "error": None}
    twse_int = None
    tpex_int = None

    try:
        twse_int, s1 = fetch_twse_amount_openapi(trade_date)
        sources["twse"] = s1
    except Exception as e:
        sources["twse"] = None
        sources["error"] = f"TWSE:{type(e).__name__}: {str(e)}"

    try:
        tpex_int, s2 = fetch_tpex_amount_candidates(trade_date)
        sources["tpex"] = s2
    except Exception as e:
        sources["tpex"] = None
        # 不覆蓋 TWSE error，改為串接
        prev = sources.get("error")
        add = f"TPEx:{type(e).__name__}: {str(e)}"
        sources["error"] = add if not prev else (str(prev) + " | " + add)

    total_int = None
    if isinstance(twse_int, int) and isinstance(tpex_int, int):
        total_int = twse_int + tpex_int

    return {
        "amount_twse_int": twse_int,
        "amount_tpex_int": tpex_int,
        "amount_total_int": total_int,
        "amount_twse_fmt": _fmt_int(twse_int),
        "amount_tpex_fmt": _fmt_int(tpex_int),
        "amount_total_fmt": _fmt_int(total_int),
        "sources": sources,
    }
