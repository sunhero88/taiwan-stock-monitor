# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone, date
from typing import Optional, Tuple, Dict, Any

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
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    trade_date: Optional[str]  # YYYY-MM-DD
    sources: Dict[str, Any]    # 詳細來源/錯誤/是否跳過SSL/是否stale


# ----------------------------
# TWSE (官方優先) - MI_INDEX JSON
# ----------------------------

def _fetch_json(url: str, *, verify_ssl: bool, timeout: int = 15) -> Dict[str, Any]:
    r = requests.get(url, headers=USER_AGENT, timeout=timeout, verify=verify_ssl)
    r.raise_for_status()
    return r.json()


def fetch_twse_amount_mi_index_json(
    trade_date: Optional[date] = None,
    *,
    verify_ssl: bool = True,
    allow_ssl_bypass: bool = True
) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    上市成交金額（元）：TWSE MI_INDEX (JSON) → 找到含「成交金額」欄位之表並加總。
    URL: https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=YYYYMMDD&type=ALL
    """
    td = trade_date or _now_taipei().date()
    ymd = td.strftime("%Y%m%d")
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={ymd}&type=ALL"

    meta = {"url": url, "verify_ssl": verify_ssl, "ssl_bypassed": False, "error": None, "data_date": None}

    def _parse(mi: Dict[str, Any]) -> Optional[int]:
        # MI_INDEX JSON 常見結構：fields1/data1, fields2/data2 ... 逐表掃描
        # 我們找 fieldsX 內含「成交金額」的那張表，並對應 dataX 加總
        for k in list(mi.keys()):
            if not k.startswith("fields"):
                continue
            idx = k.replace("fields", "")
            fields = mi.get(k)
            data = mi.get(f"data{idx}")
            if not isinstance(fields, list) or not isinstance(data, list):
                continue
            # 欄位是否含成交金額
            amt_idx = None
            for i, f in enumerate(fields):
                if "成交金額" in str(f):
                    amt_idx = i
                    break
            if amt_idx is None:
                continue

            s = 0
            for row in data:
                if not isinstance(row, list) or len(row) <= amt_idx:
                    continue
                s += _to_int_amount(row[amt_idx])
            if s > 0:
                return int(s)

        return None

    try:
        mi = _fetch_json(url, verify_ssl=verify_ssl)
        # 交易日字串（不一定有標準欄位，盡量從 date/title 推）
        meta["data_date"] = mi.get("date") or mi.get("stat") or None
        amt = _parse(mi)
        if amt is None:
            raise RuntimeError("TWSE MI_INDEX(JSON) 找不到可加總之『成交金額』表")
        return amt, meta

    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        if verify_ssl and allow_ssl_bypass:
            # 自動降級：verify=False（但要明確標示）
            try:
                mi = _fetch_json(url, verify_ssl=False)
                meta["ssl_bypassed"] = True
                meta["verify_ssl"] = False
                meta["error"] = None
                meta["data_date"] = mi.get("date") or mi.get("stat") or None
                amt = _parse(mi)
                if amt is None:
                    raise RuntimeError("TWSE MI_INDEX(JSON)（SSL bypass）仍找不到成交金額")
                return amt, meta
            except Exception as e2:
                meta["error"] = f"{type(e2).__name__}: {e2}"
        return None, meta


# ----------------------------
# TPEx (官方優先) - pricing.html 文字抓取（你原本邏輯）
# ----------------------------

def fetch_tpex_amount_pricing_html(
    *,
    verify_ssl: bool = True,
    allow_ssl_bypass: bool = True
) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    上櫃成交金額（元）：抓 pricing.html 的「總成交金額: xxx元」。
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    meta = {"url": url, "verify_ssl": verify_ssl, "ssl_bypassed": False, "error": None, "data_date": None}

    def _parse(text: str) -> Optional[int]:
        m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", text)
        if m:
            return _to_int_amount(m.group(1))
        m2 = re.search(r"總成交金額.*?([\d,]+)\s*元", text)
        if m2:
            return _to_int_amount(m2.group(1))
        return None

    try:
        r = requests.get(url, headers=USER_AGENT, timeout=15, verify=verify_ssl)
        r.raise_for_status()
        amt = _parse(r.text)
        if amt is None:
            raise RuntimeError("TPEx pricing.html 找不到『總成交金額』")
        return int(amt), meta

    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        if verify_ssl and allow_ssl_bypass:
            try:
                r = requests.get(url, headers=USER_AGENT, timeout=15, verify=False)
                r.raise_for_status()
                meta["ssl_bypassed"] = True
                meta["verify_ssl"] = False
                meta["error"] = None
                amt = _parse(r.text)
                if amt is None:
                    raise RuntimeError("TPEx pricing.html（SSL bypass）仍找不到『總成交金額』")
                return int(amt), meta
            except Exception as e2:
                meta["error"] = f"{type(e2).__name__}: {e2}"
        return None, meta


# ----------------------------
# 對外：一次拿到 TWSE + TPEx + Total + 稽核資訊
# ----------------------------

def fetch_amount_total_latest(
    trade_date: Optional[date] = None,
    *,
    verify_ssl: bool = True,
    allow_ssl_bypass: bool = True
) -> MarketAmount:
    """
    回傳：上市/上櫃/合計（元），以及來源、錯誤、是否跳過 SSL 等稽核資訊。
    """
    td = trade_date or _now_taipei().date()
    sources: Dict[str, Any] = {
        "trade_date": td.isoformat(),
        "twse": None,
        "tpex": None,
        "stale": False,
        "warning": None,
    }

    twse_amt, twse_meta = fetch_twse_amount_mi_index_json(
        td, verify_ssl=verify_ssl, allow_ssl_bypass=allow_ssl_bypass
    )
    tpex_amt, tpex_meta = fetch_tpex_amount_pricing_html(
        verify_ssl=verify_ssl, allow_ssl_bypass=allow_ssl_bypass
    )

    sources["twse"] = twse_meta
    sources["tpex"] = tpex_meta

    # 稽核：如果兩邊都拿不到 → 直接 UNKNOWN
    if twse_amt is None and tpex_amt is None:
        sources["warning"] = "官方抓取失敗且無可用 fallback（成交金額不可用）"
        return MarketAmount(
            amount_twse=None,
            amount_tpex=None,
            amount_total=None,
            trade_date=td.isoformat(),
            sources=sources,
        )

    total = (twse_amt or 0) + (tpex_amt or 0)

    # 稽核：若任一側是 SSL bypass → 明確提示（你要「真實可靠」就不能默默吃掉）
    bypass = bool((twse_meta.get("ssl_bypassed")) or (tpex_meta.get("ssl_bypassed")))
    if bypass:
        sources["warning"] = "已發生 SSL bypass（requests verify=False）。此數據仍來自官方頁面，但不屬於嚴格安全通道。"

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        trade_date=td.isoformat(),
        sources=sources,
    )


# ----------------------------
# 量能正規化（你原本的函數保留）
# ----------------------------

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

    if amount_total_prev is not None:
        slice_amount = max(0, amount_total_now - amount_total_prev)
        expected_slice = avg20_amount_total * (progress_curve(p_now, alpha=alpha) - progress_curve(p_prev, alpha=alpha))
        slice_ratio = (slice_amount / expected_slice) if expected_slice > 0 else None
        out["amount_norm_slice_ratio"] = None if slice_ratio is None else round(float(slice_ratio), 4)

    return out
