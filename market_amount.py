# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import requests

# ✅ 重要：Streamlit Cloud 常遇到 SSL 鏈問題，用 certifi 指定 CA bundle（不關閉驗證）
import certifi


TZ_TAIPEI = timezone(timedelta(hours=8))

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)
TRADING_MINUTES = 270  # 09:00~13:30

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}


def _requests_get(url: str, timeout: int = 15) -> requests.Response:
    """
    統一 requests.get：帶 UA + certifi CA bundle，避免 Streamlit Cloud SSL 問題
    """
    return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())


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


# =========================
# TWSE：優先 OpenAPI，失敗才退回 MI_INDEX HTML
# =========================
def fetch_twse_amount() -> Tuple[int, str]:
    """
    上市成交金額（元）
    優先：TWSE OpenAPI (官方) :contentReference[oaicite:2]{index=2}
    退回：TWSE MI_INDEX(HTML) 解析
    """
    # 1) TWSE OpenAPI（不帶 date 會回當日資料；你可在主程式自己決定 trade_date 版本）
    try:
        url_api = "https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX"
        r = _requests_get(url_api, timeout=15)
        r.raise_for_status()
        j = r.json()

        # OpenAPI 回傳欄位會隨報表而不同；這裡做「可容錯」的字段搜尋：
        # 常見候選：成交金額(元) / TradeValue / 成交金額
        candidates = ["成交金額", "成交金額(元)", "TradeValue", "Trade Value", "trade_value", "value"]

        total = 0
        if isinstance(j, list):
            for row in j:
                if not isinstance(row, dict):
                    continue
                found = None
                for k in candidates:
                    if k in row:
                        found = row.get(k)
                        break
                if found is None:
                    continue
                total += _to_int_amount(found)

        if total > 0:
            return int(total), "TWSE OpenAPI MI_INDEX（成交金額欄位加總）"

    except Exception:
        # 忽略，走 fallback
        pass

    # 2) fallback：MI_INDEX HTML
    url_html = "https://www.twse.com.tw/exchangeReport/MI_INDEX?date=&response=html"
    r = _requests_get(url_html, timeout=15)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("TWSE MI_INDEX 找不到可解析表格")

    # 找出含有成交金額欄位的表（容錯）
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
        raise RuntimeError("TWSE 表格找不到『成交金額』欄")

    amount = int(target[amt_col].apply(_to_int_amount).sum())
    return amount, "TWSE MI_INDEX(HTML) 成交金額欄加總"


# =========================
# TPEx：改用官方 historical market-value 頁（比 pricing.html 穩） :contentReference[oaicite:3]{index=3}
# =========================
def fetch_tpex_amount() -> Tuple[int, str]:
    """
    上櫃成交金額（元）
    使用：TPEx 英文頁 Daily Stock Market Value List（表格抓取 Trade Value）
    """
    url = "https://www.tpex.org.tw/en-us/mainboard/trading/historical/market-value.html"
    r = _requests_get(url, timeout=15)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("TPEx market-value.html 找不到可解析表格")

    # 通常第一個表就是 daily list，但仍做容錯：找含 Trade Value / Trading Value / Value 的欄
    target = None
    value_col = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        # 常見欄位名（英文站）
        for c in t.columns:
            sc = str(c).lower()
            if ("trade" in sc and "value" in sc) or ("trading" in sc and "value" in sc) or sc in ("value",):
                target = t
                value_col = c
                break
        if target is not None:
            break

    if target is None or value_col is None:
        # 最後 fallback：嘗試第一張表找含 value 的欄
        target = tables[0]
        for c in target.columns:
            if "value" in str(c).lower():
                value_col = c
                break

    if value_col is None:
        raise RuntimeError("TPEx 表格找不到成交金額/Trade Value 欄位")

    # 取「最新一筆」（有些頁面會列多日，最新可能在第一列或最後一列，這裡兩種都試）
    col_series = target[value_col].dropna().astype(str)
    if col_series.empty:
        raise RuntimeError("TPEx Trade Value 欄位是空的")

    # 嘗試取最大（通常最新成交金額較大；且可避開 NA/文字列）
    vals = [_to_int_amount(x) for x in col_series.tolist()]
    amount = max(vals) if vals else 0
    if amount <= 0:
        # 如果 max 取不到，退回最後一個非零
        for v in reversed(vals):
            if v > 0:
                amount = v
                break

    if amount <= 0:
        raise RuntimeError("TPEx 成交金額解析結果為 0")

    return int(amount), "TPEx market-value.html Trade Value（取表格有效值）"


def fetch_amount_total() -> MarketAmount:
    """
    ✅ 你指定：上市 + 上櫃合計 = amount_total
    """
    twse, s1 = fetch_twse_amount()
    tpex, s2 = fetch_tpex_amount()
    return MarketAmount(
        amount_twse=int(twse),
        amount_tpex=int(tpex),
        amount_total=int(twse) + int(tpex),
        source_twse=s1,
        source_tpex=s2,
    )


def fetch_amount_total_safe() -> Dict[str, Any]:
    """
    不拋錯版：給 main.py 直接塞 macro.overview.amount_sources 用
    """
    out = {
        "amount_twse": None,
        "amount_tpex": None,
        "amount_total": None,
        "sources": {"twse": None, "tpex": None, "error": None},
    }
    try:
        ma = fetch_amount_total()
        out["amount_twse"] = ma.amount_twse
        out["amount_tpex"] = ma.amount_tpex
        out["amount_total"] = ma.amount_total
        out["sources"]["twse"] = ma.source_twse
        out["sources"]["tpex"] = ma.source_tpex
    except Exception as e:
        out["sources"]["error"] = f"{type(e).__name__}: {str(e)}"
    return out


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
