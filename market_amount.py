# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List

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
    """把 '775,402,495,419' 或 '775402495419' 轉 int"""
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
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    trade_date: Optional[str]         # YYYY-MM-DD
    source_twse: str
    source_tpex: str
    warning: Optional[str] = None


def _fmt_yyyymmdd(dt: datetime) -> str:
    return dt.astimezone(TZ_TAIPEI).strftime("%Y%m%d")


def fetch_twse_amount(date: Optional[datetime] = None, verify_ssl: bool = True) -> Tuple[Optional[int], str]:
    """
    上市成交金額（元）
    優先用 TWSE MI_INDEX JSON，再 fallback HTML。
    """
    date = date or _now_taipei()
    yyyymmdd = _fmt_yyyymmdd(date)

    # 1) JSON
    url_json = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={yyyymmdd}&type=ALL"
    try:
        r = requests.get(url_json, headers=USER_AGENT, timeout=15, verify=verify_ssl)
        r.raise_for_status()
        j = r.json()

        # 盡量找出含「成交金額」的表格欄位
        # 有些版本會把表格放在 data、fields；也可能分 data1/data2...
        candidates: List[pd.DataFrame] = []
        if isinstance(j, dict):
            fields = j.get("fields")
            data = j.get("data")
            if fields and data and isinstance(fields, list) and isinstance(data, list):
                candidates.append(pd.DataFrame(data, columns=fields))

            # 掃描所有 key，把像表格的結構轉成 DF 試試
            for k, v in j.items():
                if k.startswith("data") and isinstance(v, list) and v and isinstance(v[0], list):
                    f = j.get(k.replace("data", "fields"))
                    if isinstance(f, list):
                        try:
                            candidates.append(pd.DataFrame(v, columns=f))
                        except Exception:
                            pass

        df_target = None
        for df in candidates:
            cols = [str(c) for c in df.columns]
            if any("成交金額" in c for c in cols):
                df_target = df
                break

        if df_target is None:
            raise RuntimeError("TWSE MI_INDEX(JSON) 找不到『成交金額』欄位")

        amt_col = None
        for c in df_target.columns:
            if "成交金額" in str(c):
                amt_col = c
                break
        if amt_col is None:
            raise RuntimeError("TWSE 成交統計表找不到『成交金額』欄")

        amount = int(df_target[amt_col].apply(_to_int_amount).sum())
        return amount, f"TWSE MI_INDEX(JSON) date={yyyymmdd} 加總"
    except Exception:
        pass

    # 2) HTML fallback
    url_html = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?date={yyyymmdd}&response=html&type=ALL"
    r = requests.get(url_html, headers=USER_AGENT, timeout=15, verify=verify_ssl)
    r.raise_for_status()
    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("TWSE MI_INDEX(HTML) 找不到可解析表格")

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
        raise RuntimeError("TWSE 成交統計表找不到『成交金額』欄(HTML)")

    amount = int(target[amt_col].apply(_to_int_amount).sum())
    return amount, f"TWSE MI_INDEX(HTML) date={yyyymmdd} 加總"


def fetch_tpex_amount(date: Optional[datetime] = None, verify_ssl: bool = True) -> Tuple[Optional[int], str]:
    """
    上櫃成交金額（元）
    以 TPEx pricing.html 文字抓取（最穩的免費路徑之一，可能會改版）。
    """
    date = date or _now_taipei()
    _ = _fmt_yyyymmdd(date)  # 目前頁面不是用 date query；保留以便未來改版

    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    r = requests.get(url, headers=USER_AGENT, timeout=15, verify=verify_ssl)
    r.raise_for_status()

    # 常見字樣：總成交金額: 175,152,956,339元
    m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
    if not m:
        m2 = re.search(r"總成交金額.*?([\d,]+)\s*元", r.text, flags=re.S)
        if not m2:
            raise RuntimeError("TPEx pricing.html 找不到『總成交金額』")
        amount = _to_int_amount(m2.group(1))
    else:
        amount = _to_int_amount(m.group(1))

    return int(amount), "TPEx pricing.html 總成交金額"


def fetch_amount_total_latest(
    trade_date: Optional[datetime] = None,
    verify_ssl: bool = True,
) -> MarketAmount:
    """
    取得當日（或指定日）的 TWSE + TPEx 成交金額。
    注意：TPEx 網頁偶有改版/阻擋；失敗時回傳 None 並在 warning 註記。
    """
    trade_date = trade_date or _now_taipei()
    trade_date_str = trade_date.astimezone(TZ_TAIPEI).strftime("%Y-%m-%d")

    warning_parts = []

    twse_amt, twse_src = None, "TWSE:UNAVAILABLE"
    tpex_amt, tpex_src = None, "TPEx:UNAVAILABLE"

    try:
        twse_amt, twse_src = fetch_twse_amount(date=trade_date, verify_ssl=verify_ssl)
    except Exception as e:
        warning_parts.append(f"TWSE amount 取得失敗: {e}")

    try:
        tpex_amt, tpex_src = fetch_tpex_amount(date=trade_date, verify_ssl=verify_ssl)
    except Exception as e:
        warning_parts.append(f"TPEx amount 取得失敗: {e}")

    total = None
    if isinstance(twse_amt, int) and isinstance(tpex_amt, int):
        total = twse_amt + tpex_amt

    warning = " | ".join(warning_parts) if warning_parts else None

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        trade_date=trade_date_str,
        source_twse=twse_src,
        source_tpex=tpex_src,
        warning=warning,
    )


def classify_ratio(r: float) -> str:
    if r < 0.8:
        return "LOW"
    if r > 1.2:
        return "HIGH"
    return "NORMAL"


def intraday_norm(
    amount_total_now: Optional[int],
    amount_total_prev: Optional[int],
    avg20_amount_total: Optional[int],
    now: Optional[datetime] = None,
    alpha: float = 0.65,
) -> Dict[str, Any]:
    """
    回傳：
    - amount_norm_cum_ratio：累積正規化比率（穩健型使用）
    - amount_norm_slice_ratio：切片正規化比率（保守型使用，需 prev）
    - amount_norm_label：NORMAL/LOW/HIGH/UNKNOWN
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

    if not amount_total_now or amount_total_now <= 0:
        return out
    if not avg20_amount_total or avg20_amount_total <= 0:
        return out

    expected_cum = avg20_amount_total * progress_curve(p_now, alpha=alpha)
    cum_ratio = (amount_total_now / expected_cum) if expected_cum > 0 else None
    out["amount_norm_cum_ratio"] = None if cum_ratio is None else round(float(cum_ratio), 4)
    if cum_ratio is not None:
        out["amount_norm_label"] = classify_ratio(float(cum_ratio))

    if amount_total_prev is not None and amount_total_prev >= 0:
        slice_amount = max(0, amount_total_now - amount_total_prev)
        expected_slice = avg20_amount_total * (progress_curve(p_now, alpha=alpha) - progress_curve(p_prev, alpha=alpha))
        slice_ratio = (slice_amount / expected_slice) if expected_slice > 0 else None
        out["amount_norm_slice_ratio"] = None if slice_ratio is None else round(float(slice_ratio), 4)

    return out
