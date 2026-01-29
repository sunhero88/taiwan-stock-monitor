# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta, timezone
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


# ----------------------------
# helpers
# ----------------------------
def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _to_int_amount(x) -> int:
    """把 '775,402,495,419' 或 '775402495419' 轉 int。"""
    if x is None:
        return 0
    s = str(x)
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else 0


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _twse_date(d: date) -> str:
    # TWSE 使用 YYYYMMDD
    return _yyyymmdd(d)


def _tpex_date(d: date) -> str:
    # TPEx 常見使用民國年格式：YYY/MM/DD
    roc_year = d.year - 1911
    return f"{roc_year:03d}/{d.month:02d}/{d.day:02d}"


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


def classify_ratio(r: float) -> str:
    if r < 0.8:
        return "LOW"
    if r > 1.2:
        return "HIGH"
    return "NORMAL"


# ----------------------------
# data model
# ----------------------------
@dataclass
class MarketAmount:
    trade_date: date
    amount_twse: int
    amount_tpex: int
    amount_total: int
    source_twse: str
    source_tpex: str


# ----------------------------
# TWSE: JSON first, HTML fallback
# ----------------------------
def fetch_twse_amount_json(trade_date: date, verify_ssl: bool = True) -> Tuple[int, str]:
    """
    上市成交金額（元）— JSON 優先。
    會嘗試解析 MI_INDEX JSON，抓取「成交金額」欄位加總。
    """
    d = _twse_date(trade_date)
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={d}&type=ALL"

    r = requests.get(url, headers=USER_AGENT, timeout=20, verify=verify_ssl)
    r.raise_for_status()
    js = r.json()

    # 常見欄位：fields + data (list of rows)
    # 這裡做「欄名包含 成交金額」的欄位加總
    fields = js.get("fields") or []
    data = js.get("data") or []

    if not fields or not data:
        # 有些情況會回傳 stat="OK" 但沒 data（非交易日）
        raise RuntimeError("TWSE JSON 無可用 data（可能非交易日或資料尚未更新）")

    # 找成交金額欄位 index
    idx = None
    for i, f in enumerate(fields):
        if "成交金額" in str(f):
            idx = i
            break
    if idx is None:
        raise RuntimeError("TWSE JSON 找不到『成交金額』欄")

    total = 0
    for row in data:
        if idx < len(row):
            total += _to_int_amount(row[idx])

    if total <= 0:
        raise RuntimeError("TWSE JSON 成交金額加總為 0（可能解析失敗或資料未更新）")

    return total, f"TWSE MI_INDEX(JSON) 加總（date={d}）"


def fetch_twse_amount_html(trade_date: date, verify_ssl: bool = True) -> Tuple[int, str]:
    """
    上市成交金額（元）— HTML fallback。
    """
    d = _twse_date(trade_date)
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?date={d}&response=html"

    r = requests.get(url, headers=USER_AGENT, timeout=20, verify=verify_ssl)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("TWSE HTML 找不到可解析表格")

    # 嘗試找含「成交金額」欄位的表
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
        raise RuntimeError("TWSE HTML 找不到『成交金額』欄")

    total = int(target[amt_col].apply(_to_int_amount).sum())
    if total <= 0:
        raise RuntimeError("TWSE HTML 成交金額加總為 0（解析失敗或資料未更新）")

    return total, f"TWSE MI_INDEX(HTML) 加總（date={d}）"


def fetch_twse_amount(trade_date: date, verify_ssl: bool = True) -> Tuple[int, str]:
    # JSON 優先，失敗再 HTML
    try:
        return fetch_twse_amount_json(trade_date, verify_ssl=verify_ssl)
    except Exception:
        return fetch_twse_amount_html(trade_date, verify_ssl=verify_ssl)


# ----------------------------
# TPEx: JSON endpoints first, HTML fallback
# ----------------------------
def _tpex_try_json_endpoints(trade_date: date, verify_ssl: bool = True) -> Tuple[int, str]:
    """
    TPEx 成交金額（元）：優先嘗試 JSON / 後交易 API。
    這裡用多端點策略：任一成功即回傳。
    """

    d_roc = _tpex_date(trade_date)

    candidates: List[Tuple[str, str]] = [
        # 常見：上櫃「每日交易資訊」(可能含成交金額/總成交金額等欄位)
        ("tpex_st43", f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={d_roc}&_={int(_now_taipei().timestamp())}"),
        # 常見：上櫃「每日收盤行情」(通常有 data，部分版本含 summary)
        ("tpex_stk_quote", f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={d_roc}&_={int(_now_taipei().timestamp())}"),
    ]

    last_err = None
    for tag, url in candidates:
        try:
            r = requests.get(url, headers=USER_AGENT, timeout=20, verify=verify_ssl)
            r.raise_for_status()
            js = r.json()

            # 可能的鍵：data / aaData / tables / summary
            # 我們用「全文搜尋：包含『成交金額』的欄位或 summary 字串」策略
            # 1) summary 型：可能直接給文字
            txt = json_dumps_safe(js)

            # 嘗試抓「總成交金額」數字（元）
            m = re.search(r"總成交金額[^0-9]*([\d,]+)", txt)
            if m:
                amt = _to_int_amount(m.group(1))
                if amt > 0:
                    return amt, f"TPEx {tag}(JSON) summary regex（d={d_roc}）"

            # 2) 若有表格 data + fields，找欄名含成交金額
            fields = js.get("fields") or js.get("Fields") or []
            data = js.get("data") or js.get("aaData") or js.get("Data") or []
            if fields and data:
                idx = None
                for i, f in enumerate(fields):
                    if "成交金額" in str(f):
                        idx = i
                        break
                if idx is not None:
                    total = 0
                    for row in data:
                        if idx < len(row):
                            total += _to_int_amount(row[idx])
                    if total > 0:
                        return total, f"TPEx {tag}(JSON) fields+data 加總（d={d_roc}）"

            # 3) 有些回傳是 dict/list 混雜：最後退回全文 regex (找第一個像成交金額的大數字)
            m2 = re.search(r"成交金額[^0-9]*([\d,]{8,})", txt)
            if m2:
                amt = _to_int_amount(m2.group(1))
                if amt > 0:
                    return amt, f"TPEx {tag}(JSON) 成交金額 regex（d={d_roc}）"

            last_err = RuntimeError(f"{tag} JSON 解析不到成交金額（可能端點改版）")
        except Exception as e:
            last_err = e

    raise RuntimeError(f"TPEx JSON 端點全部失敗：{last_err}")


def fetch_tpex_amount_html_fallback(trade_date: date, verify_ssl: bool = True) -> Tuple[int, str]:
    """
    TPEx HTML fallback：抓 pricing.html 的『總成交金額』。
    注意：此頁面可能改為動態載入，故放最後 fallback。
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    r = requests.get(url, headers=USER_AGENT, timeout=20, verify=verify_ssl)
    r.raise_for_status()

    # 網頁上可能出現：總成交金額: 175,152,956,339元
    m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
    if not m:
        m2 = re.search(r"總成交金額.*?([\d,]+)\s*元", r.text)
        if not m2:
            raise RuntimeError("TPEx pricing.html 找不到『總成交金額』")
        amount = _to_int_amount(m2.group(1))
    else:
        amount = _to_int_amount(m.group(1))

    if amount <= 0:
        raise RuntimeError("TPEx pricing.html 解析到的成交金額為 0（疑似抓取失敗）")

    return int(amount), "TPEx pricing.html 總成交金額（HTML fallback）"


def fetch_tpex_amount(trade_date: date, verify_ssl: bool = True) -> Tuple[int, str]:
    # JSON 優先，失敗再 HTML
    try:
        return _tpex_try_json_endpoints(trade_date, verify_ssl=verify_ssl)
    except Exception:
        return fetch_tpex_amount_html_fallback(trade_date, verify_ssl=verify_ssl)


# ----------------------------
# 最新可用交易日：回溯掃描
# ----------------------------
def find_latest_official_trade_date(
    lookback_days: int = 10,
    verify_ssl: bool = True,
) -> Tuple[date, Dict[str, Any]]:
    """
    從今天往回找最近可用交易日（最多 lookback_days 天）。
    成功條件：TWSE 可抓到 或 TPEx 可抓到（至少一邊成功），並回傳該日。
    """
    today = _now_taipei().date()
    debug = {"attempts": []}

    for i in range(0, max(1, lookback_days)):
        d = today - timedelta(days=i)
        rec = {"date": str(d), "twse_ok": False, "tpex_ok": False, "twse_err": None, "tpex_err": None}
        twse_amt = None
        tpex_amt = None

        try:
            twse_amt, _ = fetch_twse_amount(d, verify_ssl=verify_ssl)
            rec["twse_ok"] = True
        except Exception as e:
            rec["twse_err"] = str(e)

        try:
            tpex_amt, _ = fetch_tpex_amount(d, verify_ssl=verify_ssl)
            rec["tpex_ok"] = True
        except Exception as e:
            rec["tpex_err"] = str(e)

        debug["attempts"].append(rec)

        # 至少一邊成功，就視為「可用交易日」；另一邊可在後續顯示缺口
        if rec["twse_ok"] or rec["tpex_ok"]:
            return d, debug

    raise RuntimeError(f"回溯 {lookback_days} 天仍找不到任何可用官方交易日（TWSE/TPEx 都失敗）")


def fetch_amount_total_latest(
    lookback_days: int = 10,
    verify_ssl: bool = True,
) -> Tuple[MarketAmount, Dict[str, Any]]:
    """
    抓取「最近可用交易日」的官方成交金額（TWSE + TPEx）。
    - 可能只有一邊成功：另一邊會給 0（但 source 會標示 ERROR），供裁決層判定 degraded_mode。
    """
    d, debug = find_latest_official_trade_date(lookback_days=lookback_days, verify_ssl=verify_ssl)

    twse_amt, s1 = 0, "TWSE:ERROR"
    tpex_amt, s2 = 0, "TPEx:ERROR"
    twse_err = None
    tpex_err = None

    try:
        twse_amt, s1 = fetch_twse_amount(d, verify_ssl=verify_ssl)
    except Exception as e:
        twse_err = str(e)

    try:
        tpex_amt, s2 = fetch_tpex_amount(d, verify_ssl=verify_ssl)
    except Exception as e:
        tpex_err = str(e)

    ma = MarketAmount(
        trade_date=d,
        amount_twse=int(twse_amt) if twse_amt else 0,
        amount_tpex=int(tpex_amt) if tpex_amt else 0,
        amount_total=int((twse_amt or 0) + (tpex_amt or 0)),
        source_twse=s1 if not twse_err else f"{s1} | {twse_err}",
        source_tpex=s2 if not tpex_err else f"{s2} | {tpex_err}",
    )

    debug["resolved_trade_date"] = str(d)
    debug["twse_err"] = twse_err
    debug["tpex_err"] = tpex_err

    return ma, debug


# ----------------------------
# 舊介面相容：你的 main.py 目前用 fetch_amount_total()
# ----------------------------
def fetch_amount_total() -> MarketAmount:
    """
    相容舊介面：取最近可用交易日（預設回溯 10 天、SSL 驗證 True）。
    如需 UI 控制 verify_ssl / lookback_days，請改用 fetch_amount_total_latest(...)。
    """
    ma, _ = fetch_amount_total_latest(lookback_days=10, verify_ssl=True)
    return ma


# ----------------------------
# intraday_norm: 你原本的功能保留
# ----------------------------
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


# ----------------------------
# JSON stringify helper（避免非字串型別導致 regex 失敗）
# ----------------------------
def json_dumps_safe(obj: Any) -> str:
    try:
        import json
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return str(obj)
