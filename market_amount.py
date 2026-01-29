# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Tuple, Dict, Any

import requests

TZ_TAIPEI = timezone(timedelta(hours=8))

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

CACHE_PATH = "data/market_amount_cache.json"


def _today_taipei() -> date:
    return datetime.now(tz=TZ_TAIPEI).date()


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _roc_yyy_mm_dd(d: date) -> str:
    # TPEx 有些 API 用民國年格式：114/01/29
    roc_year = d.year - 1911
    return f"{roc_year:03d}/{d.month:02d}/{d.day:02d}"


def _safe_int(x: Any) -> int:
    if x is None:
        return 0
    s = str(x)
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else 0


@dataclass
class MarketAmount:
    trade_date: str          # YYYYMMDD
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: Optional[str]
    source_tpex: Optional[str]
    warning: Optional[str] = None


def _load_cache() -> Optional[Dict[str, Any]]:
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(payload: Dict[str, Any]) -> None:
    # Streamlit Cloud 若資料夾不存在，會炸；所以要確保 data/ 有在 repo
    try:
        import os
        os.makedirs("data", exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        # cache 寫入失敗不應中斷程式
        pass


# -------------------------
# TWSE：官方 JSON（建議）
# -------------------------
def fetch_twse_amount_json(trade_date: date, verify_ssl: bool = True, timeout: int = 15) -> Tuple[int, str]:
    """
    取「上市成交金額(元)」：TWSE MI_INDEX JSON
    目標：取出「成交統計」表中的「成交金額(元)」欄並加總
    """
    ymd = _yyyymmdd(trade_date)
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={ymd}&type=ALL"
    r = requests.get(url, headers=USER_AGENT, timeout=timeout, verify=verify_ssl)
    r.raise_for_status()
    js = r.json()

    stat = js.get("stat", "")
    if "沒有符合條件" in stat or "查無資料" in stat:
        raise RuntimeError(f"TWSE 無資料: {stat}")

    # MI_INDEX JSON 常見欄位：fields1 + data1（成交統計）
    fields = js.get("fields1") or []
    data = js.get("data1") or []

    if not fields or not data:
        # fallback：有些版本欄位名不同
        fields = js.get("fields") or fields
        data = js.get("data") or data

    if not fields or not data:
        raise RuntimeError("TWSE MI_INDEX(JSON) 結構異常，找不到 fields/data")

    # 找「成交金額」欄位 index
    amt_idx = None
    for i, f in enumerate(fields):
        if "成交金額" in str(f):
            amt_idx = i
            break
    if amt_idx is None:
        raise RuntimeError("TWSE MI_INDEX(JSON) 找不到『成交金額』欄位")

    total = 0
    for row in data:
        if not isinstance(row, list) or len(row) <= amt_idx:
            continue
        total += _safe_int(row[amt_idx])

    if total <= 0:
        raise RuntimeError("TWSE 成交金額加總為 0，疑似欄位變更或資料異常")

    return total, f"TWSE MI_INDEX(JSON) date={ymd} 成交統計加總"


# -------------------------
# TPEx：優先 OpenAPI，失敗才網頁 regex
# -------------------------
def fetch_tpex_amount_openapi(trade_date: date, verify_ssl: bool = True, timeout: int = 15) -> Tuple[int, str]:
    """
    嘗試用 TPEx OpenAPI 取得上櫃總成交金額(元)
    注意：TPEx OpenAPI 的端點可能調整，這裡做多重 fallback。
    """
    # fallback 1：常見 openapi endpoint（若失效會拋例外，交給上層回溯）
    # 這個端點在不同時期可能不同；用「可失敗」設計即可。
    ymd = _roc_yyy_mm_dd(trade_date)

    candidates = [
        # 某些版本提供「盤後資訊」彙總（JSON）
        f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_pricing?date={ymd}",
        f"https://www.tpex.org.tw/openapi/v1/tpex_pricing?date={ymd}",
    ]

    last_err = None
    for url in candidates:
        try:
            r = requests.get(url, headers=USER_AGENT, timeout=timeout, verify=verify_ssl)
            r.raise_for_status()
            js = r.json()

            # js 可能是 list[dict] 或 dict
            # 我們尋找任何 key 包含「總成交金額」
            if isinstance(js, dict):
                items = [js]
            elif isinstance(js, list):
                items = js
            else:
                items = []

            for it in items:
                if not isinstance(it, dict):
                    continue
                for k, v in it.items():
                    if "總成交金額" in str(k) or "成交金額" in str(k):
                        amt = _safe_int(v)
                        if amt > 0:
                            return amt, f"TPEx OpenAPI {url}"
            # 若找不到，視為失敗繼續試下一個
            last_err = RuntimeError(f"TPEx OpenAPI 格式未含成交金額: {url}")

        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"TPEx OpenAPI 全部失敗: {last_err}")


def fetch_tpex_amount_web(trade_date: date, verify_ssl: bool = True, timeout: int = 15) -> Tuple[int, str]:
    """
    TPEx 網頁 regex（最後手段）
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    r = requests.get(url, headers=USER_AGENT, timeout=timeout, verify=verify_ssl)
    r.raise_for_status()
    text = r.text

    m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", text)
    if not m:
        m = re.search(r"總成交金額.*?([\d,]+)\s*元", text)
    if not m:
        raise RuntimeError("TPEx pricing.html 找不到『總成交金額』")

    amt = _safe_int(m.group(1))
    if amt <= 0:
        raise RuntimeError("TPEx 總成交金額解析為 0")
    return amt, "TPEx pricing.html regex 總成交金額"


def fetch_tpex_amount(trade_date: date, verify_ssl: bool = True, timeout: int = 15) -> Tuple[int, str]:
    # 顯式多路徑：OpenAPI -> Web
    try:
        return fetch_tpex_amount_openapi(trade_date, verify_ssl=verify_ssl, timeout=timeout)
    except Exception:
        return fetch_tpex_amount_web(trade_date, verify_ssl=verify_ssl, timeout=timeout)


# -------------------------
# 回溯：找最後可用交易日
# -------------------------
def fetch_amount_total_latest(
    base_date: Optional[date] = None,
    lookback_days: int = 10,
    verify_ssl: bool = True,
    timeout: int = 15,
) -> MarketAmount:
    """
    目標：取得「最新可用交易日」的成交金額（上市+上櫃）
    - 若 base_date 是今天：會從今天往回找
    - 成功後寫入 cache
    - 全失敗：回傳 cache（若有），並附 warning
    """
    base_date = base_date or _today_taipei()

    last_err = None
    for i in range(lookback_days + 1):
        d = base_date - timedelta(days=i)
        ymd = _yyyymmdd(d)

        twse_amt = None
        tpex_amt = None
        s_twse = None
        s_tpex = None

        # TWSE
        try:
            twse_amt, s_twse = fetch_twse_amount_json(d, verify_ssl=verify_ssl, timeout=timeout)
        except Exception as e:
            last_err = e

        # TPEx
        try:
            tpex_amt, s_tpex = fetch_tpex_amount(d, verify_ssl=verify_ssl, timeout=timeout)
        except Exception as e:
            last_err = e

        # 成功條件：至少一邊有值，且總和 > 0（模擬期允許 TPEx 暫缺，但會提示）
        if (twse_amt and twse_amt > 0) or (tpex_amt and tpex_amt > 0):
            total = None
            if (twse_amt and twse_amt > 0) and (tpex_amt and tpex_amt > 0):
                total = twse_amt + tpex_amt
                warn = None
            else:
                total = (twse_amt or 0) + (tpex_amt or 0)
                warn = "成交金額來源不完整（TWSE 或 TPEx 缺一），僅供參考；裁決層需視規則降級。"

            payload = {
                "trade_date": ymd,
                "amount_twse": twse_amt,
                "amount_tpex": tpex_amt,
                "amount_total": total,
                "source_twse": s_twse,
                "source_tpex": s_tpex,
                "cached_at": datetime.now(tz=TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S"),
            }
            _save_cache(payload)

            return MarketAmount(
                trade_date=ymd,
                amount_twse=twse_amt,
                amount_tpex=tpex_amt,
                amount_total=total,
                source_twse=s_twse,
                source_tpex=s_tpex,
                warning=warn,
            )

    # 全失敗 -> 用 cache
    cache = _load_cache()
    if cache:
        return MarketAmount(
            trade_date=cache.get("trade_date", ""),
            amount_twse=cache.get("amount_twse"),
            amount_tpex=cache.get("amount_tpex"),
            amount_total=cache.get("amount_total"),
            source_twse=cache.get("source_twse"),
            source_tpex=cache.get("source_tpex"),
            warning=f"官方抓取失敗（{last_err}），已回退快取資料。",
        )

    # 連 cache 都沒有
    return MarketAmount(
        trade_date="",
        amount_twse=None,
        amount_tpex=None,
        amount_total=None,
        source_twse=None,
        source_tpex=None,
        warning=f"官方抓取失敗且無快取（{last_err}）",
    )
