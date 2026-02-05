# market_amount.py
# Predator V16.3 Stable Hybrid - Market Amount Fetcher (Patched)
# 目標：TWSE/TPEX 成交金額「可用、可降級、可稽核」

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any, List
import time
import json
import re

import requests


# =========================
# Data Structures
# =========================

@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
    allow_insecure_ssl: bool = False
    meta: Optional[Dict[str, Any]] = None


@dataclass
class WarningItem:
    ts: str
    code: str
    msg: str
    meta: Optional[Dict[str, Any]] = None


# =========================
# Helpers
# =========================

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _push_warning(warnings: List[WarningItem], code: str, msg: str, meta: Optional[Dict[str, Any]] = None) -> None:
    warnings.append(WarningItem(ts=_now_ts(), code=code, msg=msg, meta=meta))


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json,text/plain,text/html,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        return int(float(s))
    except Exception:
        return None


def _request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 8.0,
    verify: bool = True,
    max_retry: int = 3,
    backoff_sec: float = 0.6,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for i in range(max_retry):
        try:
            return session.request(method, url, params=params, timeout=timeout, verify=verify)
        except Exception as e:
            last_exc = e
            time.sleep(backoff_sec * (i + 1))
    raise last_exc if last_exc else RuntimeError("request failed with unknown error")


# =========================
# TWSE
# =========================

def _fetch_twse_amount(allow_insecure_ssl: bool, warnings: List[WarningItem]) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    TWSE 上市成交金額（元）
    解析重點（修正點）：
    - 欄位名稱在 fields9
    - 數據在 data9（最後一列通常是合計，或可找「合計」列）
    """
    session = _make_session()
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {"response": "json", "type": "ALLBUT0999"}
    verify = not bool(allow_insecure_ssl)

    meta: Dict[str, Any] = {"url": url, "params": params}

    try:
        resp = _request_with_retry(session, "GET", url, params=params, verify=verify, max_retry=3, timeout=8.0)
        meta["status_code"] = resp.status_code
        meta["final_url"] = resp.url

        if resp.status_code != 200:
            _push_warning(warnings, "TWSE_HTTP_ERROR", f"HTTP {resp.status_code}", {"head": (resp.text or "")[:200]})
            return None, f"TWSE_FAIL:HTTP_{resp.status_code}", meta

        try:
            data = resp.json()
        except Exception as je:
            _push_warning(warnings, "TWSE_JSON_PARSE_FAIL", f"{je}", {"head": (resp.text or "")[:300]})
            return None, "TWSE_FAIL:JSONDecodeError", meta

        fields9 = data.get("fields9")
        data9 = data.get("data9")

        if not isinstance(fields9, list) or not isinstance(data9, list) or not data9:
            _push_warning(warnings, "TWSE_SCHEMA_UNEXPECTED", "fields9/data9 missing or empty", {"keys": list(data.keys())[:30]})
            return None, "TWSE_FAIL:SCHEMA", meta

        # 找「成交金額」欄位 index（表頭）
        amt_idx = None
        for i, f in enumerate(fields9):
            if "成交金額" in str(f):
                amt_idx = i
                break

        if amt_idx is None:
            _push_warning(warnings, "TWSE_AMOUNT_FIELD_NOT_FOUND", "No 成交金額 in fields9", {"fields9": fields9})
            return None, "TWSE_FAIL:AMT_FIELD_NOT_FOUND", meta

        # 優先找「合計」列（第一欄通常是分類名稱）
        pick_row = None
        for row in reversed(data9):
            if isinstance(row, list) and row and ("合計" in str(row[0]) or "總計" in str(row[0])):
                pick_row = row
                break

        # 否則取最後一列（常見即合計）
        if pick_row is None:
            pick_row = data9[-1] if isinstance(data9[-1], list) else None

        if not isinstance(pick_row, list) or amt_idx >= len(pick_row):
            _push_warning(warnings, "TWSE_AMOUNT_ROW_BAD", "Picked row invalid or index out of range",
                          {"amt_idx": amt_idx, "row_head": pick_row[:5] if isinstance(pick_row, list) else None})
            return None, "TWSE_FAIL:AMOUNT_NOT_FOUND", meta

        amt = _safe_int(pick_row[amt_idx])
        if amt is None or amt <= 0:
            _push_warning(warnings, "TWSE_AMOUNT_CELL_BAD", "Amount cell not numeric", {"cell": pick_row[amt_idx]})
            return None, "TWSE_FAIL:AMOUNT_NOT_FOUND", meta

        meta["amt_idx"] = amt_idx
        meta["picked_row_first_cell"] = str(pick_row[0]) if pick_row else None
        return int(amt), "TWSE_OK:MI_INDEX_FIELDS9_DATA9", meta

    except requests.exceptions.SSLError as se:
        _push_warning(warnings, "TWSE_SSL_ERROR", f"{se}", {"url": url})
        return None, "TWSE_FAIL:SSLError", meta
    except Exception as e:
        _push_warning(warnings, "TWSE_UNKNOWN_ERROR", f"{e}", {"url": url})
        return None, f"TWSE_FAIL:{type(e).__name__}", meta


# =========================
# TPEX (先止血：多端點嘗試 + 降級)
# =========================

def _try_json_amount(resp: requests.Response) -> Optional[int]:
    try:
        js = resp.json()
    except Exception:
        return None

    # 常見候選 key（不同版本可能不同）
    candidates = [
        "amount_total", "amountTotal", "totalAmount", "trade_value", "tradeValue",
        "成交金額", "成交金額(元)", "total_trade_value", "total_trade_amount"
    ]
    for k in candidates:
        if isinstance(js, dict) and k in js:
            v = _safe_int(js.get(k))
            if v and v > 0:
                return v

    # 若是 list/dict 混合，退而求其次：掃描 10~15 位數字取最大
    raw = json.dumps(js, ensure_ascii=False)
    nums = [int(x) for x in re.findall(r"\b\d{10,15}\b", raw)]
    return max(nums) if nums else None


def _fetch_tpex_amount(allow_insecure_ssl: bool, warnings: List[WarningItem]) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    TPEX 上櫃成交金額（元）
    先止血：多端點嘗試（你後續要「可稽核加總」我再給你完整 TPEX 加總版）
    """
    session = _make_session()
    verify = not bool(allow_insecure_ssl)

    # 你可以在這裡隨時增減「可用端點」
    endpoints = [
        # 你原本用的（可能不含總額，但先嘗試）
        ("TPEX_WN1430", "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php", {"l": "zh-tw", "d": ""}),
        # 常見舊路徑（若未來恢復/改版可用）
        ("TPEX_ST43", "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php", {"l": "zh-tw"}),
    ]

    last_meta: Dict[str, Any] = {}

    for name, url, params in endpoints:
        meta = {"endpoint": name, "url": url, "params": params}
        last_meta = meta
        try:
            resp = _request_with_retry(session, "GET", url, params=params, verify=verify, max_retry=2, timeout=8.0)
            meta["status_code"] = resp.status_code
            meta["final_url"] = resp.url

            if resp.status_code != 200:
                _push_warning(warnings, "TPEX_HTTP_ERROR", f"{name} HTTP {resp.status_code}", meta)
                continue

            amt = _try_json_amount(resp)
            if amt and amt > 0:
                return int(amt), f"TPEX_OK:{name}", meta

            _push_warning(warnings, "TPEX_AMOUNT_NOT_FOUND", f"{name} JSON ok but no total amount", meta)

        except requests.exceptions.SSLError as se:
            _push_warning(warnings, "TPEX_SSL_ERROR", f"{name} {se}", meta)
        except Exception as e:
            _push_warning(warnings, "TPEX_ERROR", f"{name} {e}", meta)

    return None, "TPEX_FAIL:AMOUNT_NOT_FOUND", last_meta


# =========================
# Public API
# =========================

def fetch_amount_total(allow_insecure_ssl: bool = False) -> Tuple[MarketAmount, List[WarningItem]]:
    warnings: List[WarningItem] = []

    twse_amt, twse_src, twse_meta = _fetch_twse_amount(allow_insecure_ssl, warnings)
    tpex_amt, tpex_src, tpex_meta = _fetch_tpex_amount(allow_insecure_ssl, warnings)

    total: Optional[int] = None
    if twse_amt is not None and tpex_amt is not None:
        total = int(twse_amt) + int(tpex_amt)
    elif twse_amt is not None:
        # 先止血：至少給「上市」成交金額，避免 total 永遠 null
        total = int(twse_amt)

    result = MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=bool(allow_insecure_ssl),
        meta={"twse": twse_meta, "tpex": tpex_meta},
    )
    return result, warnings


def warnings_to_rows(warnings: List[WarningItem]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for w in warnings[-50:]:
        rows.append({"ts": w.ts, "code": w.code, "msg": w.msg, "meta": w.meta or {}})
    return rows
