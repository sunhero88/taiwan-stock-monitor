# market_amount.py
# Predator V16.3 Stable Hybrid - Market Amount Fetcher (Stable Edition)
# 目標：TWSE/TPEX 成交金額「可用、可降級、可稽核」，避免 JSON/SSL/HTML 造成系統誤判

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any, List
import re
import time
import json

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
# Helpers: Time / Warnings
# =========================

def _now_ts() -> str:
    # 你主系統若已有 timestamp util，可改成共用；這裡先用簡單版本
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _push_warning(warnings: List[WarningItem], code: str, msg: str, meta: Optional[Dict[str, Any]] = None) -> None:
    warnings.append(WarningItem(ts=_now_ts(), code=code, msg=msg, meta=meta))


# =========================
# HTTP Stable Session
# =========================

def _make_session() -> requests.Session:
    s = requests.Session()
    # 預設 headers：避免被擋或回傳非預期
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36",
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
        if isinstance(x, (int,)):
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
            resp = session.request(method, url, params=params, timeout=timeout, verify=verify)
            return resp
        except Exception as e:
            last_exc = e
            # 線性 backoff（避免太久卡住）
            time.sleep(backoff_sec * (i + 1))
    # retries exhausted
    raise last_exc if last_exc else RuntimeError("request failed with unknown error")


# =========================
# TWSE / TPEX Fetchers
# =========================

def _fetch_twse_amount(
    allow_insecure_ssl: bool,
    warnings: List[WarningItem],
) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    取得 TWSE 上市成交金額（元）
    - 首選：TWSE /exchangeReport/MI_INDEX?response=json&type=ALLBUT0999
    - 成功：回 amount(int), source string, meta
    """
    session = _make_session()
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {"response": "json", "type": "ALLBUT0999"}

    verify = not bool(allow_insecure_ssl)

    try:
        resp = _request_with_retry(session, "GET", url, params=params, verify=verify, max_retry=3, timeout=8.0)
        text = resp.text or ""
        meta = {"status_code": resp.status_code, "url": resp.url}

        if resp.status_code != 200:
            _push_warning(
                warnings,
                "TWSE_AMOUNT_HTTP_ERROR",
                f"TWSE HTTP status != 200: {resp.status_code}",
                meta={"status_code": resp.status_code, "head": text[:200]},
            )
            return None, f"TWSE_FAIL:HTTP_{resp.status_code}", meta

        # TWSE 正常是 JSON
        try:
            data = resp.json()
        except Exception as je:
            _push_warning(
                warnings,
                "TWSE_AMOUNT_PARSE_FAIL",
                f"TWSE JSON parse fail: {je}",
                meta={"head": text[:300]},
            )
            return None, "TWSE_FAIL:JSONDecodeError", meta

        # TWSE MI_INDEX 常見欄位： data["data1"] / data["data9"] 等（會變動）
        # 這裡用較穩的方式：找「成交金額」那列的數字（多種表格結構皆可）
        # 1) 尝试從 data.get("data9") / "data1" 等尋找包含 "成交金額" 或 "成交金額(元)" 的 row
        candidates = []
        for k in ("data9", "data1", "data2", "data3", "data4", "data5", "data6", "data7", "data8"):
            v = data.get(k)
            if isinstance(v, list):
                candidates.append((k, v))

        amt = None
        found_from = None

        def _scan_rows(rows: list) -> Optional[int]:
            for row in rows:
                if not isinstance(row, list) or len(row) < 2:
                    continue
                joined = " ".join([str(x) for x in row if x is not None])
                if "成交金額" in joined:
                    # 取 row 中最後一個像數字的欄位
                    for cell in reversed(row):
                        val = _safe_int(cell)
                        if val is not None and val > 0:
                            return val
            return None

        for k, rows in candidates:
            tmp = _scan_rows(rows)
            if tmp is not None:
                amt = tmp
                found_from = k
                break

        # 2) 若仍找不到，退而求其次：在整包 JSON 字串用 regex 搜尋最大額度型數字（極少用）
        if amt is None:
            raw = json.dumps(data, ensure_ascii=False)
            # 抓 10~15 位數字（成交金額通常很大），再取最大值當候選（保守：仍標警告）
            nums = [int(x) for x in re.findall(r"\b\d{10,15}\b", raw)]
            if nums:
                amt = max(nums)
                _push_warning(
                    warnings,
                    "TWSE_AMOUNT_FALLBACK_REGEX",
                    "TWSE amount fallback by regex (structure changed).",
                    meta={"max_candidate": amt, "count": len(nums)},
                )
                found_from = "regex"

        if amt is None:
            _push_warning(
                warnings,
                "TWSE_AMOUNT_NOT_FOUND",
                "TWSE JSON ok but amount not found in payload (schema change?).",
                meta={"keys": list(data.keys())[:30]},
            )
            return None, "TWSE_FAIL:AMOUNT_NOT_FOUND", meta

        meta["found_from"] = found_from
        return int(amt), f"TWSE_OK:{found_from}", meta

    except requests.exceptions.SSLError as se:
        _push_warning(
            warnings,
            "TWSE_AMOUNT_SSL_ERROR",
            f"TWSE SSL error: {se}",
            meta={"url": url},
        )
        return None, "TWSE_FAIL:SSLError", {"url": url}

    except Exception as e:
        _push_warning(
            warnings,
            "TWSE_AMOUNT_UNKNOWN_ERROR",
            f"TWSE unknown error: {e}",
            meta={"url": url},
        )
        return None, f"TWSE_FAIL:{type(e).__name__}", {"url": url}


def _fetch_tpex_amount(
    allow_insecure_ssl: bool,
    warnings: List[WarningItem],
) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    取得 TPEX 上櫃成交金額（元）
    - 首選：TPEX JSON API（常會改/擋）
    - fallback：若回 HTML（DOCTYPE），改用 regex 從 HTML 抽數字
    """
    session = _make_session()

    # TPEX 常見接口（可能隨時間調整）
    # 你若已有既有 URL，可把它換回去；此處用較通用的「首頁/日成交」型資料源
    url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
    params = {"l": "zh-tw", "d": ""}  # d 讓系統用當日/最近資料（接口行為可能不同）

    verify = not bool(allow_insecure_ssl)

    try:
        resp = _request_with_retry(session, "GET", url, params=params, verify=verify, max_retry=3, timeout=8.0)
        text = resp.text or ""
        meta = {"status_code": resp.status_code, "url": resp.url}

        if resp.status_code != 200:
            _push_warning(
                warnings,
                "TPEX_AMOUNT_HTTP_ERROR",
                f"TPEX HTTP status != 200: {resp.status_code}",
                meta={"status_code": resp.status_code, "head": text[:200]},
            )
            return None, f"TPEX_FAIL:HTTP_{resp.status_code}", meta

        # 1) 先嘗試 JSON
        try:
            data = resp.json()
            # 嘗試多種 key（TPEX 結構常變）
            # 常見：data["aaData"] 裡有總計列或某 key 有 amount
            raw = json.dumps(data, ensure_ascii=False)

            # 找「成交金額」相關欄位（若有）
            # 先找明確 key
            for k in ("amount", "trade_value", "成交金額", "成交金額(元)"):
                if k in data:
                    amt = _safe_int(data.get(k))
                    if amt is not None and amt > 0:
                        return int(amt), f"TPEX_OK:key:{k}", meta

            # 再從 JSON 字串用 regex 找 10~15 位大數字
            nums = [int(x) for x in re.findall(r"\b\d{10,15}\b", raw)]
            if nums:
                amt = max(nums)
                _push_warning(
                    warnings,
                    "TPEX_AMOUNT_FALLBACK_REGEX",
                    "TPEX amount fallback by regex (json schema uncertain).",
                    meta={"max_candidate": amt, "count": len(nums)},
                )
                return int(amt), "TPEX_WARN:regex", meta

            _push_warning(
                warnings,
                "TPEX_AMOUNT_NOT_FOUND",
                "TPEX JSON ok but amount not found in payload.",
                meta={"keys": list(data.keys())[:30]},
            )
            return None, "TPEX_FAIL:AMOUNT_NOT_FOUND", meta

        except Exception as je:
            # 2) JSON 失敗：常見為回 HTML（DOCTYPE）
            head = text[:400].replace("\n", " ")
            _push_warning(
                warnings,
                "TPEX_AMOUNT_PARSE_FAIL",
                f"TPEX JSON decode error: {je}",
                meta={"text_head": head},
            )

            # fallback：從 HTML 找「成交金額」附近的數字
            # 由於不同頁面排版不同，採較寬鬆策略：抓所有 10~15 位數字，取最大值
            nums = [int(x) for x in re.findall(r"\b\d{10,15}\b", text.replace(",", ""))]
            if nums:
                amt = max(nums)
                _push_warning(
                    warnings,
                    "TPEX_AMOUNT_HTML_FALLBACK",
                    "TPEX returned HTML; amount extracted via regex-max.",
                    meta={"max_candidate": amt, "count": len(nums)},
                )
                return int(amt), "TPEX_WARN:html_regex", meta

            return None, "TPEX_FAIL:JSONDecodeError", meta

    except requests.exceptions.SSLError as se:
        _push_warning(
            warnings,
            "TPEX_AMOUNT_SSL_ERROR",
            f"TPEX SSL error: {se}",
            meta={"url": url},
        )
        return None, "TPEX_FAIL:SSLError", {"url": url}

    except Exception as e:
        _push_warning(
            warnings,
            "TPEX_AMOUNT_UNKNOWN_ERROR",
            f"TPEX unknown error: {e}",
            meta={"url": url},
        )
        return None, f"TPEX_FAIL:{type(e).__name__}", {"url": url}


# =========================
# Public API
# =========================

def fetch_amount_total(allow_insecure_ssl: bool = False) -> Tuple[MarketAmount, List[WarningItem]]:
    """
    回傳：上市、上櫃、合計成交金額（元） + warnings
    allow_insecure_ssl=True 時允許 verify=False 以繞過憑證鏈問題。
    """
    warnings: List[WarningItem] = []

    twse_amt, twse_src, twse_meta = _fetch_twse_amount(allow_insecure_ssl, warnings)
    tpex_amt, tpex_src, tpex_meta = _fetch_tpex_amount(allow_insecure_ssl, warnings)

    total: Optional[int] = None
    if twse_amt is not None and tpex_amt is not None:
        total = int(twse_amt) + int(tpex_amt)

    meta = {"twse": twse_meta, "tpex": tpex_meta}

    result = MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=bool(allow_insecure_ssl),
        meta=meta,
    )
    return result, warnings


def warnings_to_rows(warnings: List[WarningItem]) -> List[Dict[str, Any]]:
    """
    給 Streamlit table 用：轉成 [{ts, code, msg, meta}, ...]
    """
    rows = []
    for w in warnings[-50:]:
        rows.append({
            "ts": w.ts,
            "code": w.code,
            "msg": w.msg,
            "meta": w.meta if w.meta is not None else {},
        })
    return rows
