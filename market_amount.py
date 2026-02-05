# market_amount.py
# =========================================================
# Predator V16.x - Market Amount (AUDITABLE SUM) Edition
# 目標：
# - TWSE / TPEX 不再抓「總額欄位」(易變動)
# - 改抓「逐檔每日交易資料」→ 以欄位加總成交金額
# - 產出可稽核落地檔案：
#   (1) 原始回應(raw json/text)
#   (2) 解析後逐檔表格(csv)
#   (3) 稽核摘要(audit json)
# - 可降級、可警示、可追溯
# =========================================================

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional, Tuple, Dict, Any, List
import os
import re
import json
import time
import math
import requests
from datetime import datetime


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


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _write_text(path: str, s: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(s)


def _write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            if math.isnan(x):
                return None
            return int(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        return int(float(s))
    except Exception:
        return None


def _make_session() -> requests.Session:
    s = requests.Session()
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


def _request_with_retry(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 12.0,
    verify: bool = True,
    max_retry: int = 3,
    backoff_sec: float = 0.8,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for i in range(max_retry):
        try:
            resp = session.get(url, params=params, timeout=timeout, verify=verify)
            return resp
        except Exception as e:
            last_exc = e
            time.sleep(backoff_sec * (i + 1))
    raise last_exc if last_exc else RuntimeError("request failed")


def _yyyymmdd(date_str: str) -> str:
    # "2026-02-05" -> "20260205"
    return date_str.replace("-", "")


def _to_roc_yyy_mm_dd(date_str: str) -> str:
    # "2026-02-05" -> "115/02/05"
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    roc_y = dt.year - 1911
    return f"{roc_y:03d}/{dt.month:02d}/{dt.day:02d}"


def _audit_paths(audit_dir: str, trade_date: str, tag: str) -> Dict[str, str]:
    _ensure_dir(audit_dir)
    base = os.path.join(audit_dir, f"{tag}_{trade_date.replace('-', '')}")
    return {
        "raw": base + "_raw.txt",
        "json": base + "_raw.json",
        "csv": base + "_rows.csv",
        "audit": base + "_audit.json",
    }


def _rows_to_csv(path: str, headers: List[str], rows: List[List[Any]]) -> None:
    # 最小依賴：不用 pandas
    import csv
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(r)


# =========================
# TWSE: 逐檔資料加總 (STOCK_DAY_ALL)
# =========================

def _fetch_twse_amount_auditable(
    trade_date: str,
    allow_insecure_ssl: bool,
    warnings: List[WarningItem],
    audit_dir: str,
) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    TWSE 上市：抓全市場逐檔日資料 → 以「成交金額」欄位加總
    端點：/exchangeReport/STOCK_DAY_ALL?response=json&date=YYYYMMDD
    """
    session = _make_session()
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": _yyyymmdd(trade_date)}
    verify = not bool(allow_insecure_ssl)

    paths = _audit_paths(audit_dir, trade_date, "TWSE")
    meta: Dict[str, Any] = {"url": url, "params": params}

    try:
        resp = _request_with_retry(session, url, params=params, verify=verify)
        meta["status_code"] = resp.status_code
        meta["final_url"] = resp.url
        text = resp.text or ""

        _write_text(paths["raw"], text)

        if resp.status_code != 200:
            _push_warning(warnings, "TWSE_HTTP_ERROR", f"TWSE HTTP {resp.status_code}", {"head": text[:200]})
            return None, f"TWSE_FAIL:HTTP_{resp.status_code}", meta

        try:
            js = resp.json()
            _write_json(paths["json"], js)
        except Exception as e:
            _push_warning(warnings, "TWSE_JSON_DECODE_FAIL", f"TWSE JSON decode fail: {e}", {"head": text[:300]})
            return None, "TWSE_FAIL:JSONDecodeError", meta

        fields = js.get("fields", None)
        data = js.get("data", None)

        if not isinstance(fields, list) or not isinstance(data, list):
            _push_warning(warnings, "TWSE_SCHEMA_UNEXPECTED", "TWSE missing fields/data", {"keys": list(js.keys())[:20]})
            return None, "TWSE_FAIL:SCHEMA", meta

        # 找「成交金額」欄位 index
        amt_idx = None
        for i, f in enumerate(fields):
            if "成交金額" in str(f):
                amt_idx = i
                break

        if amt_idx is None:
            _push_warning(warnings, "TWSE_AMOUNT_COL_NOT_FOUND", "TWSE fields has no 成交金額", {"fields": fields})
            return None, "TWSE_FAIL:AMOUNT_COL_NOT_FOUND", meta

        # 加總
        total = 0
        missing_rows = 0
        n_rows = 0

        # 我們也順便把逐檔 rows 落地成 CSV，方便人工抽查
        out_rows: List[List[Any]] = []
        out_headers = fields[:]  # 原始欄位

        for row in data:
            if not isinstance(row, list):
                continue
            n_rows += 1
            out_rows.append(row)
            if amt_idx >= len(row):
                missing_rows += 1
                continue
            v = _safe_int(row[amt_idx])
            if v is None:
                missing_rows += 1
                continue
            total += int(v)

        _rows_to_csv(paths["csv"], out_headers, out_rows)

        audit = {
            "market": "TWSE",
            "trade_date": trade_date,
            "rows": n_rows,
            "missing_amount_rows": missing_rows,
            "amount_sum": total,
            "amount_col": str(fields[amt_idx]),
            "amount_col_index": amt_idx,
            "raw_saved": os.path.basename(paths["raw"]),
            "json_saved": os.path.basename(paths["json"]),
            "csv_saved": os.path.basename(paths["csv"]),
        }
        _write_json(paths["audit"], audit)

        meta["audit"] = audit
        if n_rows == 0 or total <= 0:
            _push_warning(warnings, "TWSE_SUM_SUSPECT", "TWSE sum is empty/zero (suspicious)", audit)
            return None, "TWSE_FAIL:SUM_ZERO", meta

        return int(total), "TWSE_OK:AUDIT_SUM", meta

    except requests.exceptions.SSLError as e:
        _push_warning(warnings, "TWSE_SSL_ERROR", f"TWSE SSL error: {e}", meta)
        return None, "TWSE_FAIL:SSLError", meta
    except Exception as e:
        _push_warning(warnings, "TWSE_UNKNOWN_ERROR", f"TWSE error: {e}", meta)
        return None, f"TWSE_FAIL:{type(e).__name__}", meta


# =========================
# TPEX: 逐檔資料加總 (st43_result)
# =========================

def _fetch_tpex_amount_auditable(
    trade_date: str,
    allow_insecure_ssl: bool,
    warnings: List[WarningItem],
    audit_dir: str,
) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    TPEX 上櫃：抓全市場逐檔日資料 → 以「成交金額」欄位加總
    端點：st43_result.php (每日交易資訊；通常回 JSON，包含 title + aaData)
    參數 d 通常為民國年：YYY/MM/DD
    """
    session = _make_session()
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    params = {"l": "zh-tw", "d": _to_roc_yyy_mm_dd(trade_date)}
    verify = not bool(allow_insecure_ssl)

    paths = _audit_paths(audit_dir, trade_date, "TPEX")
    meta: Dict[str, Any] = {"url": url, "params": params}

    try:
        resp = _request_with_retry(session, url, params=params, verify=verify)
        meta["status_code"] = resp.status_code
        meta["final_url"] = resp.url
        text = resp.text or ""

        _write_text(paths["raw"], text)

        if resp.status_code != 200:
            _push_warning(warnings, "TPEX_HTTP_ERROR", f"TPEX HTTP {resp.status_code}", {"head": text[:200]})
            return None, f"TPEX_FAIL:HTTP_{resp.status_code}", meta

        # 先解析 JSON
        try:
            js = resp.json()
            _write_json(paths["json"], js)
        except Exception as e:
            _push_warning(warnings, "TPEX_JSON_DECODE_FAIL", f"TPEX JSON decode fail: {e}", {"head": text[:300]})
            return None, "TPEX_FAIL:JSONDecodeError", meta

        title = js.get("title", None)
        aa = js.get("aaData", None) or js.get("data", None)

        if not isinstance(aa, list):
            _push_warning(warnings, "TPEX_SCHEMA_UNEXPECTED", "TPEX missing aaData/data", {"keys": list(js.keys())[:30]})
            return None, "TPEX_FAIL:SCHEMA", meta

        # 嘗試用 title 對應欄位 index（title 若存在通常是欄位名陣列）
        amt_idx = None
        headers: List[str] = []

        if isinstance(title, list) and all(isinstance(x, str) for x in title):
            headers = title
            for i, h in enumerate(headers):
                if "成交金額" in h:
                    amt_idx = i
                    break

        # 若 title 沒有或找不到，就用常見位置 fallback（仍可稽核：會在 audit 註記）
        fallback_used = False
        if amt_idx is None:
            fallback_used = True
            # 常見 st43：欄位通常包含「成交金額(元)」，位置可能在中後段
            # 我們採「掃第一列」：找最大位數的數字欄位當候選，再配合欄位數穩健處理
            if len(aa) > 0 and isinstance(aa[0], list):
                # 先用「欄位名不存在」的情況：假設成交金額是「10~15 位」大數字欄位之一
                # 以每列取出所有可轉 int 的欄位，統計出現最像成交金額的欄位 index（眾數）
                idx_votes: Dict[int, int] = {}
                sample_n = min(50, len(aa))
                for r in aa[:sample_n]:
                    if not isinstance(r, list):
                        continue
                    for j, cell in enumerate(r):
                        v = _safe_int(cell)
                        if v is None:
                            continue
                        # 成交金額通常至少 7~8 位以上；用下限濾掉價格/股數小值
                        if v >= 10_000_000:  # 1,000萬 作為粗濾
                            idx_votes[j] = idx_votes.get(j, 0) + 1
                if idx_votes:
                    amt_idx = max(idx_votes.items(), key=lambda kv: kv[1])[0]

        if amt_idx is None:
            _push_warning(warnings, "TPEX_AMOUNT_COL_NOT_FOUND", "TPEX cannot locate 成交金額 column", {"title": title})
            return None, "TPEX_FAIL:AMOUNT_COL_NOT_FOUND", meta

        # 加總 + 落 CSV
        total = 0
        missing_rows = 0
        n_rows = 0

        out_rows: List[List[Any]] = []
        out_headers = headers if headers else [f"col_{i}" for i in range(len(aa[0]) if aa and isinstance(aa[0], list) else 0)]

        for row in aa:
            if not isinstance(row, list):
                continue
            n_rows += 1
            out_rows.append(row)
            if amt_idx >= len(row):
                missing_rows += 1
                continue
            v = _safe_int(row[amt_idx])
            if v is None:
                missing_rows += 1
                continue
            total += int(v)

        _rows_to_csv(paths["csv"], out_headers, out_rows)

        audit = {
            "market": "TPEX",
            "trade_date": trade_date,
            "roc_date": params["d"],
            "rows": n_rows,
            "missing_amount_rows": missing_rows,
            "amount_sum": total,
            "amount_col_index": amt_idx,
            "fallback_used": fallback_used,
            "raw_saved": os.path.basename(paths["raw"]),
            "json_saved": os.path.basename(paths["json"]),
            "csv_saved": os.path.basename(paths["csv"]),
        }
        if headers and amt_idx < len(headers):
            audit["amount_col"] = headers[amt_idx]

        _write_json(paths["audit"], audit)

        meta["audit"] = audit
        if n_rows == 0 or total <= 0:
            _push_warning(warnings, "TPEX_SUM_SUSPECT", "TPEX sum is empty/zero (suspicious)", audit)
            return None, "TPEX_FAIL:SUM_ZERO", meta

        # 若 fallback_used=True，給一個「可用但需注意」警示（不等於失敗）
        if fallback_used:
            _push_warning(
                warnings,
                "TPEX_FALLBACK_COLUMN_GUESS",
                "TPEX title missing; amount column guessed by numeric heuristic (still auditable via saved CSV/JSON).",
                {"amount_col_index": amt_idx, "sample_rows": min(50, len(aa))}
            )

        return int(total), "TPEX_OK:AUDIT_SUM", meta

    except requests.exceptions.SSLError as e:
        _push_warning(warnings, "TPEX_SSL_ERROR", f"TPEX SSL error: {e}", meta)
        return None, "TPEX_FAIL:SSLError", meta
    except Exception as e:
        _push_warning(warnings, "TPEX_UNKNOWN_ERROR", f"TPEX error: {e}", meta)
        return None, f"TPEX_FAIL:{type(e).__name__}", meta


# =========================
# Public API
# =========================

def fetch_amount_total(
    trade_date: str,
    allow_insecure_ssl: bool = False,
    audit_dir: str = "data/audit_market_amount",
) -> Tuple[MarketAmount, List[WarningItem]]:
    """
    回傳：TWSE / TPEX / TOTAL 成交金額（元） + warnings
    trade_date: "YYYY-MM-DD"
    """
    warnings: List[WarningItem] = []
    _ensure_dir(audit_dir)

    twse_amt, twse_src, twse_meta = _fetch_twse_amount_auditable(trade_date, allow_insecure_ssl, warnings, audit_dir)
    tpex_amt, tpex_src, tpex_meta = _fetch_tpex_amount_auditable(trade_date, allow_insecure_ssl, warnings, audit_dir)

    total = None
    if twse_amt is not None and tpex_amt is not None:
        total = int(twse_amt) + int(tpex_amt)

    meta = {
        "trade_date": trade_date,
        "audit_dir": audit_dir,
        "twse": twse_meta,
        "tpex": tpex_meta,
    }

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
    rows = []
    for w in warnings[-50:]:
        rows.append({
            "ts": w.ts,
            "code": w.code,
            "msg": w.msg,
            "meta": w.meta if w.meta is not None else {},
        })
    return rows
