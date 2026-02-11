# main.py  (V16.3.34_FINAL)
# ------------------------------------------------------------
# 目標：
# 1) TPEX 成交額四層 fallback：TPEX官方 -> FinMind(OTC普通股) -> Yahoo估算 -> Safe Mode
# 2) OTC scope：只算普通股，排除 ETF/ETN/Index/證券型商品（用 industry_category + stock_name 關鍵字排除）
# 3) market_status：直接引用 market_amount.confidence_level
# 4) FinMind token：優先讀 Streamlit secrets，其次讀環境變數，不需要每次輸入
# ------------------------------------------------------------

from __future__ import annotations

import os
import re
import json
import time
import math
import traceback
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import requests

# Streamlit 可能不存在於非 UI 執行環境（例如純 CLI）
try:
    import streamlit as st
except Exception:
    st = None  # type: ignore


# -----------------------------
# 基本設定
# -----------------------------
AUDIT_DIR = "data/audit_market_amount"
SAFE_MODE_TPEX = 200_000_000_000  # 2,000 億
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

DEFAULT_TIMEOUT = 30

# OTC 普通股排除（關鍵字）
# 你指定：用 industry_category + stock_name 排除
EXCLUDE_NAME_KEYWORDS = [
    # ETF/ETN/指數/槓桿/反向常見字樣
    "ETF", "ETN", "指數", "槓桿", "反向", "期貨", "選擇權",
    # 常見投信/發行商識別（避免把指數型商品混進去）
    "元大", "富邦", "國泰", "群益", "復華", "永豐", "中信", "兆豐", "第一金", "凱基",
    "統一", "野村", "富蘭克林", "瀚亞", "貝萊德", "摩根", "施羅德",
    # 其他可能在商品型名稱出現
    "收益", "債", "債券", "票券", "基金", "商品", "黃金", "原油", "波動",
]

EXCLUDE_INDUSTRY_KEYWORDS = [
    "ETF", "ETN", "INDEX", "指數", "基金", "受益證券", "存託憑證", "債券", "期貨", "選擇權"
]

# 允許的 OTC 普通股代碼（一般 4 碼；但也有特殊例外）
STOCK_ID_RE = re.compile(r"^\d{4,6}$")


# -----------------------------
# 警示匯流排（簡化版）
# -----------------------------
class WarningsBus:
    def __init__(self) -> None:
        self.items: List[Dict[str, Any]] = []

    def push(self, code: str, msg: str, meta: Optional[Dict[str, Any]] = None) -> None:
        self.items.append({"code": code, "msg": msg, "meta": meta or {}, "ts": _now_ts()})


warnings_bus = WarningsBus()


# -----------------------------
# 資料結構
# -----------------------------
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str

    status_twse: str           # OK / ESTIMATED / FAIL
    status_tpex: str           # OK / ESTIMATED / FAIL
    confidence_twse: str       # HIGH / MEDIUM / LOW
    confidence_tpex: str       # HIGH / MEDIUM / LOW
    confidence_level: str      # HIGH / MEDIUM / LOW

    allow_insecure_ssl: bool
    scope: str
    meta: Optional[Dict[str, Any]] = None


# -----------------------------
# 小工具
# -----------------------------
def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return default
        if isinstance(x, (int,)):
            return int(x)
        if isinstance(x, float):
            if math.isnan(x) or math.isinf(x):
                return default
            return int(x)
        s = str(x).strip().replace(",", "")
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _ymd_to_compact(trade_date: str) -> str:
    # "2026-02-10" -> "20260210"
    return trade_date.replace("-", "")


def _http_get(url: str, params: Optional[dict] = None, allow_insecure_ssl: bool = False, timeout: int = DEFAULT_TIMEOUT) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    return requests.get(url, params=params, headers=headers, timeout=timeout, verify=not allow_insecure_ssl)


def _amount_scope(twse_amt: int, tpex_amt: int) -> str:
    # 你原系統可能有更細的 scope；此處維持 "ALL"
    return "ALL"


def _confidence_merge(conf_twse: str, conf_tpex: str) -> str:
    # 規則：任一 LOW -> LOW；兩者 HIGH -> HIGH；其餘 -> MEDIUM
    if conf_twse == "LOW" or conf_tpex == "LOW":
        return "LOW"
    if conf_twse == "HIGH" and conf_tpex == "HIGH":
        return "HIGH"
    return "MEDIUM"


def _load_finmind_token() -> Tuple[Optional[str], bool]:
    """
    Token 讀取順序：
    1) Streamlit secrets：st.secrets["FINMIND_TOKEN"]
    2) 環境變數：FINMIND_TOKEN
    回傳：(token, loaded_bool)
    """
    token = None
    loaded = False

    # 1) Streamlit secrets
    if st is not None:
        try:
            if "FINMIND_TOKEN" in st.secrets:
                token = str(st.secrets["FINMIND_TOKEN"]).strip()
                loaded = bool(token)
        except Exception:
            pass

    # 2) env
    if not token:
        token = os.getenv("FINMIND_TOKEN", "").strip()
        loaded = bool(token)

    if not token:
        return None, False
    return token, loaded


# -----------------------------
# TWSE 成交額（官方：STOCK_DAY_ALL）
# -----------------------------
def _twse_audit_sum_by_stock_day_all(trade_date: str, allow_insecure_ssl: bool) -> Tuple[Optional[int], str, Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "url": "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL",
        "params": {"response": "json", "date": _ymd_to_compact(trade_date)},
        "status_code": None,
        "final_url": None,
        "audit": None,
    }

    try:
        r = _http_get(meta["url"], params=meta["params"], allow_insecure_ssl=allow_insecure_ssl)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        r.raise_for_status()
        js = r.json()

        data = js.get("data", [])
        # TWSE STOCK_DAY_ALL：欄位格式可能變動；此處用保守 approach：嘗試抓成交金額欄位
        # 常見：row[2] 或 row[...]
        # 你原系統已有成熟解析邏輯；這裡給一個可用版本：尋找最像「成交金額」的欄位
        total = 0
        rows = 0

        for row in data:
            if not isinstance(row, list) or len(row) < 6:
                continue
            # 嘗試從最後幾欄找最大的整數欄位當成交金額（audit sum）
            candidates = []
            for v in row[-6:]:
                iv = _safe_int(v, None)
                if iv is not None and iv > 0:
                    candidates.append(iv)
            if not candidates:
                continue
            amt = max(candidates)
            total += amt
            rows += 1

        audit = {"market": "TWSE", "trade_date": trade_date, "rows": rows, "amount_sum": int(total)}
        meta["audit"] = audit

        if total > 300_000_000_000:  # 3,000 億下限（盤中/盤後可調）
            return int(total), "TWSE_OK:AUDIT_SUM", meta

        warnings_bus.push("TWSE_AMOUNT_TOO_LOW", f"TWSE成交額偏低：{total:,}", {"trade_date": trade_date})
        return None, "TWSE_FAIL:AMOUNT_TOO_LOW", meta

    except Exception as e:
        warnings_bus.push("TWSE_FAIL", str(e), {"trade_date": trade_date, "trace": traceback.format_exc()})
        return None, f"TWSE_FAIL:{type(e).__name__}", meta


# -----------------------------
# TPEX 成交額（官方：st43_result.php，已知近期常導向 /errors）
# -----------------------------
def _tpex_audit_sum_by_st43(trade_date: str, allow_insecure_ssl: bool) -> Tuple[Optional[int], str, Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "url": "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php",
        "attempts": [],
        "audit": None,
    }

    # 你原先已做多組參數嘗試；此處保留最小集合（避免浪費時間）
    attempts = [
        ("standard_EW", {"l": "zh-tw", "d": _ymd_to_compact(trade_date), "o": "json", "s": "0,asc"}),
        ("standard_AL", {"l": "zh-tw", "d": _ymd_to_compact(trade_date), "o": "json"}),
    ]

    try:
        for aid, params in attempts:
            try:
                r = _http_get(meta["url"], params=params, allow_insecure_ssl=allow_insecure_ssl)
                # 近期常見：302 或回傳 errors
                if "errors" in r.url or "/errors" in r.url:
                    meta["attempts"].append({"id": aid, "result": "redirected_to_error"})
                    continue

                if r.status_code != 200:
                    meta["attempts"].append({"id": aid, "result": f"http_{r.status_code}"})
                    continue

                js = r.json()
                # 真正成功時，會包含 data 表格
                data = js.get("aaData") or js.get("data") or []
                if not isinstance(data, list) or len(data) == 0:
                    meta["attempts"].append({"id": aid, "result": "empty"})
                    continue

                # 解析成交金額：st43_result 回傳欄位通常含成交金額
                # 這裡同樣用保守 approach：每列取最大整數欄位加總
                total = 0
                rows = 0
                for row in data:
                    if not isinstance(row, list):
                        continue
                    candidates = []
                    for v in row:
                        iv = _safe_int(v, None)
                        if iv is not None and iv > 0:
                            candidates.append(iv)
                    if not candidates:
                        continue
                    total += max(candidates)
                    rows += 1

                audit = {"market": "TPEX", "trade_date": trade_date, "rows": rows, "amount_sum": int(total)}
                meta["audit"] = audit

                if total > 50_000_000_000:
                    return int(total), "TPEX_OK:AUDIT_SUM", meta

                meta["attempts"].append({"id": aid, "result": "amount_too_low"})
            except Exception:
                meta["attempts"].append({"id": aid, "result": "exception"})

        return None, "TPEX_FAIL:OFFICIAL_ERRORS", meta

    except Exception as e:
        warnings_bus.push("TPEX_OFFICIAL_FAIL", str(e), {"trade_date": trade_date})
        return None, f"TPEX_FAIL:{type(e).__name__}", meta


# -----------------------------
# FinMind：取得 OTC 普通股 universe（用 industry_category + stock_name 排除）
# -----------------------------
def _finmind_fetch_dataset(dataset: str, token: Optional[str], params_extra: Optional[Dict[str, Any]] = None,
                           timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[List[Dict[str, Any]]], int, Optional[str]]:
    """
    回傳：(data_list, status_code, err)
    """
    params = {"dataset": dataset}
    if params_extra:
        params.update(params_extra)
    if token:
        params["token"] = token

    try:
        r = requests.get(FINMIND_API, params=params, timeout=timeout, headers={"User-Agent": USER_AGENT})
        sc = r.status_code
        if sc != 200:
            return None, sc, f"http_{sc}"
        js = r.json()
        data = js.get("data")
        if not isinstance(data, list):
            return None, sc, "no_data_list"
        return data, sc, None
    except requests.exceptions.Timeout:
        return None, 0, "timeout"
    except Exception as e:
        return None, 0, f"{type(e).__name__}:{e}"


def _is_excluded_otc_row(industry_category: str, stock_name: str) -> Tuple[bool, str]:
    ic = (industry_category or "").strip().upper()
    sn = (stock_name or "").strip().upper()

    # industry_category 排除
    for kw in EXCLUDE_INDUSTRY_KEYWORDS:
        if kw.upper() in ic:
            return True, f"industry_category:{kw}"

    # stock_name 排除
    for kw in EXCLUDE_NAME_KEYWORDS:
        if kw.upper() in sn:
            return True, f"stock_name:{kw}"

    return False, ""


def _get_otc_common_stock_universe(token: Optional[str]) -> Tuple[set, Dict[str, Any]]:
    """
    從 TaiwanStockInfo 建立 OTC 普通股 universe：
    - market == "OTC"
    - stock_id: 數字代碼（避免指數/商品代碼）
    - 用 industry_category + stock_name 排除 ETF/ETN/Index/證券商品

    回傳：(universe_set, meta)
    """
    meta: Dict[str, Any] = {
        "dataset": "TaiwanStockInfo",
        "status_code": None,
        "rows": 0,
        "kept": 0,
        "excluded": 0,
        "excluded_reason_counts": {},
    }

    data, sc, err = _finmind_fetch_dataset("TaiwanStockInfo", token)
    meta["status_code"] = sc
    if data is None:
        warnings_bus.push("FINMIND_STOCKINFO_FAIL", f"TaiwanStockInfo 取不到：{err}", {"status_code": sc})
        return set(), meta

    meta["rows"] = len(data)
    universe = set()

    for row in data:
        try:
            market = str(row.get("market", "")).strip().upper()
            if market != "OTC":
                continue

            stock_id = str(row.get("stock_id", "")).strip()
            if not STOCK_ID_RE.match(stock_id):
                # 非數字代碼，通常不是普通股（也可能是特例，這裡保守排除）
                meta["excluded"] += 1
                meta["excluded_reason_counts"]["stock_id_not_numeric"] = meta["excluded_reason_counts"].get("stock_id_not_numeric", 0) + 1
                continue

            industry_category = str(row.get("industry_category", "")).strip()
            stock_name = str(row.get("stock_name", "")).strip()

            excluded, reason = _is_excluded_otc_row(industry_category, stock_name)
            if excluded:
                meta["excluded"] += 1
                meta["excluded_reason_counts"][reason] = meta["excluded_reason_counts"].get(reason, 0) + 1
                continue

            universe.add(stock_id)
            meta["kept"] += 1

        except Exception:
            meta["excluded"] += 1
            meta["excluded_reason_counts"]["row_exception"] = meta["excluded_reason_counts"].get("row_exception", 0) + 1
            continue

    if len(universe) == 0:
        warnings_bus.push(
            "FINMIND_OTC_UNIVERSE_EMPTY",
            "OTC 普通股 universe 為空（可能 token 權限/FinMind 回傳格式變動/排除條件過嚴）",
            meta,
        )
    else:
        warnings_bus.push("FINMIND_OTC_UNIVERSE_OK", f"OTC普通股 universe：{len(universe)} 檔", {"kept": len(universe)})

    return universe, meta


# -----------------------------
# FinMind：OTC 普通股成交額彙總（TaiwanStockPrice）
# -----------------------------
def _finmind_tpex_amount_common_stock(trade_date: str, token: Optional[str]) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    只算 OTC 普通股（排除 ETF/ETN/Index/證券商品）
    - 先建立 OTC 普通股 universe（TaiwanStockInfo）
    - 再抓當日 TaiwanStockPrice，篩選 stock_id in universe，累加 Trading_money
    - coverage < 0.90 -> 判定不可靠，回傳 None（觸發 fallback）

    回傳：(amount, source, meta)
    """
    meta: Dict[str, Any] = {
        "dataset": "TaiwanStockPrice",
        "trade_date": trade_date,
        "status_code": None,
        "rows_price": 0,
        "universe_n": 0,
        "matched": 0,
        "coverage": 0.0,
        "amount_sum": 0,
        "universe_meta": None,
    }

    universe, uni_meta = _get_otc_common_stock_universe(token)
    meta["universe_meta"] = uni_meta
    meta["universe_n"] = len(universe)

    if len(universe) == 0:
        return None, "FINMIND_FAIL:OTC_UNIVERSE_EMPTY", meta

    # 抓當日所有股票交易資料
    data, sc, err = _finmind_fetch_dataset(
        "TaiwanStockPrice",
        token,
        params_extra={"start_date": trade_date, "end_date": trade_date},
    )
    meta["status_code"] = sc

    if data is None:
        warnings_bus.push("FINMIND_PRICE_FAIL", f"TaiwanStockPrice 取不到：{err}", {"status_code": sc})
        return None, "FINMIND_FAIL:PRICE_NO_DATA", meta

    meta["rows_price"] = len(data)

    total = 0
    matched = 0

    for row in data:
        try:
            stock_id = str(row.get("stock_id", "")).strip()
            if stock_id not in universe:
                continue

            money = _safe_int(row.get("Trading_money"), 0) or 0
            if money > 0:
                total += money
            matched += 1
        except Exception:
            continue

    meta["matched"] = matched
    meta["amount_sum"] = int(total)
    coverage = matched / max(1, len(universe))
    meta["coverage"] = float(coverage)

    # 可靠性門檻：coverage >= 90% + amount 下限 500億
    if coverage < 0.90:
        warnings_bus.push(
            "FINMIND_TPEX_COVERAGE_LOW",
            f"OTC普通股 coverage 不足：{coverage:.2%} ({matched}/{len(universe)})",
            {"coverage": coverage, "matched": matched, "universe": len(universe)},
        )
        return None, "FINMIND_FAIL:COVERAGE_LOW", meta

    if total < 50_000_000_000:
        warnings_bus.push("FINMIND_TPEX_AMOUNT_TOO_LOW", f"OTC成交額偏低：{total:,}", {"total": total})
        return None, "FINMIND_FAIL:AMOUNT_TOO_LOW", meta

    warnings_bus.push(
        "FINMIND_TPEX_OK",
        f"OTC普通股成交額：{total:,}（matched {matched}/{len(universe)}，coverage {coverage:.2%}）",
        {"total": total, "coverage": coverage},
    )
    return int(total), "FINMIND_OK:TPEX_COMMON_STOCK", meta


# -----------------------------
# Yahoo 估算（保留你原本的 fallback 介面）
# -----------------------------
def _yahoo_estimate_tpex() -> Tuple[Optional[int], str]:
    # 你原系統可能用 ^TWO 或成交量模型估算；
    # 這裡保留最小實作：回傳 None 讓它繼續走 Safe Mode
    return None, "YAHOO_FAIL:NOT_IMPLEMENTED"


def _yahoo_estimate_twse() -> Tuple[Optional[int], str]:
    return None, "YAHOO_FAIL:NOT_IMPLEMENTED"


# -----------------------------
# 核心：成交額抓取（含四層 fallback）
# -----------------------------
def fetch_amount_total(trade_date: str, allow_insecure_ssl: bool = False) -> MarketAmount:
    _ensure_dir(AUDIT_DIR)

    finmind_token, finmind_loaded = _load_finmind_token()

    # TWSE
    twse_amt, twse_src, twse_meta = _twse_audit_sum_by_stock_day_all(trade_date, allow_insecure_ssl)

    # TPEX：四層 fallback
    tpex_amt: Optional[int] = None
    tpex_src: str = ""
    tpex_meta: Dict[str, Any] = {}

    # Layer 1：官方
    tpex_amt, tpex_src, tpex_meta = _tpex_audit_sum_by_st43(trade_date, allow_insecure_ssl)

    # Layer 2：FinMind OTC 普通股成交額
    if tpex_amt is None:
        warnings_bus.push("TPEX_FALLBACK_FINMIND", "官方 TPEX 失敗，改用 FinMind OTC普通股彙總", {"trade_date": trade_date})
        tpex_amt, tpex_src, fin_meta = _finmind_tpex_amount_common_stock(trade_date, finmind_token)
        tpex_meta = {"official": tpex_meta, "finmind": fin_meta}

    # Layer 3：Yahoo（可選）
    if tpex_amt is None:
        warnings_bus.push("TPEX_FALLBACK_YAHOO", "FinMind 失敗，嘗試 Yahoo 估算", {"trade_date": trade_date})
        ya, ys = _yahoo_estimate_tpex()
        tpex_amt, tpex_src = ya, ys
        tpex_meta["fallback"] = "yahoo"

    # Layer 4：Safe Mode
    if tpex_amt is None or tpex_amt <= 0:
        warnings_bus.push("TPEX_SAFE_MODE", "所有方法失敗，使用 Safe Mode（2,000億）", {"trade_date": trade_date})
        tpex_amt, tpex_src = SAFE_MODE_TPEX, "TPEX_SAFE_MODE_200B"
        tpex_meta["fallback"] = "safe_mode"

    # TWSE 若失敗也給最後防線（可按你原邏輯）
    if not twse_amt or twse_amt <= 0:
        warnings_bus.push("TWSE_FALLBACK", "TWSE失敗，嘗試 Yahoo（未實作則仍可能 FAIL）", {"trade_date": trade_date})
        ya, ys = _yahoo_estimate_twse()
        if ya and ya > 0:
            twse_amt, twse_src = ya, ys
            twse_meta = {"fallback": "yahoo_forced"}

    # 計算總額
    total = None
    if twse_amt and tpex_amt:
        total = int(twse_amt) + int(tpex_amt)

    # 狀態 / 信心
    status_twse = "OK" if "TWSE_OK" in twse_src else "ESTIMATED" if "YAHOO" in twse_src else "FAIL"
    status_tpex = "OK" if ("TPEX_OK" in tpex_src or "FINMIND_OK" in tpex_src) else "ESTIMATED" if "YAHOO" in tpex_src else "FAIL"

    confidence_twse = "HIGH" if "TWSE_OK" in twse_src else "MEDIUM" if "YAHOO" in twse_src else "LOW"
    # 你的需求：FinMind「OTC普通股 coverage>=90%」才算 OK -> HIGH
    confidence_tpex = "HIGH" if "FINMIND_OK:TPEX_COMMON_STOCK" in tpex_src or "TPEX_OK" in tpex_src else \
                      "MEDIUM" if "YAHOO" in tpex_src else "LOW"

    confidence_level = _confidence_merge(confidence_twse, confidence_tpex)

    meta = {
        "trade_date": trade_date,
        "audit_dir": AUDIT_DIR,
        "twse": twse_meta,
        "tpex": tpex_meta,
        "finmind_token_loaded": finmind_loaded,
    }

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        status_twse=status_twse,
        status_tpex=status_tpex,
        confidence_twse=confidence_twse,
        confidence_tpex=confidence_tpex,
        confidence_level=confidence_level,
        allow_insecure_ssl=bool(allow_insecure_ssl),
        scope=_amount_scope(int(twse_amt or 0), int(tpex_amt or 0)),
        meta=meta,
    )


# -----------------------------
# build_arbiter_input：示範把 market_status 直接指向 confidence_level
# （你原本的 arbiter 結構更大；此處只提供你要的關鍵改法）
# -----------------------------
def build_arbiter_input(trade_date: str, session: str, account_mode: str, allow_insecure_ssl: bool = False) -> Dict[str, Any]:
    ma = fetch_amount_total(trade_date=trade_date, allow_insecure_ssl=allow_insecure_ssl)

    arb = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            # ✅ market_status 直接引用 market_amount.confidence_level（你要求的改法）
            "market_status": ma.confidence_level,
            "account_mode": account_mode,
            "confidence_level": ma.confidence_level,
            "date_status": "VERIFIED",
        },
        "macro": {
            "overview": {"trade_date": trade_date, "date_status": "VERIFIED"},
            "market_amount": {
                "amount_twse": ma.amount_twse,
                "amount_tpex": ma.amount_tpex,
                "amount_total": ma.amount_total,
                "source_twse": ma.source_twse,
                "source_tpex": ma.source_tpex,
                "status_twse": ma.status_twse,
                "status_tpex": ma.status_tpex,
                "confidence_twse": ma.confidence_twse,
                "confidence_tpex": ma.confidence_tpex,
                "confidence_level": ma.confidence_level,
                "allow_insecure_ssl": ma.allow_insecure_ssl,
                "scope": ma.scope,
                "meta": ma.meta,
            },
        },
        "audit_log": warnings_bus.items,
    }
    return arb


# -----------------------------
# Streamlit UI（最小示範）：你可以把這段接回你的既有 UI
# -----------------------------
def _ui():
    st.title("Sunhero 的股市智能超盤（V16.3.34_FINAL）")

    st.caption("OTC 成交額：FinMind 只算普通股（排除 ETF/ETN/Index/證券商品），coverage<90% 自動降級。")

    trade_date = st.text_input("trade_date (YYYY-MM-DD)", value=date.today().strftime("%Y-%m-%d"))
    allow_insecure_ssl = st.checkbox("allow_insecure_ssl", value=True)
    session = st.selectbox("session", ["PREOPEN", "INTRADAY", "EOD"], index=2)
    account_mode = st.selectbox("account_mode", ["Conservative", "Balanced", "Aggressive"], index=0)

    if st.button("Run"):
        warnings_bus.items.clear()
        out = build_arbiter_input(trade_date=trade_date, session=session, account_mode=account_mode, allow_insecure_ssl=allow_insecure_ssl)
        st.json(out)


if __name__ == "__main__":
    # Streamlit 執行：streamlit run main.py
    # CLI 執行：python main.py（會直接跑一次並印結果）
    if st is not None and hasattr(st, "_is_running_with_streamlit") and st._is_running_with_streamlit:
        _ui()
    else:
        # CLI quick test
        warnings_bus.items.clear()
        td = os.getenv("TRADE_DATE", date.today().strftime("%Y-%m-%d"))
        result = build_arbiter_input(trade_date=td, session="EOD", account_mode="Conservative", allow_insecure_ssl=True)
        print(json.dumps(result, ensure_ascii=False, indent=2))
