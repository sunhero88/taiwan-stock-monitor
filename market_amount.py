# market_amount.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional, Tuple, Dict, Any, List
import os
import json
import time
import math
import requests
from datetime import datetime

# =========================
# Data Structures
# =========================
CACHE_DIR = "data/audit_market_amount"
CACHE_FILE = os.path.join(CACHE_DIR, "latest_market_cache.json")

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

def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def _write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None: return None
        s = str(x).strip().replace(",", "")
        return int(float(s))
    except: return None

def _manage_amount_cache(market: str, amount: Optional[int] = None) -> Optional[int]:
    _ensure_dir(CACHE_DIR)
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except: pass
    if amount is not None and amount > 0:
        cache[market] = {"amount": amount, "ts": _now_ts()}
        _write_json(CACHE_FILE, cache)
        return amount
    return cache.get(market, {}).get("amount")

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Referer": "https://www.tpex.org.tw/"
    })
    return s

# =========================
# Core Fetching
# =========================
def fetch_amount_total(trade_date: str, allow_insecure_ssl: bool = False, audit_dir: str = "data/audit_market_amount") -> Tuple[MarketAmount, List[WarningItem]]:
    warnings: List[WarningItem] = []
    _ensure_dir(audit_dir)
    session = _make_session()
    verify = not bool(allow_insecure_ssl)

    # 1. TWSE 抓取 (簡化邏輯，包含快取)
    twse_amt, twse_src = None, "TWSE_INIT"
    try:
        twse_url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={trade_date.replace('-', '')}"
        r_twse = session.get(twse_url, timeout=12, verify=verify)
        if r_twse.status_code == 200:
            js = r_twse.json()
            # 找到成交金額 Index
            amt_idx = next((i for i, f in enumerate(js.get('fields', [])) if "成交金額" in f), None)
            if amt_idx is not None:
                twse_amt = sum(_safe_int(row[amt_idx]) for row in js.get('data', []) if _safe_int(row[amt_idx]))
                _manage_amount_cache("TWSE", twse_amt)
                twse_src = "TWSE_OK:AUDIT_SUM"
    except Exception as e:
        twse_amt = _manage_amount_cache("TWSE")
        twse_src = "TWSE_FAIL:USING_CACHE"
        warnings.append(WarningItem(_now_ts(), "TWSE_CACHE", str(e)))

    # 2. TPEX 抓取 (修正 REDIRECT 邏輯)
    tpex_amt, tpex_src = None, "TPEX_INIT"
    try:
        dt = datetime.strptime(trade_date, "%Y-%m-%d")
        roc_date = f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"
        tpex_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
        # 關鍵：加入 allow_redirects=False
        r_tpex = session.get(tpex_url, params={"l": "zh-tw", "d": roc_date, "se": "EW"}, timeout=12, verify=verify, allow_redirects=False)
        
        if r_tpex.status_code == 200 and "errors" not in r_tpex.url:
            js = r_tpex.json()
            aa = js.get("aaData", [])
            tpex_amt = sum(_safe_int(row[3]) for row in aa if len(row) > 3 and _safe_int(row[3]))
            if tpex_amt and tpex_amt > 0:
                _manage_amount_cache("TPEX", tpex_amt)
                tpex_src = "TPEX_OK:AUDIT_SUM"
            else: raise ValueError("Data Empty")
        else: raise RuntimeError("Redirected to error page")
    except Exception as e:
        tpex_amt = _manage_amount_cache("TPEX")
        tpex_src = "TPEX_FAIL:USING_CACHE"
        warnings.append(WarningItem(_now_ts(), "TPEX_CACHE", str(e)))

    total = (twse_amt or 0) + (tpex_amt or 0)
    return MarketAmount(twse_amt, tpex_amt, total, twse_src, tpex_src, allow_insecure_ssl), warnings
