# market_amount.py
# Market Amount Provider (TW) - Audit-Locked / Tiered Fallback / Network-Retry
# - TWSE amount: STOCK_DAY_ALL audit-sum with sanity floor
# - TPEX amount: Tiered fallback (Official JSON -> Pricing HTML -> Estimate -> Constant)
#
# Output:
#   provider.fetch_market_amount(trade_dt, trade_yyyymmdd) -> dict
#
# Notes:
# - No yfinance, no pandas required
# - Designed for GitHub Actions / cloud unstable network

import time
import re
import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
TZ_TPE = timezone(timedelta(hours=8))


def now_tpe() -> datetime:
    return datetime.now(TZ_TPE)


def hash_text(txt: str) -> str:
    return hashlib.sha256((txt or "").encode("utf-8")).hexdigest()[:16]


def safe_int(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).replace(",", "").strip()
        if s in ("", "--", "nan", "None"):
            return default
        return int(float(s))
    except:
        return default


def roc_yyy_mm_dd(dt: datetime) -> str:
    # e.g. 115/02/26
    return f"{dt.year - 1911}/{dt.strftime('%m/%d')}"


def build_session(headers: Dict[str, str]) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(headers)
    return s


class MarketAmountProvider:
    """
    提供上市/上櫃成交額（全市場）並輸出可稽核 audit meta。
    """

    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.twse.com.tw/",
        }
        self.session = build_session(self.headers)

    # -------------------------
    # TWSE: STOCK_DAY_ALL audit sum
    # -------------------------
    def fetch_twse_amount_stock_day_all(self, trade_date_yyyymmdd: str) -> Tuple[Optional[int], Dict[str, Any]]:
        """
        TWSE 成交額（上市）：
        https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date=YYYYMMDD

        稽核策略：
        - 對每一列由尾端向前掃描，取第一個可解析正整數視為該列交易金額候選
        - 全部加總後必須 >= 1000 億（100_000_000_000），否則視為不可信
        """
        url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
        params = {"response": "json", "date": trade_date_yyyymmdd}

        meta = {
            "source_name": "TWSE_STOCK_DAY_ALL_AUDIT_SUM",
            "url": url,
            "params": params,
            "status_code": None,
            "final_url": None,
            "latency_ms": None,
            "rows": 0,
            "ok_rows": 0,
            "amount_sum": 0,
            "raw_hash": None,
            "error_code": None,
        }

        t0 = time.time()
        try:
            r = self.session.get(url, params=params, timeout=20)
            meta["status_code"] = r.status_code
            meta["final_url"] = r.url
            meta["latency_ms"] = int((time.time() - t0) * 1000)

            if r.status_code != 200:
                meta["error_code"] = f"HTTP_{r.status_code}"
                return None, meta

            txt = r.text or ""
            meta["raw_hash"] = hash_text(txt[:8000])

            j = r.json()
            rows = j.get("data", []) or []
            meta["rows"] = len(rows)
            if not rows:
                meta["error_code"] = "EMPTY"
                return None, meta

            amount_sum = 0
            ok_rows = 0
            for row in rows:
                best = None
                for cell in reversed(row):
                    v = safe_int(cell, None)
                    if v is not None and v > 0:
                        best = v
                        break
                if best is not None:
                    amount_sum += best
                    ok_rows += 1

            meta["amount_sum"] = int(amount_sum)
            meta["ok_rows"] = int(ok_rows)

            if amount_sum < 100_000_000_000:
                meta["error_code"] = "AMOUNT_TOO_LOW"
                return None, meta

            return int(amount_sum), meta

        except Exception as e:
            meta["latency_ms"] = int((time.time() - t0) * 1000)
            meta["error_code"] = type(e).__name__
            return None, meta

    # -------------------------
    # TPEX: Tiered fallback
    # -------------------------
    def fetch_tpex_amount_official_json(self, trade_dt: datetime) -> Tuple[Optional[int], Dict[str, Any]]:
        """
        Tier-1：TPEX 官方 JSON（st43_result）
        https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=ROC_DATE&se=EW
        回傳欄位常見包含：集合成交金額
        """
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
        params = {"l": "zh-tw", "d": roc_yyy_mm_dd(trade_dt), "se": "EW"}

        meta = {
            "tier": 1,
            "source_name": "TPEX_ST43_JSON",
            "url": url,
            "params": params,
            "status_code": None,
            "final_url": None,
            "latency_ms": None,
            "raw_hash": None,
            "error_code": None,
        }

        t0 = time.time()
        try:
            r = self.session.get(url, params=params, timeout=20, allow_redirects=True)
            meta["status_code"] = r.status_code
            meta["final_url"] = r.url
            meta["latency_ms"] = int((time.time() - t0) * 1000)

            if r.status_code != 200:
                meta["error_code"] = f"HTTP_{r.status_code}"
                return None, meta

            txt = r.text or ""
            meta["raw_hash"] = hash_text(txt[:8000])

            j = r.json()
            amt = safe_int(j.get("集合成交金額"), None)
            if amt is None or amt <= 0:
                meta["error_code"] = "FIELD_MISSING_OR_ZERO"
                return None, meta

            return int(amt), meta

        except Exception as e:
            meta["latency_ms"] = int((time.time() - t0) * 1000)
            meta["error_code"] = type(e).__name__
            return None, meta

    def fetch_tpex_amount_pricing_html(self) -> Tuple[Optional[int], Dict[str, Any]]:
        """
        Tier-2：TPEX pricing.html（摘要頁）解析「成交金額 xxx 億」
        來源頁：
        https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html
        注意：這是頁面摘要，通常反映「最近交易日」；若盤中或節假日可能不同步。
        """
        url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
        meta = {
            "tier": 2,
            "source_name": "TPEX_PRICING_HTML",
            "url": url,
            "params": None,
            "status_code": None,
            "final_url": None,
            "latency_ms": None,
            "raw_hash": None,
            "error_code": None,
        }

        t0 = time.time()
        try:
            r = self.session.get(url, timeout=20)
            meta["status_code"] = r.status_code
            meta["final_url"] = r.url
            meta["latency_ms"] = int((time.time() - t0) * 1000)

            if r.status_code != 200:
                meta["error_code"] = f"HTTP_{r.status_code}"
                return None, meta

            txt = r.text or ""
            meta["raw_hash"] = hash_text(txt[:8000])

            m = re.search(r"成交金額\s*([\d,]+)\s*億", txt)
            if not m:
                meta["error_code"] = "PATTERN_NOT_FOUND"
                return None, meta

            yi = safe_int(m.group(1), None)
            if yi is None or yi <= 0:
                meta["error_code"] = "PARSE_FAIL"
                return None, meta

            return int(yi) * 100_000_000, meta  # 億 -> NTD

        except Exception as e:
            meta["latency_ms"] = int((time.time() - t0) * 1000)
            meta["error_code"] = type(e).__name__
            return None, meta

    def tpex_estimate_from_twse(self, twse_amount: Optional[int], ratio: float = 0.22) -> Tuple[Optional[int], Dict[str, Any]]:
        """
        Tier-3：估算（當 TPEX 伺服器不穩時的可回測固定比率）
        預設 ratio=0.22（你舊架構已用過）
        """
        meta = {
            "tier": 3,
            "source_name": "TPEX_ESTIMATE_FROM_TWSE",
            "ratio": float(ratio),
            "error_code": None,
        }
        if twse_amount is None or twse_amount <= 0:
            meta["error_code"] = "TWSE_AMOUNT_MISSING"
            return None, meta
        return int(twse_amount * float(ratio)), meta

    def tpex_constant_safe(self, constant_amt: int = 200_000_000_000) -> Tuple[int, Dict[str, Any]]:
        """
        Tier-4：常數保命（最後一層）
        """
        return int(constant_amt), {
            "tier": 4,
            "source_name": "TPEX_SAFE_CONSTANT",
            "constant": int(constant_amt),
            "error_code": None,
        }

    def fetch_tpex_amount_tiered(self, trade_dt: datetime, twse_amount: Optional[int]) -> Tuple[int, Dict[str, Any]]:
        # Tier-1
        a1, m1 = self.fetch_tpex_amount_official_json(trade_dt)
        if a1 is not None:
            return int(a1), m1

        # Tier-2
        a2, m2 = self.fetch_tpex_amount_pricing_html()
        if a2 is not None:
            return int(a2), m2

        # Tier-3
        a3, m3 = self.tpex_estimate_from_twse(twse_amount, ratio=0.22)
        if a3 is not None:
            return int(a3), m3

        # Tier-4
        a4, m4 = self.tpex_constant_safe(200_000_000_000)
        return int(a4), m4

    # -------------------------
    # Public API
    # -------------------------
    def fetch_market_amount(self, trade_dt: datetime, trade_date_yyyymmdd: str) -> Dict[str, Any]:
        """
        回傳結構（建議直接嵌入你的 macro.market_amount）：
        {
          "trade_date": "YYYYMMDD",
          "amount_twse": int|None,
          "amount_tpex": int,
          "amount_total": int,
          "source_twse": "...",
          "source_tpex": "...",
          "status_twse": "OK|FAIL",
          "status_tpex": "OK|ESTIMATED",
          "confidence_twse": "HIGH|LOW",
          "confidence_tpex": "HIGH|MED|LOW",
          "audit": {
              "twse": {...meta...},
              "tpex": {...meta...}
          }
        }
        """
        twse_amt, twse_meta = self.fetch_twse_amount_stock_day_all(trade_date_yyyymmdd)
        twse_ok = (twse_amt is not None) and (twse_meta.get("error_code") is None)

        tpex_amt, tpex_meta = self.fetch_tpex_amount_tiered(trade_dt, twse_amt)

        # confidence map
        tpex_tier = tpex_meta.get("tier")
        if tpex_tier in (1, 2):
            conf_tpex = "HIGH"
            status_tpex = "OK"
        elif tpex_tier == 3:
            conf_tpex = "MED"
            status_tpex = "ESTIMATED"
        else:
            conf_tpex = "LOW"
            status_tpex = "ESTIMATED"

        out = {
            "trade_date": trade_date_yyyymmdd,
            "amount_twse": int(twse_amt) if twse_amt is not None else None,
            "amount_tpex": int(tpex_amt) if tpex_amt is not None else None,
            "amount_total": int((twse_amt or 0) + (tpex_amt or 0)),
            "source_twse": twse_meta.get("source_name"),
            "source_tpex": tpex_meta.get("source_name"),
            "status_twse": "OK" if twse_ok else "FAIL",
            "status_tpex": status_tpex,
            "confidence_twse": "HIGH" if twse_ok else "LOW",
            "confidence_tpex": conf_tpex,
            "audit": {
                "twse": twse_meta,
                "tpex": tpex_meta,
            },
        }
        return out


# -------------------------
# Optional CLI smoke test
# -------------------------
def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


if __name__ == "__main__":
    p = MarketAmountProvider()
    dt = now_tpe()
    trade_yyyymmdd = _yyyymmdd(dt)
    res = p.fetch_market_amount(dt, trade_yyyymmdd)
    print(json.dumps(res, ensure_ascii=False, indent=2))
