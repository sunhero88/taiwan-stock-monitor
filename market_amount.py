# market_amount.py
# -*- coding: utf-8 -*-
"""
Market Amount (TWSE / TPEX) - Stable / Tiered Fallback / Auditable

設計目標
- 不因單一來源（TWSE/TPEX）失效而卡死
- 每個來源都有 timeout + retry + 明確 audit
- 允許「降級」但必須留下可稽核證據鏈（audit_modules）

輸出格式（給 Arbiter / Data-Layer）
{
  "amount_twse": int|None,
  "amount_tpex": int|None,
  "amount_total": int|None,
  "source_twse": str,
  "source_tpex": str,
  "status_twse": "OK"|"FAIL",
  "status_tpex": "OK"|"FAIL"|"ESTIMATED",
  "confidence_twse": "HIGH"|"LOW",
  "confidence_tpex": "HIGH"|"LOW",
  "audit_modules": [ {module...}, ... ]
}
"""

from __future__ import annotations

import os
import time
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TZ_TPE = timezone(timedelta(hours=8))


def _now_tpe() -> datetime:
    return datetime.now(TZ_TPE)


def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _roc_yyy_mm_dd(dt: datetime) -> str:
    # TPEX 常用民國日期格式：YYY/MM/DD
    return f"{dt.year - 1911}/{dt.strftime('%m/%d')}"


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).replace(",", "").strip()
        if s in ("", "--", "None", "null"):
            return None
        return int(float(s))
    except Exception:
        return None


def _ms() -> int:
    return int(time.time() * 1000)


@dataclass
class FetchResult:
    ok: bool
    value: Optional[int]
    source: str
    confidence: str  # HIGH/LOW
    error: Optional[str]
    latency_ms: int
    status_code: Optional[int]
    final_url: Optional[str]


class MarketAmountProvider:
    """
    只負責成交額（TWSE / TPEX）
    - TWSE: Tier1 STOCK_DAY_ALL sum, Tier2 FMTQIK
    - TPEX: Tier1 st43_result, Tier2 ratio estimate, Tier3 SAFE CONSTANT
    """

    def __init__(
        self,
        *,
        tpex_safe_constant: int = 200_000_000_000,  # 2000億
        tpex_ratio_default: float = 0.22,           # 你原本用 0.22
        ratio_cache_path: str = "data/tpex_ratio_cache.json",
        timeout_sec: int = 12,
        retries_total: int = 2,
        backoff_factor: float = 0.8,
    ):
        self.tpex_safe_constant = int(tpex_safe_constant)
        self.tpex_ratio_default = float(tpex_ratio_default)
        self.ratio_cache_path = ratio_cache_path
        self.timeout_sec = int(timeout_sec)

        self.session = requests.Session()
        retry = Retry(
            total=retries_total,
            connect=retries_total,
            read=retries_total,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.tpex.org.tw/",
        }

    # -----------------------------
    # Public API
    # -----------------------------
    def fetch(self, dt: Optional[datetime] = None) -> Dict[str, Any]:
        """
        回傳：market_amount dict + audit_modules
        """
        dt = dt or _now_tpe()
        trade_date_yyyymmdd = _yyyymmdd(dt)
        roc_date = _roc_yyy_mm_dd(dt)

        audit: List[Dict[str, Any]] = []

        # ---- TWSE ----
        twse_r1 = self._fetch_twse_amount_stock_day_all(trade_date_yyyymmdd)
        audit.append(self._as_audit_module("TWSE_STOCK_DAY_ALL", dt, twse_r1))
        twse_best = twse_r1

        if not twse_best.ok:
            twse_r2 = self._fetch_twse_amount_fmtqik(trade_date_yyyymmdd)
            audit.append(self._as_audit_module("TWSE_FMTQIK", dt, twse_r2))
            if twse_r2.ok:
                twse_best = twse_r2

        # ---- TPEX ----
        tpex_r1 = self._fetch_tpex_amount_st43(roc_date)
        audit.append(self._as_audit_module("TPEX_ST43", dt, tpex_r1))
        tpex_best = tpex_r1

        if not tpex_best.ok:
            # Tier2: ratio estimate
            ratio = self._load_tpex_ratio_cache() or self.tpex_ratio_default
            est = None
            if twse_best.ok and twse_best.value:
                est = int(twse_best.value * ratio)

            tpex_r2 = FetchResult(
                ok=est is not None,
                value=est,
                source=f"TPEX_RATIO_ESTIMATE_{ratio:.4f}",
                confidence="LOW",
                error=None if est is not None else "NO_TWSE_FOR_RATIO_EST",
                latency_ms=0,
                status_code=None,
                final_url=None,
            )
            audit.append(self._as_audit_module("TPEX_RATIO_ESTIMATE", dt, tpex_r2))
            if tpex_r2.ok:
                tpex_best = tpex_r2

        if not tpex_best.ok:
            # Tier3: safe constant
            tpex_r3 = FetchResult(
                ok=True,
                value=int(self.tpex_safe_constant),
                source=f"TPEX_SAFE_CONSTANT_{int(self.tpex_safe_constant/1e9)}B",
                confidence="LOW",
                error=None,
                latency_ms=0,
                status_code=None,
                final_url=None,
            )
            audit.append(self._as_audit_module("TPEX_SAFE_CONSTANT", dt, tpex_r3))
            tpex_best = tpex_r3

        amount_twse = twse_best.value if twse_best.ok else None
        amount_tpex = tpex_best.value if tpex_best.ok else None

        amount_total = None
        if amount_twse is not None and amount_tpex is not None:
            amount_total = int(amount_twse + amount_tpex)

        out = {
            "amount_twse": amount_twse,
            "amount_tpex": amount_tpex,
            "amount_total": amount_total,
            "source_twse": twse_best.source if twse_best.ok else f"TWSE_FAIL:{twse_best.error}",
            "source_tpex": tpex_best.source if tpex_best.ok else f"TPEX_FAIL:{tpex_best.error}",
            "status_twse": "OK" if twse_best.ok else "FAIL",
            "status_tpex": "OK" if (tpex_best.ok and "ST43" in tpex_best.source) else ("ESTIMATED" if tpex_best.ok else "FAIL"),
            "confidence_twse": twse_best.confidence if twse_best.ok else "LOW",
            "confidence_tpex": tpex_best.confidence if tpex_best.ok else "LOW",
            "audit_modules": audit,
        }
        return out

    # -----------------------------
    # TWSE Tiers
    # -----------------------------
    def _fetch_twse_amount_stock_day_all(self, yyyymmdd: str) -> FetchResult:
        """
        Tier1：逐筆加總 STOCK_DAY_ALL（最抗格式變動）
        endpoint:
        https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date=YYYYMMDD
        """
        t0 = _ms()
        url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
        params = {"response": "json", "date": yyyymmdd}

        try:
            r = self.session.get(url, params=params, headers=self.headers, timeout=self.timeout_sec)
            sc = r.status_code
            final_url = r.url
            if sc != 200:
                return FetchResult(False, None, "TWSE_STOCK_DAY_ALL", "LOW", f"HTTP_{sc}", _ms() - t0, sc, final_url)

            j = r.json()
            rows = j.get("data", []) or []
            if not rows:
                return FetchResult(False, None, "TWSE_STOCK_DAY_ALL", "LOW", "EMPTY", _ms() - t0, sc, final_url)

            # 從每列尾端找可解析整數（保守法）
            amount_sum = 0
            ok_rows = 0
            for row in rows:
                best = None
                for cell in reversed(row):
                    v = _safe_int(cell)
                    if v is not None and v > 0:
                        best = v
                        break
                if best is not None:
                    amount_sum += best
                    ok_rows += 1

            # 合理性門檻（避免抓到錯欄位）
            # 台股上市成交額正常日常見 > 1000億；保守設 800億避免過度誤殺
            if amount_sum < 80_000_000_000:
                return FetchResult(False, None, "TWSE_STOCK_DAY_ALL", "LOW", f"AMOUNT_TOO_LOW:{amount_sum}", _ms() - t0, sc, final_url)

            return FetchResult(True, int(amount_sum), "TWSE_STOCK_DAY_ALL_SUM", "HIGH", None, _ms() - t0, sc, final_url)

        except Exception as e:
            return FetchResult(False, None, "TWSE_STOCK_DAY_ALL", "LOW", type(e).__name__, _ms() - t0, None, None)

    def _fetch_twse_amount_fmtqik(self, yyyymmdd: str) -> FetchResult:
        """
        Tier2：FMTQIK（有時會被擋/格式變）
        endpoint:
        https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date=YYYYMMDD
        """
        t0 = _ms()
        url = "https://www.twse.com.tw/exchangeReport/FMTQIK"
        params = {"response": "json", "date": yyyymmdd}

        try:
            r = self.session.get(url, params=params, headers=self.headers, timeout=self.timeout_sec)
            sc = r.status_code
            final_url = r.url
            if sc != 200:
                return FetchResult(False, None, "TWSE_FMTQIK", "LOW", f"HTTP_{sc}", _ms() - t0, sc, final_url)

            j = r.json()
            data = j.get("data", []) or []
            if not data:
                return FetchResult(False, None, "TWSE_FMTQIK", "LOW", "EMPTY", _ms() - t0, sc, final_url)

            # 常見：最後一列有總成交金額欄位；但可能變動
            # 這裡採「整個 data 掃描最大 int」以保守抓取（避免 index out of range）
            candidates = []
            for row in data:
                for cell in row:
                    v = _safe_int(cell)
                    if v is not None:
                        candidates.append(v)

            if not candidates:
                return FetchResult(False, None, "TWSE_FMTQIK", "LOW", "NO_NUMERIC", _ms() - t0, sc, final_url)

            best = max(candidates)
            if best < 80_000_000_000:
                return FetchResult(False, None, "TWSE_FMTQIK", "LOW", f"AMOUNT_TOO_LOW:{best}", _ms() - t0, sc, final_url)

            return FetchResult(True, int(best), "TWSE_FMTQIK_MAXSCAN", "LOW", None, _ms() - t0, sc, final_url)

        except Exception as e:
            return FetchResult(False, None, "TWSE_FMTQIK", "LOW", type(e).__name__, _ms() - t0, None, None)

    # -----------------------------
    # TPEX Tiers
    # -----------------------------
    def _fetch_tpex_amount_st43(self, roc_date: str) -> FetchResult:
        """
        Tier1：TPEX st43_result（官方 JSON）
        endpoint:
        https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php
          params: l=zh-tw, d=YYY/MM/DD, se=EW
        """
        t0 = _ms()
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
        params = {"l": "zh-tw", "d": roc_date, "se": "EW"}

        try:
            r = self.session.get(
                url,
                params=params,
                headers=self.headers,
                timeout=self.timeout_sec,
                allow_redirects=True,
            )
            sc = r.status_code
            final_url = r.url
            if sc != 200:
                return FetchResult(False, None, "TPEX_ST43", "LOW", f"HTTP_{sc}", _ms() - t0, sc, final_url)

            j = r.json()
            # 常見 key: "集合成交金額"
            v = _safe_int(j.get("集合成交金額"))
            if v is None or v <= 0:
                return FetchResult(False, None, "TPEX_ST43", "LOW", "MISSING_AMOUNT", _ms() - t0, sc, final_url)

            # 合理性：上櫃正常通常 > 200億；保守 50億
            if v < 5_000_000_000:
                return FetchResult(False, None, "TPEX_ST43", "LOW", f"AMOUNT_TOO_LOW:{v}", _ms() - t0, sc, final_url)

            return FetchResult(True, int(v), "TPEX_ST43_OFFICIAL", "HIGH", None, _ms() - t0, sc, final_url)

        except Exception as e:
            return FetchResult(False, None, "TPEX_ST43", "LOW", type(e).__name__, _ms() - t0, None, None)

    # -----------------------------
    # Ratio Cache (optional)
    # -----------------------------
    def _load_tpex_ratio_cache(self) -> Optional[float]:
        """
        ratio cache 格式建議：
        {"ratio": 0.22, "asof": "2026-02-25"}
        """
        try:
            if not os.path.exists(self.ratio_cache_path):
                return None
            with open(self.ratio_cache_path, "r", encoding="utf-8") as f:
                j = json.load(f)
            r = j.get("ratio")
            if r is None:
                return None
            r = float(r)
            if not (0.05 <= r <= 0.60):
                return None
            return r
        except Exception:
            return None

    # -----------------------------
    # Audit format helper
    # -----------------------------
    def _as_audit_module(self, name: str, dt: datetime, r: FetchResult) -> Dict[str, Any]:
        return {
            "name": name,
            "status": "OK" if r.ok else "FAIL",
            "confidence": r.confidence,
            "asof": dt.strftime("%Y-%m-%d"),
            "error": r.error,
            "latency_ms": r.latency_ms,
            "status_code": r.status_code,
            "final_url": r.final_url,
            "source": r.source,
        }


# 便利測試：python market_amount.py
if __name__ == "__main__":
    p = MarketAmountProvider()
    out = p.fetch()
    print(json.dumps(out, ensure_ascii=False, indent=2))
