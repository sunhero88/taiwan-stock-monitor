# downloader_tw.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests

# yfinance 可用則用（你 requirements 有 yfinance）
try:
    import yfinance as yf
except Exception:
    yf = None


# -----------------------------
# Settings
# -----------------------------
DEFAULT_TIMEOUT = 6.5          # 每次請求超時秒數（不要太大，否則你 UI 會一直轉）
RETRY = 2                      # 重試次數（總次數 = 1 + RETRY）
RETRY_SLEEP = 0.6              # 重試間隔

# 若 TPEX 一直拿不到，你之前用 2000 億（= 200,000,000,000）
TPEX_SAFE_CONSTANT = 200_000_000_000


# -----------------------------
# Helpers
# -----------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
                return None
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        v = float(s)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _safe_int(x) -> Optional[int]:
    v = _safe_float(x)
    return None if v is None else int(v)


def _requests_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> requests.Response:
    last_err = None
    for i in range(1 + RETRY):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
            return r
        except Exception as e:
            last_err = e
            if i < RETRY:
                time.sleep(RETRY_SLEEP)
    raise last_err


def _get_finmind_token() -> Optional[str]:
    # ✅ Streamlit Cloud Secrets or env
    # Streamlit secrets 在 downloader 端不能 import st（避免循環），所以只讀 env
    # 你可以在 Streamlit Cloud -> Settings -> Secrets 放：
    # FINMIND_TOKEN="xxxx"
    return os.environ.get("FINMIND_TOKEN") or os.environ.get("FINMIND_API_KEY")


def _prev_trade_date_iso(target_iso: str) -> str:
    # 簡化：先用 target_iso - 1 天（不做完整交易日曆）
    # 你若要嚴格交易日可再接你自己的 calendar 模組
    import datetime as dt
    d = dt.datetime.strptime(target_iso, "%Y-%m-%d").date()
    return (d - dt.timedelta(days=1)).strftime("%Y-%m-%d")


# -----------------------------
# TWII Fetch (TWSE → Yahoo → FinMind)
# -----------------------------
def fetch_twii_twse(target_iso: str) -> Tuple[Optional[float], Optional[str]]:
    """
    優先：TWSE 指數 endpoint（若遇到 SSL / 403 很常見）
    回傳：(close, error_str)
    """
    # 常見 TWSE endpoint（可能會被擋/變動）
    # 若失效，會被 fallback 接走
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
    params = {"response": "json", "date": target_iso.replace("-", ""), "type": "IND"}
    t0 = _now_ms()
    try:
        r = _requests_get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None, f"HTTP_{r.status_code}"
        j = r.json()
        # j["data9"] / j["data1"] 結構常變，這裡只做 best-effort
        # 找「發行量加權股價指數」或「加權股價指數」
        candidates = []
        for key in ("data1", "data9", "data5", "data8"):
            arr = j.get(key)
            if isinstance(arr, list):
                candidates.extend(arr)
        # row: [指數名稱, 收盤指數, 漲跌, ...] (欄位可能變)
        for row in candidates:
            if not isinstance(row, list) or len(row) < 2:
                continue
            name = str(row[0])
            if ("發行量加權股價指數" in name) or ("加權股價指數" in name) or ("TAIEX" in name):
                close = _safe_float(row[1])
                if close is not None:
                    return close, None
        return None, "PARSE_FAIL"
    except Exception as e:
        return None, f"{type(e).__name__}"


def fetch_twii_yahoo(target_iso: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    """
    Yahoo：^TWII
    回傳：(close, chg, pct, error)
    """
    if yf is None:
        return None, None, None, "YFINANCE_NOT_AVAILABLE"

    t0 = _now_ms()
    try:
        ticker = yf.Ticker("^TWII")
        # 取 7 天避免遇到非交易日
        hist = ticker.history(period="7d", interval="1d", auto_adjust=False)
        if hist is None or hist.empty:
            return None, None, None, "EMPTY"
        # 以 target_iso 當天或之前最近一筆
        # hist index 通常是 timezone-aware datetime
        # 我們只用日期字串比對
        rows = []
        for idx, row in hist.iterrows():
            d = idx.date().strftime("%Y-%m-%d")
            rows.append((d, float(row["Close"])))
        rows.sort(key=lambda x: x[0])
        # 找 <= target_iso 的最後一筆
        close = None
        prev = None
        for d, c in rows:
            if d <= target_iso:
                prev = close
                close = c
        if close is None:
            # 若 target_iso 比資料更早，就拿最後一筆
            close = rows[-1][1]
            prev = rows[-2][1] if len(rows) >= 2 else None
        chg = None if prev is None else (close - prev)
        pct = None if (prev is None or prev == 0) else (chg / prev)
        return close, chg, pct, None
    except Exception as e:
        return None, None, None, f"{type(e).__name__}"


def fetch_twii_finmind(target_iso: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    """
    FinMind：TaiwanStockIndex（不保證你帳號有沒有權限，但通常可以）
    回傳：(close, chg, pct, error)
    """
    token = _get_finmind_token()
    if not token:
        return None, None, None, "FINMIND_TOKEN_MISSING"

    # FinMind doc：dataset = TaiwanStockIndex
    # 這裡用官方 api 格式（常用）
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockIndex",
        "data_id": "TAIEX",  # 有些帳號用 TAIEX；若失敗可改 "TAIEX" / "TWII"
        "start_date": _prev_trade_date_iso(target_iso),
        "end_date": target_iso,
        "token": token,
    }

    try:
        r = _requests_get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None, None, None, f"HTTP_{r.status_code}"
        j = r.json()
        if not isinstance(j, dict) or j.get("status") != 200:
            return None, None, None, f"STATUS_{j.get('status')}"
        data = j.get("data", [])
        if not isinstance(data, list) or len(data) == 0:
            # 嘗試換 data_id
            params["data_id"] = "TWII"
            r2 = _requests_get(url, params=params, headers={"User-Agent": "Mozilla/5.0"})
            if r2.status_code != 200:
                return None, None, None, f"HTTP_{r2.status_code}"
            j2 = r2.json()
            if not isinstance(j2, dict) or j2.get("status") != 200:
                return None, None, None, f"STATUS_{j2.get('status')}"
            data = j2.get("data", [])

        if not isinstance(data, list) or len(data) == 0:
            return None, None, None, "EMPTY"

        # data: [{date, close, ...}, ...]
        # 找 <= target_iso 的最後一筆與前一筆
        rows = []
        for it in data:
            d = str(it.get("date", ""))[:10]
            c = _safe_float(it.get("close"))
            if d and c is not None:
                rows.append((d, c))
        rows.sort(key=lambda x: x[0])
        close = None
        prev = None
        for d, c in rows:
            if d <= target_iso:
                prev = close
                close = c
        if close is None:
            close = rows[-1][1]
            prev = rows[-2][1] if len(rows) >= 2 else None

        chg = None if prev is None else (close - prev)
        pct = None if (prev is None or prev == 0) else (chg / prev)
        return close, chg, pct, None
    except Exception as e:
        return None, None, None, f"{type(e).__name__}"


def get_twii_with_fallback(target_iso: str) -> Dict[str, Any]:
    """
    回傳統一格式：
    {
      "close": float|None,
      "chg": float|None,
      "pct": float|None,
      "source": "TWSE|YAHOO|FINMIND|NONE",
      "error": str|None
    }
    """
    # 1) TWSE：只拿 close（因解析易變）
    close, err = fetch_twii_twse(target_iso)
    if close is not None:
        return {"close": close, "chg": None, "pct": None, "source": "TWSE", "error": None}
    twse_err = err

    # 2) Yahoo：拿 close/chg/pct
    close, chg, pct, err = fetch_twii_yahoo(target_iso)
    if close is not None:
        return {"close": close, "chg": chg, "pct": pct, "source": "YAHOO", "error": None}
    yahoo_err = err

    # 3) FinMind
    close, chg, pct, err = fetch_twii_finmind(target_iso)
    if close is not None:
        return {"close": close, "chg": chg, "pct": pct, "source": "FINMIND", "error": None}
    finmind_err = err

    # 全掛
    return {
        "close": None,
        "chg": None,
        "pct": None,
        "source": "NONE",
        "error": f"TWSE={twse_err} | YAHOO={yahoo_err} | FINMIND={finmind_err}",
    }


# -----------------------------
# Market amount (簡化版：你可接回 market_amount.py)
# -----------------------------
def get_market_amount_safe() -> Dict[str, Any]:
    """
    這裡先用最穩定策略：TWSE/TPEX 失敗就回 None/常數，不讓整個 snapshot 爆炸。
    """
    return {
        "amount_twse": None,
        "amount_tpex": TPEX_SAFE_CONSTANT,  # 你之前的策略
        "amount_total": TPEX_SAFE_CONSTANT,
        "source_twse": "TWSE_STUB",
        "source_tpex": "TPEX_SAFE_CONSTANT",
    }


# -----------------------------
# Build snapshot
# -----------------------------
def build_snapshot(session: str, target_date: str, topn: int) -> Dict[str, Any]:
    # meta
    meta = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "session": session,
        "effective_trade_date": target_date,
    }

    # TWII
    twii = get_twii_with_fallback(target_date)

    # market amount
    amt = get_market_amount_safe()

    # 組裝 macro（依你 Arbiter 需要的欄位）
    macro = {
        "overview": {
            "trade_date": target_date,
            "twii_close": twii["close"],
            "twii_chg": twii["chg"],
            "twii_pct": twii["pct"],
        },
        "market_amount": {
            "amount_twse": amt.get("amount_twse"),
            "amount_tpex": amt.get("amount_tpex"),
            "amount_total": amt.get("amount_total"),
            "source_twse": amt.get("source_twse"),
            "source_tpex": amt.get("source_tpex"),
        },
        # 你需要的話可把 institutional / vix 等接回來
        "institutional": {},
    }

    # audit（把資料來源與錯誤寫清楚，稽核可回溯）
    audit = {
        "TWII": {
            "source": twii["source"],
            "error": twii["error"],
            "asof": target_date,
        },
        "MARKET_AMOUNT": {
            "source_twse": amt.get("source_twse"),
            "source_tpex": amt.get("source_tpex"),
        },
        "INSTITUTIONAL": {"error": None},
    }

    # stocks（此處先空，讓 analyzer 或其他模組填）
    stocks: List[Dict[str, Any]] = []

    # arb_input：給 UI 範本用
    arb_input = {
        "meta": meta,
        "macro": macro,
        "stocks": stocks,
    }

    return {
        "meta": meta,
        "macro": macro,
        "stocks": stocks,
        "audit": audit,
        "arb_input": arb_input,
    }


# -----------------------------
# ✅ Unified entrypoint (MUST match main.py call)
# -----------------------------
def get_market_snapshot(target_iso: str, session: str = "EOD", topn: int = 20) -> Dict[str, Any]:
    """
    ✅ main.py 固定呼叫：get_market_snapshot(target_iso, session=..., topn=...)
    這個簽名不要再改，避免你又 TypeError。
    """
    if not isinstance(target_iso, str) or len(target_iso) < 8:
        raise TypeError(f"target_iso 必須是 YYYY-MM-DD，收到：{target_iso}")

    session = str(session).upper().strip()
    if session not in ("EOD", "INTRADAY"):
        session = "EOD"

    topn = int(topn)
    if topn < 1:
        topn = 20

    return build_snapshot(session=session, target_date=target_iso, topn=topn)
