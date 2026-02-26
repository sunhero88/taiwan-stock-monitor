# -*- coding: utf-8 -*-
"""
TW Data Downloader (TWSE/TPEX) - Stable Orchestrated Data Layer

此檔案負責：
- TWII (加權指數) / VIX (台指VIX) / 大盤成交值 (TWSE/TPEX) / 三大法人 (TWSE T86)
- TopN (上市成交額排序) 股票資料抓取與組裝
- 產出給 arbiter / analyzer 使用的 snapshot

本版新增：TWII Tiered Fallback (TWSE → Yahoo → FinMind)
"""

from __future__ import annotations

import os
import time
import json
import math
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from datetime import datetime, timedelta

# -----------------------------
# Session / Headers
# -----------------------------
sess = requests.Session()
sess.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
)

# -----------------------------
# Small Helpers
# -----------------------------
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            if math.isnan(float(x)):
                return None
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        return float(s)
    except Exception:
        return None


def _sleep_jitter(base: float = 0.35, jitter: float = 0.35) -> None:
    time.sleep(base + random.random() * jitter)


# -----------------------------
# TWII (TWSE) - Primary
# -----------------------------
def fetch_twii_from_twse(trade_date_yyyymmdd: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    TWSE 指數 endpoint（若失敗將由 tiered fallback 接手）
    回傳：
      twii = {close, change, pct}
      meta = {module/status/confidence/asof/error/...}
    """
    t0 = time.time()
    meta: Dict[str, Any] = {
        "module": "TWSE_TWII_INDEX",
        "trade_date": trade_date_yyyymmdd,
        "status": "FAIL",
        "confidence": "LOW",
        "asof": None,
        "error_code": None,
        "error": None,
        "latency_ms": None,
        "status_code": None,
    }

    try:
        # TWSE index endpoint（你原本的作法）
        url = "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST"
        params = {"response": "json", "date": trade_date_yyyymmdd}

        r = sess.get(url, params=params, timeout=20)
        meta["status_code"] = r.status_code

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            meta["error"] = r.text[:200]
            return None, meta

        j = r.json()

        # schema 防守：資料列表可能在 different keys
        data = j.get("data") or j.get("data1") or j.get("data2") or []
        if not data:
            meta["error_code"] = "EMPTY"
            meta["error"] = "TWSE returned empty data"
            return None, meta

        # 常見 row: [時間, 指數, 漲跌, ...]
        # 你原碼用最後一筆 close、倒數第二筆 prev_close 計算 change/pct
        df = pd.DataFrame(data)
        if df.empty or df.shape[1] < 2:
            meta["error_code"] = "BAD_SCHEMA"
            meta["error"] = f"shape={df.shape}"
            return None, meta

        close = _safe_float(df.iloc[-1, 1])
        prev_close = _safe_float(df.iloc[-2, 1]) if len(df) >= 2 else None

        if close is None:
            meta["error_code"] = "CLOSE_MISSING"
            meta["error"] = "close is None"
            return None, meta

        chg = (close - prev_close) if (prev_close is not None) else None
        pct = (chg / prev_close) if (chg is not None and prev_close not in (None, 0)) else None

        twii = {"close": close, "change": chg, "pct": pct}
        meta.update({"status": "OK", "confidence": "HIGH", "asof": trade_date_yyyymmdd})
        return twii, meta

    except requests.exceptions.SSLError as e:
        meta["error_code"] = "SSLError"
        meta["error"] = str(e)
        return None, meta
    except Exception as e:
        meta["error_code"] = type(e).__name__
        meta["error"] = str(e)
        return None, meta
    finally:
        meta["latency_ms"] = int((time.time() - t0) * 1000)


# -----------------------------
# TWII Tiered Fallback (NEW)
# -----------------------------
def _get_finmind_token() -> Optional[str]:
    # 優先環境變數（Streamlit Cloud Secrets 有時會映射到 env；若未映射再嘗試 st.secrets）
    for k in ("FINMIND_TOKEN", "FINMIND_API_KEY", "FINMIND_KEY"):
        v = os.environ.get(k)
        if v:
            return v.strip()
    # 避免 downloader 層硬依賴 streamlit：有裝就用，沒裝就略過
    try:
        import streamlit as st  # type: ignore
        v = st.secrets.get("FINMIND_TOKEN") or st.secrets.get("FINMIND_API_KEY")
        if v:
            return str(v).strip()
    except Exception:
        pass
    return None


def fetch_twii_from_yahoo(trade_date_yyyymmdd: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """TWII fallback #2: Yahoo Finance (yfinance)"""
    t0 = time.time()
    meta: Dict[str, Any] = {
        "module": "YAHOO_TWII",
        "trade_date": trade_date_yyyymmdd,
        "status": "FAIL",
        "confidence": "LOW",
        "asof": None,
        "error_code": None,
        "error": None,
        "latency_ms": None,
        "symbol": None,
    }
    try:
        # 延伸 14 天視窗抓「<= trade_date 的最後一筆」，避免遇到非交易日
        end_dt = datetime.strptime(trade_date_yyyymmdd, "%Y%m%d") + timedelta(days=1)
        start_dt = end_dt - timedelta(days=14)

        try:
            import yfinance as yf  # type: ignore
        except Exception as e:
            meta["error_code"] = "YFINANCE_NOT_AVAILABLE"
            meta["error"] = str(e)
            return None, meta

        candidates = ["^TWII"]  # Yahoo 常用：台灣加權指數
        for sym in candidates:
            try:
                df = yf.download(
                    sym,
                    start=start_dt.strftime("%Y-%m-%d"),
                    end=end_dt.strftime("%Y-%m-%d"),
                    progress=False,
                )
                if df is None or df.empty:
                    continue
                df = df.dropna()
                if df.empty:
                    continue

                last_close = float(df["Close"].iloc[-1])
                asof = str(df.index[-1].date())
                prev_close = float(df["Close"].iloc[-2]) if len(df) >= 2 else None
                chg = (last_close - prev_close) if (prev_close is not None) else None
                pct = (chg / prev_close) if (chg is not None and prev_close not in (None, 0)) else None

                twii = {"close": last_close, "change": chg, "pct": pct}
                meta.update({"status": "OK", "confidence": "MED", "asof": asof, "symbol": sym})
                return twii, meta
            except Exception:
                continue

        meta["error_code"] = "EMPTY"
        meta["error"] = "No data from Yahoo candidates"
        return None, meta

    except Exception as e:
        meta["error_code"] = type(e).__name__
        meta["error"] = str(e)
        return None, meta
    finally:
        meta["latency_ms"] = int((time.time() - t0) * 1000)


def fetch_twii_from_finmind(trade_date_yyyymmdd: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    TWII fallback #3: FinMind (requires token).
    注意：最後備援以「總報酬指數(TRI)」替代 TWII 價格指數，因此標示 LOW 信心與 note。
    """
    t0 = time.time()
    meta: Dict[str, Any] = {
        "module": "FINMIND_TWII",
        "trade_date": trade_date_yyyymmdd,
        "status": "FAIL",
        "confidence": "LOW",
        "asof": None,
        "error_code": None,
        "error": None,
        "latency_ms": None,
        "dataset": None,
        "data_id": None,
        "note": None,
        "status_code": None,
    }
    token = _get_finmind_token()
    if not token:
        meta["error_code"] = "NO_TOKEN"
        meta["error"] = "FINMIND_TOKEN missing"
        meta["latency_ms"] = int((time.time() - t0) * 1000)
        return None, meta

    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        end_dt = datetime.strptime(trade_date_yyyymmdd, "%Y%m%d")
        start_dt = end_dt - timedelta(days=14)

        dataset = "TaiwanStockTotalReturnIndex"  # 搜證較穩定的公開 dataset
        data_id = "TAIEX"
        params = {
            "dataset": dataset,
            "data_id": data_id,
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": (end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        }
        headers = {"Authorization": f"Bearer {token}"}

        r = sess.get(url, params=params, headers=headers, timeout=20)
        meta["status_code"] = r.status_code

        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            meta["error"] = r.text[:200]
            meta["dataset"] = dataset
            meta["data_id"] = data_id
            return None, meta

        j = r.json()
        data = j.get("data", []) or []
        if not data:
            meta["error_code"] = "EMPTY"
            meta["error"] = "FinMind returned empty data"
            meta["dataset"] = dataset
            meta["data_id"] = data_id
            return None, meta

        df = pd.DataFrame(data)
        if "date" not in df.columns:
            meta["error_code"] = "BAD_SCHEMA"
            meta["error"] = f"columns={list(df.columns)}"
            meta["dataset"] = dataset
            meta["data_id"] = data_id
            return None, meta

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        last = df.iloc[-1]

        val = None
        for c in ("price", "index", "close", "value"):
            if c in df.columns:
                try:
                    val = float(last[c])
                    break
                except Exception:
                    pass

        if val is None:
            meta["error_code"] = "MISSING_VALUE"
            meta["error"] = f"no usable value col in {list(df.columns)}"
            meta["dataset"] = dataset
            meta["data_id"] = data_id
            return None, meta

        asof = last["date"].date().isoformat()
        twii = {"close": val, "change": None, "pct": None}
        meta.update(
            {
                "status": "OK",
                "confidence": "LOW",  # TRI != TWII
                "asof": asof,
                "dataset": dataset,
                "data_id": data_id,
                "note": "TRI_FALLBACK_NOT_EXACT_TWII",
            }
        )
        return twii, meta

    except Exception as e:
        meta["error_code"] = type(e).__name__
        meta["error"] = str(e)
        return None, meta
    finally:
        meta["latency_ms"] = int((time.time() - t0) * 1000)


def fetch_twii_tiered(trade_date_yyyymmdd: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Tiered fallback: TWSE → Yahoo → FinMind
    回傳：(twii_dict, meta)
    twii_dict = {close, change, pct}
    meta 會包含 tier/source/confidence/error 等，供 audit_modules 與 UI 顯示。
    """
    # Tier-1: TWSE
    twii, meta = fetch_twii_from_twse(trade_date_yyyymmdd)
    if twii is not None and meta.get("error_code") is None and twii.get("close") is not None:
        meta["tier"] = 1
        meta["source"] = "TWSE"
        return twii, meta

    # Tier-2: Yahoo
    twii2, meta2 = fetch_twii_from_yahoo(trade_date_yyyymmdd)
    if twii2 is not None and meta2.get("status") == "OK" and twii2.get("close") is not None:
        meta2["tier"] = 2
        meta2["source"] = "YAHOO"
        return twii2, meta2

    # Tier-3: FinMind
    twii3, meta3 = fetch_twii_from_finmind(trade_date_yyyymmdd)
    if twii3 is not None and meta3.get("status") == "OK" and twii3.get("close") is not None:
        meta3["tier"] = 3
        meta3["source"] = "FINMIND"
        return twii3, meta3

    # All failed: return the most informative meta
    meta_fail = meta
    if meta2.get("error_code"):
        meta_fail = meta2
    if meta3.get("error_code"):
        meta_fail = meta3
    meta_fail["tier"] = 0
    meta_fail["source"] = "NONE"
    return None, meta_fail


def find_prev_trade_date_for_twii(trade_date_yyyymmdd: str) -> Optional[str]:
    """找上一個「任一來源可取得 TWII」的交易日，用於 EOD Guard/缺資料回退。"""
    try:
        d = datetime.strptime(trade_date_yyyymmdd, "%Y%m%d")
    except Exception:
        return None

    # 往回最多 10 天（含周末/假日緩衝）
    for i in range(1, 11):
        cand = (d - timedelta(days=i)).strftime("%Y%m%d")
        twii, _meta = fetch_twii_tiered(cand)
        if twii is not None and twii.get("close") is not None:
            return cand
    return None


# -----------------------------
# (以下保留你原本 downloader_tw.py 內容：VIX / 成交額 / T86 / TopN / build_snapshot 等)
# 我只把「build_snapshot 內 TWII 抓取」改成呼叫 fetch_twii_tiered()
# -----------------------------

def fetch_vix_tw(trade_date_yyyymmdd: str) -> Tuple[Optional[float], Dict[str, Any]]:
    t0 = time.time()
    meta: Dict[str, Any] = {
        "module": "VIX_TW",
        "trade_date": trade_date_yyyymmdd,
        "status": "FAIL",
        "confidence": "LOW",
        "asof": None,
        "error_code": None,
        "error": None,
        "latency_ms": None,
        "status_code": None,
    }
    try:
        # 你的原本 vix 抓法（如有）
        # 這裡保留原結構，避免破壞你既有的 pipeline
        url = "https://www.taifex.com.tw/cht/3/vix"
        r = sess.get(url, timeout=20)
        meta["status_code"] = r.status_code
        if r.status_code != 200:
            meta["error_code"] = f"HTTP_{r.status_code}"
            meta["error"] = r.text[:200]
            return None, meta

        # TODO: 若你原本有解析邏輯，請維持；我先用保守方式不亂改
        meta["error_code"] = "NOT_IMPLEMENTED"
        meta["error"] = "vix parser not implemented in this snippet"
        return None, meta
    except Exception as e:
        meta["error_code"] = type(e).__name__
        meta["error"] = str(e)
        return None, meta
    finally:
        meta["latency_ms"] = int((time.time() - t0) * 1000)


# -----------------------------
# Market Amount (TWSE/TPEX) - you already have market_amount.py
# 這裡通常是 build_snapshot 會去呼叫 market_amount.get_market_amount(...)
# -----------------------------


def build_snapshot(
    trade_date_yyyymmdd: str,
    session_mode: str = "EOD",
    top_n: int = 20,
) -> Dict[str, Any]:
    """
    產出 snapshot（供 Streamlit UI 顯示 + 供 Arbiter/Analyzer 組裝 JSON）
    """
    snapshot: Dict[str, Any] = {
        "meta": {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "session": session_mode,
            "trade_date": trade_date_yyyymmdd,
        },
        "macro": {"overview": {}},
        "audit_modules": [],
    }

    # ---- TWII (Tiered) ----
    twii, twii_meta = fetch_twii_tiered(trade_date_yyyymmdd)
    snapshot["audit_modules"].append(
        {
            "name": twii_meta.get("module"),
            "status": "PASS" if twii_meta.get("status") == "OK" else "FAIL",
            "confidence": twii_meta.get("confidence"),
            "asof": twii_meta.get("asof"),
            "tier": twii_meta.get("tier"),
            "source": twii_meta.get("source"),
            "error": twii_meta.get("error_code"),
            "latency_ms": twii_meta.get("latency_ms"),
            "note": twii_meta.get("note"),
        }
    )
    snapshot["macro"]["overview"]["twii_close"] = (twii or {}).get("close")

    # 你原本還有：amount_twse / amount_tpex / total / T86 / TopN 等
    # 這裡我不擅自改動，以免造成你既有流程扭曲
    return snapshot


def validate_l1_data_integrity(trade_yyyymmdd: str) -> Dict[str, Any]:
    """
    你原本的 L1 Gate / data integrity 檢查：
    這裡把 TWII 抓取改成 tiered，避免 TWSE 掛掉就直接 FAIL
    """
    report: Dict[str, Any] = {
        "MODE": "L1_AUDIT",
        "VERDICT": "FAIL",
        "FATAL_ISSUES": [],
        "WARNINGS": [],
        "MODULES": [],
    }

    twii_data, twii_meta = fetch_twii_tiered(trade_yyyymmdd)
    report["MODULES"].append(twii_meta)

    if not twii_data or twii_data.get("close") is None:
        report["FATAL_ISSUES"].append("F1_TWII_CLOSE_MISSING")

    report["VERDICT"] = "PASS" if len(report["FATAL_ISSUES"]) == 0 else "FAIL"
    return report


# 其他你原本 downloader_tw.py 的函式（TopN、T86、成交流等）請保留原樣貼回即可。
# 如果你希望我「把你 docx 裡的原始全部內容」完整併回這個新骨架，我可以直接幫你合併成一份 100% 不缺段落的完整版。
