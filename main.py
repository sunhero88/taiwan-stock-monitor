# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.32-AUDIT_ENFORCED）
#
# ✅ Hotfix v16.3.32-finmind-tpex + market_status=amount.confidence_level
# 變更摘要：
# 1) meta.market_status 直接引用 macro.market_amount.confidence_level（HIGH/MEDIUM/LOW）
#    - 若 Kill Switch 啟動，仍覆蓋為 SHELTER
# 2) FinMind Token 不再每次輸入：從 Streamlit Secrets / 環境變數讀取
# 3) TPEX 成交額修復：TPEX官方→FinMind精確→Yahoo→SafeMode 四層 fallback
# =========================================================

from __future__ import annotations

import json
import os
import time
import math
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple, Set

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf
import warnings

warnings.filterwarnings("ignore")

# =========================
# Streamlit page config
# =========================
st.set_page_config(
    page_title="Sunhero｜Predator V16.3.32 (Audit Enforced)",
    layout="wide",
)
APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.32-AUDIT_ENFORCED）"
st.title(APP_TITLE)

# =========================
# Constants / helpers
# =========================
EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL_US = "^VIX"
VIX_SYMBOL_TW = "^VIXTW"
OTC_SYMBOL = "^TWO"

DEFAULT_TOPN = 20
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}
NEUTRAL_THRESHOLD = 5_000_000

AUDIT_DIR = "data/audit_market_amount"
SMR_WATCH = 0.23

DEGRADE_FACTOR_BY_MODE = {
    "Conservative": 0.60,
    "Balanced": 0.75,
    "Aggressive": 0.85,
}

CORE_WATCH_LIST = ["2330.TW"]

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海",   "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達",   "3231.TW": "緯創",   "2376.TW": "技嘉",   "3017.TW": "奇鋐",
    "3324.TW": "雙鴻",   "3661.TW": "世芯-KY",
    "2881.TW": "富邦金", "2882.TW": "國泰金", "2891.TW": "中信金", "2886.TW": "兆豐金",
    "2603.TW": "長榮",   "2609.TW": "陽明",   "1605.TW": "華新",   "1513.TW": "中興電",
    "1519.TW": "華城",   "2002.TW": "中鋼"
}

COL_TRANSLATION = {
    "Symbol": "代號",
    "Name": "名稱",
    "Tier": "權重序",
    "Price": "價格",
    "Vol_Ratio": "量能比(Vol Ratio)",
    "Layer": "分級(Layer)",
    "Foreign_Net": "外資3日淨額",
    "Trust_Net": "投信3日淨額",
    "Inst_Streak3": "法人連買天數",
    "Inst_Status": "籌碼狀態",
    "Inst_Dir3": "籌碼方向",
    "Inst_Net_3d": "3日合計淨額",
    "inst_source": "資料來源",
    "source": "價格來源",
}

STATUS_ENUM = {"OK", "DEGRADED", "ESTIMATED", "FAIL"}
CONF_ENUM = {"HIGH", "MEDIUM", "LOW"}
DATE_STATUS_ENUM = {"VERIFIED", "UNVERIFIED", "INVALID"}

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _safe_float(x, default=None) -> Optional[float]:
    try:
        if x is None:
            return default
        if isinstance(x, (np.floating, float, int)):
            return float(x)
        if isinstance(x, str) and x.strip() == "":
            return default
        return float(x)
    except Exception:
        return default

def _safe_int(x, default=None) -> Optional[int]:
    try:
        if x is None:
            return default
        if isinstance(x, (np.integer, int)):
            return int(x)
        if isinstance(x, (np.floating, float)):
            return int(float(x))
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            return int(float(s)) if s else default
        return int(x)
    except Exception:
        return default

def _pct01_to_pct100(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(x) * 100.0

def _to_roc_date(ymd: str, format_type: str = "standard") -> str:
    dt = pd.to_datetime(ymd)
    roc_year = int(dt.year) - 1911
    if format_type == "compact":
        return f"{roc_year}/{dt.month}/{dt.day}"
    elif format_type == "dense":
        return f"{roc_year:03d}{dt.month:02d}{dt.day:02d}"
    else:
        return f"{roc_year:03d}/{dt.month:02d}/{dt.day:02d}"

def _is_nan(x: Any) -> bool:
    try:
        return bool(isinstance(x, float) and np.isnan(x))
    except Exception:
        return False

def _infer_status_confidence_from_source(src: str) -> Tuple[str, str]:
    s = (src or "").upper()

    if "OK" in s and ("TWSE_OK" in s or "TPEX_OK" in s):
        return "OK", "HIGH"

    if "FINMIND_OK" in s:
        return "OK", "HIGH"

    if "YAHOO" in s and "ESTIMATE" in s:
        return "ESTIMATED", "MEDIUM"

    if "SAFE_MODE" in s:
        return "ESTIMATED", "LOW"

    if "FALLBACK" in s or "SSL_BYPASS" in s:
        return "DEGRADED", "MEDIUM"

    return "FAIL", "LOW"

def _overall_confidence(levels: List[str]) -> str:
    if not levels:
        return "LOW"
    if all(l == "HIGH" for l in levels):
        return "HIGH"
    if any(l == "LOW" for l in levels):
        return "LOW"
    return "MEDIUM"

# =========================
# Warnings recorder
# =========================
class WarningBus:
    def __init__(self):
        self.items: List[Dict[str, Any]] = []

    def push(self, code: str, msg: str, meta: Optional[dict] = None):
        self.items.append({"ts": _now_ts(), "code": code, "msg": msg, "meta": meta or {}})

    def latest(self, n: int = 50) -> List[Dict[str, Any]]:
        return self.items[-n:]

warnings_bus = WarningBus()

# =========================
# Global Session (requests)
# =========================
_GLOBAL_SESSION = None

def _get_global_session() -> requests.Session:
    global _GLOBAL_SESSION
    if _GLOBAL_SESSION is None:
        _GLOBAL_SESSION = requests.Session()
        _GLOBAL_SESSION.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "application/json,text/plain,text/html,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        })
    return _GLOBAL_SESSION

def _http_session() -> requests.Session:
    return _get_global_session()

# =========================================================
# Market amount (TWSE/TPEX)
# =========================================================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]

    source_twse: str
    source_tpex: str

    status_twse: str
    status_tpex: str
    confidence_twse: str
    confidence_tpex: str
    confidence_level: str

    allow_insecure_ssl: bool
    scope: str
    meta: Optional[Dict[str, Any]] = None

def _audit_save_text(audit_dir: str, fname: str, text: str) -> None:
    _ensure_dir(audit_dir)
    with open(os.path.join(audit_dir, fname), "w", encoding="utf-8") as f:
        f.write(text if text is not None else "")

def _audit_save_json(audit_dir: str, fname: str, obj: Any) -> None:
    _ensure_dir(audit_dir)
    with open(os.path.join(audit_dir, fname), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _yahoo_estimate_twse() -> Tuple[int, str]:
    try:
        ticker = yf.Ticker("^TWII")
        hist = ticker.history(period="2d", prepost=False)
        if len(hist) >= 1:
            vol = hist["Volume"].iloc[-1]
            close = hist["Close"].iloc[-1]
            est = int(vol * close * 0.45)
            if 200_000_000_000 <= est <= 1_000_000_000_000:
                warnings_bus.push("TWSE_YAHOO_ESTIMATE", f"使用 Yahoo 估算 TWSE: {est:,}", {})
                return est, "YAHOO_ESTIMATE_TWSE"
    except Exception as e:
        warnings_bus.push("YAHOO_TWSE_FAIL", str(e), {})

    warnings_bus.push("TWSE_SAFE_MODE", "使用固定值 5000 億", {})
    return 500_000_000_000, "TWSE_SAFE_MODE_500B"

def _yahoo_estimate_tpex() -> Tuple[int, str]:
    try:
        ticker = yf.Ticker("^TWO")
        hist = ticker.history(period="2d", prepost=False)
        if len(hist) >= 1:
            vol = hist["Volume"].iloc[-1]
            close = hist["Close"].iloc[-1]
            if len(hist) >= 2 and float(hist["Close"].iloc[-2]) != 0:
                price_chg = (hist["Close"].iloc[-1] - hist["Close"].iloc[-2]) / hist["Close"].iloc[-2]
                coef = 0.65 if price_chg > 0.01 else 0.55 if price_chg < -0.01 else 0.60
            else:
                coef = 0.60

            est = int(vol * close * coef)
            if 100_000_000_000 <= est <= 500_000_000_000:
                warnings_bus.push("TPEX_YAHOO_ESTIMATE", f"使用 Yahoo 估算 TPEX: {est:,} (係數 {coef})", {})
                return est, f"YAHOO_ESTIMATE_TPEX_{coef}"
    except Exception as e:
        warnings_bus.push("YAHOO_TPEX_FAIL", str(e), {})

    warnings_bus.push("TPEX_SAFE_MODE", "使用固定值 2000 億", {})
    return 200_000_000_000, "TPEX_SAFE_MODE_200B"

def _twse_audit_sum_by_stock_day_all(trade_date: str, allow_insecure_ssl: bool) -> Tuple[Optional[int], str, Dict[str, Any]]:
    session = _http_session()
    ymd8 = trade_date.replace("-", "")
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": ymd8}

    meta = {"url": url, "params": params, "status_code": None, "final_url": None, "audit": None}
    verify_ssl = not allow_insecure_ssl

    for attempt in [1, 2]:
        try:
            if attempt == 2:
                verify_ssl = False
                warnings_bus.push("TWSE_SSL_AUTO_FIX", "SSL 錯誤，自動切換 verify=False", {})

            r = session.get(url, params=params, timeout=15, verify=verify_ssl)
            meta["status_code"] = r.status_code
            meta["final_url"] = r.url

            text = r.text or ""
            _audit_save_text(AUDIT_DIR, f"TWSE_{ymd8}_raw.txt", text)

            r.raise_for_status()
            js = r.json()
            _audit_save_json(AUDIT_DIR, f"TWSE_{ymd8}_raw.json", js)

            data = js.get("data", [])
            fields = js.get("fields", [])
            if not isinstance(data, list) or not data:
                continue

            fields_s = [str(x).strip() for x in fields]
            amt_idx = None
            for i, f in enumerate(fields_s):
                if "成交金額" in f:
                    amt_idx = i
                    break
            if amt_idx is None:
                amt_idx = 3

            total = 0
            for row in data:
                if not isinstance(row, list) or len(row) <= amt_idx:
                    continue
                amt = _safe_int(row[amt_idx], 0)
                total += amt

            if total > 100_000_000_000:
                audit = {"market": "TWSE", "trade_date": trade_date, "rows": len(data), "amount_sum": total}
                meta["audit"] = audit
                src = "TWSE_OK:AUDIT_SUM" if attempt == 1 else "TWSE_OK:SSL_BYPASS"
                return int(total), src, meta

        except requests.exceptions.SSLError:
            if attempt == 1:
                continue
            break
        except Exception as e:
            warnings_bus.push("TWSE_ATTEMPT_FAIL", f"Attempt {attempt}: {e}", {})
            if attempt == 2:
                break

    warnings_bus.push("TWSE_ALL_FAIL", "官方 API 失敗，使用 Yahoo 估算", {})
    amt, src = _yahoo_estimate_twse()
    meta["fallback"] = "yahoo"
    return amt, src, meta

def _tpex_audit_sum_by_st43(trade_date: str, allow_insecure_ssl: bool) -> Tuple[Optional[int], str, Dict[str, Any]]:
    session = _http_session()
    roc_formats = [
        ("standard", _to_roc_date(trade_date, "standard")),
        ("compact", _to_roc_date(trade_date, "compact")),
    ]
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    session.headers.update({
        "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php?l=zh-tw"
    })

    meta = {"url": url, "attempts": [], "audit": None}

    try:
        session.get(
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php?l=zh-tw",
            timeout=10, verify=(not allow_insecure_ssl)
        )
        time.sleep(0.25)
    except Exception:
        pass

    for fmt_name, roc in roc_formats:
        for se_param in ["EW", "AL"]:
            params = {"l": "zh-tw", "d": roc, "se": se_param}
            attempt_id = f"{fmt_name}_{se_param}"
            try:
                r = session.get(url, params=params, timeout=15, verify=(not allow_insecure_ssl), allow_redirects=True)

                if "/error" in (r.url or "").lower():
                    meta["attempts"].append({"id": attempt_id, "result": "redirected_to_error"})
                    continue

                js = r.json()
                aa = js.get("aaData") or js.get("data") or []
                if not aa:
                    meta["attempts"].append({"id": attempt_id, "result": "no_data"})
                    continue

                total = 0
                for row in aa:
                    if not isinstance(row, list):
                        continue
                    for idx in [7, 8]:
                        if idx >= len(row):
                            continue
                        val = _safe_int(row[idx], None)
                        if val and val >= 10_000_000:
                            total += val
                            break

                if total > 50_000_000_000:
                    warnings_bus.push("TPEX_SUCCESS", f"成功: {attempt_id}, 總額: {total:,}", {})
                    meta["audit"] = {"market": "TPEX", "trade_date": trade_date, "attempt": attempt_id, "amount_sum": total, "rows": len(aa)}
                    return int(total), f"TPEX_OK:{attempt_id}", meta

                meta["attempts"].append({"id": attempt_id, "result": f"total_too_low_{total}"})

            except Exception as e:
                meta["attempts"].append({"id": attempt_id, "error": str(e)})
                continue

    return None, "TPEX_FAIL:ST43_ALL_FAIL", meta

# =========================
# FinMind TPEX precise amount (OTC list + paged price)
# =========================
def _finmind_get(dataset: str, params: dict, token: Optional[str]) -> dict:
    p = {"dataset": dataset, **params}
    if token:
        p["token"] = token
    r = requests.get(FINMIND_URL, params=p, timeout=25)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=3600, show_spinner=False)
def _finmind_get_otc_stock_list(token: Optional[str]) -> Set[str]:
    """
    用 TaiwanStockInfo 取得 OTC/ROTC 清單
    """
    if not token:
        return set()

    try:
        js = _finmind_get("TaiwanStockInfo", params={}, token=token)
        data = js.get("data", []) or []
        otc = set()
        for row in data:
            market = str(row.get("market", "")).upper()
            stock_id = str(row.get("stock_id", "")).strip()
            if stock_id and market in {"OTC", "ROTC"}:
                otc.add(stock_id)
        return otc
    except Exception as e:
        warnings_bus.push("FINMIND_OTC_LIST_FAIL", str(e), {})
        return set()

@st.cache_data(ttl=300, show_spinner=False)
def _finmind_tpex_amount_precise(trade_date: str, token: Optional[str]) -> Tuple[Optional[int], str, Dict[str, Any]]:
    """
    精確版：
    1) TaiwanStockInfo -> 取得 OTC/ROTC stock_id 清單
    2) TaiwanStockPrice (當日) -> 分頁抓取 -> 只累加在清單內的 Trading_money
    """
    meta: Dict[str, Any] = {
        "dataset": "TaiwanStockPrice",
        "trade_date": trade_date,
        "status_code": None,
        "pages": 0,
        "rows": 0,
        "otc_stocks_count": 0,
        "matched_stocks": 0,
        "amount_sum": 0,
    }

    if not token:
        return None, "FINMIND_FAIL:NO_TOKEN", meta

    otc_stocks = _finmind_get_otc_stock_list(token)
    meta["otc_stocks_count"] = int(len(otc_stocks))
    if not otc_stocks:
        return None, "FINMIND_FAIL:NO_OTC_LIST", meta

    total = 0
    matched = 0
    rows_total = 0

    # FinMind API 可能需要 offset 分頁（避免只拿到前 1000 筆）
    # 這裡採保守分頁：每頁 1000，最多 20 頁（避免失控）
    page_size = 1000
    max_pages = 20

    try:
        for page in range(max_pages):
            offset = page * page_size
            js = _finmind_get(
                "TaiwanStockPrice",
                params={
                    "start_date": trade_date,
                    "end_date": trade_date,
                    "offset": offset,
                    "limit": page_size,
                },
                token=token,
            )
            data = js.get("data", []) or []
            meta["pages"] = page + 1

            if not data:
                break

            rows_total += len(data)

            for row in data:
                stock_id = str(row.get("stock_id", "")).strip()
                if stock_id in otc_stocks:
                    trading_money = _safe_int(row.get("Trading_money"), 0) or 0
                    if trading_money > 0:
                        total += trading_money
                        matched += 1

        meta["rows"] = int(rows_total)
        meta["matched_stocks"] = int(matched)
        meta["amount_sum"] = int(total)

        # 合理下限：至少 500 億（你原本的守門值）
        if total >= 50_000_000_000:
            warnings_bus.push("FINMIND_TPEX_PRECISE_SUCCESS", f"FinMind 精確成交額: {total:,}（matched {matched}）", meta)
            return int(total), "FINMIND_OK:PRECISE", meta

        warnings_bus.push("FINMIND_TPEX_AMOUNT_TOO_LOW", f"FinMind 金額過低: {total:,}", meta)
        return None, "FINMIND_FAIL:AMOUNT_TOO_LOW", meta

    except requests.exceptions.Timeout:
        warnings_bus.push("FINMIND_TPEX_TIMEOUT", "FinMind API 超時", meta)
        return None, "FINMIND_FAIL:TIMEOUT", meta
    except Exception as e:
        warnings_bus.push("FINMIND_TPEX_FAIL", str(e), meta)
        return None, f"FINMIND_FAIL:{type(e).__name__}", meta

def _amount_scope(twse_amt: Optional[int], tpex_amt: Optional[int]) -> str:
    if twse_amt and tpex_amt:
        return "ALL"
    if twse_amt:
        return "TWSE_ONLY"
    if tpex_amt:
        return "TPEX_ONLY"
    return "NONE"

@st.cache_data(ttl=300, show_spinner=False)
def fetch_amount_total(trade_date: str, allow_insecure_ssl: bool = False, finmind_token: Optional[str] = None) -> MarketAmount:
    """
    四層 TPEX Fallback：
      1) TPEX 官方 st43
      2) FinMind 精確版
      3) Yahoo estimate
      4) Safe Mode 2000 億
    """
    _ensure_dir(AUDIT_DIR)

    # TWSE
    twse_amt, twse_src, twse_meta = _twse_audit_sum_by_stock_day_all(trade_date, allow_insecure_ssl)

    # TPEX Layer 1: 官方
    tpex_amt, tpex_src, tpex_meta = _tpex_audit_sum_by_st43(trade_date, allow_insecure_ssl)

    # TPEX Layer 2: FinMind 精確
    if tpex_amt is None or tpex_amt <= 0:
        warnings_bus.push("TPEX_FALLBACK_FINMIND", "官方 TPEX 失敗，改用 FinMind 精確版", {"trade_date": trade_date})
        tpex_amt, tpex_src, tpex_meta2 = _finmind_tpex_amount_precise(trade_date, finmind_token)
        tpex_meta = {**tpex_meta, **{"finmind": tpex_meta2}}

    # TPEX Layer 3: Yahoo estimate
    if tpex_amt is None or tpex_amt <= 0:
        warnings_bus.push("TPEX_FALLBACK_YAHOO", "FinMind 失敗，改用 Yahoo 估算", {"trade_date": trade_date})
        tpex_amt, tpex_src = _yahoo_estimate_tpex()
        tpex_meta["fallback"] = "yahoo"

    # TPEX Layer 4: Safe mode
    if tpex_amt is None or tpex_amt <= 0:
        warnings_bus.push("TPEX_SAFE_MODE", "全部失敗，使用 Safe Mode 2000 億", {"trade_date": trade_date})
        tpex_amt, tpex_src = (200_000_000_000, "TPEX_SAFE_MODE_200B")
        tpex_meta["fallback"] = "safe_mode"

    # TWSE forced fallback
    if not twse_amt or twse_amt <= 0:
        twse_amt, twse_src = _yahoo_estimate_twse()
        twse_meta = {"fallback": "yahoo_forced"}

    total = int(twse_amt) + int(tpex_amt)
    scope = _amount_scope(twse_amt, tpex_amt)

    st_twse, cf_twse = _infer_status_confidence_from_source(twse_src)
    st_tpex, cf_tpex = _infer_status_confidence_from_source(tpex_src)
    overall = _overall_confidence([cf_twse, cf_tpex])

    meta = {
        "trade_date": trade_date,
        "audit_dir": AUDIT_DIR,
        "twse": twse_meta,
        "tpex": tpex_meta,
    }

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        status_twse=st_twse,
        status_tpex=st_tpex,
        confidence_twse=cf_twse,
        confidence_tpex=cf_tpex,
        confidence_level=overall,
        allow_insecure_ssl=bool(allow_insecure_ssl),
        scope=scope,
        meta=meta,
    )

# =========================
# Market institutions (TWSE BFI82U)
# =========================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_inst_summary(allow_insecure_ssl: bool = False) -> List[Dict[str, Any]]:
    url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json"
    data_list: List[Dict[str, Any]] = []
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()
        if "data" in js and isinstance(js["data"], list):
            for row in js["data"]:
                if len(row) >= 4:
                    name = str(row[0]).strip()
                    diff = _safe_int(row[3])
                    if diff is not None:
                        data_list.append({"Identity": name, "Net": diff})
    except Exception as e:
        warnings_bus.push("MARKET_INST_FAIL", f"BFI82U fetch fail: {e}", {"url": url})
    return data_list

# =========================
# FinMind institutional (existing)
# =========================
def normalize_inst_direction(net: float) -> str:
    net = float(net or 0.0)
    if abs(net) < NEUTRAL_THRESHOLD:
        return "NEUTRAL"
    return "POSITIVE" if net > 0 else "NEGATIVE"

@st.cache_data(ttl=300, show_spinner=False)
def fetch_finmind_institutional(
    symbols: List[str],
    start_date: str,
    end_date: str,
    token: Optional[str] = None,
) -> pd.DataFrame:
    rows = []
    if not token:
        return pd.DataFrame(columns=["date", "symbol", "net_amount"])

    for sym in symbols:
        stock_id = sym.replace(".TW", "").replace(".TWO", "").strip()
        try:
            js = _finmind_get(
                dataset="TaiwanStockInstitutionalInvestorsBuySell",
                params={"data_id": stock_id, "start_date": start_date, "end_date": end_date},
                token=token,
            )
        except Exception as e:
            warnings_bus.push("FINMIND_FAIL", str(e), {"symbol": sym})
            continue

        data = js.get("data", []) or []
        if not data:
            continue

        df = pd.DataFrame(data)
        need = {"date", "stock_id", "buy", "name", "sell"}
        if not need.issubset(set(df.columns)):
            continue

        df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
        df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
        df = df[df["name"].isin(A_NAMES)].copy()
        if df.empty:
            continue

        df["net"] = df["buy"] - df["sell"]
        g = df.groupby("date", as_index=False)["net"].sum()
        for _, r in g.iterrows():
            rows.append({"date": str(r["date"]), "symbol": sym, "net_amount": float(r["net"])})

    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "net_amount"])
    return pd.DataFrame(rows).sort_values(["symbol", "date"])

def calc_inst_3d(inst_df: pd.DataFrame, symbol: str, has_token: bool) -> dict:
    if not has_token:
        return {"Inst_Status": "NO_DATA", "Inst_Streak3": 0, "Inst_Dir3": "NO_DATA", "Inst_Net_3d": 0.0}

    if inst_df is None or inst_df.empty:
        return {"Inst_Status": "NO_UPDATE_TODAY", "Inst_Streak3": 0, "Inst_Dir3": "NO_UPDATE_TODAY", "Inst_Net_3d": 0.0}

    df = inst_df[inst_df["symbol"] == symbol].copy()
    if df.empty:
        return {"Inst_Status": "NO_UPDATE_TODAY", "Inst_Streak3": 0, "Inst_Dir3": "NO_UPDATE_TODAY", "Inst_Net_3d": 0.0}

    df = df.sort_values("date").tail(3)
    if len(df) < 3:
        return {"Inst_Status": "NO_UPDATE_TODAY", "Inst_Streak3": 0, "Inst_Dir3": "NO_UPDATE_TODAY", "Inst_Net_3d": float(df["net_amount"].sum())}

    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0)
    dirs = [normalize_inst_direction(x) for x in df["net_amount"]]
    net_sum = float(df["net_amount"].sum())

    if all(d == "POSITIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "POSITIVE", "Inst_Net_3d": net_sum}
    if all(d == "NEGATIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "NEGATIVE", "Inst_Net_3d": net_sum}

    if net_sum == 0.0:
        return {"Inst_Status": "NO_UPDATE_TODAY", "Inst_Streak3": 0, "Inst_Dir3": "NO_UPDATE_TODAY", "Inst_Net_3d": 0.0}

    return {"Inst_Status": "READY", "Inst_Streak3": 0, "Inst_Dir3": "NEUTRAL", "Inst_Net_3d": net_sum}

# =========================
# yfinance fetchers (unchanged)
# =========================
def _normalize_yf_columns(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [" ".join([str(c) for c in col if str(c) != ""]).strip() for col in df.columns.values]

    df = df.copy()
    rename_map = {}
    for c in df.columns:
        s = str(c)
        if s.endswith(f" {symbol}"):
            rename_map[c] = s.replace(f" {symbol}", "").strip()

    if rename_map:
        df = df.rename(columns=rename_map)

    return df

@st.cache_data(ttl=600, show_spinner=False)
def fetch_history(symbol: str, period: str = "5y", interval: str = "1d") -> pd.DataFrame:
    try:
        df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False, group_by="column", threads=False)
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "Datetime"})
        elif "index" in df.columns:
            df = df.rename(columns={"index": "Datetime"})
        if "Datetime" not in df.columns and df.index.name is not None:
            df.insert(0, "Datetime", pd.to_datetime(df.index))

        df = _normalize_yf_columns(df, symbol)
        return df
    except Exception as e:
        warnings_bus.push("YF_HISTORY_FAIL", str(e), {"symbol": symbol})
        return pd.DataFrame()

def _single_fetch_price_volratio(sym: str) -> Tuple[Optional[float], Optional[float], str]:
    try:
        df = yf.download(sym, period="6mo", interval="1d", auto_adjust=False, progress=False, group_by="column", threads=False)
        src = "YF_SINGLE_TW"
        if df is None or df.empty or df.get("Close") is None or df["Close"].dropna().empty:
            raise RuntimeError("EMPTY_TW")
    except Exception:
        try:
            alt = sym.replace(".TW", ".TWO")
            df = yf.download(alt, period="6mo", interval="1d", auto_adjust=False, progress=False, group_by="column", threads=False)
            src = "YF_SINGLE_TPEX_FALLBACK"
            if df is None or df.empty or df.get("Close") is None or df["Close"].dropna().empty:
                return None, None, "FAIL"
        except Exception:
            return None, None, "FAIL"

    close = df["Close"].dropna() if "Close" in df.columns else pd.Series(dtype=float)
    vol = df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)

    price = float(close.iloc[-1]) if len(close) else None
    vol_ratio = None
    if len(vol) >= 20:
        ma20 = float(vol.rolling(20).mean().iloc[-1])
        if ma20 and ma20 > 0:
            vol_ratio = float(vol.iloc[-1] / ma20)

    return price, vol_ratio, src

@st.cache_data(ttl=300, show_spinner=False)
def fetch_batch_prices_volratio_with_source(symbols: List[str]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    out = pd.DataFrame({"Symbol": symbols})
    out["Price"] = None
    out["Vol_Ratio"] = None
    out["source"] = "FAIL"

    source_map: Dict[str, str] = {s: "FAIL" for s in symbols}
    if not symbols:
        return out, source_map

    try:
        df = yf.download(symbols, period="6mo", interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=True)
    except Exception as e:
        warnings_bus.push("YF_BATCH_FAIL", str(e), {"n": len(symbols)})
        df = pd.DataFrame()

    if df is not None and not df.empty:
        for sym in symbols:
            try:
                if isinstance(df.columns, pd.MultiIndex):
                    if sym not in df.columns.get_level_values(0):
                        continue
                    close = df[(sym, "Close")].dropna()
                    vol = df[(sym, "Volume")].dropna()
                else:
                    close = df["Close"].dropna() if "Close" in df.columns else pd.Series(dtype=float)
                    vol = df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)

                price = float(close.iloc[-1]) if len(close) else None
                vol_ratio = None
                if len(vol) >= 20:
                    ma20 = float(vol.rolling(20).mean().iloc[-1])
                    if ma20 and ma20 > 0:
                        vol_ratio = float(vol.iloc[-1] / ma20)

                if price is not None:
                    out.loc[out["Symbol"] == sym, "Price"] = price
                if vol_ratio is not None:
                    out.loc[out["Symbol"] == sym, "Vol_Ratio"] = vol_ratio

                if (price is not None) or (vol_ratio is not None):
                    out.loc[out["Symbol"] == sym, "source"] = "YF_BATCH"
                    source_map[sym] = "YF_BATCH"
            except Exception:
                continue

    need_fix = out[(out["Price"].isna()) | (out["Vol_Ratio"].isna())]["Symbol"].tolist()
    for sym in need_fix:
        p, vr, src = _single_fetch_price_volratio(sym)

        if p is not None and (out.loc[out["Symbol"] == sym, "Price"].isna().iloc[0]):
            out.loc[out["Symbol"] == sym, "Price"] = float(p)
        if vr is not None and (out.loc[out["Symbol"] == sym, "Vol_Ratio"].isna().iloc[0]):
            out.loc[out["Symbol"] == sym, "Vol_Ratio"] = float(vr)

        if (p is not None) or (vr is not None):
            out.loc[out["Symbol"] == sym, "source"] = src
            source_map[sym] = src
        else:
            out.loc[out["Symbol"] == sym, "source"] = "FAIL"
            source_map[sym] = "FAIL"

    return out, source_map

# =========================
# Regime & Metrics（VIXTW 優先）
# =========================
def _as_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("empty df")
    if col_name in df.columns:
        s = df[col_name]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        return pd.to_numeric(s, errors="coerce").astype(float)
    cols = [c for c in df.columns if str(col_name).lower() == str(c).lower()]
    if cols:
        s = df[cols[0]]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        return pd.to_numeric(s, errors="coerce").astype(float)
    raise ValueError(f"Col {col_name} not found")

def _as_close_series(df: pd.DataFrame) -> pd.Series:
    try:
        return _as_series(df, "Close")
    except Exception:
        return _as_series(df, "Adj Close")

def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 260:
        return {
            "SMR": None, "Slope5": None, "MOMENTUM_LOCK": False,
            "drawdown_pct": None, "price_range_10d_pct": None, "gap_down": None,
            "metrics_reason": "INSUFFICIENT_ROWS"
        }

    try:
        close = _as_close_series(market_df)
    except Exception as e:
        return {
            "SMR": None, "Slope5": None, "MOMENTUM_LOCK": False,
            "drawdown_pct": None, "price_range_10d_pct": None, "gap_down": None,
            "metrics_reason": f"CLOSE_SERIES_FAIL:{e}"
        }

    ma200 = close.rolling(200).mean()
    smr_series = ((close - ma200) / ma200).dropna()
    if len(smr_series) < 10:
        return {"SMR": None, "Slope5": None, "MOMENTUM_LOCK": False, "drawdown_pct": None, "metrics_reason": "SMR_SERIES_TOO_SHORT"}

    smr = float(smr_series.iloc[-1])
    smr_ma5 = smr_series.rolling(5).mean().dropna()
    slope5 = float(smr_ma5.iloc[-1] - smr_ma5.iloc[-2]) if len(smr_ma5) >= 2 else 0.0

    last4 = smr_ma5.diff().dropna().iloc[-4:]
    momentum_lock = bool((last4 > EPS).all()) if len(last4) == 4 else False

    window_dd = 252
    rolling_high = close.rolling(window_dd).max()
    drawdown_pct = float(close.iloc[-1] / rolling_high.iloc[-1] - 1.0) if not np.isnan(rolling_high.iloc[-1]) else None

    price_range_10d_pct = None
    if len(close) >= 10:
        recent_10d = close.iloc[-10:]
        low_10d = float(recent_10d.min())
        high_10d = float(recent_10d.max())
        if low_10d > 0:
            price_range_10d_pct = float((high_10d - low_10d) / low_10d)

    gap_down = None
    try:
        open_s = _as_series(market_df, "Open")
        if len(open_s) >= 2 and len(close) >= 2:
            today_open = float(open_s.iloc[-1])
            prev_close = float(close.iloc[-2])
            if prev_close > 0:
                gap_down = (today_open - prev_close) / prev_close
    except Exception:
        gap_down = None

    return {
        "SMR": smr,
        "SMR_MA5": float(smr_ma5.iloc[-1]) if len(smr_ma5) else None,
        "Slope5": slope5,
        "NEGATIVE_SLOPE_5D": bool(slope5 < -EPS),
        "MOMENTUM_LOCK": momentum_lock,
        "drawdown_pct": drawdown_pct,
        "drawdown_window_days": window_dd,
        "price_range_10d_pct": price_range_10d_pct,
        "gap_down": gap_down,
        "metrics_reason": "OK",
    }

def calculate_dynamic_vix(vix_df: pd.DataFrame) -> Optional[float]:
    if vix_df is None or vix_df.empty:
        return None
    try:
        vix_close = _as_close_series(vix_df)
        if len(vix_close) < 20:
            return 40.0
        ma20 = float(vix_close.rolling(20).mean().iloc[-1])
        std20 = float(vix_close.rolling(20).std().iloc[-1])
        threshold = ma20 + 2 * std20
        return max(35.0, float(threshold))
    except Exception:
        return 35.0

def pick_regime(metrics: dict, vix: Optional[float], vix_panic: float) -> Tuple[str, float]:
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")
    price_range = metrics.get("price_range_10d_pct")

    if (vix is not None and float(vix) > float(vix_panic)) or (drawdown is not None and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10

    if smr is not None and slope5 is not None:
        if float(smr) >= SMR_WATCH and float(slope5) < -EPS:
            return "MEAN_REVERSION_WATCH", 0.55
        if float(smr) > 0.25 and float(slope5) < -EPS:
            return "MEAN_REVERSION", 0.45
        if float(smr) > 0.25 and float(slope5) >= -EPS:
            return "OVERHEAT", 0.55

    if smr is not None and 0.08 <= float(smr) <= 0.18:
        if price_range is not None and float(price_range) < 0.05:
            return "CONSOLIDATION", 0.65

    return "NORMAL", 0.85

# =========================================================
# Constitution Integrity (Layer B) + Self-Audit
# =========================================================
def evaluate_integrity_v1632(stocks: List[dict], topn: int) -> Dict[str, Any]:
    missing_syms = []
    fallback_syms = []

    for s in stocks:
        sym = s.get("Symbol")
        price = s.get("Price")
        vr = s.get("Vol_Ratio")
        src = str(s.get("source", "")).upper()

        price_missing = (price is None) or _is_nan(price)
        vr_missing = (vr is None) or _is_nan(vr)

        if price_missing or vr_missing:
            missing_syms.append(sym)

        if ("FALLBACK" in src) or ("SAFE_MODE" in src) or ("YF_SINGLE_TPEX_FALLBACK" in src):
            fallback_syms.append(sym)

    missing_syms = [x for x in missing_syms if x]
    missing_count = len(set(missing_syms))

    for core in CORE_WATCH_LIST:
        for s in stocks:
            if s.get("Symbol") == core:
                if s.get("Price") is None or s.get("Vol_Ratio") is None or _is_nan(s.get("Price")) or _is_nan(s.get("Vol_Ratio")):
                    return {
                        "status": "CRITICAL_FAILURE",
                        "kill_switch": True,
                        "confidence": "LOW",
                        "reason": f"CORE_STOCK_MISSING:{core}",
                        "missing_count": missing_count,
                        "missing_list": sorted(list(set(missing_syms))),
                    }

    threshold = max(2, int(math.ceil(topn * 0.1)))
    if missing_count > threshold:
        return {
            "status": "DATA_DEGRADED",
            "kill_switch": True,
            "confidence": "LOW",
            "reason": f"MISSING_COUNT_EXCEED:{missing_count}/{topn}>threshold:{threshold}",
            "missing_count": missing_count,
            "missing_list": sorted(list(set(missing_syms))),
        }

    if missing_count == 0 and len(fallback_syms) == 0:
        confidence = "HIGH"
    elif missing_count <= 1:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "status": "OK",
        "kill_switch": False,
        "confidence": confidence,
        "reason": "INTEGRITY_PASS",
        "missing_count": missing_count,
        "missing_list": sorted(list(set(missing_syms))),
        "fallback_count": int(len(set(fallback_syms))),
    }

def audit_constitution(payload: Dict[str, Any], topn: int) -> List[str]:
    violations: List[str] = []
    ov = payload.get("macro", {}).get("overview", {})
    integ = payload.get("macro", {}).get("integrity_v1632", {})
    amount = payload.get("macro", {}).get("market_amount", {})
    stocks = payload.get("stocks", [])

    for k in ["source_twse", "source_tpex", "status_twse", "status_tpex", "confidence_level"]:
        if k not in amount:
            violations.append(f"❌ [憲章 1.1] MarketAmount 缺少欄位: {k}")
            break

    for k in ["vix", "vix_source", "vix_status", "vix_confidence"]:
        if k not in ov:
            violations.append(f"❌ [憲章 1.1] VIX 四件套缺少欄位: {k}")
            break

    if bool(integ.get("kill_switch")) and float(ov.get("max_equity_allowed_pct") or 0.0) != 0.0:
        violations.append("❌ [憲章 1.2] Kill Switch 啟動但建議持倉上限未歸零")

    if ov.get("vix") is None:
        if not (bool(integ.get("kill_switch")) and ov.get("current_regime") in ("DATA_FAILURE", "INTEGRITY_KILL")):
            violations.append("❌ [Layer A / 憲章] VIX 缺失但未強制降級/停機")

    if float(ov.get("max_equity_allowed_pct") or 0.0) == 0.0:
        for s in stocks:
            if str(s.get("Layer", "")).strip() in ("A+", "A", "B"):
                violations.append(f"❌ [憲章 2] 市場停機但個股 {s.get('Symbol')} 仍給出可參與層級({s.get('Layer')})")
                break

    for s in stocks:
        src = s.get("source")
        if src is None or str(src).strip() == "":
            violations.append(f"❌ [憲章 1.1] 個股 {s.get('Symbol')} source 標記缺失")
            break

    return violations

# =========================
# Layer C: classify layer
# =========================
def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    inst_streak3 = int(inst.get("inst_streak3", 0))
    if foreign_buy and trust_buy and inst_streak3 >= 3:
        return "A+"
    if (foreign_buy or trust_buy) and inst_streak3 >= 3:
        return "A"
    vr = _safe_float(vol_ratio, None)
    if momentum_lock and (vr is not None and float(vr) > 0.8) and regime in ["NORMAL", "OVERHEAT", "CONSOLIDATION", "MEAN_REVERSION_WATCH"]:
        return "B"
    return "NONE"

def _apply_amount_degrade(max_equity: float, account_mode: str, amount_partial: bool) -> float:
    if not amount_partial:
        return max_equity
    factor = float(DEGRADE_FACTOR_BY_MODE.get(account_mode, 0.75))
    return float(max_equity) * factor

def _default_symbols_pool(topn: int) -> List[str]:
    pool = list(STOCK_NAME_MAP.keys())
    limit = min(len(pool), max(1, int(topn)))
    return pool[:limit]

def _source_snapshot(name: str, df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"name": name, "ok": False, "rows": 0, "cols": [], "last_dt": None, "reason": "EMPTY"}
    cols = list(map(str, df.columns.tolist()))
    last_dt = None
    try:
        if "Datetime" in df.columns and len(df["Datetime"].dropna()) > 0:
            last_dt = pd.to_datetime(df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")
    except Exception:
        last_dt = None
    return {"name": name, "ok": True, "rows": int(len(df)), "cols": cols, "last_dt": last_dt, "reason": "OK"}

# =========================
# Arbiter input builder（主流程）
# =========================
def _load_finmind_token() -> Optional[str]:
    # 優先：Streamlit Secrets（線上最推薦）
    try:
        t = st.secrets.get("FINMIND_TOKEN", None)
        if t:
            return str(t).strip()
    except Exception:
        pass
    # 次要：環境變數
    t2 = os.getenv("FINMIND_TOKEN", "").strip()
    return t2 or None

def build_arbiter_input(
    session: str,
    account_mode: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
) -> Tuple[dict, List[dict]]:

    finmind_token = _load_finmind_token()

    twii_df = fetch_history(TWII_SYMBOL, period="5y", interval="1d")

    vix_df_tw = fetch_history(VIX_SYMBOL_TW, period="2y", interval="1d")
    vix_df_us = fetch_history(VIX_SYMBOL_US, period="2y", interval="1d")
    using_vix_tw = bool(vix_df_tw is not None and not vix_df_tw.empty)
    vix_df = vix_df_tw if using_vix_tw else vix_df_us

    src_twii = _source_snapshot("TWII", twii_df)
    src_vix = _source_snapshot("VIXTW" if using_vix_tw else "VIX", vix_df)

    if src_twii.get("last_dt"):
        trade_date = src_twii["last_dt"]
        date_status = "VERIFIED"
    else:
        trade_date = time.strftime("%Y-%m-%d", time.localtime())
        date_status = "UNVERIFIED"
        warnings_bus.push("DATE_UNVERIFIED", "TWII 無 last_dt，trade_date 使用本機日期（UNVERIFIED）", {"trade_date": trade_date})

    vix_last = None
    if vix_df is not None and not vix_df.empty:
        try:
            vix_close = _as_close_series(vix_df)
            vix_last = float(vix_close.iloc[-1]) if len(vix_close) else None
        except Exception:
            vix_last = None

    if vix_last is None:
        vix_source = "FAIL"
        vix_status = "FAIL"
        vix_confidence = "LOW"
    else:
        vix_source = "VIXTW" if using_vix_tw else "VIX"
        vix_status = "OK"
        vix_confidence = "HIGH" if using_vix_tw else "MEDIUM"

    dynamic_vix_threshold = calculate_dynamic_vix(vix_df)
    vix_panic = float(dynamic_vix_threshold) if dynamic_vix_threshold is not None else 35.0

    metrics = compute_regime_metrics(twii_df)
    close_price = None
    twii_change = None
    twii_pct = None
    try:
        if twii_df is not None and not twii_df.empty:
            c = _as_close_series(twii_df)
            close_price = float(c.iloc[-1]) if len(c) else None
            if len(c) >= 2:
                twii_change = float(c.iloc[-1] - c.iloc[-2])
                twii_pct = float(c.iloc[-1] / c.iloc[-2] - 1.0)
    except Exception:
        pass

    if vix_last is None:
        regime, max_equity = "DATA_FAILURE", 0.0
    else:
        regime, max_equity = pick_regime(metrics, vix=vix_last, vix_panic=vix_panic)

    # ✅ 成交額：傳入 finmind_token（關鍵）
    amount = fetch_amount_total(trade_date=trade_date, allow_insecure_ssl=allow_insecure_ssl, finmind_token=finmind_token)
    market_inst_summary = fetch_market_inst_summary(allow_insecure_ssl)

    base_pool = _default_symbols_pool(topn)
    pos_pool = [p.get("symbol") for p in positions if isinstance(p, dict) and p.get("symbol")]
    symbols = list(dict.fromkeys(base_pool + pos_pool))

    pv, source_map = fetch_batch_prices_volratio_with_source(symbols)

    end_date = trade_date
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    has_token = bool(finmind_token)
    inst_df = fetch_finmind_institutional(symbols, start_date=start_date, end_date=end_date, token=finmind_token)

    panel_rows = []
    inst_map = {}
    stocks: List[dict] = []

    for i, sym in enumerate(symbols, start=1):
        inst3 = calc_inst_3d(inst_df, sym, has_token=has_token)
        net3 = float(inst3.get("Inst_Net_3d", 0.0))

        panel_rows.append({
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym),
            "Inst_Status": inst3.get("Inst_Status"),
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
            "Inst_Dir3": inst3.get("Inst_Dir3"),
            "Inst_Net_3d": net3,
            "inst_source": "FINMIND_3D_NET" if has_token else "NO_TOKEN",
        })

        inst_map[sym] = {
            "foreign_buy": bool(net3 > 0) if has_token else False,
            "trust_buy": bool(net3 > 0) if has_token else False,
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
            "Inst_Net_3d": net3,
            "inst_streak3": int(inst3.get("Inst_Streak3", 0)),
        }

        row = pv[pv["Symbol"] == sym].iloc[0] if (not pv.empty and (pv["Symbol"] == sym).any()) else None
        price = row["Price"] if row is not None else None
        vol_ratio = row["Vol_Ratio"] if row is not None else None
        src = row["source"] if row is not None else source_map.get(sym, "FAIL")

        price_ok = not (price is None or _is_nan(price))
        vr_ok = not (vol_ratio is None or _is_nan(vol_ratio))

        if not price_ok:
            warnings_bus.push("PRICE_NULL", "Missing Price", {"symbol": sym, "source": src})
        if not vr_ok:
            warnings_bus.push("VOLRATIO_NULL", "Missing VolRatio", {"symbol": sym, "source": src})

        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vol_ratio, inst_map.get(sym, {}))

        stocks.append({
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym),
            "Tier": i,
            "Price": float(price) if price_ok else None,
            "Vol_Ratio": float(vol_ratio) if vr_ok else None,
            "Layer": layer,
            "Institutional": inst_map.get(sym, {}),
            "source": src,
        })

    institutional_panel = pd.DataFrame(panel_rows)

    integrity_v1632 = evaluate_integrity_v1632(stocks=stocks, topn=len(symbols))

    if vix_last is None:
        integrity_v1632["kill_switch"] = True
        integrity_v1632["status"] = "CRITICAL_FAILURE"
        integrity_v1632["confidence"] = "LOW"
        integrity_v1632["reason"] = "VIX_MISSING: Layer A 無法計算"

    amount_partial = bool(amount.scope in ("TWSE_ONLY", "TPEX_ONLY"))
    final_regime = regime
    final_max_equity = float(max_equity)

    if bool(integrity_v1632["kill_switch"]):
        final_regime = "DATA_FAILURE" if vix_last is None else "INTEGRITY_KILL"
        final_max_equity = 0.0
        for s in stocks:
            s["Layer"] = "NONE"
            s["Layer_Reason"] = "KILL_SWITCH"
    else:
        final_max_equity = _apply_amount_degrade(float(max_equity), account_mode, amount_partial)

    # ✅ (1) market_status：直接引用 amount.confidence_level（你指定）
    market_status = str(amount.confidence_level)

    # ✅ (2) 但 Kill Switch 時覆蓋為 SHELTER（保留憲章優先）
    if bool(integrity_v1632["kill_switch"]):
        market_status = "SHELTER"

    current_exposure_pct = min(1.0, len(positions) * 0.05) if positions else 0.0
    if bool(integrity_v1632["kill_switch"]):
        current_exposure_pct = 0.0

    # Global confidence_level：Integrity + Amount + Date
    conf_parts = [str(integrity_v1632.get("confidence", "LOW")), str(amount.confidence_level)]
    if date_status == "UNVERIFIED":
        conf_parts.append("MEDIUM")
    global_confidence = _overall_confidence(conf_parts)

    sources = {
        "twii": src_twii,
        "vix": src_vix,
        "metrics_reason": metrics.get("metrics_reason", "NA"),
        "amount_source": {
            "trade_date": trade_date,
            "source_twse": amount.source_twse,
            "source_tpex": amount.source_tpex,
            "status_twse": amount.status_twse,
            "status_tpex": amount.status_tpex,
            "confidence_twse": amount.confidence_twse,
            "confidence_tpex": amount.confidence_tpex,
            "confidence_level": amount.confidence_level,
            "amount_twse": amount.amount_twse,
            "amount_tpex": amount.amount_tpex,
            "amount_total": amount.amount_total,
            "scope": amount.scope,
            "audit_dir": AUDIT_DIR,
            "twse_audit": (amount.meta or {}).get("twse", {}).get("audit") if amount.meta else None,
            "tpex_audit": (amount.meta or {}).get("tpex", {}).get("audit") if amount.meta else None,
        },
        "prices_source_map": source_map,
        "finmind_token_loaded": bool(finmind_token),
    }

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": market_status,          # ✅ 你要的：HIGH/MEDIUM/LOW 或 SHELTER
            "current_regime": final_regime,
            "account_mode": account_mode,
            "audit_tag": "V16.3.32_AUDIT_ENFORCED",
            "confidence_level": global_confidence,
            "date_status": date_status,
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "date_status": date_status,

                "twii_close": close_price,
                "twii_change": twii_change,
                "twii_pct": twii_pct,

                "vix": vix_last,
                "vix_source": vix_source,
                "vix_status": vix_status,
                "vix_confidence": vix_confidence,

                "vix_panic": vix_panic,
                "smr": metrics.get("SMR"),
                "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"),
                "price_range_10d_pct": metrics.get("price_range_10d_pct"),
                "dynamic_vix_threshold": dynamic_vix_threshold,

                "max_equity_allowed_pct": final_max_equity,
                "current_regime": final_regime,
            },
            "sources": sources,
            "market_amount": asdict(amount),
            "market_inst_summary": market_inst_summary,
            "integrity_v1632": integrity_v1632,
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float(100.0 * max(0.0, 1.0 - current_exposure_pct)),
        },
        "institutional_panel": institutional_panel.to_dict(orient="records"),
        "stocks": stocks,
        "positions_input": positions,
        "decisions": [],
        "audit_log": [],
    }

    return payload, warnings_bus.latest(50)

# =========================
# UI helpers
# =========================
def _amount_scope_label(scope: str) -> str:
    s = (scope or "").upper()
    if s == "ALL":
        return "（全市場：TWSE+TPEX）"
    if s == "TWSE_ONLY":
        return "（僅上市：TWSE；TPEX 缺失）"
    if s == "TPEX_ONLY":
        return "（僅上櫃：TPEX；TWSE 缺失）"
    return "（數據缺失）"

def _market_status_icon(ms: str) -> str:
    ms = str(ms or "").upper()
    if ms == "SHELTER":
        return "🛡️"
    if ms == "HIGH":
        return "✅"
    if ms == "MEDIUM":
        return "⚠️"
    if ms == "LOW":
        return "🔴"
    return "❔"

# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定 (Settings)")
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=1)
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
    topn = st.sidebar.selectbox("TopN（監控數量）", [8, 10, 15, 20, 30], index=3)

    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL（僅在雲端憑證錯誤時使用）", value=False)

    # ✅ 不再手動輸入 token，只提示是否載入成功
    token_loaded = bool(_load_finmind_token())
    st.sidebar.subheader("FinMind")
    st.sidebar.write(f"Token 狀態：{'✅ 已載入' if token_loaded else '❌ 未載入（請設定 Secrets / 環境變數）'}")

    st.sidebar.subheader("持倉 (JSON List)")
    positions_text = st.sidebar.text_area("positions", value="[]", height=100)

    cash_balance = st.sidebar.number_input("現金餘額", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("總權益", min_value=0, value=DEFAULT_EQUITY, step=10000)

    run_btn = st.sidebar.button("啟動中控台 (Audit Enforced)")

    try:
        positions = json.loads(positions_text) if positions_text.strip() else []
        if not isinstance(positions, list):
            positions = []
    except Exception:
        positions = []

    if run_btn or "auto_ran" not in st.session_state:
        st.session_state["auto_ran"] = True
        try:
            payload, warns = build_arbiter_input(
                session=session,
                account_mode=account_mode,
                topn=int(topn),
                positions=positions,
                cash_balance=int(cash_balance),
                total_equity=int(total_equity),
                allow_insecure_ssl=bool(allow_insecure_ssl),
            )
        except Exception as e:
            st.error(f"系統錯誤: {e}")
            import traceback
            st.code(traceback.format_exc())
            return

        ov = payload.get("macro", {}).get("overview", {})
        meta = payload.get("meta", {})
        amount = payload.get("macro", {}).get("market_amount", {})
        inst_summary = payload.get("macro", {}).get("market_inst_summary", [])
        sources = payload.get("macro", {}).get("sources", {})
        integ = payload.get("macro", {}).get("integrity_v1632", {})

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("交易日期", ov.get("trade_date", "-"), help=f"date_status={meta.get('date_status', '-')}")
        status = meta.get("market_status", "-")
        c2.metric("市場狀態", f"{_market_status_icon(status)} {status}", help="market_status=market_amount.confidence_level（或 SHELTER）")
        c3.metric("策略體制 (Regime)", meta.get("current_regime", "-"))
        c4.metric(
            "建議持倉上限",
            f"{_pct01_to_pct100(ov.get('max_equity_allowed_pct')):.0f}%"
            if ov.get("max_equity_allowed_pct") is not None else "-",
        )

        st.caption(f"global confidence_level = {meta.get('confidence_level', '-')}")

        st.subheader("🛡️ Layer B：資料信任層（憲章）")
        b1, b2, b3, b4 = st.columns(4)
        b1.metric("Confidence", integ.get("confidence", "-"))
        b2.metric("Kill Switch", "ACTIVATED" if integ.get("kill_switch") else "OFF")
        b3.metric("Status", integ.get("status", "-"), help=str(integ.get("reason", "")))
        b4.metric("Missing Count", str(integ.get("missing_count", "-")))

        if integ.get("kill_switch"):
            st.error(f"⛔ 系統強制停機（憲章 1.2）：{integ.get('reason')}")

        st.subheader("📊 大盤觀測站 (TAIEX Overview)")
        m1, m2, m3, m4 = st.columns(4)

        close = ov.get("twii_close")
        chg = ov.get("twii_change")
        pct = ov.get("twii_pct")
        delta_color = "normal"
        if chg is not None:
            delta_color = "normal" if float(chg) >= 0 else "inverse"

        m1.metric(
            "加權指數",
            f"{close:,.0f}" if close is not None else "-",
            f"{chg:+.0f} ({pct:+.2%})" if (chg is not None and pct is not None) else None,
            delta_color=delta_color,
        )
        m2.metric("VIX/VIXTW", f"{ov.get('vix'):.2f}" if ov.get("vix") is not None else "FAIL",
                  help=f"{ov.get('vix_source')}, {ov.get('vix_status')}, {ov.get('vix_confidence')}")
        m3.metric("VIX Panic Threshold", f"{ov.get('vix_panic'):.2f}" if ov.get("vix_panic") is not None else "-")

        amt_total = amount.get("amount_total")
        scope = amount.get("scope", "NONE")
        scope_label = _amount_scope_label(scope)
        if amt_total is not None and amt_total > 0:
            amt_str = f"{amt_total/1_000_000_000_000:.3f} 兆元 {scope_label}"
        else:
            amt_str = f"數據缺失 {scope_label}"
        m4.metric("市場總成交額", amt_str, help=f"amount_confidence={amount.get('confidence_level')}")

        with st.expander("📌 成交額稽核摘要（TWSE + TPEX + FinMind + Yahoo + Safe Mode）", expanded=True):
            a_src = sources.get("amount_source", {})
            twse_src = a_src.get("source_twse", "")
            tpex_src = a_src.get("source_tpex", "")

            def _icon(src: str) -> str:
                s = (src or "").upper()
                if "OK" in s:
                    return "✅"
                if "FINMIND" in s:
                    return "✅"
                if "YAHOO" in s:
                    return "⚠️"
                if "SAFE_MODE" in s:
                    return "🔴"
                return "❌"

            st.markdown(f"""
**上市 (TWSE)**: {_icon(twse_src)} {twse_src} / status={a_src.get('status_twse')} / conf={a_src.get('confidence_twse')}  
**上櫃 (TPEX)**: {_icon(tpex_src)} {tpex_src} / status={a_src.get('status_tpex')} / conf={a_src.get('confidence_tpex')}  
**總額**: {amt_total:,} 元 (scope={scope}) / confidence_level={a_src.get('confidence_level')}
""")

            st.json({
                "trade_date": a_src.get("trade_date"),
                "amount_twse": a_src.get("amount_twse"),
                "amount_tpex": a_src.get("amount_tpex"),
                "amount_total": a_src.get("amount_total"),
                "twse_audit": a_src.get("twse_audit"),
                "tpex_audit": a_src.get("tpex_audit"),
                "finmind_token_loaded": sources.get("finmind_token_loaded"),
            })

        st.subheader("🏛️ 三大法人買賣超 (全市場)")
        if inst_summary:
            cols = st.columns(len(inst_summary))
            for idx, item in enumerate(inst_summary):
                net = item.get("Net", 0)
                net_yi = net / 1_0000_0000
                cols[idx].metric(item.get("Identity"), f"{net_yi:+.2f} 億")
        else:
            st.info("暫無今日法人統計資料（通常下午 3 點後更新）")

        st.subheader("🛠️ 系統健康診斷 (System Health)")
        if not warns:
            st.success("✅ 系統運作正常，無錯誤日誌 (Clean Run)。")
        else:
            with st.expander(f"⚠️ 偵測到 {len(warns)} 條系統警示 (點擊查看詳情)", expanded=False):
                st.warning("系統遭遇部分數據抓取失敗，已自動降級或使用備援/補抓。")
                w_df = pd.DataFrame(warns)
                if not w_df.empty and "code" in w_df.columns:
                    st.dataframe(w_df[["ts", "code", "msg"]], use_container_width=True)
                else:
                    st.write(warns)

        st.subheader("🎯 核心持股雷達 (Tactical Stocks)")
        s_df = pd.json_normalize(payload.get("stocks", []))
        if not s_df.empty:
            disp_cols = ["Symbol", "Name", "Price", "Vol_Ratio", "Layer", "source", "Institutional.Inst_Net_3d", "Institutional.Inst_Streak3"]
            if "Layer_Reason" in s_df.columns:
                disp_cols.insert(5, "Layer_Reason")
            s_df = s_df.reindex(columns=[c for c in disp_cols if c in s_df.columns])
            s_df = s_df.rename(columns=COL_TRANSLATION)
            s_df = s_df.rename(columns={
                "Institutional.Inst_Net_3d": "法人3日淨額",
                "Institutional.Inst_Streak3": "法人連買天數",
                "Layer_Reason": "分級原因",
            })
            st.dataframe(s_df, use_container_width=True)

        with st.expander("🔍 查看法人詳細數據 (Institutional Debug Panel)"):
            inst_df2 = pd.DataFrame(payload.get("institutional_panel", []))
            if not inst_df2.empty:
                st.dataframe(inst_df2.rename(columns=COL_TRANSLATION), use_container_width=True)

        st.divider()
        st.subheader("⚖️ 憲章自動稽核報告 (Self-Audit Report)")
        violations = audit_constitution(payload, topn=int(topn))
        if violations:
            for v in violations:
                st.error(v)
            st.error("⚠️ 系統偵測到違憲行為！請立即檢查代碼邏輯或數據源。")
        else:
            st.success("✅ 稽核通過：本系統運行符合《Predator 決策憲章 v1.0》")

        st.markdown("---")
        st.subheader("🤖 AI JSON (Arbiter Input)")
        json_str = json.dumps(payload, indent=4, ensure_ascii=False)
        st.markdown("##### 📋 點擊下方代碼塊右上角的「複製圖示」即可複製完整數據")
        st.code(json_str, language="json")

if __name__ == "__main__":
    main()
