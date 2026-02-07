# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.5）
# 終極修復版 - 可貼版（單檔 main.py）
# 修復重點：
#   (1) SSL 錯誤自動切換 verify=False
#   (2) TWSE/TPEX 雙路 Yahoo Fallback
#   (3) Safe Mode 固定值（最後防線）
#   (4) 強制成功模式（保證有數據）
#   (5) 補齊 amount_source.trade_date，避免 UI 顯示空值
# =========================================================

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

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
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3.5）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.5 終極修復版）"
st.title(APP_TITLE)


# =========================
# Constants / helpers
# =========================
EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

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
    "foreign_buy": "外資買超",
    "trust_buy": "投信買超",
}


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
# Global Session
# =========================
_GLOBAL_SESSION = None


def _get_global_session() -> requests.Session:
    global _GLOBAL_SESSION
    if _GLOBAL_SESSION is None:
        _GLOBAL_SESSION = requests.Session()
        _GLOBAL_SESSION.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "application/json,text/plain,text/html,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        })
    return _GLOBAL_SESSION


def _http_session() -> requests.Session:
    return _get_global_session()


# =========================
# Market amount (TWSE/TPEX) - 終極修復版
# =========================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
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


def _audit_save_csv(audit_dir: str, fname: str, df: pd.DataFrame) -> None:
    _ensure_dir(audit_dir)
    df.to_csv(os.path.join(audit_dir, fname), index=False, encoding="utf-8-sig")


def _yahoo_estimate_twse() -> Tuple[int, str]:
    """Yahoo Finance 估算上市成交額"""
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
    """Yahoo Finance 估算上櫃成交額"""
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
    """TWSE 抓取 + SSL 自動修復"""
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
                amt_idx = 3  # 預設第 4 欄

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
    """TPEX 抓取 + 多重 Fallback"""
    session = _http_session()
    roc_formats = [
        ("standard", _to_roc_date(trade_date, "standard")),
        ("compact", _to_roc_date(trade_date, "compact")),
    ]
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

    session.headers.update({
        "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php?l=zh-tw"
    })

    meta = {"url": url, "attempts": []}

    # PRIME
    try:
        session.get(
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php?l=zh-tw",
            timeout=10, verify=False
        )
        time.sleep(0.3)
    except Exception:
        pass

    for fmt_name, roc in roc_formats:
        for se_param in ["EW", "AL"]:
            params = {"l": "zh-tw", "d": roc, "se": se_param}
            attempt_id = f"{fmt_name}_{se_param}"

            try:
                r = session.get(url, params=params, timeout=15, verify=False, allow_redirects=True)

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
                    return int(total), f"TPEX_OK:{attempt_id}", meta
                meta["attempts"].append({"id": attempt_id, "result": f"total_too_low_{total}"})

            except Exception as e:
                meta["attempts"].append({"id": attempt_id, "error": str(e)})
                continue

    warnings_bus.push("TPEX_ALL_FAIL", "所有方法失敗，使用 Yahoo 估算", {})
    amt, src = _yahoo_estimate_tpex()
    meta["fallback"] = "yahoo"
    return amt, src, meta


def _amount_scope(twse_amt: Optional[int], tpex_amt: Optional[int]) -> str:
    if twse_amt and tpex_amt:
        return "ALL"
    if twse_amt:
        return "TWSE_ONLY"
    if tpex_amt:
        return "TPEX_ONLY"
    return "NONE"


def fetch_amount_total(trade_date: str, allow_insecure_ssl: bool = False) -> MarketAmount:
    """終極修復版：確保一定有數據"""
    _ensure_dir(AUDIT_DIR)

    twse_amt, twse_src, twse_meta = _twse_audit_sum_by_stock_day_all(trade_date, allow_insecure_ssl)
    tpex_amt, tpex_src, tpex_meta = _tpex_audit_sum_by_st43(trade_date, allow_insecure_ssl)

    if not twse_amt or twse_amt <= 0:
        twse_amt, twse_src = _yahoo_estimate_twse()
        twse_meta = {"fallback": "yahoo_forced"}

    if not tpex_amt or tpex_amt <= 0:
        tpex_amt, tpex_src = _yahoo_estimate_tpex()
        tpex_meta = {"fallback": "yahoo_forced"}

    total = int(twse_amt) + int(tpex_amt)
    scope = _amount_scope(twse_amt, tpex_amt)

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
        allow_insecure_ssl=bool(allow_insecure_ssl),
        scope=scope,
        meta=meta,
    )


# =========================
# Market institutions (TWSE BFI82U)
# =========================
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
# FinMind helpers
# =========================
def _finmind_headers(token: Optional[str]) -> dict:
    return {"Authorization": f"Bearer {token}"} if token else {}


def _finmind_get(dataset: str, params: dict, token: Optional[str]) -> dict:
    p = {"dataset": dataset, **params}
    r = requests.get(FINMIND_URL, headers=_finmind_headers(token), params=p, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize_inst_direction(net: float) -> str:
    net = float(net or 0.0)
    if abs(net) < NEUTRAL_THRESHOLD:
        return "NEUTRAL"
    return "POSITIVE" if net > 0 else "NEGATIVE"


def fetch_finmind_institutional(
    symbols: List[str],
    start_date: str,
    end_date: str,
    token: Optional[str] = None,
) -> pd.DataFrame:
    rows = []
    for sym in symbols:
        stock_id = sym.replace(".TW", "").strip()
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


def calc_inst_3d(inst_df: pd.DataFrame, symbol: str) -> dict:
    if inst_df is None or inst_df.empty:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df = inst_df[inst_df["symbol"] == symbol].copy()
    if df.empty:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df = df.sort_values("date").tail(3)
    if len(df) < 3:
        return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Dir3": "PENDING", "Inst_Net_3d": 0.0}

    df["net_amount"] = pd.to_numeric(df["net_amount"], errors="coerce").fillna(0)
    dirs = [normalize_inst_direction(x) for x in df["net_amount"]]
    net_sum = float(df["net_amount"].sum())

    if all(d == "POSITIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "POSITIVE", "Inst_Net_3d": net_sum}
    if all(d == "NEGATIVE" for d in dirs):
        return {"Inst_Status": "READY", "Inst_Streak3": 3, "Inst_Dir3": "NEGATIVE", "Inst_Net_3d": net_sum}

    return {"Inst_Status": "READY", "Inst_Streak3": 0, "Inst_Dir3": "NEUTRAL", "Inst_Net_3d": net_sum}


# =========================
# yfinance fetchers + 欄位修正
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


@st.cache_data(ttl=60 * 10, show_spinner=False)
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


def _single_fetch_price_volratio(sym: str) -> Tuple[Optional[float], Optional[float]]:
    try:
        df = yf.download(sym, period="6mo", interval="1d", auto_adjust=False, progress=False, group_by="column", threads=False)
        if df is None or df.empty:
            return None, None
        close = df["Close"].dropna() if "Close" in df.columns else pd.Series(dtype=float)
        vol = df["Volume"].dropna() if "Volume" in df.columns else pd.Series(dtype=float)

        price = float(close.iloc[-1]) if len(close) else None
        vol_ratio = None
        if len(vol) >= 20:
            ma20 = float(vol.rolling(20).mean().iloc[-1])
            if ma20 and ma20 > 0:
                vol_ratio = float(vol.iloc[-1] / ma20)
        return price, vol_ratio
    except Exception:
        return None, None


@st.cache_data(ttl=60 * 5, show_spinner=False)
def fetch_batch_prices_volratio(symbols: List[str]) -> pd.DataFrame:
    out = pd.DataFrame({"Symbol": symbols})
    out["Price"] = None
    out["Vol_Ratio"] = None
    out["source"] = "NONE"
    if not symbols:
        return out

    try:
        df = yf.download(symbols, period="6mo", interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=False)
    except Exception as e:
        warnings_bus.push("YF_BATCH_FAIL", str(e), {"n": len(symbols)})
        return out

    if df is None or df.empty:
        return out

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

            out.loc[out["Symbol"] == sym, "Price"] = price
            out.loc[out["Symbol"] == sym, "Vol_Ratio"] = vol_ratio
            out.loc[out["Symbol"] == sym, "source"] = "YF_BATCH"
        except Exception:
            continue

    need_fix = out[(out["Price"].isna()) | (out["Vol_Ratio"].isna())]["Symbol"].tolist()
    if need_fix:
        for sym in need_fix:
            p, vr = _single_fetch_price_volratio(sym)
            if p is not None and (out.loc[out["Symbol"] == sym, "Price"].isna().iloc[0]):
                out.loc[out["Symbol"] == sym, "Price"] = float(p)
            if vr is not None and (out.loc[out["Symbol"] == sym, "Vol_Ratio"].isna().iloc[0]):
                out.loc[out["Symbol"] == sym, "Vol_Ratio"] = float(vr)
            if p is not None or vr is not None:
                out.loc[out["Symbol"] == sym, "source"] = "YF_SINGLE"

    return out


# =========================
# Regime & Metrics
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


def _calc_ma14_monthly_from_daily(df_daily: pd.DataFrame) -> Optional[float]:
    try:
        if df_daily is None or df_daily.empty:
            return None
        df = df_daily.copy()
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.set_index("Datetime")
        close = _as_close_series(df)
        monthly = close.resample("M").last().dropna()
        if len(monthly) < 14:
            return None
        ma14 = monthly.rolling(14).mean().dropna()
        return float(ma14.iloc[-1])
    except Exception:
        return None


def _extract_close_price(df_daily: pd.DataFrame) -> Optional[float]:
    try:
        if df_daily is None or df_daily.empty:
            return None
        close = _as_close_series(df_daily)
        return float(close.iloc[-1]) if len(close) else None
    except Exception:
        return None


def _count_close_below_ma_days(df_daily: pd.DataFrame, ma14_monthly: Optional[float]) -> int:
    try:
        if ma14_monthly is None or df_daily is None or df_daily.empty:
            return 0
        close = _as_close_series(df_daily)
        if len(close) < 2:
            return 0
        thresh = float(ma14_monthly) * 0.96
        recent = close.iloc[-5:].tolist()
        cnt = 0
        for v in reversed(recent):
            if float(v) < thresh:
                cnt += 1
            else:
                break
        return int(cnt)
    except Exception:
        return 0


def pick_regime(
    metrics: dict,
    vix: Optional[float] = None,
    ma14_monthly: Optional[float] = None,
    close_price: Optional[float] = None,
    close_below_ma_days: int = 0,
    vix_panic: float = 35.0,
    **kwargs,
) -> Tuple[str, float]:

    if "vixpanic" in kwargs and kwargs["vixpanic"]:
        vix_panic = float(kwargs["vixpanic"])
    if "vipxanic" in kwargs and kwargs["vipxanic"]:
        vix_panic = float(kwargs["vipxanic"])

    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    drawdown = metrics.get("drawdown_pct")
    price_range = metrics.get("price_range_10d_pct")

    if (vix is not None and float(vix) > float(vix_panic)) or (drawdown is not None and float(drawdown) <= -0.18):
        return "CRASH_RISK", 0.10

    if ma14_monthly and close_price and int(close_below_ma_days) >= 2 and float(close_price) < float(ma14_monthly) * 0.96:
        return "HIBERNATION", 0.20

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


def compute_integrity_and_kill(stocks: List[dict], amount: MarketAmount, metrics: dict) -> dict:
    n = len(stocks)
    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)

    amount_total_null = (amount.amount_total is None or amount.amount_total <= 0)
    amount_partial = bool(amount.scope in ("TWSE_ONLY", "TPEX_ONLY"))
    amount_scope = str(amount.scope)

    denom = max(1, (2 * n + 1))
    core_missing = price_null + volratio_null + (1 if amount_total_null else 0)
    core_missing_pct = float(core_missing / denom)

    gap_down = metrics.get("gap_down")
    is_gap_crash = bool(gap_down is not None and gap_down <= -0.07)

    kill = False
    reasons = []

    if n > 0 and price_null == n:
        kill = True
        reasons.append(f"price_null={price_null}/{n}")
    if n > 0 and volratio_null == n:
        kill = True
        reasons.append(f"volratio_null={volratio_null}/{n}")
    if amount_total_null:
        reasons.append("amount_total_null=True")
    if core_missing_pct >= 0.50:
        kill = True
        reasons.append(f"core_missing_pct={core_missing_pct:.2f}")
    if is_gap_crash:
        kill = True
        reasons.append(f"GAP_DOWN_CRASH({gap_down:.1%})")

    if amount_partial:
        reasons.append(f"amount_scope={amount_scope}")

    return {
        "n": n,
        "price_null": price_null,
        "volratio_null": volratio_null,
        "core_missing_pct": core_missing_pct,
        "amount_total_null": amount_total_null,
        "amount_partial": amount_partial,
        "amount_scope": amount_scope,
        "is_gap_crash": is_gap_crash,
        "kill": bool(kill),
        "reason": ("DATA_MISSING " + ", ".join(reasons)) if reasons else "OK",
        "metrics_reason": metrics.get("metrics_reason", "NA"),
    }


def build_active_alerts(integrity: dict, amount: MarketAmount) -> List[str]:
    alerts = []
    if integrity.get("kill"):
        alerts.append("KILL_SWITCH_ACTIVATED")
    if integrity.get("is_gap_crash"):
        alerts.append("CRITICAL: 市場跳空重挫 (>7%)")

    if amount.amount_total is None or amount.amount_total <= 0:
        alerts.append("DEGRADED_AMOUNT: 成交額數據完全缺失")
    elif integrity.get("amount_partial"):
        scope = integrity.get("amount_scope", "PARTIAL")
        if scope == "TWSE_ONLY":
            alerts.append("DEGRADED_AMOUNT_PARTIAL: 目前僅有『上市(TWSE)』成交額；上櫃(TPEX)缺失")
        elif scope == "TPEX_ONLY":
            alerts.append("DEGRADED_AMOUNT_PARTIAL: 目前僅有『上櫃(TPEX)』成交額；上市(TWSE)缺失")
        else:
            alerts.append(f"DEGRADED_AMOUNT_PARTIAL: 成交額涵蓋不足（scope={scope}）")

    n = int(integrity.get("n") or 0)
    if n > 0 and int(integrity.get("price_null") or 0) == n:
        alerts.append("CRITICAL: 所有個股價格=null")
    if n > 0 and int(integrity.get("volratio_null") or 0) == n:
        alerts.append("CRITICAL: 所有個股量能=null")
    cm = float(integrity.get("core_missing_pct") or 0.0)
    if cm >= 0.50:
        alerts.append(f"DATA_INTEGRITY_FAILURE: 缺失率={cm:.2f}")
    if integrity.get("kill"):
        alerts.append("FORCED_ALL_CASH: 強制避險模式")

    if "YAHOO" in amount.source_twse or "SAFE_MODE" in amount.source_twse:
        alerts.append(f"TWSE 使用估算值: {amount.source_twse}")
    if "YAHOO" in amount.source_tpex or "SAFE_MODE" in amount.source_tpex:
        alerts.append(f"TPEX 使用估算值: {amount.source_tpex}")

    return alerts


def _apply_amount_degrade(max_equity: float, account_mode: str, amount_partial: bool) -> float:
    if not amount_partial:
        return max_equity
    factor = float(DEGRADE_FACTOR_BY_MODE.get(account_mode, 0.75))
    return float(max_equity) * factor


# =========================
# Arbiter input builder
# =========================
def _default_symbols_pool(topn: int) -> List[str]:
    pool = list(STOCK_NAME_MAP.keys())
    limit = min(len(pool), max(1, int(topn)))
    return pool[:limit]


def _source_snapshot(name: str, df: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"name": name, "ok": False, "rows": 0, "cols": [], "last_dt": None, "reason": "EMPTY"}
    cols = list(map(str, df.columns.tolist()))
    last_dt = None
    try:
        if "Datetime" in df.columns:
            last_dt = pd.to_datetime(df["Datetime"].dropna().iloc[-1]).strftime("%Y-%m-%d")
    except Exception:
        last_dt = None
    return {"name": name, "ok": True, "rows": int(len(df)), "cols": cols, "last_dt": last_dt, "reason": "OK"}


def build_arbiter_input(
    session: str,
    account_mode: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
    finmind_token: Optional[str],
) -> Tuple[dict, List[dict]]:

    twii_df = fetch_history(TWII_SYMBOL, period="5y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")

    src_twii = _source_snapshot("TWII", twii_df, TWII_SYMBOL)
    src_vix = _source_snapshot("VIX", vix_df, VIX_SYMBOL)

    trade_date = src_twii.get("last_dt") or time.strftime("%Y-%m-%d", time.localtime())

    vix_last = None
    if vix_df is not None and not vix_df.empty:
        try:
            vix_close = _as_close_series(vix_df)
            vix_last = float(vix_close.iloc[-1]) if len(vix_close) else None
        except Exception:
            vix_last = None

    dynamic_vix_threshold = calculate_dynamic_vix(vix_df)

    metrics = compute_regime_metrics(twii_df)
    close_price = _extract_close_price(twii_df)
    ma14_monthly = _calc_ma14_monthly_from_daily(twii_df)
    close_below_days = _count_close_below_ma_days(twii_df, ma14_monthly)

    twii_change = None
    twii_pct = None
    if twii_df is not None and not twii_df.empty:
        try:
            c = _as_close_series(twii_df)
            if len(c) >= 2:
                twii_change = float(c.iloc[-1] - c.iloc[-2])
                twii_pct = float(c.iloc[-1] / c.iloc[-2] - 1.0)
        except Exception:
            pass

    regime, max_equity = pick_regime(
        metrics,
        vix=vix_last,
        ma14_monthly=ma14_monthly,
        close_price=close_price,
        close_below_ma_days=close_below_days,
    )

    amount = fetch_amount_total(trade_date=trade_date, allow_insecure_ssl=allow_insecure_ssl)
    market_inst_summary = fetch_market_inst_summary(allow_insecure_ssl)

    base_pool = _default_symbols_pool(topn)
    pos_pool = [p.get("symbol") for p in positions if isinstance(p, dict) and p.get("symbol")]
    symbols = list(dict.fromkeys(base_pool + pos_pool))

    pv = fetch_batch_prices_volratio(symbols)

    end_date = trade_date
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    inst_df = fetch_finmind_institutional(symbols, start_date=start_date, end_date=end_date, token=finmind_token)

    panel_rows = []
    inst_map = {}
    stocks = []

    for i, sym in enumerate(symbols, start=1):
        inst3 = calc_inst_3d(inst_df, sym)
        net3 = float(inst3.get("Inst_Net_3d", 0.0))

        p_row = {
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym),
            "Foreign_Net": net3,
            "Trust_Net": net3,
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
            "Inst_Status": inst3.get("Inst_Status", "PENDING"),
            "Inst_Dir3": inst3.get("Inst_Dir3", "PENDING"),
            "Inst_Net_3d": net3,
            "inst_source": "FINMIND_3D_NET",
        }
        panel_rows.append(p_row)

        inst_map[sym] = {
            "foreign_buy": bool(net3 > 0),
            "trust_buy": bool(net3 > 0),
            "Inst_Streak3": int(inst3.get("Inst_Streak3", 0)),
            "Inst_Net_3d": net3,
            "inst_streak3": int(inst3.get("Inst_Streak3", 0)),
        }

        row = pv[pv["Symbol"] == sym].iloc[0] if (not pv.empty and (pv["Symbol"] == sym).any()) else None
        price = row["Price"] if row is not None else None
        vol_ratio = row["Vol_Ratio"] if row is not None else None

        if price is None or (isinstance(price, float) and np.isnan(price)):
            warnings_bus.push("PRICE_NULL", "Missing Price", {"symbol": sym, "src": (row["source"] if row is not None else "NONE")})
        if vol_ratio is None or (isinstance(vol_ratio, float) and np.isnan(vol_ratio)):
            warnings_bus.push("VOLRATIO_NULL", "Missing VolRatio", {"symbol": sym, "src": (row["source"] if row is not None else "NONE")})

        inst_data = inst_map.get(sym, {"foreign_buy": False, "trust_buy": False, "inst_streak3": 0})
        layer = classify_layer(regime, bool(metrics.get("MOMENTUM_LOCK", False)), vol_ratio, inst_data)

        stocks.append({
            "Symbol": sym,
            "Name": STOCK_NAME_MAP.get(sym, sym),
            "Tier": i,
            "Price": None if (price is None or (isinstance(price, float) and np.isnan(price))) else float(price),
            "Vol_Ratio": None if (vol_ratio is None or (isinstance(vol_ratio, float) and np.isnan(vol_ratio))) else float(vol_ratio),
            "Layer": layer,
            "Institutional": inst_data,
        })

    institutional_panel = pd.DataFrame(panel_rows)

    integrity = compute_integrity_and_kill(stocks, amount, metrics)
    active_alerts = build_active_alerts(integrity, amount)

    final_regime = "UNKNOWN" if integrity["kill"] else regime
    final_max_equity = 0.0 if integrity["kill"] else _apply_amount_degrade(float(max_equity), account_mode, bool(integrity.get("amount_partial")))

    current_exposure_pct = min(1.0, len(positions) * 0.05) if positions else 0.0

    if integrity["kill"]:
        market_status = "SHELTER"
        current_exposure_pct = 0.0
        final_regime = "UNKNOWN"
        final_max_equity = 0.0
    else:
        market_status = "NORMAL"
        if amount.amount_total is None or amount.amount_total <= 0:
            market_status = "DEGRADED"
        elif ("YAHOO" in amount.source_twse or "YAHOO" in amount.source_tpex or "SAFE_MODE" in amount.source_twse or "SAFE_MODE" in amount.source_tpex):
            market_status = "ESTIMATED"

    sources = {
        "twii": src_twii,
        "vix": src_vix,
        "metrics_reason": metrics.get("metrics_reason", "NA"),
        "amount_source": {
            # ✅ 補齊 trade_date，避免 UI expander 顯示空值
            "trade_date": trade_date,
            "source_twse": amount.source_twse,
            "source_tpex": amount.source_tpex,
            "amount_twse": amount.amount_twse,
            "amount_tpex": amount.amount_tpex,
            "amount_total": amount.amount_total,
            "scope": amount.scope,
            "audit_dir": AUDIT_DIR,
            "twse_audit": (amount.meta or {}).get("twse", {}).get("audit") if amount.meta else None,
            "tpex_audit": (amount.meta or {}).get("tpex", {}).get("audit") if amount.meta else None,
        },
    }

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "session": session,
            "market_status": market_status,
            "current_regime": final_regime,
            "account_mode": account_mode,
            "audit_tag": "V16.3.5_ULTIMATE_FIX",
        },
        "macro": {
            "overview": {
                "trade_date": trade_date,
                "twii_close": close_price,
                "twii_change": twii_change,
                "twii_pct": twii_pct,
                "vix": vix_last,
                "smr": metrics.get("SMR"),
                "slope5": metrics.get("Slope5"),
                "drawdown_pct": metrics.get("drawdown_pct"),
                "price_range_10d_pct": metrics.get("price_range_10d_pct"),
                "dynamic_vix_threshold": dynamic_vix_threshold,
                "max_equity_allowed_pct": final_max_equity,
            },
            "sources": sources,
            "market_amount": asdict(amount),
            "market_inst_summary": market_inst_summary,
            "integrity": integrity,
        },
        "portfolio": {
            "total_equity": int(total_equity),
            "cash_balance": int(cash_balance),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float(100.0 * max(0.0, 1.0 - current_exposure_pct)),
            "active_alerts": active_alerts,
        },
        "institutional_panel": institutional_panel.to_dict(orient="records"),
        "stocks": stocks,
        "positions_input": positions,
        "decisions": [],
        "audit_log": [],
    }

    return payload, warnings_bus.latest(50)


# =========================
# UI
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


def main():
    st.sidebar.header("設定 (Settings)")
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=1)
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
    topn = st.sidebar.selectbox("TopN（監控數量）", [8, 10, 15, 20, 30], index=3)

    # 預設開啟不安全 SSL（解決憑證問題）
    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL", value=True)

    st.sidebar.subheader("FinMind")
    finmind_token = st.sidebar.text_input("FinMind Token", type="password").strip() or None

    st.sidebar.subheader("持倉 (JSON List)")
    positions_text = st.sidebar.text_area("positions", value="[]", height=100)

    cash_balance = st.sidebar.number_input("現金餘額", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("總權益", min_value=0, value=DEFAULT_EQUITY, step=10000)

    run_btn = st.sidebar.button("啟動中控台")

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
                finmind_token=finmind_token,
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

        # --- 1. 關鍵指標 ---
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("交易日期", ov.get("trade_date", "-"))

        status = meta.get("market_status", "-")
        if status == "ESTIMATED":
            c2.metric("市場狀態", f"⚠️ {status}", help="使用估算數據")
        elif status == "DEGRADED":
            c2.metric("市場狀態", f"🔴 {status}", help="數據缺失")
        elif status == "SHELTER":
            c2.metric("市場狀態", f"🛡️ {status}", help="KILL SWITCH 觸發")
        else:
            c2.metric("市場狀態", f"✅ {status}")

        c3.metric("策略體制 (Regime)", meta.get("current_regime", "-"))
        c4.metric(
            "建議持倉上限",
            f"{_pct01_to_pct100(ov.get('max_equity_allowed_pct')):.0f}%"
            if ov.get("max_equity_allowed_pct") is not None else "-",
        )

        # --- 2. 大盤與成交量 ---
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
        m2.metric("VIX 恐慌指數", f"{ov.get('vix'):.2f}" if ov.get("vix") is not None else "-")

        amt_total = amount.get("amount_total")
        scope = amount.get("scope", "NONE")
        scope_label = _amount_scope_label(scope)

        if amt_total is not None and amt_total > 0:
            amt_str = f"{amt_total/1_000_000_000_000:.3f} 兆元 {scope_label}"
        else:
            amt_str = f"數據缺失 {scope_label}"

        m3.metric("市場總成交額", amt_str)
        m4.metric("SMR 乖離率", f"{ov.get('smr'):.4f}" if ov.get("smr") is not None else "-")

        # --- 2.1 成交額稽核摘要 ---
        with st.expander("📌 成交額稽核摘要（TWSE + TPEX + Yahoo Fallback + Safe Mode）", expanded=True):
            a_src = sources.get("amount_source", {})

            twse_src = a_src.get("source_twse", "")
            tpex_src = a_src.get("source_tpex", "")

            if "OK" in twse_src:
                twse_icon = "✅"
            elif "YAHOO" in twse_src:
                twse_icon = "⚠️"
            elif "SAFE_MODE" in twse_src:
                twse_icon = "🔴"
            else:
                twse_icon = "❌"

            if "OK" in tpex_src:
                tpex_icon = "✅"
            elif "YAHOO" in tpex_src:
                tpex_icon = "⚠️"
            elif "SAFE_MODE" in tpex_src:
                tpex_icon = "🔴"
            else:
                tpex_icon = "❌"

            st.markdown(f"""
**上市 (TWSE)**: {twse_icon} {twse_src}  
**上櫃 (TPEX)**: {tpex_icon} {tpex_src}  
**總額**: {amt_total:,} 元 ({scope})

**圖例**:  
✅ 官方數據成功 | ⚠️ Yahoo 估算 | 🔴 Safe Mode 固定值 | ❌ 完全失敗
""")

            st.json({
                "trade_date": a_src.get("trade_date"),
                "amount_twse": a_src.get("amount_twse"),
                "amount_tpex": a_src.get("amount_tpex"),
                "amount_total": a_src.get("amount_total"),
                "twse_audit": a_src.get("twse_audit"),
                "tpex_audit": a_src.get("tpex_audit"),
            })

        # --- 3. 三大法人全市場買賣超 ---
        st.subheader("🏛️ 三大法人買賣超 (全市場)")
        if inst_summary:
            cols = st.columns(len(inst_summary))
            for idx, item in enumerate(inst_summary):
                net = item.get("Net", 0)
                net_yi = net / 1_0000_0000
                cols[idx].metric(item.get("Identity"), f"{net_yi:+.2f} 億")
        else:
            st.info("暫無今日法人統計資料（通常下午 3 點後更新）")

        # --- 4. 警報區 ---
        alerts = payload.get("portfolio", {}).get("active_alerts", [])
        if alerts:
            st.subheader("⚠️ 戰術警報 (Active Alerts)")
            for a in alerts:
                if "CRITICAL" in a or "KILL" in a:
                    st.error(a)
                elif "估算" in a or "ESTIMATE" in a:
                    st.warning(f"⚠️ {a}")
                else:
                    st.warning(a)

        # --- 5. 系統診斷 ---
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

        # --- 6. 個股分析 ---
        st.subheader("🎯 核心持股雷達 (Tactical Stocks)")
        s_df = pd.json_normalize(payload.get("stocks", []))
        if not s_df.empty:
            disp_cols = ["Symbol", "Name", "Price", "Vol_Ratio", "Layer", "Institutional.Inst_Net_3d", "Institutional.Inst_Streak3"]
            s_df = s_df.reindex(columns=disp_cols, fill_value=0)
            s_df = s_df.rename(columns=COL_TRANSLATION)
            s_df = s_df.rename(columns={
                "Institutional.Inst_Net_3d": "法人3日淨額",
                "Institutional.Inst_Streak3": "法人連買天數",
            })
            st.dataframe(s_df, use_container_width=True)

        # --- 7. 法人明細 ---
        with st.expander("🔍 查看法人詳細數據 (Institutional Debug Panel)"):
            inst_df2 = pd.DataFrame(payload.get("institutional_panel", []))
            if not inst_df2.empty:
                st.dataframe(inst_df2.rename(columns=COL_TRANSLATION), use_container_width=True)

        # --- 8. AI JSON 一鍵複製 ---
        st.markdown("---")
        c_copy1, _ = st.columns([0.8, 0.2])
        with c_copy1:
            st.subheader("🤖 AI JSON (Arbiter Input)")

        json_str = json.dumps(payload, indent=4, ensure_ascii=False)
        st.markdown("##### 📋 點擊下方代碼塊右上角的「複製圖示」即可複製完整數據")
        st.code(json_str, language="json")


if __name__ == "__main__":
    main()
