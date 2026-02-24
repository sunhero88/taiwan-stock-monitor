# main.py
# Predator V16.3.32 — FINAL (TWSE T86 first, Market Guard + Stale Kill Switch + Integrity Validation)
# 使用方式：streamlit run main.py

import os
import json
import math
import time
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone, date

import requests
import pandas as pd
import yfinance as yf
import streamlit as st

# =========================
# 基本設定
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

APP_TITLE = "Sunhero 的股市智能超盤（Predator V16.3.32 FINAL）"
VERSION_TAG = "V16.3.32_AUDIT_ENFORCED_FINAL_T86_TWSE"

DATA_DIR = "data"
AUDIT_DIR = os.path.join(DATA_DIR, "audit_market_amount")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

MARKET_JSON_PATH = os.path.join(DATA_DIR, "predator_market.json")
INST_CACHE_PATH = os.path.join(CACHE_DIR, "institutional_cache.json")

# 你的 Top20（可自行調整）
TOP20 = [
    ("2330", "台積電"),
    ("2317", "鴻海"),
    ("2454", "聯發科"),
    ("2308", "台達電"),
    ("2382", "廣達"),
    ("3231", "緯創"),
    ("2376", "技嘉"),
    ("3017", "奇鋐"),
    ("3324", "雙鴻"),       # 上櫃
    ("3661", "世芯-KY"),
    ("2881", "富邦金"),
    ("2882", "國泰金"),
    ("2891", "中信金"),
    ("2886", "兆豐金"),
    ("2603", "長榮"),
    ("2609", "陽明"),
    ("1605", "華新"),
    ("1513", "中興電"),
    ("1519", "華城"),
    ("2002", "中鋼"),
]

# 上櫃/上市判斷（簡化：你可自行維護名單；本版重點是 TWSE T86）
TPEX_SET = {"3324"}  # 可再加：上櫃股票代碼集合

# User-Agent（避免部分站點擋爬）
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# =========================
# 時間治理：Market Guard（00:00~15:30 執行 EOD → 回退上一交易日）
# =========================
def taipei_now() -> datetime:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz)

def apply_market_guard(session_type: str) -> Tuple[bool, date, str]:
    """
    若為 EOD 且時間在 00:00~15:30 之間，自動回退至上一個有效交易日資料。
    """
    now = taipei_now()
    guard_active = False
    eff_date = now.date()
    note = "NORMAL"

    if session_type.upper() == "EOD":
        if now.hour < 15 or (now.hour == 15 and now.minute < 30):
            guard_active = True
            eff_date = now.date() - timedelta(days=1)
            note = "MARKET_GUARD_ACTIVE"

    return guard_active, eff_date, note


# =========================
# 交易日判定：以 TWII 有資料為準（最多回退 10 天）
# =========================
def resolve_effective_trade_date(target_date: date, max_back: int = 10) -> Tuple[Optional[date], str]:
    """
    用 yfinance 的 ^TWII 作為交易日存在性的判斷基準。
    回傳 (有效交易日, 狀態)；若失敗回傳 (None, UNVERIFIED)
    """
    for i in range(max_back + 1):
        d = target_date - timedelta(days=i)
        start = (d - timedelta(days=3)).strftime("%Y-%m-%d")
        end = (d + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            df = yf.download("^TWII", start=start, end=end, progress=False, auto_adjust=False)
            if df is None or df.empty:
                continue
            df.index = pd.to_datetime(df.index).tz_localize(None)
            # 找 <= d 的最後一筆
            eligible = df[df.index.date <= d]
            if eligible.empty:
                continue
            last_dt = eligible.index[-1].date()
            return last_dt, "VERIFIED"
        except Exception:
            continue
    return None, "UNVERIFIED"


# =========================
# Market Amount：TWSE 官方（可稽核） + TPEX Safe Mode（依 ADR 放生）
# =========================
def fetch_twse_amount_stock_day_all(trade_date: date) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    TWSE: STOCK_DAY_ALL（加總成交金額）
    回傳 (amount_sum, audit_meta)
    """
    yyyymmdd = trade_date.strftime("%Y%m%d")
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": yyyymmdd}
    audit = {
        "market": "TWSE",
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "url": url,
        "params": params,
        "status_code": None,
        "rows": 0,
        "amount_sum": None,
        "error": None,
    }

    try:
        r = SESSION.get(url, params=params, timeout=15)
        audit["status_code"] = r.status_code
        if r.status_code != 200:
            audit["error"] = f"HTTP_{r.status_code}"
            return None, audit

        data = r.json()
        # data 內通常有 data 字段；每列有成交金額欄位（位置不一定完全一致）
        rows = data.get("data", []) or []
        audit["rows"] = len(rows)

        # 以「成交金額」欄位名定位（優先），失敗則採數字欄位猜測（但加防呆）
        fields = data.get("fields", []) or []
        amt_idx = None
        for i, f in enumerate(fields):
            if "成交金額" in str(f):
                amt_idx = i
                break

        amount_sum = 0
        ok_rows = 0

        for row in rows:
            try:
                if amt_idx is not None and amt_idx < len(row):
                    v = str(row[amt_idx]).replace(",", "").strip()
                    if v.isdigit():
                        amount_sum += int(v)
                        ok_rows += 1
                else:
                    # fallback：找該列中「最大且合理」的整數欄位（避免抓到成交股數/筆數）
                    candidates = []
                    for cell in row:
                        s = str(cell).replace(",", "").strip()
                        if s.isdigit():
                            candidates.append(int(s))
                    if candidates:
                        # 成交金額通常是列內最大值之一
                        v = max(candidates)
                        # 過小排除
                        if v > 1_000_000:
                            amount_sum += v
                            ok_rows += 1
            except Exception:
                continue

        audit["ok_rows"] = ok_rows
        audit["amount_sum"] = amount_sum if amount_sum > 10_000_000_000 else None  # 小於 100 億視為失敗
        return audit["amount_sum"], audit

    except Exception as e:
        audit["error"] = f"{type(e).__name__}"
        return None, audit


# =========================
# TWSE T86（三大法人）：免費公開資料
# =========================
def fetch_twse_t86_all(trade_date: date) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    TWSE T86：法人買賣超（依股票）
    https://www.twse.com.tw/rwd/zh/fund/T86?date=YYYYMMDD&selectType=ALLBUT0999&response=json
    """
    yyyymmdd = trade_date.strftime("%Y%m%d")
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    params = {"date": yyyymmdd, "selectType": "ALLBUT0999", "response": "json"}

    audit = {
        "market": "TWSE_T86",
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "url": url,
        "params": params,
        "status_code": None,
        "rows": 0,
        "error": None,
    }

    try:
        r = SESSION.get(url, params=params, timeout=20)
        audit["status_code"] = r.status_code
        if r.status_code != 200:
            audit["error"] = f"HTTP_{r.status_code}"
            return None, audit

        j = r.json()
        fields = j.get("fields", [])
        rows = j.get("data", []) or []
        audit["rows"] = len(rows)
        if not rows:
            audit["error"] = "EMPTY_DATA"
            return None, audit

        df = pd.DataFrame(rows, columns=fields)
        # 常見欄位：證券代號、證券名稱、外資及陸資(不含外資自營商)買賣超股數、投信買賣超股數、自營商(自行買賣)買賣超股數、自營商(避險)買賣超股數、三大法人買賣超股數...
        return df, audit

    except Exception as e:
        audit["error"] = f"{type(e).__name__}"
        return None, audit


def _to_int_safe(x) -> int:
    s = str(x).replace(",", "").strip()
    if s in {"", "--", "None", "nan"}:
        return 0
    try:
        return int(float(s))
    except Exception:
        return 0


def compute_twse_inst_3d_net(stock_id: str, trade_dates: List[date]) -> Tuple[int, int, int, int, Dict[str, Any]]:
    """
    回傳 (foreign_3d, trust_3d, dealer_3d, total_3d, debug)
    """
    foreign_sum = 0
    trust_sum = 0
    dealer_sum = 0
    debug = {"stock_id": stock_id, "days": []}

    for d in trade_dates:
        df, audit = fetch_twse_t86_all(d)
        day_info = {"date": d.strftime("%Y-%m-%d"), "ok": False, "audit": audit, "row_found": False}
        if df is None or df.empty:
            debug["days"].append(day_info)
            continue

        # 對齊欄位名（用 contains 避免不同中文標點）
        col_id = None
        col_foreign = None
        col_trust = None
        col_dealer_self = None
        col_dealer_hedge = None
        col_total = None

        for c in df.columns:
            if "證券代號" in c:
                col_id = c
            if "外資" in c and "買賣超" in c and "不含外資自營商" in c:
                col_foreign = c
            if c.strip() == "投信買賣超股數" or ("投信" in c and "買賣超" in c):
                col_trust = c
            if "自營商(自行買賣)" in c and "買賣超" in c:
                col_dealer_self = c
            if "自營商(避險)" in c and "買賣超" in c:
                col_dealer_hedge = c
            if ("三大法人" in c and "買賣超" in c) or (c.strip() == "三大法人買賣超股數"):
                col_total = c

        if col_id is None:
            debug["days"].append(day_info)
            continue

        row = df[df[col_id].astype(str).str.strip() == stock_id]
        if row.empty:
            debug["days"].append(day_info)
            continue

        day_info["row_found"] = True
        day_info["ok"] = True

        f = _to_int_safe(row.iloc[0][col_foreign]) if col_foreign else 0
        t = _to_int_safe(row.iloc[0][col_trust]) if col_trust else 0
        ds = _to_int_safe(row.iloc[0][col_dealer_self]) if col_dealer_self else 0
        dh = _to_int_safe(row.iloc[0][col_dealer_hedge]) if col_dealer_hedge else 0
        dsum = ds + dh
        foreign_sum += f
        trust_sum += t
        dealer_sum += dsum

        debug["days"].append({**day_info, "foreign": f, "trust": t, "dealer": dsum})

    total_sum = foreign_sum + trust_sum + dealer_sum
    return foreign_sum, trust_sum, dealer_sum, total_sum, debug


# =========================
# 籌碼緩存：避免殭屍數據（只允許同交易日）
# =========================
def load_inst_cache() -> Dict[str, Any]:
    if not os.path.exists(INST_CACHE_PATH):
        return {}
    try:
        with open(INST_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_inst_cache(payload: Dict[str, Any]) -> None:
    try:
        with open(INST_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# =========================
# Stale Data Kill Switch（過期籌碼熔斷）
# =========================
def apply_stale_inst_kill_switch(institutional_panel: List[Dict[str, Any]], stocks: List[Dict[str, Any]]) -> bool:
    stale_detected = False

    for inst in institutional_panel:
        if inst.get("Inst_Status") == "NO_UPDATE_TODAY":
            inst["Inst_Net_3d"] = 0.0
            inst["Inst_Streak3"] = 0
            inst["Inst_Dir3"] = "NO_DATA"
            stale_detected = True

    # Layer 熔斷：只要被標 stale，或 Inst_Net_3d 被歸零，就禁止加 Layer
    for s in stocks:
        inst = s.get("Institutional", {}) or {}
        if inst.get("stale", False) or (inst.get("Inst_Net_3d", 0.0) == 0.0 and inst.get("Inst_Streak3", 0) == 0):
            s["Layer"] = "NONE"

    return stale_detected


# =========================
# Integrity Validation（核心指標缺失 → DATA_FAILURE & max_equity=0）
# =========================
def integrity_check_market(twii_close, smr) -> Optional[Dict[str, Any]]:
    if twii_close is None or (isinstance(twii_close, float) and math.isnan(twii_close)):
        return {"current_regime": "DATA_FAILURE", "max_equity_allowed_pct": 0.0, "confidence_level": "LOW"}
    if smr is None or (isinstance(smr, float) and math.isnan(smr)):
        return {"current_regime": "DATA_FAILURE", "max_equity_allowed_pct": 0.0, "confidence_level": "LOW"}
    return None


def enforce_confidence_level(confidence_level: str, twii_close, smr, stale_flag: bool, t86_ok: bool) -> str:
    if twii_close is None or smr is None:
        return "LOW"
    if stale_flag:
        return "LOW"
    if not t86_ok:
        # 法人缺失不至於熔斷，但要降級，避免誤判
        return "LOW" if confidence_level != "HIGH" else "MEDIUM"
    return confidence_level


# =========================
# TWII / VIX / 指標（SMR、slope5、drawdown、10d range）
# =========================
def fetch_index_series(ticker: str, lookback_days: int = 260) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    用 yfinance 抓 index 歷史序列
    """
    end = taipei_now().date() + timedelta(days=1)
    start = end - timedelta(days=lookback_days * 2)  # buffer
    audit = {"ticker": ticker, "ok": False, "rows": 0, "last_dt": None, "reason": None}

    try:
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), progress=False, auto_adjust=False)
        if df is None or df.empty:
            audit["reason"] = "EMPTY"
            return pd.DataFrame(), audit
        df = df.dropna(subset=["Close"])
        df.index = pd.to_datetime(df.index).tz_localize(None)
        audit["ok"] = True
        audit["rows"] = int(df.shape[0])
        audit["last_dt"] = str(df.index[-1].date())
        audit["reason"] = "OK"
        return df, audit
    except Exception as e:
        audit["reason"] = f"{type(e).__name__}"
        return pd.DataFrame(), audit


def compute_metrics(twii_df: pd.DataFrame, trade_date: date) -> Dict[str, Any]:
    """
    以 trade_date 的最後一筆 Close 作為收盤價
    """
    if twii_df is None or twii_df.empty:
        return {
            "twii_close": None, "twii_change": None, "twii_pct": None,
            "smr": None, "slope5": None, "drawdown_pct": None, "price_range_10d_pct": None,
            "metrics_reason": "NO_TWII"
        }

    eligible = twii_df[twii_df.index.date <= trade_date]
    if eligible.empty:
        return {
            "twii_close": None, "twii_change": None, "twii_pct": None,
            "smr": None, "slope5": None, "drawdown_pct": None, "price_range_10d_pct": None,
            "metrics_reason": "NO_ELIGIBLE_ROW"
        }

    closes = eligible["Close"].copy()
    last_close = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else None

    twii_change = (last_close - prev_close) if prev_close is not None else None
    twii_pct = (twii_change / prev_close) if (prev_close and prev_close != 0) else None

    # SMR：用 14 日報酬波動（簡化示意：你可用你原本 SMR 定義替換）
    ret = closes.pct_change().dropna()
    smr = float(ret.tail(14).std()) if len(ret) >= 14 else None

    # slope5：近 5 日線性趨勢（用差分近似）
    slope5 = float((closes.iloc[-1] - closes.iloc[-6]) / closes.iloc[-6]) if len(closes) >= 6 else None

    # drawdown：近 252 日最大回撤（這裡用當前回撤：close / rolling_max - 1）
    window = closes.tail(252)
    roll_max = window.cummax()
    drawdown_pct = float((window.iloc[-1] / roll_max.iloc[-1]) - 1.0) if len(window) >= 2 else None

    # 10d range： (max-min)/close
    w10 = closes.tail(10)
    price_range_10d_pct = float((w10.max() - w10.min()) / w10.iloc[-1]) if len(w10) >= 2 else None

    return {
        "twii_close": last_close,
        "twii_change": twii_change,
        "twii_pct": twii_pct,
        "smr": smr,
        "slope5": slope5,
        "drawdown_pct": drawdown_pct,
        "price_range_10d_pct": price_range_10d_pct,
        "metrics_reason": "OK"
    }


# =========================
# Stock Prices（Top20）：yfinance 批次抓
# =========================
def yf_symbol(stock_id: str) -> str:
    # 台股上市/上櫃多數為 .TW；你的現行系統也用 .TW
    return f"{stock_id}.TW"

def fetch_top20_prices(trade_date: date) -> Tuple[pd.DataFrame, Dict[str, str]]:
    symbols = [yf_symbol(sid) for sid, _ in TOP20]
    # 抓近 30 天方便算量比
    end = trade_date + timedelta(days=1)
    start = trade_date - timedelta(days=40)

    src_map = {sym: "YF_BATCH" for sym in symbols}

    try:
        df = yf.download(symbols, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
                         progress=False, group_by="ticker", auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame(), src_map
        return df, src_map
    except Exception:
        return pd.DataFrame(), src_map

def compute_price_and_vol_ratio(yf_df: pd.DataFrame, sym: str, trade_date: date) -> Tuple[Optional[float], Optional[float]]:
    """
    Price：trade_date 的 Close
    Vol_Ratio：trade_date Volume / 20日平均 Volume
    """
    try:
        if isinstance(yf_df.columns, pd.MultiIndex):
            sub = yf_df[sym].dropna(how="all")
            if sub.empty:
                return None, None
            sub.index = pd.to_datetime(sub.index).tz_localize(None)
            eligible = sub[sub.index.date <= trade_date]
            if eligible.empty:
                return None, None
            close = eligible["Close"].iloc[-1]
            vol = eligible["Volume"].iloc[-1]
            v20 = eligible["Volume"].tail(20)
            vavg = v20.mean() if len(v20) >= 5 else None
            ratio = (float(vol) / float(vavg)) if (vavg and vavg > 0) else None
            return (float(close) if pd.notna(close) else None), (float(ratio) if ratio is not None else None)
        else:
            # 單一 ticker 結構（很少發生在 batch）
            return None, None
    except Exception:
        return None, None


# =========================
# 簡易 Layer 訊號（示意）：你可替換成你的 Predator Layer Engine
# 這裡只做到「籌碼 + 量比」的可解釋輸出，不做預測、不補資料。
# =========================
def assign_layer(vol_ratio: Optional[float], inst_dir3: str, inst_streak3: int) -> str:
    # 只示意，不做誇張判斷：需同時滿足量能與籌碼方向
    if vol_ratio is None:
        return "NONE"
    if inst_dir3 == "POSITIVE" and inst_streak3 >= 3 and vol_ratio >= 1.10:
        return "A+"
    return "NONE"


# =========================
# 主流程：生成 JSON
# =========================
def build_predator_payload() -> Dict[str, Any]:
    # 1) Market Guard
    guard_active, guard_base_date, guard_note = apply_market_guard("EOD")
    # 2) 解出有效交易日（若 guard_active，基準日已回退一天）
    eff_trade_date, date_status = resolve_effective_trade_date(guard_base_date)

    meta_now = taipei_now().strftime("%Y-%m-%d %H:%M:%S")

    # 若連交易日都找不到 → 憲章：UNVERIFIED + max_equity=0
    if eff_trade_date is None:
        return {
            "meta": {
                "timestamp": meta_now,
                "session": "EOD",
                "market_status": "LOW",
                "current_regime": "DATA_FAILURE",
                "account_mode": "Conservative",
                "audit_tag": VERSION_TAG,
                "confidence_level": "LOW",
                "date_status": "UNVERIFIED",
            },
            "macro": {
                "overview": {
                    "trade_date": None,
                    "date_status": "UNVERIFIED",
                    "twii_close": None,
                    "twii_change": None,
                    "twii_pct": None,
                    "vix": None,
                    "vix_source": "VIX",
                    "vix_status": "FAIL",
                    "vix_confidence": "LOW",
                    "vix_panic": 35.0,
                    "smr": None,
                    "slope5": None,
                    "drawdown_pct": None,
                    "price_range_10d_pct": None,
                    "dynamic_vix_threshold": 35.0,
                    "max_equity_allowed_pct": 0.0,
                    "current_regime": "DATA_FAILURE",
                    "guard": {"active": guard_active, "note": guard_note},
                },
                "sources": {"reason": "NO_VALID_TRADE_DATE"},
                "market_amount": {
                    "amount_twse": None,
                    "amount_tpex": None,
                    "amount_total": None,
                    "source_twse": "TWSE_FAIL",
                    "source_tpex": "TPEX_SAFE_MODE",
                    "status_twse": "FAIL",
                    "status_tpex": "ESTIMATED",
                    "confidence_twse": "LOW",
                    "confidence_tpex": "LOW",
                    "confidence_level": "LOW",
                    "scope": "ALL",
                    "meta": {},
                },
                "market_inst_summary": [],
                "integrity_v1632": {
                    "status": "OK",
                    "kill_switch": True,
                    "confidence": "LOW",
                    "reason": "UNVERIFIED_TRADE_DATE",
                    "missing_count": 1,
                    "missing_list": ["trade_date"],
                    "fallback_count": 0
                }
            },
            "portfolio": {"total_equity": 2000000, "cash_balance": 2000000, "current_exposure_pct": 0.0, "cash_pct": 100.0},
            "institutional_panel": [],
            "stocks": [],
            "positions_input": [],
            "decisions": [],
            "audit_log": [],
        }

    # 3) 指數序列
    twii_df, twii_audit = fetch_index_series("^TWII", lookback_days=260)
    vix_df, vix_audit = fetch_index_series("^VIX", lookback_days=520)

    metrics = compute_metrics(twii_df, eff_trade_date)
    twii_close = metrics["twii_close"]
    smr = metrics["smr"]

    # VIX：取 <= trade_date 的最後一筆
    vix_val = None
    vix_last_dt = None
    if vix_df is not None and not vix_df.empty:
        elig = vix_df[vix_df.index.date <= eff_trade_date]
        if not elig.empty:
            vix_val = float(elig["Close"].iloc[-1])
            vix_last_dt = str(elig.index[-1].date())

    # 4) Market Amount
    twse_amt, twse_audit = fetch_twse_amount_stock_day_all(eff_trade_date)
    # ADR：TPEX 放生 → 固定 Safe Mode（你可調成 200B / 150B 等）
    tpex_amt = 200_000_000_000
    tpex_src = "TPEX_SAFE_MODE_200B"

    amount_total = (twse_amt or 0) + (tpex_amt or 0)

    # 5) TWSE T86：計算 Top20 的 3 日淨買超（只抓 TWSE 公開）
    # 先求最近 3 個交易日（以 ^TWII 有資料為準）
    trade_dates = []
    cur = eff_trade_date
    while len(trade_dates) < 3 and (eff_trade_date - cur).days <= 10:
        d, stt = resolve_effective_trade_date(cur)
        if d is not None and (len(trade_dates) == 0 or d != trade_dates[-1]):
            if d not in trade_dates:
                trade_dates.append(d)
        cur = cur - timedelta(days=1)
    trade_dates = sorted(trade_dates)  # 舊 → 新

    # t86 可用性：至少最新日能抓到資料
    t86_df_latest, t86_audit_latest = fetch_twse_t86_all(eff_trade_date)
    t86_ok = (t86_df_latest is not None and not t86_df_latest.empty and t86_audit_latest.get("error") is None)

    # 6) 籌碼 cache：只允許同 trade_date，否則標 NO_UPDATE_TODAY
    inst_cache = load_inst_cache()
    cache_date = inst_cache.get("trade_date")
    cache_by_stock = inst_cache.get("by_stock", {})

    institutional_panel = []
    inst_debug_map = {}

    for sid, name in TOP20:
        sym = yf_symbol(sid)
        is_tpex = sid in TPEX_SET

        # 上櫃：本版先不抓 TPEX 法人（ADR-001/002：先上 TWSE T86）
        if is_tpex:
            inst_status = "NO_DATA"
            inst_dir3 = "NO_DATA"
            inst_streak3 = 0
            inst_net_3d = 0.0
            inst_source = "TPEX_NOT_IMPLEMENTED_ADR_T86_TWSE_FIRST"
            institutional_panel.append({
                "Symbol": sym, "Name": name,
                "Inst_Status": inst_status,
                "Inst_Streak3": inst_streak3,
                "Inst_Dir3": inst_dir3,
                "Inst_Net_3d": inst_net_3d,
                "inst_source": inst_source
            })
            continue

        # 上市：抓 TWSE T86 3日淨買超（以「外資+投信+自營」合計）
        # 若 t86_ok==False，避免殭屍：只用 cache（但必須同日），否則 NO_UPDATE_TODAY
        if not t86_ok:
            if cache_date == eff_trade_date.strftime("%Y-%m-%d") and sid in cache_by_stock:
                c = cache_by_stock[sid]
                institutional_panel.append({
                    "Symbol": sym, "Name": name,
                    "Inst_Status": "READY",
                    "Inst_Streak3": int(c.get("Inst_Streak3", 0)),
                    "Inst_Dir3": str(c.get("Inst_Dir3", "NEUTRAL")),
                    "Inst_Net_3d": float(c.get("Inst_Net_3d", 0.0)),
                    "inst_source": "CACHE_SAME_DAY"
                })
            else:
                institutional_panel.append({
                    "Symbol": sym, "Name": name,
                    "Inst_Status": "NO_UPDATE_TODAY",
                    "Inst_Streak3": 0,
                    "Inst_Dir3": "NO_DATA",
                    "Inst_Net_3d": 0.0,
                    "inst_source": "TWSE_T86_UNAVAILABLE_NO_CACHE"
                })
            continue

        # 正常抓取
        foreign_3d, trust_3d, dealer_3d, total_3d, dbg = compute_twse_inst_3d_net(sid, trade_dates)
        inst_debug_map[sid] = dbg

        inst_net_3d = float(total_3d)
        if inst_net_3d > 0:
            inst_dir3 = "POSITIVE"
        elif inst_net_3d < 0:
            inst_dir3 = "NEGATIVE"
        else:
            inst_dir3 = "NEUTRAL"

        # streak：簡化版（連續 3 日皆同方向才算 3）
        # 以 dbg["days"] 中各日 total 方向判斷
        dirs = []
        for day in dbg.get("days", []):
            if day.get("ok") and day.get("row_found"):
                t = (day.get("foreign", 0) + day.get("trust", 0) + day.get("dealer", 0))
                if t > 0:
                    dirs.append(1)
                elif t < 0:
                    dirs.append(-1)
                else:
                    dirs.append(0)
        inst_streak3 = 0
        if len(dirs) >= 3:
            if all(x > 0 for x in dirs[-3:]):
                inst_streak3 = 3
            elif all(x < 0 for x in dirs[-3:]):
                inst_streak3 = 3

        institutional_panel.append({
            "Symbol": sym, "Name": name,
            "Inst_Status": "READY",
            "Inst_Streak3": inst_streak3,
            "Inst_Dir3": inst_dir3,
            "Inst_Net_3d": inst_net_3d,
            "inst_source": "TWSE_T86_3D_NET"
        })

    # 7) 寫回 cache（只寫入本 trade_date 的資料）
    cache_payload = {
        "trade_date": eff_trade_date.strftime("%Y-%m-%d"),
        "by_stock": {}
    }
    for inst in institutional_panel:
        sid = inst["Symbol"].replace(".TW", "").replace(".TWO", "")
        # 只存上市
        if sid.isdigit() and sid not in TPEX_SET and inst.get("Inst_Status") == "READY":
            cache_payload["by_stock"][sid] = {
                "Inst_Streak3": inst.get("Inst_Streak3", 0),
                "Inst_Dir3": inst.get("Inst_Dir3", "NEUTRAL"),
                "Inst_Net_3d": inst.get("Inst_Net_3d", 0.0),
            }
    save_inst_cache(cache_payload)

    # 8) 股票價格 / 量比
    yf_prices_df, prices_src_map = fetch_top20_prices(eff_trade_date)

    stocks = []
    for sid, name in TOP20:
        sym = yf_symbol(sid)
        price, vol_ratio = compute_price_and_vol_ratio(yf_prices_df, sym, eff_trade_date)
        # 對應法人
        inst_row = next((x for x in institutional_panel if x["Symbol"] == sym), None) or {}
        inst_dir3 = inst_row.get("Inst_Dir3", "NO_DATA")
        inst_streak3 = int(inst_row.get("Inst_Streak3", 0))
        inst_net_3d = float(inst_row.get("Inst_Net_3d", 0.0))

        layer = assign_layer(vol_ratio, inst_dir3, inst_streak3)

        stocks.append({
            "Symbol": sym,
            "Name": name,
            "Tier": int([x[0] for x in TOP20].index(sid) + 1),
            "Price": price,
            "Vol_Ratio": vol_ratio,
            "Layer": layer,
            "Institutional": {
                "foreign_buy": True if inst_dir3 == "POSITIVE" else False,
                "trust_buy": True if inst_dir3 == "POSITIVE" else False,
                "Inst_Streak3": inst_streak3,
                "Inst_Net_3d": inst_net_3d,
                "inst_streak3": inst_streak3,
                "stale": True if inst_row.get("Inst_Status") == "NO_UPDATE_TODAY" else False
            },
            "source": prices_src_map.get(sym, "YF_BATCH")
        })

    # 9) 殭屍籌碼熔斷
    stale_flag = apply_stale_inst_kill_switch(institutional_panel, stocks)

    # 10) Layer A：Integrity Validation
    integrity_override = integrity_check_market(twii_close, smr)

    # regime（示意：你可替換成你的完整 Regime Engine）
    current_regime = "OVERHEAT"  # 先保留你目前狀態標記
    max_equity_allowed_pct = 0.55
    confidence_level = "MEDIUM"
    market_status = "LOW"  # 你提供的例子是 LOW

    if integrity_override:
        current_regime = integrity_override["current_regime"]
        max_equity_allowed_pct = integrity_override["max_equity_allowed_pct"]
        confidence_level = integrity_override["confidence_level"]
        market_status = "LOW"

    # Market Guard 觸發時：符合你需求（08:00 顯示昨日判定並 max_equity=0）
    guard_block_equity = False
    if guard_active:
        # 你指定：00:00~15:30 EOD → 顯示上一交易日最終判定，且 max_equity=0.0
        max_equity_allowed_pct = 0.0
        confidence_level = "LOW"
        guard_block_equity = True

    # confidence_level 強制規則
    confidence_level = enforce_confidence_level(confidence_level, twii_close, smr, stale_flag, t86_ok)

    # 11) 法人彙總（市場層）：用最新日 T86 df 做合計（僅 TWSE）
    market_inst_summary = []
    if t86_ok and t86_df_latest is not None and not t86_df_latest.empty:
        # 只顯示「合計」列（若有）
        try:
            # 常見列含 "合計"
            name_col = None
            for c in t86_df_latest.columns:
                if "證券名稱" in c:
                    name_col = c
                    break
            total_col = None
            for c in t86_df_latest.columns:
                if ("三大法人" in c and "買賣超" in c) or (c.strip() == "三大法人買賣超股數"):
                    total_col = c
                    break
            if name_col and total_col:
                total_row = t86_df_latest[t86_df_latest[name_col].astype(str).str.contains("合計", na=False)]
                if not total_row.empty:
                    v = _to_int_safe(total_row.iloc[0][total_col])
                    market_inst_summary.append({"Identity": "合計（TWSE 三大法人買賣超股數）", "Net": v})
        except Exception:
            pass

    # 12) 組裝 Payload（貼近你提供的 schema）
    payload = {
        "meta": {
            "timestamp": meta_now,
            "session": "EOD",
            "market_status": market_status,
            "current_regime": current_regime,
            "account_mode": "Conservative",
            "audit_tag": VERSION_TAG,
            "confidence_level": confidence_level,
            "date_status": date_status
        },
        "macro": {
            "overview": {
                "trade_date": eff_trade_date.strftime("%Y-%m-%d"),
                "date_status": date_status,
                "twii_close": metrics["twii_close"],
                "twii_change": metrics["twii_change"],
                "twii_pct": metrics["twii_pct"],
                "vix": vix_val,
                "vix_source": "VIX",
                "vix_status": "OK" if vix_val is not None else "FAIL",
                "vix_confidence": "MEDIUM" if vix_val is not None else "LOW",
                "vix_panic": 35.0,
                "smr": metrics["smr"],
                "slope5": metrics["slope5"],
                "drawdown_pct": metrics["drawdown_pct"],
                "price_range_10d_pct": metrics["price_range_10d_pct"],
                "dynamic_vix_threshold": 35.0,
                "max_equity_allowed_pct": max_equity_allowed_pct,
                "current_regime": current_regime,
                "guard": {
                    "active": guard_active,
                    "note": guard_note,
                    "block_equity": guard_block_equity
                }
            },
            "sources": {
                "twii": {
                    "name": "TWII",
                    "ok": bool(twii_audit.get("ok")),
                    "rows": twii_audit.get("rows"),
                    "cols": list(twii_df.columns) if twii_df is not None and not twii_df.empty else [],
                    "last_dt": twii_audit.get("last_dt"),
                    "reason": twii_audit.get("reason")
                },
                "vix": {
                    "name": "VIX",
                    "ok": bool(vix_audit.get("ok")),
                    "rows": vix_audit.get("rows"),
                    "cols": list(vix_df.columns) if vix_df is not None and not vix_df.empty else [],
                    "last_dt": vix_last_dt or vix_audit.get("last_dt"),
                    "reason": vix_audit.get("reason")
                },
                "metrics_reason": metrics.get("metrics_reason"),
                "amount_source": {
                    "trade_date": eff_trade_date.strftime("%Y-%m-%d"),
                    "source_twse": "TWSE_OK:AUDIT_SUM" if twse_amt else "TWSE_FAIL",
                    "source_tpex": tpex_src,
                    "status_twse": "OK" if twse_amt else "FAIL",
                    "status_tpex": "ESTIMATED",
                    "confidence_twse": "HIGH" if twse_amt else "LOW",
                    "confidence_tpex": "LOW",
                    "confidence_level": "LOW" if (not twse_amt) else confidence_level,
                    "amount_twse": twse_amt,
                    "amount_tpex": tpex_amt,
                    "amount_total": amount_total,
                    "scope": "ALL",
                    "audit_dir": AUDIT_DIR,
                    "twse_audit": {
                        "market": "TWSE",
                        "trade_date": eff_trade_date.strftime("%Y-%m-%d"),
                        "rows": twse_audit.get("rows"),
                        "amount_sum": twse_audit.get("amount_sum")
                    },
                    "tpex_audit": None
                },
                "prices_source_map": prices_src_map,
                "t86_latest": t86_audit_latest,
                "t86_ok": t86_ok,
            },
            "market_amount": {
                "amount_twse": twse_amt,
                "amount_tpex": tpex_amt,
                "amount_total": amount_total,
                "source_twse": "TWSE_OK:AUDIT_SUM" if twse_amt else "TWSE_FAIL",
                "source_tpex": tpex_src,
                "status_twse": "OK" if twse_amt else "FAIL",
                "status_tpex": "ESTIMATED",
                "confidence_twse": "HIGH" if twse_amt else "LOW",
                "confidence_tpex": "LOW",
                "confidence_level": confidence_level,
                "allow_insecure_ssl": True,
                "scope": "ALL",
                "meta": {
                    "trade_date": eff_trade_date.strftime("%Y-%m-%d"),
                    "audit_dir": AUDIT_DIR,
                    "twse": twse_audit,
                    "tpex": {
                        "fallback": "SAFE_MODE",
                        "audit": None
                    }
                }
            },
            "market_inst_summary": market_inst_summary,
            "integrity_v1632": {
                "status": "OK",
                "kill_switch": bool(integrity_override is not None) or (date_status != "VERIFIED"),
                "confidence": confidence_level,
                "reason": "INTEGRITY_PASS" if integrity_override is None else "INTEGRITY_FAIL",
                "missing_count": 0 if integrity_override is None else 1,
                "missing_list": [] if integrity_override is None else ["twii_close_or_smr"],
                "fallback_count": int(1 if tpex_src.startswith("TPEX_SAFE_MODE") else 0)
            }
        },
        "portfolio": {
            "total_equity": 2000000,
            "cash_balance": 2000000,
            "current_exposure_pct": 0.0,
            "cash_pct": 100.0
        },
        "institutional_panel": institutional_panel,
        "stocks": stocks,
        "positions_input": [],
        "decisions": [],
        "audit_log": []
    }

    # 13) 寫檔（方便稽核）
    try:
        with open(MARKET_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # 14) 同步落地 TWSE audit 檔（成交額）
    try:
        audit_file = os.path.join(AUDIT_DIR, f"twse_amount_{eff_trade_date.strftime('%Y%m%d')}.json")
        with open(audit_file, "w", encoding="utf-8") as f:
            json.dump(twse_audit, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # 15) 附加 debug（不在 UI 顯示，必要時你可展開）
    payload["_debug"] = {"twse_t86_map": inst_debug_map}

    return payload


# =========================
# UI（你要求：不要一堆代碼，而是清楚貼出指數/漲跌/成交額/法人/Top20）
# =========================
def fmt_money(n: Optional[int]) -> str:
    if n is None:
        return "—"
    # 以「億元」顯示（關鍵數據）
    return f"{n/100_000_000:,.1f} 億"

def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x*100:.2f}%"

def fmt_num(x: Optional[float], nd=2) -> str:
    if x is None:
        return "—"
    return f"{x:.{nd}f}"

def render_ui(payload: Dict[str, Any]) -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    meta = payload.get("meta", {})
    ov = payload.get("macro", {}).get("overview", {})
    ma = payload.get("macro", {}).get("market_amount", {})
    inst_sum = payload.get("macro", {}).get("market_inst_summary", [])
    stocks = payload.get("stocks", [])
    inst_panel = payload.get("institutional_panel", [])

    # Header 狀態
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1.2, 1.2, 1.2])
    c1.metric("交易日", ov.get("trade_date", "—"))
    c2.metric("Regime", meta.get("current_regime", "—"))
    c3.metric("Confidence", meta.get("confidence_level", "—"))
    c4.metric("Max Equity", fmt_pct(ov.get("max_equity_allowed_pct", 0.0)))
    c5.metric("Market Status", meta.get("market_status", "—"))

    guard = ov.get("guard", {}) or {}
    if guard.get("active"):
        st.warning(f"Market Guard 啟動：今日數據尚未完成 → 顯示 {ov.get('trade_date')} 的最終判定；max_equity 已強制為 0.0。")

    st.divider()

    # 指數與成交額
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("TWII 收盤", fmt_num(ov.get("twii_close"), 2),
              delta=(fmt_num(ov.get("twii_change"), 2) if ov.get("twii_change") is not None else None))
    a2.metric("TWII 漲跌幅", fmt_pct(ov.get("twii_pct")))
    a3.metric("VIX", fmt_num(ov.get("vix"), 2))
    a4.metric("SMR（14d波動）", fmt_num(ov.get("smr"), 4))

    b1, b2, b3 = st.columns(3)
    b1.metric("上市成交額（TWSE）", fmt_money(ma.get("amount_twse")))
    b2.metric("上櫃成交額（TPEX）", fmt_money(ma.get("amount_tpex")),
              delta=("Safe Mode" if ma.get("source_tpex", "").startswith("TPEX_SAFE_MODE") else None))
    b3.metric("市場總成交額（估）", fmt_money(ma.get("amount_total")))

    st.caption(
        f"資料稽核：TWSE={ma.get('source_twse')}；TPEX={ma.get('source_tpex')}（依 ADR 採 Safe Mode，避免投入成本侵蝕 ROI）"
    )

    st.divider()

    # 市場法人彙總（若有）
    if inst_sum:
        st.subheader("三大法人（市場層摘要）")
        st.json(inst_sum)
    else:
        st.subheader("三大法人（市場層摘要）")
        st.info("目前僅先落地 TWSE T86（免費公開）。若需 TPEX 法人彙總，建議另立 ADR（避免拖累主系統穩定）。")

    st.divider()

    # Top20 表格（親和版）
    st.subheader("Top20 監控清單（價格 / 量比 / 籌碼 / Layer）")

    inst_map = {x["Symbol"]: x for x in inst_panel}
    rows = []
    for s in stocks:
        sym = s.get("Symbol")
        inst = inst_map.get(sym, {})
        rows.append({
            "Tier": s.get("Tier"),
            "代碼": sym.replace(".TW", ""),
            "名稱": s.get("Name"),
            "收盤價": s.get("Price"),
            "量比(20d)": s.get("Vol_Ratio"),
            "籌碼狀態": inst.get("Inst_Status"),
            "3日方向": inst.get("Inst_Dir3"),
            "3日淨買超股數": inst.get("Inst_Net_3d"),
            "Layer": s.get("Layer"),
            "價格來源": s.get("source"),
            "籌碼來源": inst.get("inst_source"),
        })

    df = pd.DataFrame(rows)
    # 友善格式化
    if not df.empty:
        df["收盤價"] = df["收盤價"].map(lambda x: "—" if x is None else f"{x:,.2f}")
        df["量比(20d)"] = df["量比(20d)"].map(lambda x: "—" if x is None else f"{x:.2f}")
        df["3日淨買超股數"] = df["3日淨買超股數"].map(lambda x: "—" if x is None else f"{int(x):,}")
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()

    # 下載 / 檢視 JSON
    st.subheader("輸出與稽核")
    st.caption(f"已落地：{MARKET_JSON_PATH}（可供你的 Arbiter/回測/稽核鏈使用）")
    st.download_button(
        label="下載本次 JSON（predator_market.json）",
        data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        file_name="predator_market.json",
        mime="application/json"
    )

    with st.expander("展開查看原始 JSON（除錯/稽核）", expanded=False):
        # 不展 _debug（避免太吵）；需要你再開
        p = dict(payload)
        if "_debug" in p:
            p["_debug"] = {"note": "已隱藏（需要再開）"}
        st.json(p)


# =========================
# 入口
# =========================
def main():
    st.sidebar.header("控制台")
    st.sidebar.caption("V16.3.32 FINAL：Market Guard + Stale Kill Switch + Integrity Validation + TWSE T86")

    if st.sidebar.button("立即刷新（重新抓取）"):
        st.session_state["force_refresh"] = True

    force_refresh = st.session_state.get("force_refresh", False)

    # 快取 UI 端 payload，避免每次重跑都打外部
    cache_key = "payload_cache"
    cache_ts_key = "payload_cache_ts"

    if (not force_refresh) and (cache_key in st.session_state):
        payload = st.session_state[cache_key]
    else:
        with st.spinner("抓取市場資料中（TWII / VIX / TWSE成交額 / TWSE T86）..."):
            payload = build_predator_payload()
        st.session_state[cache_key] = payload
        st.session_state[cache_ts_key] = taipei_now().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state["force_refresh"] = False

    ts = st.session_state.get(cache_ts_key, "—")
    st.sidebar.metric("最後刷新", ts)

    render_ui(payload)

if __name__ == "__main__":
    main()
