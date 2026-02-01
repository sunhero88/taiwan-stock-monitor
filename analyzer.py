# analyzer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

TZ_TAIPEI = timezone(timedelta(hours=8))

EPS = 1e-4
DEFAULT_DYNAMIC_VIX_THRESHOLD = 35.0

# ---------------------------
# Helpers
# ---------------------------
def now_taipei() -> datetime:
    return datetime.now(TZ_TAIPEI)

def dt_str(dt: datetime) -> str:
    return dt.astimezone(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M")

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace(",", "").strip()
            if x in ("", "-", "—", "N/A", "None", "null"):
                return default
        return float(x)
    except Exception:
        return default

def safe_int(x, default=None):
    try:
        f = safe_float(x, None)
        if f is None:
            return default
        return int(f)
    except Exception:
        return default

def floor_pct(x: float) -> int:
    # 無條件捨去至整數
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0
    return int(math.floor(x))

def yesno(b: bool) -> str:
    return "Yes" if bool(b) else "No"

def pct(a: float, b: float) -> float:
    # (a-b)/b
    if b == 0 or b is None or a is None:
        return 0.0
    return (a - b) / b

# ---------------------------
# Market Meta (Index / MA / SMR / Regime)
# ---------------------------
def fetch_yf_history(symbol: str, period: str, interval: str) -> pd.DataFrame:
    t = yf.Ticker(symbol)
    df = t.history(period=period, interval=interval, auto_adjust=False)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    return df

def latest_trading_day_from_yfinance() -> Optional[date]:
    # 以 ^TWII 最近可用收盤日做「官方交易日代理」
    df = fetch_yf_history("^TWII", period="10d", interval="1d")
    if df.empty:
        return None
    last_dt = df.index[-1].to_pydatetime().date()
    return last_dt

def compute_index_meta(session: str) -> Dict[str, Any]:
    """
    session:
      PREOPEN: 顯示「上一交易日收盤」
      INTRADAY: 取 yfinance 最新一筆（通常是當日盤中或延遲），並以前一日 close 計算漲跌
      EOD: 顯示「最新交易日收盤」
    """
    out: Dict[str, Any] = {
        "symbol": "^TWII",
        "date": None,
        "close": None,
        "chg": None,
        "chg_pct": None,
        "source": "yfinance",
        "error": None,
    }

    df = fetch_yf_history("^TWII", period="15d", interval="1d")
    if df.empty or len(df) < 2:
        out["error"] = "YF_TWII_EMPTY"
        return out

    # yfinance daily：最後一列通常是最近交易日 close
    last = df.iloc[-1]
    prev = df.iloc[-2]
    last_date = df.index[-1].date()

    close = float(last["Close"])
    prev_close = float(prev["Close"])
    chg = close - prev_close
    chg_pct = (chg / prev_close) * 100.0 if prev_close != 0 else 0.0

    if session == "PREOPEN":
        # 盤前：顯示「昨日（最後收盤日）」
        close = float(prev_close)
        # 昨日漲跌：用 prev vs df[-3]
        if len(df) >= 3:
            prev2_close = float(df.iloc[-3]["Close"])
            chg = close - prev2_close
            chg_pct = (chg / prev2_close) * 100.0 if prev2_close != 0 else 0.0
        out["date"] = str(df.index[-2].date())
    else:
        out["date"] = str(last_date)

    out["close"] = round(close, 4)
    out["chg"] = round(chg, 4)
    out["chg_pct"] = round(chg_pct, 4)
    return out

def compute_ma200_and_smr() -> Dict[str, Any]:
    out = {
        "ma200": None,
        "smr": None,
        "smr_ma5": None,
        "slope5": None,
        "error": None,
    }
    df = fetch_yf_history("^TWII", period="2y", interval="1d")
    if df.empty or len(df) < 210:
        out["error"] = "YF_TWII_INSUFFICIENT"
        return out

    close = df["Close"].astype(float)
    ma200 = close.rolling(200).mean().iloc[-1]
    last_close = close.iloc[-1]
    smr = (last_close - ma200) / ma200 if ma200 and ma200 != 0 else 0.0

    smr_series = (close - close.rolling(200).mean()) / close.rolling(200).mean()
    smr_ma5 = smr_series.rolling(5).mean()
    slope5 = smr_ma5.iloc[-1] - smr_ma5.iloc[-2] if len(smr_ma5.dropna()) >= 2 else 0.0

    out["ma200"] = float(ma200)
    out["smr"] = float(smr)
    out["smr_ma5"] = float(smr_ma5.iloc[-1]) if not math.isnan(float(smr_ma5.iloc[-1])) else None
    out["slope5"] = float(slope5)
    return out

def compute_ma14_monthly() -> Dict[str, Any]:
    # 以 yfinance 1mo 取月K，MA14_monthly = 14 個完整月份 close 平均
    out = {"ma14_monthly": None, "error": None}
    df = fetch_yf_history("^TWII", period="5y", interval="1mo")
    if df.empty or len(df) < 15:
        out["error"] = "YF_TWII_1MO_INSUFFICIENT"
        return out
    close = df["Close"].astype(float)
    ma14 = close.rolling(14).mean().iloc[-1]
    out["ma14_monthly"] = float(ma14)
    return out

def compute_vix() -> Dict[str, Any]:
    out = {"vix": None, "date": None, "source": "yfinance", "error": None}
    df = fetch_yf_history("^VIX", period="15d", interval="1d")
    if df.empty:
        out["error"] = "YF_VIX_EMPTY"
        return out
    out["vix"] = float(df["Close"].iloc[-1])
    out["date"] = str(df.index[-1].date())
    return out

def compute_boolean_status(smr: float, slope5: float) -> Dict[str, str]:
    # 你 V15.6.5 要求 Yes/No 形式
    SMR_OVER_0_25 = (smr is not None and smr > 0.25)
    # 連續性判定（NEGATIVE_SLOPE_5D / SLOPE5_4DAY_LOCK / MOMENTUM_LOCK_ACTIVE）
    # 這裡用近 10 日 SMR_MA5 斜率簡化；要更嚴格可再擴窗
    df = fetch_yf_history("^TWII", period="40d", interval="1d")
    if df.empty or len(df) < 12:
        return {
            "SMR_OVER_0.25": yesno(SMR_OVER_0_25),
            "NEGATIVE_SLOPE_5D": "No",
            "SLOPE5_4DAY_LOCK": "No",
            "MOMENTUM_LOCK_ACTIVE": "No",
            "CREDIT_STRESS": "No",
        }

    close = df["Close"].astype(float)
    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    smr_ma5 = smr_series.rolling(5).mean()
    # slope series
    slope = smr_ma5.diff()

    last10 = slope.dropna().tail(10)
    NEGATIVE_SLOPE_5D = (len(last10) >= 5 and all(last10.tail(5) < -EPS))
    SLOPE5_4DAY_LOCK = (len(last10) >= 4 and all(last10.tail(4) > EPS))
    MOMENTUM_LOCK_ACTIVE = SLOPE5_4DAY_LOCK  # 先用同義（你定義是 4 consecutive days）

    # CREDIT_STRESS: 若沒有 HY spread 就只能 No + 註記（在 risk_alerts）
    return {
        "SMR_OVER_0.25": yesno(SMR_OVER_0_25),
        "NEGATIVE_SLOPE_5D": yesno(NEGATIVE_SLOPE_5D),
        "SLOPE5_4DAY_LOCK": yesno(SLOPE5_4DAY_LOCK),
        "MOMENTUM_LOCK_ACTIVE": yesno(MOMENTUM_LOCK_ACTIVE),
        "CREDIT_STRESS": "No",
    }

def compute_regime(smr: float, slope5: float, vix: float, drawdown_pct: float, ma14_monthly: float, twii_close: float) -> str:
    # 優先序：CRASH_RISK > HIBERNATION > MEAN_REVERSION > OVERHEAT > NORMAL
    if drawdown_pct is not None and drawdown_pct >= 18.0:
        return "CRASH_RISK"
    if vix is not None and vix >= 40.0:
        return "CRASH_RISK"

    # HIBERNATION: Close < MA20_Monthly(連續3日) —— 這版先用 MA14_monthly 做「官方防線代理」
    # 若你要嚴格 MA20_Monthly，需要額外計算月線 20 個完整月份平均並判斷連3日（日K）<該值
    if ma14_monthly is not None and twii_close is not None:
        # 不做連3日，避免過度推論；要嚴格可擴充
        pass

    if smr is not None and smr > 0.25 and slope5 is not None and slope5 < -EPS:
        return "MEAN_REVERSION"
    if smr is not None and smr > 0.25 and slope5 is not None and slope5 >= -EPS:
        return "OVERHEAT"
    return "NORMAL"

def compute_drawdown_pct() -> float:
    df = fetch_yf_history("^TWII", period="1y", interval="1d")
    if df.empty:
        return 0.0
    close = df["Close"].astype(float)
    peak = close.cummax()
    dd = (close - peak) / peak
    dd_pct = float(abs(dd.min()) * 100.0)
    return dd_pct

# ---------------------------
# Official market data (TWSE / TPEx)
# ---------------------------
@dataclass
class SourceResult:
    ok: bool
    df: pd.DataFrame
    source: str
    error: Optional[str] = None
    date_str: Optional[str] = None

def http_get(url: str, verify_ssl: bool, timeout: int = 15) -> requests.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Sunhero-Predator/1.0; +https://streamlit.app)"
    }
    return requests.get(url, headers=headers, timeout=timeout, verify=verify_ssl)

def fetch_twse_stock_day_all(verify_ssl: bool) -> SourceResult:
    # TWSE 全市場日行情 (上市)
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    try:
        r = http_get(url, verify_ssl=verify_ssl, timeout=20)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data)
        if df.empty:
            return SourceResult(False, pd.DataFrame(), "TWSE_OPENAPI", "TWSE_EMPTY")
        # 欄位整理
        # 常見欄位：Code, Name, TradeVolume, TradeValue, Open, High, Low, Close, Change, Transaction
        # 轉成 numeric
        for c in ["TradeVolume", "TradeValue", "Open", "High", "Low", "Close", "Change", "Transaction"]:
            if c in df.columns:
                df[c] = df[c].apply(lambda x: safe_float(x, np.nan))
        # 日期不一定在這個 endpoint 給；用 yfinance 最近日當作 official_date 代理
        return SourceResult(True, df, "TWSE_OPENAPI", None, None, None)
    except Exception as e:
        return SourceResult(False, pd.DataFrame(), "TWSE_OPENAPI", f"TWSE_ERR:{type(e).__name__}")

def fetch_tpex_pricing_html(verify_ssl: bool) -> SourceResult:
    # TPEx 交易資訊頁面（容易改版；抓不到就降級）
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    try:
        r = http_get(url, verify_ssl=verify_ssl, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
        # 這頁主要是彙總，不是全股票清單；我們只拿「上櫃成交金額」做 amount_tpex 代理
        m = re.search(r"成交金額\s*([\d,]+)\s*億", text)
        if not m:
            return SourceResult(False, pd.DataFrame(), "TPEX_HTML", "TPEX_PRICING_NOT_FOUND")
        amt_yi = safe_float(m.group(1), None)
        df = pd.DataFrame([{"amount_tpex_yi": amt_yi}])
        return SourceResult(True, df, "TPEX_HTML", None, None, None)
    except Exception as e:
        return SourceResult(False, pd.DataFrame(), "TPEX_HTML", f"TPEX_ERR:{type(e).__name__}")

def compute_amount_total_best_effort(verify_ssl: bool) -> Dict[str, Any]:
    """
    目標：TWSE amount + TPEx amount（億）+ total
    - TWSE：若抓得到 STOCK_DAY_ALL，sum TradeValue / 1e8 = 億（TradeValue 常是元）
    - TPEx：pricing.html 只能拿到彙總（億）
    """
    out = {
        "twse_yi": None,
        "tpex_yi": None,
        "total_yi": None,
        "sources": {"twse": None, "tpex": None},
        "warning": None,
        "error": None,
    }

    twse = fetch_twse_stock_day_all(verify_ssl=verify_ssl)
    if twse.ok:
        # TradeValue 若是元，換算億：/1e8
        if "TradeValue" in twse.df.columns:
            tv = twse.df["TradeValue"].dropna()
            twse_yi = float(tv.sum() / 1e8)
            out["twse_yi"] = round(twse_yi, 2)
            out["sources"]["twse"] = twse.source
        else:
            out["warning"] = "TWSE_NO_TRADEVALUE"
    else:
        out["sources"]["twse"] = twse.error

    tpex = fetch_tpex_pricing_html(verify_ssl=verify_ssl)
    if tpex.ok:
        out["tpex_yi"] = safe_float(tpex.df.iloc[0].get("amount_tpex_yi"), None)
        out["sources"]["tpex"] = tpex.source
    else:
        out["sources"]["tpex"] = tpex.error

    if out["twse_yi"] is not None and out["tpex_yi"] is not None:
        out["total_yi"] = round(out["twse_yi"] + out["tpex_yi"], 2)
    return out

# ---------------------------
# Top20 ranking (true market ranking by turnover)
# ---------------------------
def build_topn_by_turnover(topn: int, verify_ssl: bool, min_price: float = 1.0) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    True ranking definition:
      TopN = 全市場(上市)當日成交金額 TradeValue 排序前 N
    若 TWSE API 失敗 → 回傳空 DF，並標示 error，使上層 Gate 降級
    """
    meta = {"source": None, "error": None, "note": None}
    twse = fetch_twse_stock_day_all(verify_ssl=verify_ssl)
    if not twse.ok:
        meta["source"] = twse.source
        meta["error"] = twse.error
        return pd.DataFrame(), meta

    df = twse.df.copy()
    meta["source"] = twse.source

    # 過濾 ETF/權證等：簡化用代碼格式 + Close價格
    # 只保留 4~6 位數代碼（台股常見），可再依你的需求精煉
    if "Code" not in df.columns or "TradeValue" not in df.columns or "Close" not in df.columns:
        meta["error"] = "TWSE_SCHEMA_CHANGED"
        return pd.DataFrame(), meta

    df = df[df["Code"].astype(str).str.match(r"^\d{4}$")]
    df = df[df["Close"].apply(lambda x: safe_float(x, np.nan)).notna()]
    df = df[df["Close"] >= min_price]
    df = df[df["TradeValue"].notna()]

    df = df.sort_values("TradeValue", ascending=False).head(topn).copy()
    df["symbol"] = df["Code"].astype(str) + ".TW"
    df["name"] = df.get("Name", "")
    df["close"] = df["Close"].astype(float)
    df["volume"] = df.get("TradeVolume", np.nan)
    df["turnover"] = df["TradeValue"].astype(float)

    # 附加少量技術欄位：只對 TopN 個股用 yfinance 計算（成本可控）
    rows = []
    for sym, nm in zip(df["symbol"], df["name"]):
        tech = compute_stock_tech(sym)
        row = {
            "symbol": sym,
            "name": nm,
            "date": tech.get("date"),
            "close": tech.get("close"),
            "ret20_pct": tech.get("ret20_pct"),
            "vol_ratio": tech.get("vol_ratio"),
            "ma_bias_pct": tech.get("ma_bias_pct"),
            "volume": tech.get("volume"),
        }
        # score：中立但可解釋（重點：成交金額排序已是「真排名」，score只是輔助）
        score = 0.0
        if row["ret20_pct"] is not None:
            score += row["ret20_pct"] * 0.6
        if row["vol_ratio"] is not None:
            score += (row["vol_ratio"] - 1.0) * 20.0  # vol_ratio=1.5 → +10 分
        if row["ma_bias_pct"] is not None:
            score += row["ma_bias_pct"] * 0.4
        row["score"] = round(float(score), 4)
        rows.append(row)

    top_df = pd.DataFrame(rows)
    # rank：以 turnover 真排名為主（TWSE turnover 排序），此處以 rows 的順序保留
    top_df["rank"] = list(range(1, len(top_df) + 1))
    return top_df, meta

def compute_stock_tech(symbol: str) -> Dict[str, Any]:
    """
    TopN 個股技術：用 yfinance 計算
    - close: 最新日 close
    - ret20_pct: 20 日報酬(%)：close / close[-21] - 1
    - vol_ratio: 今日量 / 20日均量
    - ma_bias_pct: (close - MA20)/MA20(%)
    """
    out = {
        "symbol": symbol,
        "date": None,
        "close": None,
        "volume": None,
        "ret20_pct": None,
        "vol_ratio": None,
        "ma_bias_pct": None,
        "error": None,
    }
    df = fetch_yf_history(symbol, period="60d", interval="1d")
    if df.empty or len(df) < 25:
        out["error"] = "YF_STOCK_INSUFFICIENT"
        return out

    close = df["Close"].astype(float)
    vol = df["Volume"].astype(float)
    last_close = float(close.iloc[-1])
    last_vol = float(vol.iloc[-1])
    last_date = df.index[-1].date()

    ma20 = close.rolling(20).mean().iloc[-1]
    vol20 = vol.rolling(20).mean().iloc[-1]
    ret20 = (last_close / float(close.iloc[-21]) - 1.0) * 100.0 if float(close.iloc[-21]) != 0 else 0.0
    vol_ratio = (last_vol / float(vol20)) if vol20 and vol20 != 0 else None
    ma_bias = ((last_close - float(ma20)) / float(ma20)) * 100.0 if ma20 and ma20 != 0 else None

    out["date"] = str(last_date)
    out["close"] = round(last_close, 4)
    out["volume"] = int(last_vol)
    out["ret20_pct"] = round(float(ret20), 4)
    out["vol_ratio"] = round(float(vol_ratio), 4) if vol_ratio is not None else None
    out["ma_bias_pct"] = round(float(ma_bias), 4) if ma_bias is not None else None
    return out

# ---------------------------
# Institutional (best-effort placeholder)
# ---------------------------
def compute_institutional_stub(sim_free: bool = True) -> Dict[str, Any]:
    # 你要做到「正確最新」：法人資料若拿不到，就必須明確標示不可用，並由 Gate 禁止 BUY/TRIAL
    if sim_free:
        return {
            "inst_status": "UNAVAILABLE(SIM_FREE)",
            "inst_dir3": "MISSING",
            "inst_streak3": 0,
            "inst_dates_3d": [],
            "note": "SIM-FREE: 法人資料未接入（避免 402/付費限制），Gate 應視為降級禁止 BUY/TRIAL。",
        }
    return {
        "inst_status": "PENDING",
        "inst_dir3": "PENDING",
        "inst_streak3": 0,
        "inst_dates_3d": [],
        "note": "法人資料接入中",
    }

# ---------------------------
# Arbiter Input Builder + Data Health Gate
# ---------------------------
def data_health_gate(meta: Dict[str, Any], twii_date: Optional[str], top_df: pd.DataFrame, top_meta: Dict[str, Any],
                     inst: Dict[str, Any], amount: Dict[str, Any], latest_trade_day: Optional[date]) -> Dict[str, Any]:
    """
    依你 V15.6.x 精神：任何關鍵資料缺失 / 日期不符 → degraded_mode=true → 禁止 BUY/TRIAL
    """
    gate = {
        "degraded_mode": False,
        "degraded_reason": None,
        "kill_switch": False,
        "v14_watch": False,
        "market_status": "NORMAL",
    }

    # 必要：TopN 不能空
    if top_df is None or top_df.empty:
        gate["degraded_mode"] = True
        gate["degraded_reason"] = f"TOPN_EMPTY({top_meta.get('error')})"

    # 必要：指數日期要能核對「最新交易日」
    if latest_trade_day is not None and twii_date is not None:
        try:
            d = datetime.strptime(twii_date, "%Y-%m-%d").date()
            if d != latest_trade_day:
                gate["degraded_mode"] = True
                gate["degraded_reason"] = f"DATA_STALE(index_date={d}, latest={latest_trade_day})"
        except Exception:
            gate["degraded_mode"] = True
            gate["degraded_reason"] = "BAD_INDEX_DATE"

    # 法人不可用 → 依你的規則，Conservative 不得單靠技術；但在 SIM-FREE 我們直接標示降級最安全
    if inst.get("inst_status") not in ("READY",):
        gate["degraded_mode"] = True
        gate["degraded_reason"] = gate["degraded_reason"] or f"INST_NOT_READY({inst.get('inst_status')})"

    # 成交金額：若總額不可得，不一定要降級，但會提高保守性（你也遇過這塊常失敗）
    # 這裡不強制降級，改放 warning
    if amount.get("total_yi") is None:
        gate["market_status"] = "DEGRADED" if gate["degraded_mode"] else "NORMAL"

    gate["market_status"] = "DEGRADED" if gate["degraded_mode"] else "NORMAL"
    return gate

def enforce_decision_action_consistency(decision: str, action_size_pct: int) -> Tuple[str, int, Optional[str]]:
    """
    你的規則：BUY/TRIAL 必須 >0；HOLD/WATCH 必須=0；REDUCE<0；不一致→全 WATCH
    """
    d = (decision or "").upper().strip()
    err = None
    if d in ("BUY", "TRIAL") and action_size_pct <= 0:
        err = "DECISION_SIZE_MISMATCH"
    if d in ("HOLD", "WATCH") and action_size_pct != 0:
        err = "DECISION_SIZE_MISMATCH"
    if d == "REDUCE" and action_size_pct >= 0:
        err = "DECISION_SIZE_MISMATCH"
    if d == "SELL" and action_size_pct != -100:
        err = "DECISION_SIZE_MISMATCH"

    if err:
        return "WATCH", 0, err
    return d, action_size_pct, None

def build_arbiter_input(
    session: str,
    topn: int,
    positions: List[Dict[str, Any]],
    cash_balance: int,
    total_equity: int,
    verify_ssl: bool,
    sim_free: bool = True,
) -> Dict[str, Any]:

    ts_now = now_taipei()
    latest_trade_day = latest_trading_day_from_yfinance()

    # 1) Index / VIX / SMR / MA
    twii = compute_index_meta(session=session)
    ma = compute_ma200_and_smr()
    ma14 = compute_ma14_monthly()
    vix = compute_vix()
    drawdown = compute_drawdown_pct()

    smr = ma.get("smr", 0.0) if ma.get("smr") is not None else 0.0
    slope5 = ma.get("slope5", 0.0) if ma.get("slope5") is not None else 0.0
    vix_val = vix.get("vix", None)
    twii_close = twii.get("close", None)

    boolean_status = compute_boolean_status(smr=smr, slope5=slope5)

    current_regime = compute_regime(
        smr=smr,
        slope5=slope5,
        vix=vix_val if vix_val is not None else 0.0,
        drawdown_pct=drawdown,
        ma14_monthly=ma14.get("ma14_monthly", None),
        twii_close=twii_close,
    )

    # 2) amount_total (best-effort)
    amount = compute_amount_total_best_effort(verify_ssl=verify_ssl)

    # 3) TopN by turnover
    top_df, top_meta = build_topn_by_turnover(topn=topn, verify_ssl=verify_ssl)

    # 4) Institutional (SIM-FREE stub)
    inst = compute_institutional_stub(sim_free=sim_free)

    # 5) Gate
    gate = data_health_gate(
        meta={},
        twii_date=twii.get("date"),
        top_df=top_df,
        top_meta=top_meta,
        inst=inst,
        amount=amount,
        latest_trade_day=latest_trade_day,
    )

    # 6) Build stocks[] with top20_flag + tier
    stocks = []
    top_symbols = set(top_df["symbol"].tolist()) if not top_df.empty else set()

    # Orphan holding: in positions but not in topN
    pos_symbols = set([p.get("symbol") for p in positions if p.get("symbol")])

    # TopN stocks
    for _, r in top_df.iterrows():
        sym = r["symbol"]
        tier = 1 if int(r["rank"]) <= 10 else 2
        stocks.append({
            "symbol": sym,
            "name": r.get("name") or "",
            "price": r.get("close"),
            "rank": int(r["rank"]),
            "tier_level": tier,
            "top20_flag": True,
            "ret20_pct": r.get("ret20_pct"),
            "vol_ratio": r.get("vol_ratio"),
            "ma_bias_pct": r.get("ma_bias_pct"),
            "volume": r.get("volume"),
            "score": r.get("score"),
            "inst": {
                "inst_status": inst.get("inst_status"),
                "inst_dir3": inst.get("inst_dir3"),
                "inst_streak3": inst.get("inst_streak3"),
            }
        })

    # Orphan holdings appended (not counted in topN ranking)
    for sym in sorted(list(pos_symbols - top_symbols)):
        tech = compute_stock_tech(sym)
        stocks.append({
            "symbol": sym,
            "name": "",
            "price": tech.get("close"),
            "rank": None,
            "tier_level": None,
            "top20_flag": False,
            "orphan_holding": True,
            "ret20_pct": tech.get("ret20_pct"),
            "vol_ratio": tech.get("vol_ratio"),
            "ma_bias_pct": tech.get("ma_bias_pct"),
            "volume": tech.get("volume"),
            "score": None,
            "inst": {
                "inst_status": inst.get("inst_status"),
                "inst_dir3": inst.get("inst_dir3"),
                "inst_streak3": inst.get("inst_streak3"),
            }
        })

    # 7) Build meta / macro
    arb = {
        "meta": {
            "system": "Predator V15.7 (SIM-FREE / Top20+Positions)",
            "timestamp": dt_str(ts_now),
            "session": session,
            "market": "tw-share",
            "topn_target": int(topn),
            "topn_actual": int(len(top_df)) if top_df is not None else 0,
            "snapshot_date": twii.get("date"),
            "snapshot_source": top_meta.get("source"),
            "verify_ssl": bool(verify_ssl),
        },
        "market_meta": {
            "taiex": {
                "date": twii.get("date"),
                "close": twii.get("close"),
                "chg": twii.get("chg"),
                "chg_pct": twii.get("chg_pct"),
                "source": twii.get("source"),
                "error": twii.get("error"),
            },
            "vix": {
                "date": vix.get("date"),
                "value": vix.get("vix"),
                "source": vix.get("source"),
                "error": vix.get("error"),
                "dynamic_vix_threshold": DEFAULT_DYNAMIC_VIX_THRESHOLD,
            },
            "regime_metrics": {
                "MA200": ma.get("ma200"),
                "SMR": ma.get("smr"),
                "SMR_MA5": ma.get("smr_ma5"),
                "Slope5": ma.get("slope5"),
                "MA14_Monthly": ma14.get("ma14_monthly"),
                "drawdown_pct": round(drawdown, 4),
            },
            "boolean_status": boolean_status,
            "current_regime": current_regime,
        },
        "macro": {
            "overview": {
                "trade_date": twii.get("date"),
                "data_mode": session,
                "amount_twse_yi": amount.get("twse_yi"),
                "amount_tpex_yi": amount.get("tpex_yi"),
                "amount_total_yi": amount.get("total_yi"),
                "amount_sources": amount.get("sources"),
                "amount_warning": amount.get("warning"),
                "inst_status": inst.get("inst_status"),
                "inst_dir3": inst.get("inst_dir3"),
                "inst_streak3": inst.get("inst_streak3"),
                "inst_dates_3d": inst.get("inst_dates_3d"),
                "data_date_proxy": str(latest_trade_day) if latest_trade_day else None,
                "kill_switch": gate.get("kill_switch", False),
                "v14_watch": gate.get("v14_watch", False),
                "degraded_mode": gate.get("degraded_mode", False),
                "degraded_reason": gate.get("degraded_reason"),
            }
        },
        "account": {
            "cash_balance": int(cash_balance),
            "total_equity": int(total_equity),
            "positions": positions,
        },
        "stocks": stocks,
        "portfolio_summary": {
            "total_equity": int(total_equity),
            "max_equity_allowed_pct": 40.0 if current_regime == "OVERHEAT" else 70.0,
            "current_exposure_pct": round((1.0 - (cash_balance / total_equity)) * 100.0, 4) if total_equity > 0 else 0.0,
            "cash_pct": round((cash_balance / total_equity) * 100.0, 4) if total_equity > 0 else 100.0,
        },
        "gate": {
            "market_status": gate.get("market_status"),
            "degraded_mode": gate.get("degraded_mode"),
            "degraded_reason": gate.get("degraded_reason"),
            "note": "若 degraded_mode=true → Arbiter 必須禁止 BUY/TRIAL（符合你的風控哲學）。"
        },
        "risk_alerts": [],
    }

    # 信用壓力資料未接入 → 明確告知（不讓 Arbiter 推論）
    if boolean_status.get("CREDIT_STRESS") == "No":
        arb["risk_alerts"].append("CREDIT_STRESS_DATA_NOT_CONNECTED")

    # amount 不可得 → 提醒
    if amount.get("total_yi") is None:
        arb["risk_alerts"].append("AMOUNT_TOTAL_UNAVAILABLE(best-effort)")

    # TopN 來源錯誤 → 提醒
    if top_meta.get("error"):
        arb["risk_alerts"].append(f"TOPN_BUILD_FAILED({top_meta.get('error')})")

    return arb
