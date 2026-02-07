# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.18-FINAL）
# 針對雲端環境 + 假日 + IP封鎖 的最終防崩潰版本
#
# [核心修復]
# 1. UI 防爆：修復 SMR/VIX 為 None 時導致的 TypeError 崩潰。
# 2. 假日邏輯：自動鎖定「最近交易日」，避免週六抓不到數據。
# 3. 資源控管：維持 TopN=8，保護雲端主機記憶體。
# =========================================================

from __future__ import annotations

import json
import os
import re
import time
import gc
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf
from bs4 import BeautifulSoup 
import warnings

# 抑制警告
warnings.filterwarnings('ignore')

# =========================
# Streamlit page config
# =========================
st.set_page_config(
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3.18）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / V16.3.18-FINAL）"
st.title(APP_TITLE)

# =========================
# Global Constants
# =========================
DEFAULT_TOPN = 8  # 雲端安全值
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
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

# =========================
# Helpers
# =========================
def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _safe_int(x, default=None) -> Optional[int]:
    try:
        if x is None: return default
        if isinstance(x, (int, float)): return int(x)
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            return int(float(s)) if s else default
        return int(x)
    except:
        return default

def _pct01_to_pct100(x: Optional[float]) -> Optional[float]:
    return float(x) * 100.0 if x is not None else None

# =========================
# Warnings recorder
# =========================
class WarningBus:
    def __init__(self):
        self.items = []
    def push(self, code: str, msg: str, meta: Optional[dict] = None):
        self.items.append({"ts": _now_ts(), "code": code, "msg": msg, "meta": meta or {}})
    def latest(self, n: int = 50):
        return self.items[-n:]

warnings_bus = WarningBus()

# =========================
# Session Management
# =========================
def _http_session() -> requests.Session:
    s = requests.Session()
    # 盡量模擬真實瀏覽器，減少被擋機率
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/json,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    return s

# =========================
# 核心修復：數據抓取邏輯
# =========================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
    scope: str
    meta: Optional[Dict[str, Any]] = None

def _fetch_twse_robust(trade_date: str) -> Tuple[int, str]:
    """上市 (TWSE)"""
    date_str = trade_date.replace("-", "")
    
    # 1. 官方 API
    try:
        url = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date_str}"
        r = _http_session().get(url, timeout=3) # 縮短超時，避免卡住
        if r.status_code == 200:
            data = r.json()
            if 'data' in data and len(data['data']) > 0:
                val_str = data['data'][-1][2].replace(',', '')
                return int(val_str), "TWSE_OFFICIAL_API"
    except Exception:
        pass
    
    # 2. Yahoo 估算 (5日保護)
    try:
        t = yf.Ticker("^TWII")
        h = t.history(period="5d") 
        if not h.empty:
            last = h.iloc[-1]
            est = int(last['Volume'] * last['Close'] * 0.5) 
            return est, "TWSE_YAHOO_EST"
    except:
        pass
    
    return 300_000_000_000, "TWSE_SAFE_MODE"

def _fetch_tpex_robust(trade_date: str) -> Tuple[int, str]:
    """上櫃 (TPEX) - 雲端多重備援"""
    
    # 1. HiStock (結構簡單，較不易擋)
    try:
        url = "https://histock.tw/index/TWO"
        r = _http_session().get(url, timeout=5)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for span in soup.find_all(['span', 'div']):
                if "成交金額" in span.text or "成交值" in span.text:
                    match = re.search(r'(\d{3,5}\.?\d*)', span.text)
                    if match:
                        val = float(match.group(1))
                        amt = int(val * 100_000_000) if val < 10000 else int(val)
                        if amt > 10_000_000_000: return amt, "HISTOCK_WEB"
    except Exception:
        pass

    # 2. 鉅亨網 API
    try:
        url = "https://market-api.api.cnyes.com/nexus/api/v2/mainland/index/quote"
        params = {"symbols": "OTC:OTC01:INDEX"}
        r = _http_session().get(url, params=params, timeout=3)
        if r.status_code == 200:
            items = r.json().get('data', {}).get('items', [])
            for item in items:
                if 'OTC' in item.get('symbol', ''):
                    val = item.get('turnover')
                    if val:
                        amt = float(val)
                        if amt < 10000: amt = int(amt * 100_000_000) 
                        else: amt = int(amt)
                        if amt > 10_000_000_000: return amt, "CNYES_API"
    except Exception:
        pass

    # 3. Yahoo Finance 估算 (5日保護)
    try:
        t = yf.Ticker("^TWO")
        h = t.history(period="5d") 
        if not h.empty:
            last = h.iloc[-1]
            est = int(last['Volume'] * last['Close'] * 1000 * 0.6)
            if est < 10_000_000_000: est *= 1000
            return est, "YAHOO_EST_CALC"
    except Exception:
        pass

    # 4. 保底值
    return 1_700_000_000_000, "DOOMSDAY_SAFE_VAL_1700B" 

def fetch_amount_total(trade_date: str) -> MarketAmount:
    _ensure_dir(AUDIT_DIR)
    twse_amt, twse_src = _fetch_twse_robust(trade_date)
    tpex_amt, tpex_src = _fetch_tpex_robust(trade_date)
    total = twse_amt + tpex_amt
    return MarketAmount(twse_amt, tpex_amt, total, twse_src, tpex_src, "FULL", {"trade_date": trade_date})

# =========================
# Data Fetchers (Light)
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def fetch_history_light(symbol: str) -> pd.DataFrame:
    try:
        # [CRITICAL] 抓5天，確保遇到假日不回傳空值
        df = yf.download(symbol, period="5d", interval="1d", progress=False, threads=False)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(symbol, axis=1, level=1, drop_level=True)
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_batch_light(symbols: List[str]) -> pd.DataFrame:
    out = pd.DataFrame({"Symbol": symbols, "Price": None, "Vol_Ratio": None})
    if not symbols: return out
    try:
        # [CRITICAL] 抓5天
        data = yf.download(symbols, period="5d", progress=False, group_by="ticker", threads=False)
        for i, sym in out.iterrows():
            try:
                if len(symbols) == 1: df = data
                else:
                    if isinstance(data.columns, pd.MultiIndex):
                        try: df = data.xs(sym, axis=1, level=0)
                        except: continue
                    else: continue

                if isinstance(df, pd.DataFrame) and not df.empty:
                    close = df["Close"].dropna()
                    vol = df["Volume"].dropna()
                    if not close.empty: out.at[i, "Price"] = float(close.iloc[-1])
                    if len(vol) >= 20:
                        ma20 = vol.rolling(20).mean().iloc[-1]
                        if ma20 > 0: out.at[i, "Vol_Ratio"] = float(vol.iloc[-1] / ma20)
            except: pass
    except: pass
    return out

# =========================
# Logic Builders
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df.empty or len(market_df) < 5: 
        return {"SMR": None, "Slope5": None} # 數據不足回傳 None
    
    close = market_df["Close"]
    ma200 = close.rolling(200).mean()
    smr_series = ((close - ma200) / ma200).dropna()
    
    if smr_series.empty: return {"SMR": None}
    
    smr = float(smr_series.iloc[-1])
    slope5 = 0.0
    if len(smr_series) >= 2: # 避免索引錯誤
        slope5 = float(smr_series.rolling(5).mean().diff().iloc[-1])
        
    return {"SMR": smr, "Slope5": slope5}

def pick_regime(metrics: dict, vix: float) -> Tuple[str, float]:
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    
    # [FIX] 如果 SMR 是 None，直接回傳預設值，不要報錯
    if smr is None: return "DATA_INSUFFICIENT", 0.0
    if slope5 is None: slope5 = 0.0

    if vix > 35.0: return "CRASH_RISK", 0.10
    if smr >= SMR_WATCH and slope5 < 0: return "MEAN_REVERSION_WATCH", 0.55
    if smr > 0.25: return "OVERHEAT", 0.55
    if 0.08 <= smr <= 0.18: return "CONSOLIDATION", 0.65
    return "NORMAL", 0.85

def classify_layer(regime: str, vol_ratio: Optional[float], inst: dict) -> str:
    streak = inst.get("Inst_Streak3", 0)
    if streak >= 3: return "A"
    if vol_ratio and vol_ratio > 0.8 and regime == "NORMAL": return "B"
    return "NONE"

# =========================
# Main Execution
# =========================
def build_arbiter_input(session, account_mode, topn, positions, cash, equity, token):
    gc.collect() 
    
    # 1. Market Data
    twii = fetch_history_light(TWII_SYMBOL)
    vix = fetch_history_light(VIX_SYMBOL)
    
    # [TIME MACHINE FIX] 自動鎖定最後有效交易日
    if not twii.empty:
        last_dt = twii.index[-1] # 這會抓到週五的日期
        trade_date_str = last_dt.strftime("%Y-%m-%d")
        
        # 使用最後一筆收盤價，而不是 iloc[-1] (如果是空的)
        twii_close = float(twii["Close"].iloc[-1])
        
        if not vix.empty:
            vix_last = float(vix["Close"].iloc[-1])
        else:
            vix_last = 20.0 # VIX保底
            
        amount = fetch_amount_total(trade_date_str)
    else:
        # 完全抓不到歷史數據 (嚴重IP封鎖)
        trade_date_str = _now_ts().split()[0]
        twii_close = 0.0
        vix_last = 20.0
        amount = MarketAmount(None, None, None, "FAIL", "FAIL", "NONE")

    metrics = compute_regime_metrics(twii)
    regime, max_equity = pick_regime(metrics, vix_last)
    
    # 3. Stocks
    base_pool = list(STOCK_NAME_MAP.keys())[:topn] 
    pv = fetch_batch_light(base_pool)
    stocks = []
    for sym in base_pool:
        row = pv[pv["Symbol"] == sym]
        p = float(row["Price"].iloc[0]) if not row.empty and not pd.isna(row["Price"].iloc[0]) else None
        v = float(row["Vol_Ratio"].iloc[0]) if not row.empty and not pd.isna(row["Vol_Ratio"].iloc[0]) else None
        inst_data = {"Inst_Streak3": 0}
        layer = classify_layer(regime, v, inst_data)
        stocks.append({
            "Symbol": sym, "Name": STOCK_NAME_MAP.get(sym, sym), "Tier": 0, "Price": p, "Vol_Ratio": v, "Layer": layer, "Institutional": inst_data
        })

    market_status = "OK" if "HISTOCK" in amount.source_tpex or "CNYES" in amount.source_tpex else "ESTIMATED"
    if "DOOMSDAY" in amount.source_tpex: market_status = "SAFE_MODE"

    return {
        "meta": {"timestamp": _now_ts(), "market_status": market_status, "current_regime": regime},
        "macro": {
            "overview": {
                "trade_date": trade_date_str,
                "twii_close": twii_close,
                "vix": vix_last,
                "smr": metrics["SMR"],
                "max_equity_allowed_pct": max_equity
            },
            "market_amount": asdict(amount),
        },
        "portfolio": {"total_equity": equity, "cash_balance": cash, "active_alerts": []},
        "stocks": stocks,
    }, warnings_bus.latest()

# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定 (Settings)")
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"])
    topn = st.sidebar.selectbox("TopN（監控數量 - 雲端版限制）", [5, 8, 10, 15], index=1)
    
    run_btn = st.sidebar.button("啟動中控台 (V16.3.18)")
    
    if run_btn:
        with st.spinner("執行中 (自動校正交易日)..."):
            payload, warns = build_arbiter_input("INTRADAY", account_mode, topn, [], 2000000, 2000000, None)
            
        ov = payload["macro"]["overview"]
        amt = payload["macro"]["market_amount"]
        
        st.subheader(f"📊 市場儀表板 ({ov['trade_date']})")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("加權指數", f"{ov['twii_close']:,.0f}")
        c2.metric("VIX", f"{ov['vix']:.2f}")
        
        amt_val = (amt['amount_total'] or 0) / 100_000_000
        src_label = amt['source_tpex']
        
        c3.metric("總成交額 (億)", f"{amt_val:,.0f}", help=f"來源: {src_label}")
        
        # [CRITICAL FIX] 安全顯示 SMR，避免 TypeError
        smr_val = ov.get('smr')
        if smr_val is not None:
            c4.metric("SMR 乖離", f"{smr_val:.4f}")
        else:
            c4.metric("SMR 乖離", "N/A (數據不足)")
        
        if "HISTOCK" in src_label or "CNYES" in src_label:
            st.success(f"✅ 成功獲取數據 ({src_label})")
        elif "YAHOO" in src_label:
            st.warning(f"⚠️ 使用 Yahoo 數據 ({src_label})")
        else:
            st.error(f"🔴 使用保底數據 ({src_label})")
            
        with st.expander("🛠️ 系統診斷日誌", expanded=False):
            if warns: st.dataframe(pd.DataFrame(warns)[['code', 'msg']], use_container_width=True)
            st.json(payload)

if __name__ == "__main__":
    main()
