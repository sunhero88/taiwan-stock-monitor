# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.16-HI_STOCK）
# 針對「雲端 IP 被封鎖」的突圍版
#
# 關鍵修正 (V16.3.16)：
# 1. [新增來源] 加入 HiStock (嗨投資) 網頁解析，作為對抗 IP 封鎖的奇兵。
# 2. [除錯面板] 新增 Debug Logs 顯示區，讓你知道為什麼 Yahoo/CnYES 失敗。
# 3. [穩定架構] 維持 TopN=10 與 GC 機制，確保伺服器不當機。
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
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3.16）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / V16.3.16-HI_STOCK）"
st.title(APP_TITLE)

# =========================
# Global Constants
# =========================
# 雲端環境資源有限，強制預設較小的監控數量
DEFAULT_TOPN = 10
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
# Warnings & Debug recorder
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
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/json,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    return s

# =========================
# 核心修復：HiStock/鉅亨網/Yahoo 抓取邏輯 (V16.3.16)
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
    """上市 (TWSE) 抓取"""
    date_str = trade_date.replace("-", "")
    url = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date_str}"
    
    # 1. 官方 API
    try:
        r = _http_session().get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if 'data' in data and len(data['data']) > 0:
                val_str = data['data'][-1][2].replace(',', '')
                return int(val_str), "TWSE_OFFICIAL_API"
        else:
            warnings_bus.push("TWSE_API_ERR", f"Status: {r.status_code}")
    except Exception as e:
        warnings_bus.push("TWSE_API_FAIL", str(e))
    
    # 2. Yahoo 估算
    try:
        t = yf.Ticker("^TWII")
        h = t.history(period="5d") 
        if not h.empty:
            last_row = h.iloc[-1]
            est = int(last_row['Volume'] * last_row['Close'] * 0.5) 
            return est, "TWSE_YAHOO_EST"
        else:
            warnings_bus.push("TWSE_YAHOO_EMPTY", "Yahoo returned no data")
    except Exception as e:
        warnings_bus.push("TWSE_YAHOO_FAIL", str(e))
    
    return 300_000_000_000, "TWSE_SAFE_MODE"

def _fetch_tpex_robust(trade_date: str) -> Tuple[int, str]:
    """
    上櫃 (TPEX) 抓取 - 雲端突圍版
    策略：HiStock (新) -> 鉅亨網 -> Yahoo 解析 -> Yahoo 估算
    """
    
    # 策略 0: HiStock (嗨投資) - 新增救援王
    # 說明：此網站結構簡單，較少阻擋雲端 IP
    try:
        url = "https://histock.tw/index/TWO"
        r = _http_session().get(url, timeout=8)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            # HiStock 的成交值通常在特定的區塊
            # 我們搜尋 "成交金額" 關鍵字
            found_val = None
            
            # 方法 A: 找含有 "億" 的文字
            for span in soup.find_all(['span', 'div', 'li']):
                text = span.get_text()
                if "成交金額" in text or "成交值" in text:
                    # 尋找附近的數字
                    match = re.search(r'(\d{3,5}\.?\d*)', text)
                    if match:
                        val = float(match.group(1))
                        # HiStock 單位通常是億
                        if val < 10000: found_val = int(val * 100_000_000)
                        else: found_val = int(val)
                        break
            
            if found_val and found_val > 10_000_000_000:
                return found_val, "HISTOCK_WEB_PARSE"
            else:
                warnings_bus.push("HISTOCK_PARSE_FAIL", "Found nothing valid")
        else:
            warnings_bus.push("HISTOCK_HTTP_FAIL", f"Status: {r.status_code}")
    except Exception as e:
        warnings_bus.push("HISTOCK_FAIL", str(e))

    # 策略 1: 鉅亨網 API
    try:
        url = "https://market-api.api.cnyes.com/nexus/api/v2/mainland/index/quote"
        params = {"symbols": "OTC:OTC01:INDEX"}
        r = _http_session().get(url, params=params, timeout=5)
        if r.status_code == 200:
            items = r.json().get('data', {}).get('items', [])
            for item in items:
                if 'OTC' in item.get('symbol', ''):
                    val = item.get('turnover')
                    if val:
                        amt = float(val)
                        if amt < 10000: amt = int(amt * 100_000_000) 
                        else: amt = int(amt)
                        if amt > 10_000_000_000:
                            return amt, "CNYES_API"
        else:
            warnings_bus.push("CNYES_HTTP_FAIL", f"Status: {r.status_code}")
    except Exception as e:
        warnings_bus.push("CNYES_FAIL", str(e))

    # 策略 2: Yahoo 網頁解析
    try:
        url = "https://tw.stock.yahoo.com/quote/^TWO"
        r = _http_session().get(url, timeout=8)
        soup = BeautifulSoup(r.text, 'html.parser')
        found_val = None
        for span in soup.find_all('span'):
            if "成交值" in span.text:
                container = span.parent.parent
                if container:
                    match = re.search(r'成交值.*?(\d{3,5}\.?\d*)', container.get_text())
                    if match:
                        found_val = float(match.group(1))
                        break
        if found_val:
            return int(found_val * 100_000_000), "YAHOO_WEB_PARSE"
        else:
            warnings_bus.push("YAHOO_PARSE_FAIL", "Selector failed")
    except Exception as e:
        warnings_bus.push("YAHOO_REQ_FAIL", str(e))

    # 策略 3: Yahoo Finance 估算
    try:
        t = yf.Ticker("^TWO")
        h = t.history(period="5d") 
        if not h.empty:
            last_row = h.iloc[-1]
            est = int(last_row['Volume'] * last_row['Close'] * 1000 * 0.6)
            if est < 10_000_000_000: est *= 1000
            return est, "YAHOO_EST_CALC"
        else:
            warnings_bus.push("YAHOO_YF_EMPTY", "yfinance returned empty")
    except Exception as e:
        warnings_bus.push("YAHOO_YF_FAIL", str(e))

    # 策略 4: 保底值
    return 1_700_000_000_000, "DOOMSDAY_SAFE_VAL_1700B" 

def fetch_amount_total(trade_date: str) -> MarketAmount:
    _ensure_dir(AUDIT_DIR)
    twse_amt, twse_src = _fetch_twse_robust(trade_date)
    tpex_amt, tpex_src = _fetch_tpex_robust(trade_date)
    total = twse_amt + tpex_amt
    return MarketAmount(twse_amt, tpex_amt, total, twse_src, tpex_src, "FULL", {"trade_date": trade_date})

# =========================
# Data Fetchers
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def fetch_history_light(symbol: str) -> pd.DataFrame:
    try:
        df = yf.download(symbol, period="1y", interval="1d", progress=False, threads=False)
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
        data = yf.download(symbols, period="6mo", progress=False, group_by="ticker", threads=False)
        for i, sym in out.iterrows():
            try:
                if len(symbols) == 1:
                    df = data
                else:
                    if isinstance(data.columns, pd.MultiIndex):
                        try: df = data.xs(sym, axis=1, level=0)
                        except: continue
                    else: continue

                if isinstance(df, pd.DataFrame) and not df.empty:
                    close = df["Close"].dropna() if "Close" in df.columns else pd.Series()
                    vol = df["Volume"].dropna() if "Volume" in df.columns else pd.Series()
                    
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
    if market_df.empty or len(market_df) < 60: return {"SMR": None, "Slope5": None}
    close = market_df["Close"]
    ma200 = close.rolling(200).mean()
    smr_series = ((close - ma200) / ma200).dropna()
    if smr_series.empty: return {"SMR": None}
    smr = float(smr_series.iloc[-1])
    slope5 = float(smr_series.rolling(5).mean().diff().iloc[-1]) if len(smr_series) > 5 else 0.0
    return {"SMR": smr, "Slope5": slope5}

def pick_regime(metrics: dict, vix: float) -> Tuple[str, float]:
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    if vix > 35.0: return "CRASH_RISK", 0.10
    if smr is not None and slope5 is not None:
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
    gc.collect() # 記憶體回收
    
    # 1. Market
    twii = fetch_history_light(TWII_SYMBOL)
    vix = fetch_history_light(VIX_SYMBOL)
    vix_last = float(vix["Close"].iloc[-1]) if not vix.empty else 20.0
    metrics = compute_regime_metrics(twii)
    regime, max_equity = pick_regime(metrics, vix_last)
    
    # 2. Amount
    date_str = twii.index[-1].strftime("%Y-%m-%d") if not twii.empty else _now_ts().split()[0]
    amount = fetch_amount_total(date_str)
    
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
                "trade_date": date_str,
                "twii_close": float(twii["Close"].iloc[-1]) if not twii.empty else 0,
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
    topn = st.sidebar.selectbox("TopN（監控數量 - 雲端版限制）", [5, 8, 10, 15], index=2)
    
    run_btn = st.sidebar.button("啟動中控台 (V16.3.16)")
    
    if run_btn:
        with st.spinner("正在執行多重數據源抓取 (HiStock/CnYES/Yahoo)..."):
            payload, warns = build_arbiter_input("INTRADAY", account_mode, topn, [], 2000000, 2000000, None)
            
        ov = payload["macro"]["overview"]
        amt = payload["macro"]["market_amount"]
        
        st.subheader("📊 市場儀表板 (V16.3.16-HI_STOCK)")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("加權指數", f"{ov['twii_close']:,.0f}")
        c2.metric("VIX", f"{ov['vix']:.2f}")
        
        amt_val = amt['amount_total'] / 100_000_000
        src_label = amt['source_tpex']
        
        c3.metric("總成交額 (億)", f"{amt_val:,.0f}", help=f"來源: {src_label}")
        c4.metric("SMR 乖離", f"{ov['smr']:.4f}")
        
        if "HISTOCK" in src_label:
            st.success(f"✅ 成功從 HiStock 獲取數據 ({src_label})")
        elif "CNYES" in src_label:
            st.success(f"✅ 成功從鉅亨網 API 獲取數據 ({src_label})")
        elif "YAHOO" in src_label:
            st.warning(f"⚠️ 使用 Yahoo 數據 ({src_label})")
        else:
            st.error(f"🔴 使用保底數據 ({src_label})")
            
        # Debug Panel
        with st.expander("🛠️ 系統診斷日誌 (Debug Logs)", expanded=False):
            if warns:
                st.dataframe(pd.DataFrame(warns)[['ts', 'code', 'msg']], use_container_width=True)
            else:
                st.write("無錯誤日誌。")
            st.json(payload)

if __name__ == "__main__":
    main()
