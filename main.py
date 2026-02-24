# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.43 架構校準版）
# =========================================================
from __future__ import annotations
import json, os, re, time, requests, warnings, pytz
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

warnings.filterwarnings('ignore')

# =========================
# 1. 初始化與常數
# =========================
st.set_page_config(
    page_title="Sunhero｜Predator V16.3.43",
    layout="wide",
    initial_sidebar_state="expanded"
)

APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator V16.3.43 校準版)"
st.title(APP_TITLE)

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
SMR_BLOW_OFF = 0.33

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY", "2881.TW": "富邦金", "2882.TW": "國泰金",
    "2891.TW": "中信金", "2886.TW": "兆豐金", "2603.TW": "長榮", "2609.TW": "陽明",
    "1605.TW": "華新", "1513.TW": "中興電", "1519.TW": "華城", "2002.TW": "中鋼"
}

# =========================
# 2. 核心計算模組
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 250:
        return {"twii_close": None, "SMR": None, "Slope5": None, "Acceleration": 0.0, "Blow_Off_Phase": False}
    
    close = market_df["Close"].iloc[:, 0] if isinstance(market_df["Close"], pd.DataFrame) else market_df["Close"]
    twii_close = float(close.iloc[-1])
    
    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    slope5_series = smr_series.diff(5)
    accel_series = slope5_series.diff(2)
    
    smr_val = float(smr_series.iloc[-1])
    slope5_val = float(slope5_series.iloc[-1])
    accel_val = float(accel_series.iloc[-1])
    
    return {
        "twii_close": twii_close,
        "SMR": smr_val,
        "Slope5": slope5_val,
        "Acceleration": accel_val,
        "Blow_Off_Phase": bool(smr_val >= SMR_BLOW_OFF and slope5_val >= 0.08),
        "MOMENTUM_LOCK": bool(slope5_val > 0)
    }

# =========================
# 3. 數據抓取模組 (Schema 修正)
# =========================
def get_taipei_now(): return datetime.now(pytz.timezone('Asia/Taipei'))

@dataclass
class MarketAmount:
    amount_total_raw: int
    amount_total_blended: int
    confidence_level: str
    status_tpex: str

def fetch_blended_amount(trade_date: str) -> MarketAmount:
    # 這裡實作 Raw (僅確定來源) 與 Blended (含估算) 的區分
    try:
        # 模擬從官方抓取的真實數據
        twse_raw = 946709763869
        # 若 TPEX 斷鏈，則標記為 ESTIMATED 且降低信心
        return MarketAmount(twse_raw, twse_raw + 200000000000, "LOW", "ESTIMATED")
    except:
        return MarketAmount(0, 0, "LOW", "FAIL")

def _single_fetch(sym: str) -> Tuple[Optional[float], Optional[float]]:
    try:
        df = yf.download(sym, period="2mo", progress=False)
        if not df.empty:
            c = df["Close"].iloc[:, 0] if isinstance(df["Close"], pd.DataFrame) else df["Close"]
            v = df["Volume"].iloc[:, 0] if isinstance(df["Volume"], pd.DataFrame) else df["Volume"]
            ma20 = v.rolling(20).mean().iloc[-1]
            return float(c.iloc[-1]), float(v.iloc[-1]/ma20) if ma20 > 0 else 1.0
    except: pass
    return None, None

def fetch_inst_data(symbols: List[str], target_date: str, token: str) -> dict:
    # 此處為佔位符，實際應調用 FinMind
    return {}

# =========================
# 4. 主程序與 UI
# =========================
def main():
    st.sidebar.header("⚙️ 系統設定")
    session = st.sidebar.selectbox("時段", ["INTRADAY", "EOD"])
    topn = st.sidebar.slider("監控 TopN", 5, 20, 20)
    token = st.sidebar.text_input("FinMind Token (選填)", type="password")
    
    now = get_taipei_now()
    trade_date = now.strftime("%Y-%m-%d")
    progress = max(0.01, (now - now.replace(hour=9, minute=0)).total_seconds() / 16200) if session == "INTRADAY" else 1.0

    with st.spinner("Predator 引擎正在進行架構校準掃描..."):
        # 市場宏觀
        twii_df = yf.download(TWII_SYMBOL, period="2y", progress=False)
        m = compute_regime_metrics(twii_df)
        vix_df = yf.download(VIX_SYMBOL, period="1mo", progress=False)
        v_s = vix_df["Close"].iloc[:, 0] if isinstance(vix_df["Close"], pd.DataFrame) else vix_df["Close"]
        vix_last = float(v_s.iloc[-1]) if not vix_df.empty else 20.0
        
        # 信心與狀態自動降級
        amt_info = fetch_blended_amount(trade_date)
        market_status = "NORMAL"
        if amt_info.confidence_level != "HIGH":
            market_status = "DEGRADED" # 🌟 核心修正：自動降級
        
        # 體制與資金上限 (已包含信心懲罰)
        smr = m["SMR"]
        conf_penalty = 0.5 if amt_info.confidence_level == "LOW" else 1.0
        
        if vix_last <= 0 or vix_last > 100: regime, base_limit = "DATA_FAILURE", 0.0
        elif m["Blow_Off_Phase"]: regime, base_limit = "CRITICAL_OVERHEAT", 0.10
        elif smr > 0.25: regime, base_limit = "OVERHEAT", 0.40
        else: regime, base_limit = "NORMAL", 0.85
        
        final_limit = base_limit * conf_penalty

    # UI 呈現
    st.subheader("📡 市場體制與風險雷達")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("市場狀態", market_status, delta=amt_info.confidence_level)
    c2.metric("策略體制", regime)
    c3.metric("建議持倉上限", f"{final_limit*100:.1f}%")
    c4.metric("二階加速度", f"{m['Acceleration']:.4f}")

    if m["Blow_Off_Phase"]:
        st.error("🚨 警告：進入末端加速段 (Blow-off)！符合 SMR>0.33 且 Slope5>0.08。")
    if amt_info.confidence_level == "LOW":
        st.warning("⚠️ 注意：數據信心等級為 LOW。市場成交量為估算值，market_status 已降級。")

    # 個股清單 (Schema 校準版)
    st.subheader("🎯 核心持股雷達 (Schema 校準版)")
    rows = []
    for i, sym in enumerate(list(STOCK_NAME_MAP.keys())[:topn], 1):
        price, vr_raw = _single_fetch(sym)
        vr = vr_raw / progress if vr_raw else None
        
        # 🌟 核心修正：Tier 拆分為 rank 與 tier_level
        # 🌟 核心修正：Inst_Net_3d 設為 None (null) 代表未更新，而非 0.0
        rows.append({
            "Symbol": sym,
            "Name": STOCK_NAME_MAP[sym],
            "rank": i,
            "tier_level": 2 if smr > 0.25 else 1, # 過熱環境強制標記為 Weak (2)
            "Price": price,
            "Vol_Ratio": vr,
            "Institutional": {
                "Inst_Status": "NO_UPDATE_TODAY",
                "Inst_Net_3d": None, # ❌ 禁止假零
                "inst_freshenss": False
            }
        })
    
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    # JSON 輸出 (Arbiter Input)
    st.markdown("---")
    st.subheader("🤖 AI JSON (校準後的數據結構)")
    payload = {
        "meta": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "market_status": market_status,
            "current_regime": regime,
            "confidence_level": amt_info.confidence_level
        },
        "macro": {
            "overview": m,
            "market_amount": asdict(amt_info)
        },
        "stocks": rows
    }
    st.code(json.dumps(payload, indent=4, ensure_ascii=False), language="json")

if __name__ == "__main__":
    main()
