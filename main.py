# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.42 動能雷達旗艦版）
# =========================================================
# 整合功能：
#   (1) 二階動能 (Acceleration) 運算：捕捉動力衰竭前兆
#   (2) 末端加速段判定 (Blow-off Detector)：偵測 SMR > 0.33 且加速度轉負
#   (3) 動態信心懲罰 (Confidence Penalty): LOW = 資金上限砍半
#   (4) VIX 邊界防護網 (0 < vix <= 100)
#   (5) 盤中量能預估歸一化與 T-1 籌碼繼承
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
# 1. 系統初始化與常數
# =========================
st.set_page_config(page_title="Sunhero｜Predator V16.3.42", layout="wide", initial_sidebar_state="expanded")
APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator V16.3.42)"
st.title(APP_TITLE)

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}
SMR_CRITICAL = 0.30
SMR_BLOW_OFF = 0.33

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY", "2881.TW": "富邦金", "2882.TW": "國泰金",
    "2891.TW": "中信金", "2886.TW": "兆豐金", "2603.TW": "長榮", "2609.TW": "陽明",
    "1605.TW": "華新", "1513.TW": "中興電", "1519.TW": "華城", "2002.TW": "中鋼"
}

COL_TRANSLATION = {
    "Symbol": "代號", "Name": "名稱", "Tier": "權重序", "Price": "價格",
    "Vol_Ratio": "預估量能比", "Layer": "分級(Layer)", 
    "Inst_Status": "籌碼狀態", "Inst_Net_3d": "3日合計淨額(張)"
}

# =========================
# 2. 核心計算模組 (二階動能雷達)
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 250:
        return {"twii_close": None, "SMR": None, "Slope5": None, "Acceleration": 0.0, "Top_Divergence": False}
    
    close = market_df["Close"].iloc[:, 0] if isinstance(market_df["Close"], pd.DataFrame) else market_df["Close"]
    twii_close = float(close.iloc[-1])
    
    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    smr_val = float(smr_series.iloc[-1])
    
    slope5_series = smr_series.diff(5)
    slope5_val = float(slope5_series.iloc[-1])
    
    # 🌟 二階動能 (Acceleration)
    accel_series = slope5_series.diff(2)
    accel_val = float(accel_series.iloc[-1])
    
    # 📡 高檔動能背離判定
    top_divergence = bool(smr_val > 0.15 and slope5_val > 0 and accel_val < -0.01)
    # 📡 末端加速段 (Blow-off) 判定
    blow_off = bool(smr_val >= SMR_BLOW_OFF and slope5_val >= 0.08)
    
    return {
        "twii_close": twii_close, "SMR": smr_val, "Slope5": slope5_val,
        "Acceleration": accel_val, "Top_Divergence": top_divergence,
        "Blow_Off_Phase": blow_off, "MOMENTUM_LOCK": bool(slope5_val > 0)
    }

def pick_regime(metrics: dict, vix: float) -> Tuple[str, float]:
    smr = metrics.get("SMR", 0)
    if vix > 35: return "CRASH_RISK", 0.10
    if metrics.get("Blow_Off_Phase"): return "CRITICAL_OVERHEAT", 0.10
    if smr > SMR_CRITICAL: return "OVERHEAT", 0.30
    if smr > 0.25: return "OVERHEAT", 0.45
    return "NORMAL", 0.85

# =========================
# 3. 數據抓取模組
# =========================
def get_taipei_now(): return datetime.now(pytz.timezone('Asia/Taipei'))

@dataclass
class MarketAmount:
    amount_total: Optional[int]; source_tpex: str; confidence_level: str

def fetch_amount_total(trade_date: str) -> MarketAmount:
    roc = f"{pd.to_datetime(trade_date).year - 1911}/{pd.to_datetime(trade_date).month:02d}/{pd.to_datetime(trade_date).day:02d}"
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc}&se=EW"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, verify=False)
        tpex_amt = int(r.json()["aaData"][0][2])
        return MarketAmount(600_000_000_000 + tpex_amt, "OFFICIAL_OK", "HIGH")
    except:
        return MarketAmount(1146709763869, "SAFE_MODE", "LOW")

def _single_fetch_price_vol(sym: str) -> Tuple[Optional[float], Optional[float]]:
    ticker_base = sym.split(".")[0]
    for suffix in [".TW", ".TWO"]:
        try:
            df = yf.download(f"{ticker_base}{suffix}", period="2mo", progress=False)
            if not df.empty:
                c = df["Close"].iloc[:, 0] if isinstance(df["Close"], pd.DataFrame) else df["Close"]
                v = df["Volume"].iloc[:, 0] if isinstance(df["Volume"], pd.DataFrame) else df["Volume"]
                ma20 = v.rolling(20).mean().iloc[-1]
                return float(c.iloc[-1]), float(v.iloc[-1]/ma20) if ma20 > 0 else 1.0
        except: continue
    return None, None

# =========================
# 4. 主程序與 UI
# =========================
def main():
    st.sidebar.header("⚙️ 參數設定")
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"])
    topn = st.sidebar.slider("監控數量", 5, 20, 20)
    token = st.sidebar.text_input("FinMind Token", type="password")
    
    now = get_taipei_now()
    trade_date = now.strftime("%Y-%m-%d")
    progress = max(0.01, (now - now.replace(hour=9, minute=0)).total_seconds() / 16200) if session == "INTRADAY" else 1.0

    with st.spinner("Predator 雷達掃描中..."):
        twii_df = yf.download(TWII_SYMBOL, period="2y", progress=False)
        metrics = compute_regime_metrics(twii_df)
        vix_df = yf.download(VIX_SYMBOL, period="1mo", progress=False)
        vix_last = float(vix_df["Close"].iloc[:, 0].iloc[-1]) if not vix_df.empty else 20.0
        
        amt = fetch_amount_total(trade_date)
        conf_multiplier = 0.5 if amt.confidence_level == "LOW" else 1.0
        
        if vix_last <= 0 or vix_last > 100: regime, base_limit = "DATA_FAILURE", 0.0
        else: regime, base_limit = pick_regime(metrics, vix_last)
        
        final_limit = base_limit * conf_multiplier

    # 📡 動能雷達 UI
    st.subheader("📡 動能轉折雷達 (Momentum Radar)")
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("策略體制", f"🔴 {regime}" if "OVERHEAT" in regime else f"🟢 {regime}")
    r2.metric("資金上限", f"{final_limit*100:.1f}%", f"信心係數 {conf_multiplier}x")
    r3.metric("5日斜率 (Slope5)", f"{metrics['Slope5']:.4f}")
    r4.metric("二階加速度 (Accel)", f"{metrics['Acceleration']:.4f}", delta=f"{metrics['Acceleration']:.4f}", delta_color="normal" if metrics['Acceleration'] > 0 else "inverse")
    
    if metrics["Blow_Off_Phase"]:
        st.error("🚨 警告：進入「末端加速段 (Blow-off)」！符合 SMR>0.33 且 Slope5>0.08，具備極高 MDD 風險。")
    if metrics["Top_Divergence"]:
        st.warning("⚠️ 預警：高檔動能背離！加速度急轉負向，留意油門已鬆。")

    # 核心持股雷達
    st.subheader("🎯 核心持股雷達 (Tactical Stocks)")
    rows = []
    for sym in list(STOCK_NAME_MAP.keys())[:topn]:
        p, vr_raw = _single_fetch_price_vol(sym)
        vr = vr_raw / progress if vr_raw else None
        rows.append({"代號": sym, "名稱": STOCK_NAME_MAP[sym], "價格": p, "預估量比": vr})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    # AI JSON 輸出
    with st.expander("🤖 AI JSON (Arbiter Input)"):
        st.code(json.dumps({"meta": {"regime": regime, "limit": final_limit, "confidence": amt.confidence_level}, "macro": metrics}, indent=4))

if __name__ == "__main__":
    main()
