# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.43 架構校準版）
# =========================================================
# 核心修正：
#   (1) 修正 Tier 語意：拆分為 rank(1-20) 與 tier_level(Strong/Weak)
#   (2) 修正法人佔位符：未更新則 Inst_Net_3d 為 null，避免 0.0 誤判
#   (3) 狀態降級：confidence != HIGH 時 market_status 強制改為 DEGRADED
#   (4) 成交量雙軌制：區分 amount_total_raw 與 amount_total_blended
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

warnings.filterwarnings('ignore')

# =========================
# 1. 初始化與常數
# =========================
st.set_page_config(page_title="Sunhero｜Predator V16.3.43", layout="wide")
APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator V16.3.43 架構校準版)"
st.title(APP_TITLE)

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
SMR_CRITICAL = 0.30
SMR_BLOW_OFF = 0.33

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY", "2881.TW": "富邦金", "2882.TW": "國泰金",
    "2891.TW": "中信金", "2886.TW": "兆豐金", "2603.TW": "長榮", "2609.TW": "陽明",
    "1605.TW": "華新", "1513.TW": "中興電", "1519.TW": "華城", "2002.TW": "中鋼"
}

# =========================
# 2. 核心邏輯模組
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 250:
        return {"twii_close": None, "SMR": None, "Slope5": None, "Acceleration": 0.0}
    close = market_df["Close"].iloc[:, 0] if isinstance(market_df["Close"], pd.DataFrame) else market_df["Close"]
    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    slope5_series = smr_series.diff(5)
    accel_series = slope5_series.diff(2)
    return {
        "twii_close": float(close.iloc[-1]),
        "SMR": float(smr_series.iloc[-1]),
        "Slope5": float(slope5_series.iloc[-1]),
        "Acceleration": float(accel_series.iloc[-1]),
        "Blow_Off_Phase": bool(smr_series.iloc[-1] >= SMR_BLOW_OFF and slope5_series.iloc[-1] >= 0.08)
    }

# =========================
# 3. 數據抓取
# =========================
def get_taipei_now(): return datetime.now(pytz.timezone('Asia/Taipei'))

def fetch_blended_amount(trade_date: str) -> dict:
    # 邏輯：TWSE 為 Raw，TPEX 若為估算則信心 LOW
    try:
        # 此處簡化為模擬數據
        twse_raw = 946709763869
        tpex_est = 200000000000
        return {
            "amount_total_raw": twse_raw,
            "amount_total_blended": twse_raw + tpex_est,
            "confidence_level": "LOW", # 因為 TPEX 是估算的
            "status_tpex": "ESTIMATED"
        }
    except:
        return {"amount_total_raw": 0, "amount_total_blended": 0, "confidence_level": "LOW", "status_tpex": "FAIL"}

def _single_fetch(sym: str):
    try:
        df = yf.download(sym, period="2mo", progress=False)
        if not df.empty:
            c = df["Close"].iloc[:, 0].iloc[-1] if isinstance(df["Close"], pd.DataFrame) else df["Close"].iloc[-1]
            return float(c)
    except: return None
    return None

# =========================
# 4. 主程序 (Schema 校準)
# =========================
def main():
    now = get_taipei_now()
    trade_date = now.strftime("%Y-%m-%d")
    
    twii_df = yf.download(TWII_SYMBOL, period="2y", progress=False)
    m = compute_regime_metrics(twii_df)
    amt_info = fetch_blended_amount(trade_date)
    
    # 🌟 修正點：根據信心等級自動降級 market_status
    market_status = "NORMAL"
    if amt_info["confidence_level"] != "HIGH":
        market_status = "DEGRADED"

    # 🌟 修正點：Schema 校準 (Tier -> rank + tier_level)
    stocks_output = []
    for i, (sym, name) in enumerate(list(STOCK_NAME_MAP.items()), 1):
        price = _single_fetch(sym)
        # 硬性定義：目前環境下全部降級為 Weak (2)
        tier_level = 2 
        # 修正點：Inst_Net_3d 使用 None (null) 代表數據未更新
        stocks_output.append({
            "Symbol": sym,
            "Name": name,
            "rank": i,           # 原本的 Tier 改名為 rank
            "tier_level": tier_level, # 新增語意明確的 Strong(1)/Weak(2)
            "Price": price,
            "Institutional": {
                "Inst_Status": "NO_UPDATE_TODAY",
                "Inst_Net_3d": None, # 修正點：禁止用 0.0，改用 null
                "inst_data_fresh": False # 新增：數據新鮮度旗標
            }
        })

    payload = {
        "meta": {
            "timestamp": _now_ts := time.strftime("%Y-%m-%d %H:%M:%S"),
            "market_status": market_status, # 已連動降級
            "confidence_level": amt_info["confidence_level"]
        },
        "macro": {
            "overview": m,
            "market_amount": amt_info # 已區分 raw/blended
        },
        "stocks": stocks_output
    }

    # UI 呈現
    st.write(f"當前市場狀態: **{market_status}** ({amt_info['confidence_level']})")
    if m["Blow_Off_Phase"]:
        st.error("🚨 警告：偵測到末端加速段 (Blow-off Phase)！")
    
    st.subheader("🎯 核心持股雷達 (Schema 校準版)")
    st.dataframe(pd.DataFrame(stocks_output))

    st.markdown("---")
    st.subheader("🤖 AI JSON (校準後的 Schema)")
    st.code(json.dumps(payload, indent=4, ensure_ascii=False), language="json")

if __name__ == "__main__":
    main()
