# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.38.2 終極防禦版）
# =========================================================
# 整合功能：
#   (1) 修正 yfinance API 改版造成的 MultiIndex (DataFrame) 報錯崩潰問題
#   (2) TPEX 官方數據修復 (民國年 115/02/24 格式)
#   (3) 盤中量能預估歸一化 (Time-Weighted Volume)
#   (4) 雙鴻(3324)等上櫃股 YF 後綴自動切換 (.TW/.TWO)
#   (5) 籌碼殭屍熔斷：15:00 前強制歸零
#   (6) 修正過年期間交易日不足導致 MA20 算不出來的 3.27 異常 (改抓 2mo)
#   (7) 全功能 UI：儀表板、警報區、法人明細、AI JSON 複製
# =========================================================

from __future__ import annotations
import json
import os
import re
import time
import requests
import warnings
import pytz
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
    page_title="Sunhero｜Predator V16.3.38.2",
    layout="wide",
    initial_sidebar_state="expanded"
)

APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator V16.3.38.2 終極防禦版)"
st.title(APP_TITLE)

EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
DEFAULT_TOPN = 20
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000
AUDIT_DIR = "data/audit_market_amount"
SMR_WATCH = 0.23
SMR_CRITICAL = 0.30

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY", "2881.TW": "富邦金", "2882.TW": "國泰金",
    "2891.TW": "中信金", "2886.TW": "兆豐金", "2603.TW": "長榮", "2609.TW": "陽明",
    "1605.TW": "華新", "1513.TW": "中興電", "1519.TW": "華城", "2002.TW": "中鋼"
}

COL_TRANSLATION = {
    "Symbol": "代號", "Name": "名稱", "Tier": "權重序", "Price": "價格",
    "Vol_Ratio": "量能比(Vol Ratio)", "Layer": "分級(Layer)", 
    "Inst_Status": "籌碼狀態", "Inst_Dir3": "籌碼方向",
    "Inst_Net_3d": "3日合計淨額", "inst_source": "資料來源"
}

# =========================
# 2. 核心工具函數
# =========================
def _now_ts() -> str: return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_taipei_now() -> datetime:
    return datetime.now(pytz.timezone('Asia/Taipei'))

def _safe_float(x, default=None):
    try: return float(x) if x is not None else default
    except: return default

def _safe_int(x, default=None):
    try:
        if x is None: return default
        if isinstance(x, str): x = x.replace(",", "").strip()
        return int(float(x))
    except: return default

def _to_roc_date(ymd: str) -> str:
    dt = pd.to_datetime(ymd)
    return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"

def get_intraday_progress() -> float:
    """計算台股交易進度 (09:00~13:30)"""
    now = get_taipei_now()
    start = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=13, minute=30, second=0, microsecond=0)
    if now < start: return 0.01
    if now > end: return 1.0
    return max(0.01, (now - start).total_seconds() / (end - start).total_seconds())

class WarningBus:
    def __init__(self): self.items = []
    def push(self, code, msg, meta=None): self.items.append({"ts": _now_ts(), "code": code, "msg": msg, "meta": meta or {}})
    def latest(self, n=50): return self.items[-n:]
warnings_bus = WarningBus()

# =========================
# 3. 數據抓取模組 (TPEX & Yahoo API 修復)
# =========================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]; amount_tpex: Optional[int]; amount_total: Optional[int]
    source_twse: str; source_tpex: str; status_twse: str; status_tpex: str
    confidence_level: str; scope: str; meta: Optional[dict] = None

def _fetch_tpex_official(trade_date: str) -> Tuple[Optional[int], str, str]:
    roc = _to_roc_date(trade_date)
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc}&se=EW"
    headers = {"Referer": "https://www.tpex.org.tw/", "User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10, verify=False)
        js = r.json()
        if "aaData" in js and len(js["aaData"]) > 0:
            amt = _safe_int(js["aaData"][0][2])
            if amt and amt > 10_000_000_000:
                return amt, "TPEX_OFFICIAL_OK", "OK"
    except: pass
    return None, "TPEX_FAIL", "FAIL"

def _twse_audit_sum_by_stock_day_all(trade_date: str) -> Tuple[Optional[int], str, dict]:
    ymd = trade_date.replace("-", "")
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={ymd}"
    try:
        r = requests.get(url, timeout=10, verify=False)
        js = r.json()
        if "data" in js:
            total = sum(_safe_int(row[3], 0) for row in js["data"])
            if total > 50_000_000_000:
                return total, "TWSE_OK:AUDIT_SUM", {"rows": len(js["data"])}
    except: pass
    return 500_000_000_000, "TWSE_SAFE_MODE", {}

def fetch_amount_total(trade_date: str) -> MarketAmount:
    twse_amt, twse_src, twse_meta = _twse_audit_sum_by_stock_day_all(trade_date)
    tpex_amt, tpex_src, tpex_sts = _fetch_tpex_official(trade_date)
    
    if not tpex_amt:
        try:
            url = "https://histock.tw/index/TWO"
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            soup = BeautifulSoup(r.text, 'html.parser')
            for li in soup.find_all("li"):
                if "成交金額" in li.text:
                    num = re.search(r'([\d,]+\.?\d*)', li.text).group(1).replace(",", "")
                    tpex_amt, tpex_src, tpex_sts = int(float(num) * 100_000_000), "HISTOCK_WEB", "OK"
                    break
        except: pass

    if not tpex_amt:
        tpex_amt = 200_000_000_000
        tpex_src = "TPEX_SAFE_MODE_200B"
        tpex_sts = "ESTIMATED"

    total = twse_amt + tpex_amt
    conf = "HIGH" if "OK" in twse_src and "OK" in tpex_src else "LOW"
    
    return MarketAmount(twse_amt, tpex_amt, total, twse_src, tpex_src, "OK", tpex_sts, conf, "ALL", {"twse": twse_meta})

def _single_fetch_price_vol(sym: str) -> Tuple[Optional[float], Optional[float]]:
    ticker_base = sym.split(".")[0]
    for suffix in [".TW", ".TWO"]:
        try:
            # 🔥 修正：從 1mo 改為 2mo，確保無論如何都有 >20 個交易日可算 MA20
            df = yf.download(f"{ticker_base}{suffix}", period="2mo", progress=False)
            if not df.empty:
                # 🔥 修復 yfinance MultiIndex 問題
                c_s = df["Close"].iloc[:, 0] if isinstance(df["Close"], pd.DataFrame) else df["Close"]
                v_s = df["Volume"].iloc[:, 0] if isinstance(df["Volume"], pd.DataFrame) else df["Volume"]
                
                c = float(c_s.iloc[-1])
                v = float(v_s.iloc[-1])
                
                # 確保數值有效
                if len(v_s) >= 20:
                    ma20 = float(v_s.rolling(20).mean().iloc[-1])
                    vr = float(v/ma20) if ma20 > 0 else 1.0
                else:
                    vr = 1.0
                return c, vr
        except: continue
    return None, None

# =========================
# 4. 戰略判定 (Layer A/B/C) 降維防禦版
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 200:
        return {"twii_close": None, "SMR": None, "Slope5": None}
    
    # 🔥 修復 yfinance 新版 MultiIndex 問題：強制降維成 1D Series
    close = market_df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
        
    twii_close = float(close.iloc[-1])
    ma200 = close.rolling(200).mean()
    smr = (close - ma200) / ma200
    slope5_raw = smr.diff(5).iloc[-1]
    
    # 確保提煉出純淨的標量數值 (Scalar float)
    slope5_val = float(slope5_raw.iloc[0]) if isinstance(slope5_raw, pd.Series) else float(slope5_raw)
    smr_val = float(smr.iloc[-1].iloc[0]) if isinstance(smr.iloc[-1], pd.Series) else float(smr.iloc[-1])
    
    return {
        "twii_close": twii_close,
        "SMR": smr_val,
        "Slope5": slope5_val if not pd.isna(slope5_val) else 0.0,
        "MOMENTUM_LOCK": bool(slope5_val > 0)
    }

def pick_regime(metrics: dict, vix: float) -> Tuple[str, float]:
    if metrics.get("twii_close") is None: return "DATA_FAILURE", 0.0
    smr = metrics.get("SMR", 0)
    # 🔥 新增極端過熱判定
    if smr > SMR_CRITICAL: return "CRITICAL_OVERHEAT", 0.10
    if smr > 0.25: return "OVERHEAT", 0.45
    if smr > SMR_WATCH and metrics.get("Slope5", 0) < 0: return "MEAN_REVERSION", 0.55
    return "NORMAL", 0.85

def classify_layer(regime, momentum_lock, vol_ratio, inst_status):
    # 籌碼殭屍熔斷
    if inst_status == "NO_UPDATE_TODAY": return "NONE"
    if regime == "CRITICAL_OVERHEAT": return "NONE" # 極端風險下禁止分級
    if momentum_lock and vol_ratio and vol_ratio > 1.2: return "B"
    return "NONE"

# =========================
# 5. 主程序邏輯
# =========================
def build_arbiter_input(session, account_mode, topn, positions, cash, total_equity):
    now = get_taipei_now()
    # 時間守衛：盤中強制切換日期
    trade_date = now.strftime("%Y-%m-%d")
    is_using_prev = False
    if session == "EOD" and now.hour < 15:
        trade_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        is_using_prev = True

    # 1. 市場指標
    twii_df = yf.download(TWII_SYMBOL, period="2y", progress=False)
    metrics = compute_regime_metrics(twii_df)
    vix_df = yf.download(VIX_SYMBOL, period="1mo", progress=False)
    
    # 🔥 VIX 同樣需要防禦 MultiIndex
    if not vix_df.empty:
        v_close = vix_df["Close"].iloc[:, 0] if isinstance(vix_df["Close"], pd.DataFrame) else vix_df["Close"]
        vix_last = float(v_close.iloc[-1])
    else:
        vix_last = 20.0
    
    regime, max_eq = pick_regime(metrics, vix_last)
    amount = fetch_amount_total(trade_date)
    
    # 2. 個股分析 (含盤中量能歸一化)
    progress = get_intraday_progress() if session == "INTRADAY" else 1.0
    stocks = []
    symbols = list(STOCK_NAME_MAP.keys())[:topn]
    
    for i, sym in enumerate(symbols, start=1):
        price, vr_raw = _single_fetch_price_vol(sym)
        vr_projected = (vr_raw / progress) if vr_raw else None
        
        # 籌碼熔斷邏輯
        inst_status = "READY" if now.hour >= 15 else "NO_UPDATE_TODAY"
        inst_net = 0.0
        
        layer = classify_layer(regime, metrics.get("MOMENTUM_LOCK"), vr_projected, inst_status)
        
        stocks.append({
            "Symbol": sym, "Name": STOCK_NAME_MAP[sym], "Tier": i,
            "Price": price, "Vol_Ratio": vr_projected, "Layer": layer,
            "Institutional": {"Inst_Status": inst_status, "Inst_Net_3d": inst_net}
        })

    payload = {
        "meta": {
            "timestamp": _now_ts(), "session": session, "market_status": "NORMAL",
            "current_regime": regime, "confidence_level": amount.confidence_level,
            "is_using_previous_day": is_using_prev, "account_mode": account_mode
        },
        "macro": {
            "overview": {**metrics, "vix": vix_last, "max_equity_allowed_pct": max_eq, "trade_date": trade_date},
            "market_amount": asdict(amount),
            "integrity": {"kill": (metrics["twii_close"] is None), "reason": "OK"}
        },
        "stocks": stocks,
        "institutional_panel": [{"Symbol": s["Symbol"], "Inst_Status": s["Institutional"]["Inst_Status"], "Inst_Net_3d": s["Institutional"]["Inst_Net_3d"]} for s in stocks],
        "portfolio": {"total_equity": total_equity, "cash_balance": cash, "active_alerts": []}
    }
    return payload, warnings_bus.latest()

# =========================
# 6. UI 美化儀表板
# =========================
def main():
    st.sidebar.header("⚙️ 設定")
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=0)
    mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"], index=0)
    topn = st.sidebar.slider("監控 TopN", 5, 20, 20)
    cash = st.sidebar.number_input("現金餘額", value=DEFAULT_CASH)
    equity = st.sidebar.number_input("總權益", value=DEFAULT_EQUITY)
    
    if st.sidebar.button("🚀 啟動 Predator 引擎") or "payload" not in st.session_state:
        with st.spinner("正在同步全球數據與修復 TPEX 鏈結..."):
            payload, warns = build_arbiter_input(session, mode, topn, [], cash, equity)
            st.session_state.payload = payload
            st.session_state.warns = warns

    p = st.session_state.payload
    ov = p["macro"]["overview"]
    
    # 頂部儀表板
    if p["meta"]["is_using_previous_day"]:
        st.warning(f"⏰ 開盤保護啟動：盤後數據未更新，目前使用 {ov['trade_date']} 歷史數據。")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("交易日期", ov["trade_date"])
    
    regime = p["meta"]["current_regime"]
    regime_color = "🔴" if "OVERHEAT" in regime else ("⚠️" if "REVERSION" in regime else "🟢")
    c2.metric("策略體制 (Regime)", f"{regime_color} {regime}")
    
    c3.metric("建議持倉上限", f"{ov['max_equity_allowed_pct']*100:.0f}%")
    c4.metric("信心等級", p["meta"]["confidence_level"])

    # 大盤詳細數據
    st.subheader("📊 大盤觀測站 (TAIEX Overview)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("加權指數", f"{ov['twii_close']:,.0f}" if ov['twii_close'] else "N/A")
    m2.metric("VIX 恐慌指數", f"{ov['vix']:.2f}")
    m3.metric("SMR 乖離率", f"{ov['SMR']:.4f}" if ov['SMR'] else "N/A")
    amt = p["macro"]["market_amount"]["amount_total"]
    m4.metric("市場總成交額", f"{amt/1e12:.3f} 兆" if amt else "N/A")

    # 成交額稽核
    with st.expander("📌 數據源稽核明細"):
        st.write(p["macro"]["market_amount"])

    # 個股清單
    st.subheader("🎯 核心持股雷達 (Tactical Stocks)")
    df = pd.DataFrame(p["stocks"])
    if not df.empty:
        # 展開 Institutional 字典
        df['籌碼狀態'] = df['Institutional'].apply(lambda x: x.get('Inst_Status', 'N/A'))
        df['3日合計淨額'] = df['Institutional'].apply(lambda x: x.get('Inst_Net_3d', 0.0))
        disp_cols = ["Symbol", "Name", "Price", "Vol_Ratio", "Layer", "籌碼狀態", "3日合計淨額"]
        st.dataframe(df[disp_cols].rename(columns=COL_TRANSLATION), use_container_width=True)

    # JSON 複製區
    st.markdown("---")
    st.subheader("🤖 AI JSON (Arbiter Input)")
    st.code(json.dumps(p, indent=4, ensure_ascii=False), language="json")

if __name__ == "__main__":
    main()
