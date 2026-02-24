# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.40 機構風控版）
# =========================================================
# 核心架構升級：
#   (1) 動態信心懲罰 (Confidence Penalty): LOW = 資金上限砍半
#   (2) 盤中籌碼繼承 (T-1 Fallback): 15:00 前自動使用昨日法人數據
#   (3) VIX 邊界防護網 (0 < vix <= 100)，防禦資料源污染
#   (4) 修正 yfinance API MultiIndex 報錯崩潰問題
#   (5) TPEX 官方數據修復 (民國年 115/02/24 格式) + 盤中量能歸一化
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
    page_title="Sunhero｜Predator V16.3.40",
    layout="wide",
    initial_sidebar_state="expanded"
)

APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator V16.3.40 機構風控版)"
st.title(APP_TITLE)

EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
DEFAULT_TOPN = 20
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000
AUDIT_DIR = "data/audit_market_amount"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}

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
    "Vol_Ratio": "預估量能比", "Layer": "分級(Layer)", 
    "Inst_Status": "籌碼狀態", "Inst_Net_3d": "3日合計淨額(張)"
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
    """計算台股交易進度 (09:00~13:30)，用於預估全天量能"""
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
# 3. 數據抓取模組 (TPEX, Yahoo, FinMind)
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
            df = yf.download(f"{ticker_base}{suffix}", period="2mo", progress=False)
            if not df.empty:
                c_s = df["Close"].iloc[:, 0] if isinstance(df["Close"], pd.DataFrame) else df["Close"]
                v_s = df["Volume"].iloc[:, 0] if isinstance(df["Volume"], pd.DataFrame) else df["Volume"]
                
                c = float(c_s.iloc[-1])
                v = float(v_s.iloc[-1])
                
                if len(v_s) >= 20:
                    ma20 = float(v_s.rolling(20).mean().iloc[-1])
                    vr = float(v/ma20) if ma20 > 0 else 1.0
                else:
                    vr = 1.0
                return c, vr
        except: continue
    return None, None

def fetch_finmind_institutional(symbols: List[str], start_date: str, end_date: str, token: Optional[str] = None) -> pd.DataFrame:
    """ 🔥 恢復 FinMind 籌碼爬蟲 (支援 T-1 盤中繼承) """
    rows = []
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    for sym in symbols:
        stock_id = sym.replace(".TW", "").replace(".TWO", "").strip()
        try:
            params = {"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "data_id": stock_id, "start_date": start_date, "end_date": end_date}
            r = requests.get(FINMIND_URL, headers=headers, params=params, timeout=5)
            if r.status_code == 200:
                data = r.json().get("data", [])
                if data:
                    df = pd.DataFrame(data)
                    df["buy"] = pd.to_numeric(df.get("buy", 0), errors="coerce").fillna(0)
                    df["sell"] = pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
                    df = df[df["name"].isin(A_NAMES)]
                    df["net"] = df["buy"] - df["sell"]
                    g = df.groupby("date", as_index=False)["net"].sum()
                    for _, row in g.iterrows():
                        rows.append({"date": str(row["date"]), "symbol": sym, "net_amount": float(row["net"])})
        except: pass
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["date", "symbol", "net_amount"])

def calc_inst_3d(inst_df: pd.DataFrame, symbol: str, target_date: str) -> dict:
    if inst_df.empty: return {"Inst_Status": "NO_DATA", "Inst_Net_3d": 0.0}
    df = inst_df[inst_df["symbol"] == symbol].sort_values("date")
    if df.empty: return {"Inst_Status": "NO_DATA", "Inst_Net_3d": 0.0}
    
    # 確認資料有到達 target_date
    has_target = (df["date"] == target_date).any()
    if not has_target:
        return {"Inst_Status": "NO_DATA", "Inst_Net_3d": 0.0}
        
    df = df.tail(3)
    net_sum = float(df["net_amount"].sum())
    return {"Inst_Status": "READY", "Inst_Net_3d": net_sum}

# =========================
# 4. 戰略判定與風控熔斷
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 200:
        return {"twii_close": None, "SMR": None, "Slope5": None}
    
    close = market_df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
        
    twii_close = float(close.iloc[-1])
    ma200 = close.rolling(200).mean()
    smr = (close - ma200) / ma200
    slope5_raw = smr.diff(5).iloc[-1]
    
    slope5_val = float(slope5_raw.iloc[0]) if isinstance(slope5_raw, pd.Series) else float(slope5_raw)
    smr_val = float(smr.iloc[-1].iloc[0]) if isinstance(smr.iloc[-1], pd.Series) else float(smr.iloc[-1])
    
    return {
        "twii_close": twii_close,
        "SMR": smr_val,
        "Slope5": slope5_val if not pd.isna(slope5_val) else 0.0,
        "MOMENTUM_LOCK": bool(slope5_val > 0)
    }

def compute_integrity_and_kill(metrics: dict, vix_last: float) -> dict:
    kill = False
    reasons = []
    
    if metrics.get("twii_close") is None:
        kill = True
        reasons.append("TWII_CLOSE_MISSING")
        
    # 🔥 VIX 異常防護網 (過濾 1940 這種髒數據)
    vix_invalid = (vix_last <= 0 or vix_last > 100)
    if vix_invalid:
        kill = True
        reasons.append(f"VIX_ANOMALY_DETECTED({vix_last})")
        
    return {
        "kill": kill,
        "vix_invalid": vix_invalid,
        "reason": ", ".join(reasons) if reasons else "OK"
    }

def pick_regime(metrics: dict, vix: float) -> Tuple[str, float]:
    smr = metrics.get("SMR", 0)
    if vix > 35 and vix <= 100: return "CRASH_RISK", 0.10
    if smr > SMR_CRITICAL: return "CRITICAL_OVERHEAT", 0.10
    if smr > 0.25: return "OVERHEAT", 0.45
    if smr > SMR_WATCH and metrics.get("Slope5", 0) < 0: return "MEAN_REVERSION", 0.55
    return "NORMAL", 0.85

def classify_layer(regime, momentum_lock, vol_ratio, inst_status):
    # 🔥 支援盤中繼承模式 (USING_T_MINUS_1) 參與分級
    if inst_status not in ["READY", "USING_T_MINUS_1"]: return "NONE"
    if regime in ["CRITICAL_OVERHEAT", "CRASH_RISK", "DATA_FAILURE"]: return "NONE"
    if momentum_lock and vol_ratio and vol_ratio > 1.2: return "B"
    return "NONE"

# =========================
# 5. 主程序邏輯 (機構引擎)
# =========================
def build_arbiter_input(session, account_mode, topn, positions, cash, total_equity, finmind_token):
    now = get_taipei_now()
    trade_date = now.strftime("%Y-%m-%d")
    is_using_prev = False
    
    # 宏觀時間守衛
    if session == "EOD" and now.hour < 15:
        trade_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        is_using_prev = True

    # 🔥 籌碼專屬時間守衛 (T-1 繼承邏輯)
    inst_date = now.strftime("%Y-%m-%d")
    is_inst_stale = False
    if now.hour < 15:
        prev = now - timedelta(days=1)
        while prev.weekday() >= 5: prev -= timedelta(days=1)
        inst_date = prev.strftime("%Y-%m-%d")
        is_inst_stale = True

    # 1. 市場指標與 VIX 防護
    twii_df = yf.download(TWII_SYMBOL, period="2y", progress=False)
    metrics = compute_regime_metrics(twii_df)
    
    vix_df = yf.download(VIX_SYMBOL, period="1mo", progress=False)
    if not vix_df.empty:
        v_close = vix_df["Close"].iloc[:, 0] if isinstance(vix_df["Close"], pd.DataFrame) else vix_df["Close"]
        vix_last = float(v_close.iloc[-1])
    else:
        vix_last = 20.0

    integrity = compute_integrity_and_kill(metrics, vix_last)
    amount = fetch_amount_total(trade_date)
    
    # 🔥 動態信心懲罰 (Confidence Penalty) 計算
    final_confidence = "LOW" if integrity["kill"] else amount.confidence_level
    confidence_multiplier = 1.0
    if final_confidence == "MEDIUM": confidence_multiplier = 0.8
    elif final_confidence == "LOW": confidence_multiplier = 0.5

    if integrity["kill"]:
        regime = "DATA_FAILURE"
        final_max_eq = 0.0
    else:
        regime, base_max_eq = pick_regime(metrics, vix_last)
        final_max_eq = base_max_eq * confidence_multiplier # 懲罰打折

    # 2. 個股分析與法人繼承
    symbols = list(STOCK_NAME_MAP.keys())[:topn]
    
    # 執行 FinMind 爬蟲
    start_date = (pd.to_datetime(inst_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    inst_df = fetch_finmind_institutional(symbols, start_date, inst_date, finmind_token)
    
    progress = get_intraday_progress() if session == "INTRADAY" else 1.0
    stocks = []
    
    for i, sym in enumerate(symbols, start=1):
        price, vr_raw = _single_fetch_price_vol(sym)
        vr_projected = (vr_raw / progress) if vr_raw else None
        
        # 籌碼提取 (T-1 繼承)
        inst_data_3d = calc_inst_3d(inst_df, sym, inst_date)
        inst_net = inst_data_3d.get("Inst_Net_3d", 0.0)
        
        # 如果是 15:00 前，狀態標記為 USING_T_MINUS_1
        if is_inst_stale and inst_data_3d.get("Inst_Status") == "READY":
            inst_status = "USING_T_MINUS_1"
        else:
            inst_status = inst_data_3d.get("Inst_Status", "NO_DATA")
        
        layer = classify_layer(regime, metrics.get("MOMENTUM_LOCK"), vr_projected, inst_status)
        
        stocks.append({
            "Symbol": sym, "Name": STOCK_NAME_MAP[sym], "Tier": i,
            "Price": price, "Vol_Ratio": vr_projected, "Layer": layer,
            "Institutional": {"Inst_Status": inst_status, "Inst_Net_3d": inst_net}
        })

    # 組合警報
    alerts = []
    if integrity["vix_invalid"]:
        alerts.append(f"🚨 CRITICAL: VIX 數值異常 ({vix_last})，已觸發風控強制熔斷！")
    if final_confidence != "HIGH":
        alerts.append(f"⚠️ RISK: 數據信心等級為 {final_confidence}，資金上限已自動套用 {confidence_multiplier}x 懲罰折扣。")
    if is_inst_stale:
        alerts.append("ℹ️ INFO: 盤中模式啟動，法人籌碼自動繼承 T-1 日 (昨日) 數據做為參考。")

    payload = {
        "meta": {
            "timestamp": _now_ts(), "session": session, "market_status": "NORMAL" if not integrity["kill"] else "SHELTER",
            "current_regime": regime, "confidence_level": final_confidence,
            "confidence_multiplier": confidence_multiplier,
            "is_using_previous_day": is_using_prev, "account_mode": account_mode
        },
        "macro": {
            "overview": {**metrics, "vix": vix_last, "max_equity_allowed_pct": final_max_eq, "trade_date": trade_date},
            "market_amount": asdict(amount),
            "integrity": integrity
        },
        "stocks": stocks,
        "institutional_panel": [{"Symbol": s["Symbol"], "Inst_Status": s["Institutional"]["Inst_Status"], "Inst_Net_3d": s["Institutional"]["Inst_Net_3d"]} for s in stocks],
        "portfolio": {"total_equity": total_equity, "cash_balance": cash, "active_alerts": alerts}
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
    
    st.sidebar.subheader("API 授權")
    finmind_token = st.sidebar.text_input("FinMind Token (盤中籌碼必填)", type="password")
    
    if st.sidebar.button("🚀 啟動 Predator 引擎") or "payload" not in st.session_state:
        with st.spinner("正在同步全球數據與執行機構風控稽核..."):
            payload, warns = build_arbiter_input(session, mode, topn, [], cash, equity, finmind_token)
            st.session_state.payload = payload
            st.session_state.warns = warns

    p = st.session_state.payload
    ov = p["macro"]["overview"]
    
    # 警報區
    alerts = p["portfolio"].get("active_alerts", [])
    for alert in alerts:
        if "CRITICAL" in alert: st.error(alert)
        elif "RISK" in alert: st.warning(alert)
        else: st.info(alert)

    if p["meta"]["is_using_previous_day"]:
        st.warning(f"⏰ 盤後數據未更新，大盤目前使用 {ov['trade_date']} 歷史數據。")

    # 頂部儀表板
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("交易日期", ov["trade_date"])
    
    regime = p["meta"]["current_regime"]
    regime_color = "🔴" if "OVERHEAT" in regime or "FAILURE" in regime else ("⚠️" if "REVERSION" in regime else "🟢")
    c2.metric("策略體制 (Regime)", f"{regime_color} {regime}")
    
    # 顯示打折後的上限與折扣係數
    discount = p["meta"]["confidence_multiplier"]
    discount_txt = f" (已打 {discount} 折)" if discount < 1.0 else ""
    c3.metric("建議持倉上限", f"{ov['max_equity_allowed_pct']*100:.1f}%", discount_txt, delta_color="off")
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
        df['籌碼狀態'] = df['Institutional'].apply(lambda x: x.get('Inst_Status', 'N/A'))
        df['3日合計淨額(張)'] = df['Institutional'].apply(lambda x: x.get('Inst_Net_3d', 0.0))
        disp_cols = ["Symbol", "Name", "Price", "Vol_Ratio", "Layer", "籌碼狀態", "3日合計淨額(張)"]
        st.dataframe(df[disp_cols].rename(columns=COL_TRANSLATION), use_container_width=True)

    # JSON 複製區
    st.markdown("---")
    st.subheader("🤖 AI JSON (Arbiter Input)")
    st.code(json.dumps(p, indent=4, ensure_ascii=False), language="json")

if __name__ == "__main__":
    main()
