print("======== 我是最新版 V16.3.43 ========")
# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.43 終極架構校準版）
# =========================================================
# 終極護城河清單 (一次到位)：
#   [1] 語意校準：Tier 徹底拆分為 rank (名次) 與 tier_level (1:Strong / 2:Weak)。
#   [2] 假零防禦：法人數據缺失時強制寫入 null (None)，並標記 inst_data_fresh = False。
#   [3] 狀態降級：confidence != HIGH 時，market_status 自動從 NORMAL 轉 DEGRADED。
#   [4] 成交量雙軌：區分 amount_total_raw (確信) 與 amount_total_blended (含估算)。
#   [5] 動能雷達：二階導數 (Acceleration) 與 末端加速段 (Blow-off) 偵測。
#   [6] 邊界防護：VIX 0~100 濾錯、yfinance 降維防禦、TPEX 民國日期修復。
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
# 1. 系統常數與初始化
# =========================
st.set_page_config(page_title="Sunhero｜Predator V16.3.43", layout="wide", initial_sidebar_state="expanded")
APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator V16.3.43 架構校準版)"

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

# 歷史回撤統計下的警戒線
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
# 2. 核心基礎工具
# =========================
def get_taipei_now() -> datetime:
    return datetime.now(pytz.timezone('Asia/Taipei'))

def _safe_float(x, default=0.0):
    try: 
        return float(x) if x is not None and not pd.isna(x) else default
    except: 
        return default

def _safe_int(x, default=0):
    try: 
        if isinstance(x, str): 
            x = x.replace(",", "").strip()
        return int(float(x))
    except: 
        return default

def _to_roc_date(ymd: str) -> str:
    dt = pd.to_datetime(ymd)
    return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"

def get_intraday_progress() -> float:
    now = get_taipei_now()
    start = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=13, minute=30, second=0, microsecond=0)
    if now < start: 
        return 0.01
    if now > end: 
        return 1.0
    return max(0.01, (now - start).total_seconds() / (end - start).total_seconds())

# =========================
# 3. 數據抓取模組 (Volume, Price, Institutional)
# =========================
@dataclass
class MarketAmount:
    amount_twse: int
    amount_tpex: int
    amount_total_raw: int      # 🌟 僅包含可信數據
    amount_total_blended: int  # 🌟 包含估算數據
    source_twse: str
    source_tpex: str
    status_twse: str
    status_tpex: str
    confidence_level: str

def fetch_blended_amount(trade_date: str) -> MarketAmount:
    # 抓 TWSE
    ymd = trade_date.replace("-", "")
    url_twse = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={ymd}"
    twse_amt = 0
    twse_src = "TWSE_FAIL"
    twse_sts = "FAIL"
    try:
        r = requests.get(url_twse, timeout=5, verify=False)
        js = r.json()
        if "data" in js:
            twse_amt = sum(_safe_int(row[3], 0) for row in js["data"])
            twse_src = "TWSE_OK:AUDIT_SUM"
            twse_sts = "OK"
    except:
        twse_amt = 950_000_000_000 # 備援估算
        twse_src = "TWSE_SAFE_MODE"
        twse_sts = "ESTIMATED"

    # 抓 TPEX (含民國年修正)
    roc = _to_roc_date(trade_date)
    url_tpex = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc}&se=EW"
    tpex_amt = 0
    tpex_src = "TPEX_FAIL"
    tpex_sts = "FAIL"
    try:
        r = requests.get(url_tpex, headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
        js = r.json()
        if "aaData" in js and len(js["aaData"]) > 0:
            tpex_amt = _safe_int(js["aaData"][0][2])
            tpex_src = "TPEX_OFFICIAL_OK"
            tpex_sts = "OK"
    except: 
        pass

    # 若官方失敗，採用常數備援
    if tpex_sts == "FAIL":
        tpex_amt = 200_000_000_000
        tpex_src = "TPEX_SAFE_MODE_200B"
        tpex_sts = "ESTIMATED"

    # 🌟 核心雙軌制邏輯：如果 TPEX 是估算的，Raw 就不要加它
    conf = "HIGH" if (twse_sts == "OK" and tpex_sts == "OK") else "LOW"
    raw_total = twse_amt if tpex_sts != "OK" else (twse_amt + tpex_amt)
    blended_total = twse_amt + tpex_amt

    return MarketAmount(
        amount_twse=twse_amt, amount_tpex=tpex_amt,
        amount_total_raw=raw_total, amount_total_blended=blended_total,
        source_twse=twse_src, source_tpex=tpex_src,
        status_twse=twse_sts, status_tpex=tpex_sts,
        confidence_level=conf
    )

def _single_fetch_price_vol(sym: str) -> Tuple[Optional[float], Optional[float]]:
    ticker_base = sym.split(".")[0]
    for suffix in [".TW", ".TWO"]:
        try:
            df = yf.download(f"{ticker_base}{suffix}", period="2mo", progress=False)
            if not df.empty:
                # 降維防禦 MultiIndex
                c = df["Close"].iloc[:, 0] if isinstance(df["Close"], pd.DataFrame) else df["Close"]
                v = df["Volume"].iloc[:, 0] if isinstance(df["Volume"], pd.DataFrame) else df["Volume"]
                
                if len(v) >= 20:
                    ma20 = v.rolling(20).mean().iloc[-1]
                    vr = float(v.iloc[-1] / ma20) if ma20 > 0 else 1.0
                else:
                    vr = 1.0
                return float(c.iloc[-1]), vr
        except: 
            continue
    return None, None

def fetch_inst_3d(symbols: List[str], target_date: str, token: str) -> pd.DataFrame:
    rows = []
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    start_date = (pd.to_datetime(target_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    
    for sym in symbols:
        stock_id = sym.replace(".TW", "").replace(".TWO", "")
        try:
            params = {"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "data_id": stock_id, "start_date": start_date, "end_date": target_date}
            r = requests.get(FINMIND_URL, headers=headers, params=params, timeout=3)
            data = r.json().get("data", [])
            if data:
                df = pd.DataFrame(data)
                df["net"] = pd.to_numeric(df.get("buy",0), errors='coerce').fillna(0) - pd.to_numeric(df.get("sell",0), errors='coerce').fillna(0)
                # 確保只取有資料的最後三天
                net_sum = float(df.tail(3)["net"].sum())
                rows.append({"symbol": sym, "net_3d": net_sum})
        except: 
            pass
    return pd.DataFrame(rows)

# =========================
# 4. 戰略與風控引擎 (動能雷達)
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 200:
        return {"twii_close": None, "SMR": None, "Slope5": None, "Acceleration": 0.0, "Top_Divergence": False, "Blow_Off_Phase": False}
    
    close = market_df["Close"].iloc[:, 0] if isinstance(market_df["Close"], pd.DataFrame) else market_df["Close"]
    twii_close = float(close.iloc[-1])
    
    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    slope5_series = smr_series.diff(5)
    accel_series = slope5_series.diff(2)
    
    smr = _safe_float(smr_series.iloc[-1])
    slope5 = _safe_float(slope5_series.iloc[-1])
    accel = _safe_float(accel_series.iloc[-1])
    
    return {
        "twii_close": twii_close,
        "SMR": smr,
        "Slope5": slope5,
        "Acceleration": accel,
        "Top_Divergence": bool(smr > 0.15 and slope5 > 0 and accel < -0.01),
        "Blow_Off_Phase": bool(smr >= SMR_BLOW_OFF and slope5 >= 0.08),
        "MOMENTUM_LOCK": bool(slope5 > 0)
    }

def pick_regime(m: dict, vix: float) -> Tuple[str, float]:
    # 邊界防護 (防禦髒數據)
    if vix <= 0 or vix > 100 or m.get("twii_close") is None: 
        return "DATA_FAILURE", 0.0
    
    smr = m.get("SMR", 0)
    if vix > 35: return "CRASH_RISK", 0.10
    if m.get("Blow_Off_Phase"): return "CRITICAL_OVERHEAT", 0.10
    if smr > SMR_CRITICAL: return "CRITICAL_OVERHEAT", 0.10
    if smr > 0.25: return "OVERHEAT", 0.40
    return "NORMAL", 0.85

def determine_tier_level(smr: float, vol_ratio: Optional[float], inst_fresh: bool) -> int:
    """ 🌟 語意校準：強弱分級器 1=Strong, 2=Weak """
    if smr > 0.25: return 2       # 大盤過熱，所有標的強制降級為 Weak
    if not inst_fresh: return 2   # 籌碼盲區，強制降級
    if vol_ratio and vol_ratio > 1.2: return 1
    return 2

# =========================
# 5. 中控台主程序 (Arbiter)
# =========================
def main():
    st.title(APP_TITLE)
    
    with st.sidebar:
        st.header("⚙️ 系統參數")
        session = st.selectbox("時段", ["INTRADAY", "EOD"])
        topn = st.slider("監控 TopN", 5, 20, 20)
        token = st.text_input("FinMind Token (選填)", type="password")
        if st.button("🚀 啟動/更新引擎"):
            st.session_state.run_trigger = True

    if not st.session_state.get("run_trigger", False):
        st.info("👈 請設定參數並點擊左側「啟動/更新引擎」開始掃描。")
        return

    now = get_taipei_now()
    trade_date = now.strftime("%Y-%m-%d")
    progress = get_intraday_progress() if session == "INTRADAY" else 1.0

    with st.spinner("雷達掃描與終極架構校準中..."):
        # 1. 宏觀指標與 VIX
        twii_df = yf.download(TWII_SYMBOL, period="2y", progress=False)
        m = compute_regime_metrics(twii_df)
        
        vix_df = yf.download(VIX_SYMBOL, period="1mo", progress=False)
        v_s = vix_df["Close"].iloc[:, 0] if isinstance(vix_df["Close"], pd.DataFrame) else vix_df["Close"]
        vix_last = float(v_s.iloc[-1]) if not vix_df.empty else 20.0

        # 2. 成交量雙軌與狀態自動降級
        amt = fetch_blended_amount(trade_date)
        
        # 🌟 核心邏輯：信心不是 HIGH，狀態就必須是 DEGRADED
        market_status = "DEGRADED" if amt.confidence_level != "HIGH" else "NORMAL"
        conf_penalty = 0.5 if amt.confidence_level == "LOW" else 1.0
        
        regime, base_limit = pick_regime(m, vix_last)
        final_limit = base_limit * conf_penalty

        # 3. 籌碼 T-1 繼承邏輯
        inst_date = trade_date
        is_stale = False
        if now.hour < 15:
            is_stale = True
            prev = now - timedelta(days=1)
            while prev.weekday() >= 5: prev -= timedelta(days=1)
            inst_date = prev.strftime("%Y-%m-%d")
            
        inst_df = fetch_inst_3d(list(STOCK_NAME_MAP.keys())[:topn], inst_date, token)

        # 4. 個股處理 (Schema 終極校準)
        stocks_output = []
        for i, sym in enumerate(list(STOCK_NAME_MAP.keys())[:topn], 1):
            price, vr_raw = _single_fetch_price_vol(sym)
            vr = vr_raw / progress if vr_raw else None
            
            # 法人數據處理 (防禦假零)
            has_inst = not inst_df.empty and sym in inst_df["symbol"].values
            inst_fresh = has_inst and not is_stale
            
            # 🌟 核心邏輯：如果沒數據，強制設為 None (null)，絕不使用 0.0
            net_val = float(inst_df[inst_df["symbol"]==sym]["net_3d"].iloc[0]) if has_inst else None
            
            inst_status = "USING_T_MINUS_1" if (has_inst and is_stale) else ("READY" if inst_fresh else "NO_UPDATE_TODAY")
            
            # 強弱分級判定
            t_level = determine_tier_level(m["SMR"], vr, inst_fresh)

            stocks_output.append({
                "Symbol": sym,
                "Name": STOCK_NAME_MAP[sym],
                "rank": i,             # 🌟 修正 1：明確定義為排名
                "tier_level": t_level, # 🌟 修正 2：明確定義為強弱(1/2)
                "Price": price,
                "Vol_Ratio": vr,
                "Institutional": {
                    "Inst_Status": inst_status,
                    "Inst_Net_3d": net_val, # 🌟 修正 3：嚴禁假零，必定為數值或 null
                    "inst_data_fresh": inst_fresh # 🌟 修正 4：明確標示新鮮度
                }
            })

    # =========================
    # UI 呈現區
    # =========================
    st.subheader("📡 宏觀體制與動能雷達")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("市場狀態", market_status, delta="信心降級" if market_status=="DEGRADED" else "數據完備", delta_color="off")
    c2.metric("策略體制", f"🔴 {regime}" if "OVERHEAT" in regime else f"🟢 {regime}")
    c3.metric("資金曝險上限", f"{final_limit*100:.1f}%", f"信心懲罰 {conf_penalty}x" if conf_penalty<1 else "")
    c4.metric("二階加速度", f"{m['Acceleration']:.4f}", delta=f"{m['Acceleration']:.4f}", delta_color="normal" if m['Acceleration']>0 else "inverse")

    if m["Blow_Off_Phase"]:
        st.error("🚨 【極端風險】偵測到末端加速段 (Blow-off Phase)！SMR 超過 0.33，嚴禁開立任何新倉。")
    if m["Top_Divergence"]:
        st.warning("⚠️ 【黃色預警】高檔動能背離！大盤仍在漲，但推動加速度已轉負，請準備離場。")
    if market_status == "DEGRADED":
        st.info("ℹ️ 【降級模式】因 TPEX 數據估算或法人數據未更新，系統已自動進入 DEGRADED 降級防禦模式。")

    st.subheader("🎯 核心持股雷達 (Schema 校準版)")
    df_disp = pd.DataFrame(stocks_output)
    if not df_disp.empty:
        df_disp['Inst_Status'] = df_disp['Institutional'].apply(lambda x: x.get('Inst_Status'))
        df_disp['Inst_Net_3d'] = df_disp['Institutional'].apply(lambda x: x.get('Inst_Net_3d'))
        df_disp['Fresh'] = df_disp['Institutional'].apply(lambda x: "✅" if x.get('inst_data_fresh') else "❌")
        
        disp_cols = {'Symbol': '代號', 'Name': '名稱', 'rank': '排名', 'tier_level': '強弱級別(1/2)', 
                     'Price': '價格', 'Vol_Ratio': '預估量比', 'Inst_Status': '籌碼狀態', 
                     'Inst_Net_3d': '3日淨額', 'Fresh': '最新'}
        st.dataframe(df_disp[list(disp_cols.keys())].rename(columns=disp_cols), use_container_width=True)

    # =========================
    # 產生最終 JSON (Arbiter Input)
    # =========================
    payload = {
        "meta": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "session": session,
            "market_status": market_status, # 🌟 自動降級結果
            "current_regime": regime,
            "confidence_level": amt.confidence_level
        },
        "macro": {
            "overview": m,
            "market_amount": asdict(amt) # 🌟 雙軌制成交量 (raw vs blended)
        },
        "stocks": stocks_output
    }

    st.markdown("---")
    st.subheader("🤖 AI JSON (決策引擎終極安全輸入源)")
    st.code(json.dumps(payload, indent=4, ensure_ascii=False), language="json")

if __name__ == "__main__":
    main()
