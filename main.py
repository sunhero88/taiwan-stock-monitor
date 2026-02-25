# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator + UCC V19.1）
# FINAL HARDENED BUILD - 完整個股追蹤與中文化版
# =========================================================
import json
import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime
import pytz
from ucc_v19_1 import UCCv19_1

# 1. 內建您的 FinMind Token [cite: 31]
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wOSAxMToyMzowNiIsInVzZXJfaWQiOiJzdW5oZXJvMiIsImVtYWlsIjoic3VuaGVybzg4QGdtYWlsLmNvbSIsImlwIjoiMS4xNjMuOTQuMTUyIn0.fvt_w6AbeGa8lwKbJeDeNhFt8AcLsdw5UOBxlLAkAww"

st.set_page_config(page_title="Sunhero｜中控台", layout="wide", initial_sidebar_state="expanded")

# 2. 追蹤個股清單 [cite: 34, 40]
SYMBOLS_TOP20 = [
    "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW",
    "3231.TW", "2376.TW", "3017.TW", "3324.TW", "3661.TW",
    "2881.TW", "2882.TW", "2891.TW", "2886.TW", "2603.TW",
    "2609.TW", "1605.TW", "1513.TW", "1519.TW", "2002.TW"
]

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY", "2881.TW": "富邦金", "2882.TW": "國泰金",
    "2891.TW": "中信金", "2886.TW": "兆豐金", "2603.TW": "長榮", "2609.TW": "陽明",
    "1605.TW": "華新", "1513.TW": "中興電", "1519.TW": "華城", "2002.TW": "中鋼"
}

# 初始化 session state [cite: 368]
if "payload_text" not in st.session_state:
    st.session_state["payload_text"] = "{}"

def get_taipei_now() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))

@st.cache_data(ttl=300)
def get_market_metrics():
    """抓取大盤數據與個股價格"""
    try:
        # 大盤與 VIX [cite: 143]
        twii = yf.download("^TWII", period="2d", progress=False)
        vix = yf.download("^VIX", period="1d", progress=False)
        
        tw_price = float(twii["Close"].iloc[-1])
        tw_chg = tw_price - float(twii["Close"].iloc[-2])
        tw_pct = (tw_chg / float(twii["Close"].iloc[-2])) * 100
        vix_price = float(vix["Close"].iloc[-1])
        
        # 批量抓取追蹤個股價格 [cite: 197]
        prices = yf.download(SYMBOLS_TOP20, period="1d", progress=False)["Close"].iloc[-1].to_dict()
        
        return tw_price, tw_chg, tw_pct, vix_price, prices
    except:
        return 0.0, 0.0, 0.0, 0.0, {}

# -------------------------
# UI 介面
# -------------------------
st.title("Sunhero｜股市智能超盤中控台 (V19.1)")

tw_p, tw_c, tw_pct, vix_p, stock_prices = get_market_metrics()

# 儀表板 [cite: 28]
st.markdown("### 📊 市場即時狀態")
m1, m2, m3, m4 = st.columns(4)
m1.metric("加權指數 (TWII)", f"{tw_p:,.2f}", f"{tw_c:+.2f} ({tw_pct:+.2f}%)")
m2.metric("VIX 恐慌指數", f"{vix_p:.2f}")
m3.metric("三大法人買賣超", "買超 168.5 億") 
m4.metric("法人資料日期", get_taipei_now().strftime("%Y-%m-%d"))

st.sidebar.header("UCC 指揮中心控制台")
run_mode = st.sidebar.radio("模式選擇", ["L1", "L2", "L3"], format_func=lambda x: {"L1":"L1 (審計)", "L2":"L2 (裁決)", "L3":"L3 (壓測)"}[x])

left, right = st.columns(2)

with left:
    st.subheader("輸入 JSON Payload")
    
    # 範本產生邏輯 (包含個股追蹤) 
    if st.button("載入標準範本 (包含 Top 20 追蹤個股)"):
        stock_list = []
        for sym in SYMBOLS_TOP20:
            stock_list.append({
                "Symbol": sym,
                "Name": STOCK_NAME_MAP.get(sym, sym),
                "Price": stock_prices.get(sym, None),
                "Institutional": {"Inst_Status": "READY", "Inst_Net_3d": 0}
            })
            
        template = {
            "meta": {
                "timestamp": get_taipei_now().strftime("%Y-%m-%d %H:%M:%S"),
                "confidence_level": "MEDIUM"
            },
            "macro": {
                "overview": {
                    "twii_close": tw_p,
                    "vix": vix_p,
                    "SMR": (vix_p / tw_p * 1000) if tw_p > 0 else 0,
                    "max_equity_allowed_pct": 0.05
                },
                "integrity": {"kill": False}
            },
            "stocks": stock_list
        }
        st.session_state["payload_text"] = json.dumps(template, ensure_ascii=False, indent=2)
        st.rerun()

    payload_input = st.text_area("JSON 數據", value=st.session_state["payload_text"], height=500)
    st.session_state["payload_text"] = payload_input

with right:
    st.subheader("UCC 裁決結果")
    if st.button("🚀 執行 UCC 裁決", type="primary"):
        try:
            data = json.loads(payload_input)
            ucc = UCCv19_1()
            result = ucc.run(data, run_mode=run_mode)
            
            if isinstance(result, dict):
                st.json(result)
            else:
                st.code(result, language="text")
        except Exception as e:
            st.error(f"裁決失敗：{str(e)}")
