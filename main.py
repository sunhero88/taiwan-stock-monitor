# main.py
import json
import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime
import pytz
from ucc_v19_1 import UCCv19_1

# 1. 內建您的 Token
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wOSAxMToyMzowNiIsInVzZXJfaWQiOiJzdW5oZXJvMiIsImVtYWlsIjoic3VuaGVybzg4QGdtYWlsLmNvbSIsImlwIjoiMS4xNjMuOTQuMTUyIn0.fvt_w6AbeGa8lwKbJeDeNhFt8AcLsdw5UOBxlLAkAww"

st.set_page_config(page_title="Sunhero｜中控台", layout="wide")

SYMBOLS_TOP20 = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "3017.TW", "3324.TW", "3661.TW", "2881.TW", "2882.TW", "2891.TW", "2886.TW", "2603.TW", "2609.TW", "1605.TW", "1513.TW", "1519.TW", "2002.TW"]
STOCK_MAP = {"2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科"} # 縮略示意

if "payload_text" not in st.session_state: st.session_state["payload_text"] = "{}"

@st.cache_data(ttl=300)
def get_market_metrics():
    df = yf.download("^TWII", period="2d", progress=False)
    vix = yf.download("^VIX", period="1d", progress=False)
    curr = float(df["Close"].iloc[-1])
    chg = curr - float(df["Close"].iloc[-2])
    pct = (chg / float(df["Close"].iloc[-2])) * 100
    # 抓取 Top 20 價格
    prices = yf.download(SYMBOLS_TOP20, period="1d", progress=False)["Close"].iloc[-1].to_dict()
    return curr, chg, pct, float(vix["Close"].iloc[-1]), prices

st.title("Sunhero｜股市智能超盤中控台 (V19.1)")
tw_p, tw_c, tw_pct, vix_p, s_prices = get_market_metrics()

# 儀表板
st.markdown("### 📊 市場即時狀態")
m1, m2, m3, m4 = st.columns(4)
m1.metric("加權指數 (TWII)", f"{tw_p:,.2f}", f"{tw_c:+.2f} ({tw_pct:+.2f}%)")
m2.metric("VIX 恐慌指數", f"{vix_p:.2f}")
m3.metric("三大法人買賣超", "買超 168.5 億")
m4.metric("法人資料日期", "2026-02-25")

run_mode = st.sidebar.radio("模式選擇", ["L1", "L2", "L3"], format_func=lambda x: {"L1":"L1 (審計)", "L2":"L2 (裁決)", "L3":"L3 (壓測)"}[x])

left, right = st.columns(2)
with left:
    st.subheader("輸入 JSON Payload")
    if st.button("載入標準範本 (包含 Top 20 個股)"):
        stocks = [{"Symbol": s, "Name": STOCK_MAP.get(s, s), "Price": s_prices.get(s)} for s in SYMBOLS_TOP20]
        template = {
            "meta": {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            "macro": {"overview": {"twii_close": tw_p, "SMR": (vix_p/tw_p*1000), "max_equity_allowed_pct": 0.05}},
            "stocks": stocks
        }
        st.session_state["payload_text"] = json.dumps(template, ensure_ascii=False, indent=2)
        st.rerun()
    p_input = st.text_area("JSON 數據內容", value=st.session_state["payload_text"], height=500)
    st.session_state["payload_text"] = p_input

with right:
    st.subheader("UCC 裁決結果")
    if st.button("🚀 執行 UCC 裁決", type="primary"):
        try:
            data = json.loads(p_input)
            result = UCCv19_1().run(data, run_mode=run_mode)
            st.json(result) if isinstance(result, dict) else st.code(result)
        except Exception as e: st.error(f"解析錯誤: {str(e)}")
