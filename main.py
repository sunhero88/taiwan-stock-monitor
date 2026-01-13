# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import datetime
from pathlib import Path
import time

# 設定頁面
st.set_page_config(page_title="宇宙第一股市智能分析", layout="wide")

root_dir = Path(__file__).parent.absolute()

# 標題
st.title("宇宙第一股市智能分析系統 V12.3")
st.markdown("**全自動版 - 打開即時更新數據 + 報告**（雲端部署中，無需執行命令）")

# 側邊欄選擇市場
market_options = ["tw", "us", "jp", "hk", "kr"]
market = st.sidebar.selectbox("選擇市場", market_options, index=0, help="選擇要分析的市場")

# ──────────────────────────────
# 即時數據抓取函式（取代 downloader.py）
@st.cache_data(ttl=300)  # 每 5 分鐘自動更新一次
def fetch_latest_data(market='tw'):
    data = {"status": "success", "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    if market == 'tw':
        # 台股大盤指數
        twii = yf.Ticker("^TWII")
        hist = twii.history(period="1d")
        if not hist.empty:
            data['twii_price'] = round(hist['Close'].iloc[-1], 2)
            data['twii_change'] = round(hist['Close'].pct_change().iloc[-1] * 100, 2)
        else:
            data['twii_price'] = "抓取失敗"
            data['twii_change'] = "N/A"

        # 簡單模擬三大法人（未來可加真實爬蟲）
        data['foreign_net'] = "外資買超 85 億（模擬）"
        data['trust_net'] = "投信買超 12 億（模擬）"
        data['dealer_net'] = "自營商買超 45 億（模擬）"

    return data

# ──────────────────────────────
# 自動生成報告函式（網頁載入即跑）
def auto_generate_report():
    with st.spinner("自動抓取最新數據與生成報告..."):
        latest_data = fetch_latest_data(market)
        st.subheader("即時數據概覽")
        st.json(latest_data)

        try:
            import analyzer
            images, df_res, text_reports, red_flags = analyzer.run(market)

            st.subheader("智能分析報告")
            st.text_area("文字報告", text_reports, height=400)
            
            if df_res is not None:
                st.dataframe(df_res)
            
            if images:
                for img in images:
                    st.image(img, width=600)

            st.subheader("紅旗自動偵測")
            if red_flags:
                for flag in red_flags:
                    st.error(f"⚠️ {flag}")
            else:
                st.success("目前無紅旗觸發")

            # 自動存檔
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = root_dir / f"report_auto_{timestamp}.txt"
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(text_reports)
            st.success(f"自動報告已存檔：{report_path.name}")

        except Exception as e:
            st.error(f"自動分析中斷：{e}")

# 網頁載入時自動執行一次
if 'auto_run' not in st.session_state:
    auto_generate_report()
    st.session_state.auto_run = True

# 自動每 5 分鐘重新載入一次（可調整秒數）
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > 300:  # 300 秒 = 5 分鐘
    st.rerun()
    st.session_state.last_refresh = time.time()

# 手動刷新按鈕（可選）
if st.button("手動刷新最新報告"):
    auto_generate_report()

# 頁尾說明
st.markdown("---")
st.caption("系統開啟即自動抓取最新數據與生成報告，每 5 分鐘自動刷新。僅供個人參考，不構成投資建議。")