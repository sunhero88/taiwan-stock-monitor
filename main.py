# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import analyzer
import sys

def fetch_data_online(market_id):
    """直接從 Yahoo Finance 抓取數據 (不經硬碟)"""
    # 這裡以台灣權值股為範例，您可以根據 market_id 切換清單
    symbols = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "2412.TW"]
    if market_id == "us":
        symbols = ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD"]
    
    data_list = []
    for s in symbols:
        try:
            ticker = yf.Ticker(s)
            hist = ticker.history(period="1mo")
            if not hist.empty:
                hist['Symbol'] = s
                data_list.append(hist)
        except:
            continue
    
    if data_list:
        return pd.concat(data_list)
    return pd.DataFrame()

if __name__ == "__main__":
    if "--cli" in sys.argv:
        # GitHub Action 模式繼續使用原本的檔案邏輯
        _, df, reports = analyzer.run("tw-share")
        # (發信邏輯...)
    else:
        # Streamlit Cloud 網頁模式
        st.set_page_config(page_title="Predator V14.0", layout="wide")
        st.title("🦅 Predator 指揮中心 V14.0 (雲端即時版)")
        
        m = st.sidebar.selectbox("切換市場", ["tw-share", "us"])
        
        if st.button("🔥 啟動雲端介入分析"):
            with st.spinner("🚀 正在從網路介入即時數據..."):
                # 直接抓取數據
                raw_df = fetch_data_online(m)
                
                if not raw_df.empty:
                    # 直接丟給分析引擎
                    _, df_res, text_reports = analyzer.run_analysis(raw_df)
                    
                    st.success("✅ 數據介入成功")
                    st.code(text_reports.get("📊 今日個股績效榜", ""), language="markdown")
                    st.dataframe(df_res.tail(20), use_container_width=True)
                else:
                    st.error("❌ 無法從網路獲取數據，請檢查 API 狀態。")
