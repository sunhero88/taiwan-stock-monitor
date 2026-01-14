# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import analyzer
import sys

def fetch_online_data(market_id):
    """全雲端數據介入：直接從網路獲取最新真實資訊"""
    # 擴大掃描池，讓智能篩選有更多選擇 (以台灣權值股與熱門股為例)
    if market_id == "tw-share":
        symbols = [
            "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", 
            "2357.TW", "3231.TW", "2376.TW", "6669.TW", "2408.TW",
            "2603.TW", "2609.TW", "2615.TW", "2303.TW", "2881.TW"
        ]
    else:
        symbols = ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN", "NFLX", "AVGO"]
    
    data_list = []
    for s in symbols:
        try:
            # 抓取 2 個月數據以計算 MA20
            df = yf.download(s, period="2mo", interval="1d", progress=False)
            if not df.empty:
                df['Symbol'] = s
                data_list.append(df)
        except:
            continue
    
    return pd.concat(data_list) if data_list else pd.DataFrame()

if __name__ == "__main__":
    st.set_page_config(page_title="Predator V14.0", layout="wide")
    st.title("🦅 Predator 指揮中心 V14.0 (智能選股版)")
    
    # 側邊欄配置
    m = st.sidebar.selectbox("切換監控市場", ["tw-share", "us"])
    st.sidebar.info("模式：全雲端即時介入\n狀態：無需本地數據")

    if st.button("🔥 啟動智能關鍵十股分析"):
        with st.spinner("🚀 正在從網路介入數據並進行智能評分..."):
            # 1. 直接連網抓取
            raw_data = fetch_online_data(m)
            
            if not raw_data.empty:
                # 2. 執行智能分析
                top_10_df, report_text = analyzer.run_analysis(raw_data)
                
                if not top_10_df.empty:
                    st.success(f"✅ 成功從市場篩選出 10 檔關鍵標的")
                    
                    # 3. 補回：📋 數據介入結果 (供複製)
                    st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                    final_report = f"【V14.0 智能關鍵十股】\n市場：{m}\n指標：[乖離率, 量能比, 戰術標籤]\n\n"
                    final_report += report_text
                    st.code(final_report, language="markdown")
                    
                    # 4. 詳細數據表格
                    st.subheader("📊 智能評分詳細指標")
                    display_cols = ['Symbol', 'Close', 'MA_Bias', 'Vol_Ratio', 'Body_Power', 'Score', 'Predator_Tag']
                    st.dataframe(top_10_df[display_cols].style.highlight_max(axis=0, subset=['Score']), use_container_width=True)
                else:
                    st.error("分析結果為空，請確認市場是否開盤或數據源正常。")
            else:
                st.error("❌ 無法連網獲取真實數據，請檢查 GitHub 網路權限。")

    st.divider()
    st.caption("數據來源：Yahoo Finance Real-time | 系統架構：全雲端無感化介入")
