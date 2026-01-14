# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import analyzer

def fetch_market_data(m_id):
    """直接介入全球金融伺服器獲取最新數據"""
    # 擴展監控清單，確保智能篩選有足夠樣本
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "2357.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN", "NFLX", "AVGO", "SMCI", "ARM"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    try:
        # 下載過去 2 個月的日線數據，確保計算 MA20 穩定
        data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
        
        all_data = []
        for s in symbols:
            if s in data.columns.levels[0]:
                s_df = data[s].dropna().copy()
                s_df['Symbol'] = s
                all_data.append(s_df)
        
        return pd.concat(all_data) if all_data else pd.DataFrame()
    except:
        return pd.DataFrame()

# --- 網頁介面呈現 ---
st.set_page_config(page_title="Predator 指揮中心 V14.0", layout="wide", page_icon="🦅")
st.title("🦅 Predator 指揮中心 V14.0 (智能選股版)")

market = st.sidebar.selectbox("切換介入市場", ["tw-share", "us"])

if st.button("🔥 啟動智能關鍵十股分析"):
    with st.spinner("🚀 正在從雲端接入最新真實數據..."):
        raw_df = fetch_market_data(market)
        
        if not raw_df.empty:
            top_10, report_text = analyzer.run_analysis(raw_df)
            
            if not top_10.empty:
                st.success("✅ 數據介入完成：已智能鎖定今日關鍵十股")
                
                # 🚀 達成「免轉貼」目的：直接生成複製區塊
                st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                final_output = f"【Predator V14.0 智能十股】\n市場：{market}\n\n{report_text}"
                st.code(final_output, language="markdown")
                
                # 詳細數據展示
                st.subheader("📊 關鍵標的技術指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Vol_Ratio', 'Body_Power', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error(report_text) # 顯示 analyzer 回傳的具體錯誤
        else:
            st.error("❌ 無法獲取雲端數據，請確認 Yahoo Finance API 是否正常。")

st.divider()
st.caption("數據來源：Yahoo Finance Real-time | 環境：Streamlit Cloud | 無需本地 CSV 檔案")
