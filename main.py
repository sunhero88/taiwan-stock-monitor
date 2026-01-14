# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer

def fetch_inst_data():
    """直接從證交所 API 介入三大法人買賣超數據 (台股)"""
    try:
        url = "https://www.twse.com.tw/fund/T86?response=json&selectType=ALL"
        r = requests.get(url, timeout=10)
        data = r.json()
        if data['stat'] == 'OK':
            # 欄位說明: 0代號, 18三大法人合計買賣超張數
            df_inst = pd.DataFrame(data['data'])[[0, 18]]
            df_inst.columns = ['Symbol', 'Inst_Net']
            df_inst['Symbol'] = df_inst['Symbol'].apply(lambda x: x.strip() + ".TW")
            df_inst['Inst_Net'] = df_inst['Inst_Net'].str.replace(',', '').astype(float)
            return df_inst
    except: return pd.DataFrame()

def fetch_market_data(m_id):
    """全雲端數據介入"""
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 下載價量數據
    data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    # 下載籌碼數據
    inst_df = fetch_inst_data() if m_id == "tw-share" else pd.DataFrame()
    
    all_data = []
    for s in symbols:
        if s in data.columns.levels[0]:
            s_df = data[s].dropna().copy()
            s_df['Symbol'] = s
            # 對齊法人籌碼 (僅台股)
            if not inst_df.empty and s in inst_df['Symbol'].values:
                s_df['Inst_Net'] = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
            else:
                s_df['Inst_Net'] = 0
            all_data.append(s_df)
    return pd.concat(all_data) if all_data else pd.DataFrame()

# --- Streamlit UI ---
st.set_page_config(page_title="Predator V14.0", layout="wide", page_icon="🦅")
st.title("🦅 Predator 指揮中心 V14.0 (智能選股與籌碼版)")

market = st.sidebar.selectbox("介入市場", ["tw-share", "us"])

if st.button("🔥 啟動智能關鍵十股分析"):
    with st.spinner("🚀 正在從雲端接入最新真實資訊與法人籌碼..."):
        raw_df = fetch_market_data(market)
        
        if not raw_df.empty:
            top_10, report_text = analyzer.run_analysis(raw_df)
            
            if not top_10.empty:
                st.success("✅ 數據介入完成：已智能鎖定今日關鍵十股")
                
                # 🚀 補回：📋 數據介入結果 (供複製給 Gem)
                st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                final_output = f"【Predator V14.0 智能十股】\n市場：{market}\n指標：[乖離率, 法人買賣(張), 戰術標籤]\n\n{report_text}"
                st.code(final_output, language="markdown")
                
                # 詳細數據展示
                st.subheader("📊 智能排序詳細指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("分析結果為空")
        else:
            st.error("無法連網獲取數據")
