# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer

def fetch_inst_data():
    """從證交所 API 直接介入三大法人買賣超 (增加防護標頭確保抓取成功)"""
    try:
        url = "https://www.twse.com.tw/fund/T86?response=json&selectType=ALL"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        if data.get('stat') == 'OK' and 'data' in data:
            df = pd.DataFrame(data['data'])[[0, 18]]
            df.columns = ['Symbol', 'Inst_Net']
            df['Symbol'] = df['Symbol'].str.strip() + ".TW"
            df['Inst_Net'] = df['Inst_Net'].str.replace(',', '').astype(float)
            return df
    except:
        return pd.DataFrame()

def fetch_market_data(m_id):
    """全雲端數據抓取邏輯"""
    # 監控池：台股權值與熱門標的
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 下載價量
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    # 下載籌碼 (僅台股)
    inst_df = fetch_inst_data() if m_id == "tw-share" else pd.DataFrame()
    
    all_res = []
    for s in symbols:
        if s in raw_data.columns.levels[0]:
            s_df = raw_data[s].dropna().copy()
            s_df['Symbol'] = s
            # 對齊籌碼
            if not inst_df.empty and s in inst_df['Symbol'].values:
                s_df['Inst_Net'] = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
            else:
                s_df['Inst_Net'] = 0
            all_res.append(s_df)
    return pd.concat(all_res) if all_res else pd.DataFrame()

# --- Streamlit UI 呈現 ---
st.set_page_config(page_title="Predator V14.0", layout="wide")
st.title("🦅 Predator 指揮中心 V14.0 (雲端即時版)")

market = st.sidebar.selectbox("切換介入市場", ["tw-share", "us"])

if st.button("🔥 啟動智能關鍵十股分析"):
    with st.spinner("🚀 正在從雲端接入最新真實資訊與法人籌碼..."):
        full_df = fetch_market_data(market)
        
        if not full_df.empty:
            top_10, report_text = analyzer.run_analysis(full_df)
            
            if not top_10.empty:
                st.success("✅ 數據介入完成：已智能鎖定今日關鍵十股")
                
                # 🚀 達成「免轉貼」目的：直接產出複製區塊
                st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                final_output = f"【Predator V14.0 智能十股】\n市場：{market}\n指標：[乖離率, 法人買賣(張), 戰術標籤]\n\n{report_text}"
                st.code(final_output, language="markdown")
                
                # 數據表格
                st.subheader("📊 詳細戰術指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("分析結果為空，可能目前非交易時段或 API 延遲。")
        else:
            st.error("無法連網獲取數據，請檢查 GitHub 網路權限。")

st.divider()
st.caption("數據來源：TWSE API & Yahoo Finance | 環境：Streamlit Cloud (無感化介入)")
