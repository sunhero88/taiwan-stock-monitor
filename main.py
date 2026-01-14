# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
import time
import random

def fetch_inst_data():
    """從證交所 API 獲取三大法人買賣超 (終極穩定版)"""
    url = "https://www.twse.com.tw/fund/T86?response=json&selectType=ALL"
    # 🚀 模擬真人的標頭
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/zh/page/trading/fund/T86.html',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    for attempt in range(2): # 失敗會自動重試
        try:
            session = requests.Session()
            # 取得基礎 Cookie 授權
            session.get("https://www.twse.com.tw/zh/index.html", headers=headers, timeout=10)
            time.sleep(random.uniform(1.0, 2.0)) # 隨機延遲模擬真人
            
            r = session.get(url, headers=headers, timeout=15)
            data = r.json()
            
            if data.get('stat') == 'OK' and 'data' in data:
                df_inst = pd.DataFrame(data['data'])[[0, 18]] # 0:代號, 18:合計
                df_inst.columns = ['Symbol', 'Inst_Net']
                df_inst['Symbol'] = df_inst['Symbol'].str.strip() + ".TW"
                df_inst['Inst_Net'] = df_inst['Inst_Net'].str.replace(',', '').astype(float)
                return df_inst
        except:
            time.sleep(2)
            continue
    return pd.DataFrame()

def fetch_market_data(m_id):
    """雲端即時抓取數據"""
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 下載價量
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    # 抓取籌碼 (僅台股)
    inst_df = fetch_inst_data() if m_id == "tw-share" else pd.DataFrame()
    
    all_res = []
    for s in symbols:
        if s in raw_data.columns.levels[0]:
            s_df = raw_data[s].dropna().copy()
            s_df['Symbol'] = s
            # 🚀 籌碼對齊
            if not inst_df.empty and s in inst_df['Symbol'].values:
                s_df['Inst_Net'] = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
            else:
                s_df['Inst_Net'] = 0
            all_res.append(s_df)
    return pd.concat(all_res) if all_res else pd.DataFrame()

# UI 呈現
st.set_page_config(page_title="Predator V14.0", layout="wide")
st.title("🦅 Predator 指揮中心 V14.0 (雲端穩定版)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動智能關鍵十股分析"):
    with st.spinner("🚀 正在接入最新資訊與法人籌碼..."):
        full_df = fetch_market_data(market)
        if not full_df.empty:
            top_10, report_text = analyzer.run_analysis(full_df)
            
            if not top_10.empty:
                st.success("✅ 數據與籌碼介入成功")
                
                # 🚀 複製區塊
                st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                timestamp = time.strftime('%Y-%m-%d %H:%M')
                st.code(f"【Predator V14.0 智能十股】\n市場：{market}\n時間：{timestamp}\n\n{report_text}")
                
                # 表格
                st.subheader("📊 關鍵標的指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("分析異常，請重試。")
        else:
            st.error("無法取得雲端數據。")
