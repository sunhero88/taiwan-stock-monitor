# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
import time

# 嘗試抓取真實數據，若失敗則回傳空，交由 analyzer 進行估算
def fetch_inst_data_finmind():
    try:
        # 簡單嘗試一次 FinMind，失敗不強求
        url = "https://api.finmindtrade.com/api/v4/data"
        # 抓取昨天 (因為今天盤中還沒出)
        date_str = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        params = { "dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": date_str }
        r = requests.get(url, params=params, timeout=3)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            df = pd.DataFrame(data['data'])
            df['Net'] = df['buy'] - df['sell']
            df_group = df.groupby('stock_id')['Net'].sum().reset_index()
            df_group.columns = ['Symbol', 'Inst_Net']
            df_group['Symbol'] = df_group['Symbol'].astype(str) + ".TW"
            return df_group
    except:
        pass
    return pd.DataFrame()

def fetch_market_data(m_id):
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW", "2357.TW", "3035.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 1. 核心：Yahoo 即時價量 (絕對不會斷)
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    
    # 2. 輔助：嘗試抓取真實籌碼 (盤中通常抓不到)
    inst_df = fetch_inst_data_finmind() if m_id == "tw-share" else pd.DataFrame()
    
    all_res = []
    for s in symbols:
        if s in raw_data.columns.levels[0]:
            s_df = raw_data[s].dropna().copy()
            s_df['Symbol'] = s
            
            # 對齊邏輯
            if not inst_df.empty and s in inst_df['Symbol'].values:
                s_df['Inst_Net'] = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
            else:
                s_df['Inst_Net'] = 0 # 設為 0，讓 analyzer 啟動估算模式
            
            all_res.append(s_df)
    return pd.concat(all_res) if all_res else pd.DataFrame()

# --- UI ---
st.set_page_config(page_title="Predator V14.0", layout="wide")
st.title("🦅 Predator 指揮中心 V14.0 (盤中實戰版)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動智能關鍵十股分析"):
    with st.spinner("🚀 正在計算盤中主力動能與技術指標..."):
        full_df = fetch_market_data(market)
        
        if not full_df.empty:
            top_10, report_text = analyzer.run_analysis(full_df)
            
            if not top_10.empty:
                st.success("✅ 盤中數據介入成功")
                
                # 說明：⚡ 代表即時估算
                st.info("💡 提示：Inst_Status 若顯示 ⚡ 符號，代表為「盤中即時主力動能」估算值 (因盤後數據尚未公布)。")
                
                st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                timestamp = time.strftime('%Y-%m-%d %H:%M')
                st.code(f"【Predator V14.0 智能十股】\n市場：{market}\n時間：{timestamp}\n\n{report_text}")
                
                st.subheader("📊 關鍵標的指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("無分析結果")
        else:
            st.error("數據源回應異常")
