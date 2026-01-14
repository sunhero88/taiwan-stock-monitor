# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
import time
from datetime import datetime, timedelta
import pytz

def fetch_inst_data_finmind():
    """
    從 FinMind Open Data API 獲取三大法人買賣超
    優點: 不會封鎖 Streamlit Cloud IP，穩定性極高
    """
    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    
    # 🔄 自動回溯機制：嘗試回推過去 5 天
    for i in range(5):
        check_date = now - timedelta(days=i)
        
        # 如果是「今天」且時間還沒到 15:00，直接跳過 (資料尚未產出)
        if i == 0 and check_date.hour < 15:
            continue
            
        date_str = check_date.strftime('%Y-%m-%d')
        
        # FinMind API: 獲取全市場個股三大法人買賣超
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {
            "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
            "date": date_str
        }
        
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            
            if data.get('msg') == 'success' and data.get('data'):
                # 資料處理
                df = pd.DataFrame(data['data'])
                # FinMind 回傳欄位: date, stock_id, name(法人別), buy(股), sell(股)
                # 我們需要計算「合計淨買賣超」 (Buy - Sell)
                df['Net'] = df['buy'] - df['sell']
                
                # 依股票代號分組，加總三大法人的淨買賣超
                df_group = df.groupby('stock_id')['Net'].sum().reset_index()
                df_group.columns = ['Symbol', 'Inst_Net']
                
                # 格式化代號
                df_group['Symbol'] = df_group['Symbol'].astype(str) + ".TW"
                
                st.sidebar.success(f"📅 籌碼數據源：FinMind ({date_str})")
                return df_group
        except Exception as e:
            print(f"FinMind 連線嘗試失敗: {e}")
            continue
            
    st.sidebar.warning("⚠️ 連續 5 日無籌碼數據 (可能為連假)")
    return pd.DataFrame()

def fetch_market_data(m_id):
    """全雲端即時抓取"""
    # 監控清單
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW", "2357.TW", "3035.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 1. 下載價量 (Yahoo Finance)
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    
    # 2. 下載籌碼 (FinMind - 僅台股)
    inst_df = fetch_inst_data_finmind() if m_id == "tw-share" else pd.DataFrame()
    
    all_res = []
    for s in symbols:
        if s in raw_data.columns.levels[0]:
            s_df = raw_data[s].dropna().copy()
            s_df['Symbol'] = s
            
            # 3. 籌碼對齊
            if not inst_df.empty and s in inst_df['Symbol'].values:
                # FinMind 單位是「股」，analyzer 會自動處理
                net_val = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
                s_df['Inst_Net'] = net_val
            else:
                s_df['Inst_Net'] = 0
            
            all_res.append(s_df)
            
    return pd.concat(all_res) if all_res else pd.DataFrame()

# --- UI 介面 ---
st.set_page_config(page_title="Predator V14.0", layout="wide")
st.title("🦅 Predator 指揮中心 V14.0 (FinMind 數據版)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動智能關鍵十股分析"):
    with st.spinner("🚀 正在從 FinMind 接入真實籌碼..."):
        full_df = fetch_market_data(market)
        
        if not full_df.empty:
            top_10, report_text = analyzer.run_analysis(full_df)
            
            if not top_10.empty:
                st.success("✅ 數據介入成功")
                
                # 📋 複製區塊
                st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                timestamp = time.strftime('%Y-%m-%d %H:%M')
                st.code(f"【Predator V14.0 智能十股】\n市場：{market}\n時間：{timestamp}\n\n{report_text}")
                
                # 📊 數據表格
                st.subheader("📊 關鍵標的指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("分析結果為空。")
        else:
            st.error("無法取得數據。")
