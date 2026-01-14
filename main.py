# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
import time
import random
from datetime import datetime, timedelta
import pytz

def fetch_inst_data():
    """
    從證交所獲取三大法人買賣超 (智能日期回溯版)
    解決: 盤中抓不到數據、假日抓不到數據的問題
    """
    # 設定台灣時區
    tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tz)
    
    # 模擬真實瀏覽器標頭
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.twse.com.tw/zh/page/trading/fund/T86.html',
        'Accept': 'application/json'
    }

    # 🔄 自動回溯機制：嘗試回推過去 5 天，直到抓到數據為止
    for i in range(5):
        check_date = now - timedelta(days=i)
        
        # 如果是「今天」且時間還沒到 15:00，直接跳過 (因為證交所還沒開獎)
        if i == 0 and check_date.hour < 15:
            continue
            
        date_str = check_date.strftime('%Y%m%d')
        # 使用新版 RWD API (比舊版穩定)
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALL&response=json"
        
        try:
            time.sleep(random.uniform(0.5, 1.5)) # 禮貌性延遲
            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()
            
            if data.get('stat') == 'OK' and 'data' in data:
                # 抓到了！處理數據
                df_inst = pd.DataFrame(data['data'])
                # 欄位對應: Index 0=代號, Index 18=三大法人合計買賣超(張) - 依據新版API格式
                # 註: RWD API 的欄位順序可能不同，通常 0是代號, 18是三大法人合計
                # 若 RWD API 回傳格式有變，這裡取 [0] 跟 [18] (合計)
                df_inst = df_inst.iloc[:, [0, 18]]
                df_inst.columns = ['Symbol', 'Inst_Net']
                
                df_inst['Symbol'] = df_inst['Symbol'].astype(str) + ".TW"
                df_inst['Inst_Net'] = df_inst['Inst_Net'].str.replace(',', '').astype(float)
                
                # 成功獲取後，在側邊欄提示數據日期
                st.sidebar.success(f"📅 已獲取籌碼數據：{date_str}")
                return df_inst
        except Exception:
            continue # 失敗就找前一天
            
    # 如果找了5天都沒資料 (連假或IP被封鎖)
    st.sidebar.warning("⚠️ 無法獲取近 5 日籌碼，可能為連假或連線限制")
    return pd.DataFrame()

def fetch_market_data(m_id):
    """雲端即時抓取數據"""
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW", "2357.TW", "3035.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 1. 下載價量 (Yahoo)
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    
    # 2. 下載籌碼 (TWSE - 僅台股)
    inst_df = fetch_inst_data() if m_id == "tw-share" else pd.DataFrame()
    
    all_res = []
    for s in symbols:
        if s in raw_data.columns.levels[0]:
            s_df = raw_data[s].dropna().copy()
            s_df['Symbol'] = s
            
            # 3. 籌碼對齊合併
            if not inst_df.empty and s in inst_inst_df['Symbol'].values:
                # 將張數轉為 float 並存入
                net_val = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
                s_df['Inst_Net'] = net_val
            else:
                s_df['Inst_Net'] = 0
            
            all_res.append(s_df)
            
    return pd.concat(all_res) if all_res else pd.DataFrame()

# --- Streamlit UI ---
st.set_page_config(page_title="Predator V14.0", layout="wide")
st.title("🦅 Predator 指揮中心 V14.0 (智能回溯版)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動智能關鍵十股分析"):
    with st.spinner("🚀 正在接入真實資訊 (自動校正日期)..."):
        full_df = fetch_market_data(market)
        
        if not full_df.empty:
            top_10, report_text = analyzer.run_analysis(full_df)
            
            if not top_10.empty:
                st.success("✅ 數據與籌碼介入成功")
                
                # 📋 複製區塊
                st.subheader("📋 數據介入結果 (複製給 Predator Gem)")
                timestamp = time.strftime('%Y-%m-%d %H:%M')
                st.code(f"【Predator V14.0 智能十股】\n市場：{market}\n時間：{timestamp}\n\n{report_text}")
                
                # 📊 數據表格
                st.subheader("📊 關鍵標的指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("分析結果為空，請稍後重試。")
        else:
            st.error("無法取得雲端數據。")
