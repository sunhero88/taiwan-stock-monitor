# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
import time
from datetime import datetime, timedelta
import pytz

TW_TZ = pytz.timezone('Asia/Taipei')

# --- 1. 抓取全球指數 ---
def fetch_global_indices():
    """抓取 大盤、道瓊、那指、費半"""
    tickers = {
        "^TWII": "台股加權",
        "^DJI": "道瓊工業",
        "^IXIC": "那斯達克",
        "^SOX": "費城半導體"
    }
    try:
        # 下載最近 2 天數據以計算漲跌
        data = yf.download(list(tickers.keys()), period="5d", progress=False)
        indices_info = {}
        
        for ticker, name in tickers.items():
            # 處理 MultiIndex
            try:
                hist = data['Close'][ticker].dropna()
                if len(hist) >= 2:
                    price = hist.iloc[-1]
                    prev = hist.iloc[-2]
                    change = price - prev
                    pct = (change / prev) * 100
                    
                    indices_info[ticker] = {
                        "Name": name,
                        "Price": f"{price:,.0f}",
                        "Change": change,
                        "Pct": pct,
                        "Color": "off" if change == 0 else ("inverse" if change < 0 else "normal") # 配合 st.metric 顏色
                    }
            except:
                continue
        return indices_info
    except:
        return {}

# --- 2. 抓取大盤總籌碼 (全市場) ---
def fetch_market_total_inst():
    """抓取全市場法人買賣超金額 (億元)"""
    now = datetime.now(TW_TZ)
    # 盤中同樣無法取得，回傳 None
    if now.hour < 15:
        return "⚡盤中統計中..."
        
    date_str = now.strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockTotalInstitutionalInvestors",
        "date": date_str
    }
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            df = pd.DataFrame(data['data'])
            # 加總買賣超 (單位：元) -> 轉為億元
            total_buy = df['buy'].sum()
            total_sell = df['sell'].sum()
            net = (total_buy - total_sell) / 100000000 # 億
            
            return f"🔴+{net:.1f}億" if net > 0 else f"🔵{net:.1f}億"
    except:
        pass
    return "⚡待更新"

# --- 3. 抓取個股籌碼 (保留原本邏輯) ---
def fetch_inst_data_finmind_stock():
    now = datetime.now(TW_TZ)
    if now.hour < 15: return pd.DataFrame() # 盤中回傳空

    date_str = now.strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    params = { "dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": date_str }
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            df = pd.DataFrame(data['data'])
            df['Net'] = df['buy'] - df['sell']
            df_group = df.groupby('stock_id')['Net'].sum().reset_index()
            df_group.columns = ['Symbol', 'Inst_Net']
            df_group['Symbol'] = df_group['Symbol'].astype(str) + ".TW"
            return df_group
    except: pass
    return pd.DataFrame()

def fetch_market_data(m_id):
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW", "2357.TW", "3035.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 1. 抓取即時價量
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    
    # 2. 抓取個股真實籌碼 (15:00後)
    inst_df = fetch_inst_data_finmind_stock() if m_id == "tw-share" else pd.DataFrame()
    
    all_res = []
    for s in symbols:
        if s in raw_data.columns.levels[0]:
            s_df = raw_data[s].dropna().copy()
            s_df['Symbol'] = s
            if not inst_df.empty and s in inst_df['Symbol'].values:
                s_df['Inst_Net'] = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
            else:
                s_df['Inst_Net'] = 0
            all_res.append(s_df)
    return pd.concat(all_res) if all_res else pd.DataFrame()

# --- Streamlit UI 介面 ---
st.set_page_config(page_title="Predator V14.0", layout="wide")

st.title("🦅 Predator 指揮中心 V14.0 (宏觀戰情版)")
market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動全域掃描與分析"):
    with st.spinner("🚀 正在連線全球交易所獲取指數、籌碼與個股動能..."):
        
        # A. 獲取宏觀數據
        indices = fetch_global_indices()
        market_inst_total = fetch_market_total_inst() if market == "tw-share" else "N/A"
        
        # B. 獲取個股數據
        full_df = fetch_market_data(market)
        
        if not full_df.empty:
            top_10, report_text = analyzer.run_analysis(full_df)
            macro_analysis = analyzer.analyze_market_trend(indices, market_inst_total)
            
            # --- 1. 顯示宏觀指標 (Metrics) ---
            st.subheader("🌍 宏觀戰情室")
            col1, col2, col3, col4, col5 = st.columns(5)
            
            # 顯示指數卡片
            def show_metric(col, key, label):
                item = indices.get(key)
                if item:
                    col.metric(label, item['Price'], f"{item['Change']:.2f} ({item['Pct']:.2f}%)")
                else:
                    col.metric(label, "Loading...", "0.00")

            show_metric(col1, "^TWII", "🇹🇼 台股加權")
            show_metric(col2, "^DJI", "🇺🇸 道瓊工業")
            show_metric(col3, "^SOX", "🇺🇸 費城半導體") # 對台股最重要
            
            # 顯示大盤籌碼
            col4.metric("💰 全市場法人", market_inst_total, "今日動向")
            
            st.divider()

            if not top_10.empty:
                st.success("✅ 全域分析完成")
                
                # --- 2. 📋 整合報告區塊 (一鍵複製) ---
                st.subheader("📋 戰情摘要 (複製給 Predator Gem)")
                timestamp = datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M')
                
                # 組合 宏觀 + 個股 的完整報告
                final_report = f"""【Predator V14.0 戰情日報】
時間：{timestamp} | 市場：{market}

[🌍 宏觀分析]
{macro_analysis}

[🦅 智能十股]
{report_text}
"""
                st.code(final_report, language="markdown")
                
                # --- 3. 詳細表格 ---
                st.subheader("📊 關鍵標的指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("個股分析結果為空")
        else:
            st.error("數據獲取異常")
