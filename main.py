# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
import time
from datetime import datetime
import pytz

TW_TZ = pytz.timezone('Asia/Taipei')

# --- 數據抓取模組 (維持原樣，確保功能完整) ---
def fetch_detailed_indices():
    tickers = {
        "^TWII": "🇹🇼 加權指數",
        "^TWOII": "🇹🇼 櫃買指數",
        "^SOX": "🇺🇸 費城半導體",
        "^DJI": "🇺🇸 道瓊工業"
    }
    data_list = []
    try:
        raw_data = yf.download(list(tickers.keys()), period="5d", progress=False)
        for ticker, name in tickers.items():
            try:
                if isinstance(raw_data.columns, pd.MultiIndex):
                    if ticker in raw_data.columns.get_level_values(1): hist = raw_data.xs(ticker, axis=1, level=1)
                    else: hist = yf.Ticker(ticker).history(period="5d")
                else: hist = raw_data

                if not hist.empty and len(hist) >= 2:
                    col = 'Close' if 'Close' in hist.columns else hist.columns[0]
                    latest = hist.iloc[-1]
                    prev = hist.iloc[-2]
                    price = latest[col]
                    change = price - prev[col]
                    pct = (change / prev[col]) * 100
                    
                    data_list.append({
                        "指數名稱": name, "現價": f"{price:,.0f}", "漲跌": f"{change:+.2f}", "幅度": f"{pct:+.2f}%",
                        "開盤": f"{latest.get('Open', 0):,.0f}", "最高": f"{latest.get('High', 0):,.0f}", "最低": f"{latest.get('Low', 0):,.0f}", "昨收": f"{prev[col]:,.0f}"
                    })
            except:
                data_list.append({"指數名稱": name, "現價": "-", "漲跌": "-", "幅度": "-", "開盤": "-", "最高": "-", "最低": "-", "昨收": "-"})
                continue
    except: pass
    return pd.DataFrame(data_list)

def fetch_market_amount():
    now = datetime.now(TW_TZ)
    if now.hour < 15 and now.hour >= 9: return "⚡ 盤中統計中"
    date_str = now.strftime('%Y-%m-%d')
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params={"dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": date_str}, timeout=3)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'): return f"{data['data'][0]['Trading_Money'] / 100000000:.0f} 億"
    except: pass
    return "待更新"

def fetch_market_total_inst():
    now = datetime.now(TW_TZ)
    if now.hour < 15: return "⚡ 盤中動能觀測"
    date_str = now.strftime('%Y-%m-%d')
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params={"dataset": "TaiwanStockTotalInstitutionalInvestors", "date": date_str}, timeout=3)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            df = pd.DataFrame(data['data'])
            net = (df['buy'].sum() - df['sell'].sum()) / 100000000
            return f"🔴+{net:.1f}億" if net > 0 else f"🔵{net:.1f}億"
    except: pass
    return "待更新"

def fetch_inst_data_finmind_stock():
    now = datetime.now(TW_TZ)
    if now.hour < 15: return pd.DataFrame()
    date_str = now.strftime('%Y-%m-%d')
    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params={"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": date_str}, timeout=5)
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
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
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

# --- UI ---
st.set_page_config(page_title="Predator V15.0", layout="wide")
st.title("🦅 Predator 指揮中心 V15.0 (結構化戰略版)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動全域掃描與結構分析"):
    with st.spinner("🚀 正在執行：技術面篩選 ➔ 動能估算 ➔ 基本面結構掃描..."):
        
        # 1. 宏觀
        indices_df = fetch_detailed_indices()
        total_amount = fetch_market_amount()
        total_inst = fetch_market_total_inst()
        
        # 2. 個股
        full_df = fetch_market_data(market)
        
        st.subheader("🌍 宏觀戰情室")
        c1, c2 = st.columns(2)
        c1.metric("💰 大盤成交金額", total_amount)
        c2.metric("🏦 全市場法人", total_inst)
        
        if not indices_df.empty:
            def color_change(val):
                if isinstance(val, str) and '+' in val: return 'color: #ff4b4b'
                elif isinstance(val, str) and '-' in val: return 'color: #00c853'
                return ''
            st.dataframe(indices_df.style.applymap(color_change, subset=['漲跌', '幅度']), use_container_width=True, hide_index=True)
            
        st.divider()

        if not full_df.empty:
            top_10, _ = analyzer.run_analysis(full_df)
            
            if not top_10.empty:
                st.success("✅ V15.0 結構化數據構建完成")
                
                # 3. 生成 JSON (含基本面)
                timestamp = datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M')
                macro_dict = {"overview": {"amount": total_amount, "inst_net": total_inst}, "indices": indices_df.to_dict(orient='records') if not indices_df.empty else []}
                
                # 呼叫新的 JSON 生成器
                json_payload = analyzer.generate_ai_json(market, timestamp, macro_dict, top_10)
                
                st.subheader("🤖 AI 戰略數據包 (JSON V15.0)")
                st.caption("新增：Inst_Net_Raw 數值填充、Structure (OPM/QoQ/PE) 基本面數據")
                st.code(json_payload, language="json")
                
                st.subheader("📊 關鍵標的指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
            else:
                st.error("分析結果為空")
        else:
            st.error("數據獲取異常")
