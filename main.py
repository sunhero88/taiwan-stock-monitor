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

# --- 1. 抓取詳細指數數據 (含開高低收) ---
def fetch_detailed_indices():
    """
    抓取詳細的大盤、櫃買、美股指數數據
    包含: 現價, 漲跌, 幅度, 開盤, 最高, 最低, 昨收
    """
    # 定義要抓取的指數代碼
    # 註: Yahoo Finance 對台股類股指數支援不穩，故以龍頭股或ETF做參考，
    # 這裡我們主要抓取 加權、櫃買(嘗試 ^TWOII)、道瓊、費半
    tickers = {
        "^TWII": "🇹🇼 加權指數",
        "^TWOII": "🇹🇼 櫃買指數", # 櫃買有時會抓不到
        "^SOX": "🇺🇸 費城半導體",
        "^DJI": "🇺🇸 道瓊工業"
    }
    
    data_list = []
    
    try:
        # 下載數據
        raw_data = yf.download(list(tickers.keys()), period="5d", progress=False)
        
        for ticker, name in tickers.items():
            try:
                # 處理 MultiIndex 結構
                df = raw_data.xs(ticker, axis=1, level=1) if isinstance(raw_data.columns, pd.MultiIndex) else raw_data
                
                # 針對單一 ticker 再次確認
                if ticker not in raw_data.columns.levels[0]:
                     # 有時 download 會失敗，這裡做容錯
                     pass

                # 提取該指數的 OHLC
                # 注意：yfinance 的結構有時是 (Price, Ticker) 有時是 (Ticker, Price)
                # 這裡使用更穩健的單一提取法
                stock = yf.Ticker(ticker)
                hist = stock.history(period="5d")
                
                if not hist.empty:
                    latest = hist.iloc[-1]
                    prev = hist.iloc[-2]
                    
                    price = latest['Close']
                    change = price - prev['Close']
                    pct = (change / prev['Close']) * 100
                    
                    # 構建資料列
                    data_list.append({
                        "指數名稱": name,
                        "現價": f"{price:,.0f}",
                        "漲跌": f"{change:+.2f}",
                        "幅度": f"{pct:+.2f}%",
                        "開盤": f"{latest['Open']:,.0f}",
                        "最高": f"{latest['High']:,.0f}",
                        "最低": f"{latest['Low']:,.0f}",
                        "昨收": f"{prev['Close']:,.0f}",
                        # 成交金額通常 Yahoo 只有 Volume (股數)，這邊先留著，下面用 FinMind 補強
                        "成交量": f"{latest['Volume']/1000000:.1f}M" if latest['Volume'] > 0 else "-" 
                    })
            except Exception as e:
                # 抓不到就填空值，不報錯
                data_list.append({
                    "指數名稱": name, "現價": "-", "漲跌": "-", "幅度": "-", 
                    "開盤": "-", "最高": "-", "最低": "-", "昨收": "-"
                })
                continue
                
    except Exception as e:
        st.error(f"指數數據獲取異常: {e}")
        
    return pd.DataFrame(data_list)

# --- 2. 抓取大盤總成交金額 (FinMind) ---
def fetch_market_amount():
    """抓取加權與櫃買的真實成交金額 (億元)"""
    now = datetime.now(TW_TZ)
    if now.hour < 9: return "開盤前"
    
    # 這裡我們用簡單的估算或 API，FinMind 盤後才有準確金額
    # 盤中我們先回傳 "統計中"
    if now.hour < 15:
        return "⚡ 盤中統計中"
        
    date_str = now.strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    params = { "dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": date_str }
    
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            # FinMind TAIEX 的成交金額單位是元
            amount = data['data'][0]['Trading_Money']
            return f"{amount / 100000000:.0f} 億"
    except:
        pass
    return "待更新"

# --- 3. 抓取全市場法人 (維持原樣) ---
def fetch_market_total_inst():
    now = datetime.now(TW_TZ)
    if now.hour < 15: return "⚡盤中動能觀測"
    date_str = now.strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    params = { "dataset": "TaiwanStockTotalInstitutionalInvestors", "date": date_str }
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            df = pd.DataFrame(data['data'])
            net = (df['buy'].sum() - df['sell'].sum()) / 100000000
            return f"🔴+{net:.1f}億" if net > 0 else f"🔵{net:.1f}億"
    except: pass
    return "待更新"

# --- 4. 抓取個股與籌碼 (維持原樣) ---
def fetch_inst_data_finmind_stock():
    now = datetime.now(TW_TZ)
    if now.hour < 15: return pd.DataFrame()
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

# --- UI 介面 ---
st.set_page_config(page_title="Predator V14.0", layout="wide")

st.title("🦅 Predator 指揮中心 V14.0 (宏觀全景版)")
market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動全域掃描與分析"):
    with st.spinner("🚀 正在連線交易所獲取全景行情..."):
        
        # 1. 獲取宏觀指數 (詳細版)
        indices_df = fetch_detailed_indices()
        total_amount = fetch_market_amount()
        total_inst = fetch_market_total_inst()
        
        # 2. 獲取個股
        full_df = fetch_market_data(market)
        
        # 顯示宏觀戰情室
        st.subheader("🌍 宏觀戰情室 (Market Overview)")
        
        # 顯示大盤資金概況
        c1, c2 = st.columns(2)
        c1.metric("💰 大盤成交金額", total_amount)
        c2.metric("🏦 全市場法人動向", total_inst)
        
        # 顯示詳細指數表格 (比照 Yahoo 股市)
        if not indices_df.empty:
            # 針對漲跌欄位做顏色處理 (UI 美化)
            def color_change(val):
                if isinstance(val, str) and '+' in val:
                    return 'color: #ff4b4b' # 紅色 (漲)
                elif isinstance(val, str) and '-' in val:
                    return 'color: #00c853' # 綠色 (跌)
                return ''
            
            st.dataframe(
                indices_df.style.applymap(color_change, subset=['漲跌', '幅度']),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("指數數據暫時無法取得")
            
        st.divider()

        if not full_df.empty:
            top_10, report_text = analyzer.run_analysis(full_df)
            
            # 生成宏觀分析文字
            macro_text = analyzer.analyze_market_trend(
                # 這裡簡單轉換 indices_df 給 analyzer 使用
                {row['指數名稱']: {'Change': float(row['漲跌']), 'Pct': float(row['幅度'].strip('%'))} 
                 for _, row in indices_df.iterrows() if row['漲跌'] != '-'}, 
                total_inst
            )

            st.success("✅ 全域分析完成")
            
            st.subheader("📋 戰情摘要 (複製給 Predator Gem)")
            timestamp = datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M')
            final_report = f"""【Predator V14.0 戰情日報】
時間：{timestamp} | 市場：{market}

[🌍 宏觀數據]
成交金額：{total_amount} | 法人動向：{total_inst}
{indices_df.to_string(index=False) if not indices_df.empty else "指數數據N/A"}

[🦅 宏觀分析]
{macro_text}

[⚡ 智能十股]
{report_text}
"""
            st.code(final_report, language="markdown")
            
            st.subheader("📊 關鍵標的指標")
            st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
        else:
            st.error("個股數據獲取異常")
