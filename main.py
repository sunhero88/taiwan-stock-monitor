# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
import time
from datetime import datetime
import pytz

# 設定台灣時區
TW_TZ = pytz.timezone('Asia/Taipei')

# ==========================================
# 1. 數據抓取模組 (Fetch Modules)
# ==========================================

def fetch_detailed_indices():
    """
    抓取詳細的大盤、櫃買、美股指數數據
    包含: 現價, 漲跌, 幅度, 開盤, 最高, 最低, 昨收
    """
    tickers = {
        "^TWII": "🇹🇼 加權指數",
        "^TWOII": "🇹🇼 櫃買指數",
        "^SOX": "🇺🇸 費城半導體",
        "^DJI": "🇺🇸 道瓊工業"
    }
    
    data_list = []
    
    try:
        # 下載數據 (5天以確保能計算漲跌)
        raw_data = yf.download(list(tickers.keys()), period="5d", progress=False)
        
        for ticker, name in tickers.items():
            try:
                # 兼容 yfinance 不同版本的 MultiIndex 結構
                # 嘗試提取 Close 欄位
                if isinstance(raw_data.columns, pd.MultiIndex):
                    # 檢查 ticker 是否在第二層
                    if ticker in raw_data.columns.get_level_values(1):
                        hist = raw_data.xs(ticker, axis=1, level=1)
                    else:
                        # 備用方案：單獨抓取
                        hist = yf.Ticker(ticker).history(period="5d")
                else:
                    hist = raw_data

                # 確保有數據
                if not hist.empty and len(hist) >= 2:
                    # 統一欄位名稱 (有些版本是 'Close', 有些是 'Adj Close')
                    close_col = 'Close' if 'Close' in hist.columns else hist.columns[0]
                    
                    latest = hist.iloc[-1]
                    prev = hist.iloc[-2]
                    
                    price = latest[close_col]
                    prev_close = prev[close_col]
                    change = price - prev_close
                    pct = (change / prev_close) * 100
                    
                    # 構建資料列
                    data_list.append({
                        "指數名稱": name,
                        "現價": f"{price:,.0f}",
                        "漲跌": f"{change:+.2f}",
                        "幅度": f"{pct:+.2f}%",
                        "開盤": f"{latest.get('Open', 0):,.0f}",
                        "最高": f"{latest.get('High', 0):,.0f}",
                        "最低": f"{latest.get('Low', 0):,.0f}",
                        "昨收": f"{prev_close:,.0f}"
                    })
            except Exception:
                # 容錯處理：如果某個指數抓不到，填空值
                data_list.append({
                    "指數名稱": name, "現價": "-", "漲跌": "-", "幅度": "-", 
                    "開盤": "-", "最高": "-", "最低": "-", "昨收": "-"
                })
                continue
                
    except Exception as e:
        st.error(f"指數獲取異常: {e}")
        
    return pd.DataFrame(data_list)

def fetch_market_amount():
    """抓取加權成交金額 (億元)"""
    now = datetime.now(TW_TZ)
    # 盤中回傳統計中
    if now.hour < 15 and now.hour >= 9:
        return "⚡ 盤中統計中"
    
    # 嘗試抓取 FinMind
    date_str = now.strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    params = { "dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": date_str }
    
    try:
        r = requests.get(url, params=params, timeout=3)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            amount = data['data'][0]['Trading_Money']
            return f"{amount / 100000000:.0f} 億"
    except:
        pass
    return "待更新"

def fetch_market_total_inst():
    """抓取全市場法人買賣超 (億元)"""
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return "⚡ 盤中動能觀測"
        
    date_str = now.strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    params = { "dataset": "TaiwanStockTotalInstitutionalInvestors", "date": date_str }
    try:
        r = requests.get(url, params=params, timeout=3)
        data = r.json()
        if data.get('msg') == 'success' and data.get('data'):
            df = pd.DataFrame(data['data'])
            # 買 - 賣
            net = (df['buy'].sum() - df['sell'].sum()) / 100000000
            return f"🔴+{net:.1f}億" if net > 0 else f"🔵{net:.1f}億"
    except:
        pass
    return "待更新"

def fetch_inst_data_finmind_stock():
    """抓取個股法人籌碼 (僅盤後有效)"""
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return pd.DataFrame() # 盤中回傳空 -> 觸發估算模式

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
    except:
        pass
    return pd.DataFrame()

def fetch_market_data(m_id):
    """整合抓取：價量 + 籌碼"""
    # 監控清單
    targets = {
        "tw-share": ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW", "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW", "2357.TW", "3035.TW"],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"]
    }
    symbols = targets.get(m_id, targets["tw-share"])
    
    # 1. 抓取 Yahoo 即時價量
    raw_data = yf.download(symbols, period="2mo", interval="1d", group_by='ticker', progress=False)
    
    # 2. 抓取 FinMind 真實籌碼 (僅盤後)
    inst_df = fetch_inst_data_finmind_stock() if m_id == "tw-share" else pd.DataFrame()
    
    all_res = []
    for s in symbols:
        # 確保該股票有抓到數據
        if s in raw_data.columns.levels[0]:
            s_df = raw_data[s].dropna().copy()
            s_df['Symbol'] = s
            
            # 對齊籌碼數據
            if not inst_df.empty and s in inst_df['Symbol'].values:
                s_df['Inst_Net'] = inst_df.loc[inst_df['Symbol'] == s, 'Inst_Net'].values[0]
            else:
                s_df['Inst_Net'] = 0 # 設為 0 -> analyzer 會轉為估算模式
            
            all_res.append(s_df)
            
    return pd.concat(all_res) if all_res else pd.DataFrame()

# ==========================================
# 2. Streamlit UI 主程式
# ==========================================

st.set_page_config(page_title="Predator V14.0", layout="wide")
st.title("🦅 Predator 指揮中心 V14.0 (AI 戰術全景版)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動全域掃描與分析"):
    with st.spinner("🚀 正在連線全球交易所與 AI 數據接口..."):
        
        # --- A. 數據獲取層 ---
        indices_df = fetch_detailed_indices()
        total_amount = fetch_market_amount()
        total_inst = fetch_market_total_inst()
        full_df = fetch_market_data(market)
        
        # --- B. 宏觀戰情室顯示 ---
        st.subheader("🌍 宏觀戰情室 (Market Overview)")
        
        c1, c2 = st.columns(2)
        c1.metric("💰 大盤成交金額", total_amount)
        c2.metric("🏦 全市場法人動向", total_inst)
        
        if not indices_df.empty:
            # 顏色樣式函數
            def color_change(val):
                if isinstance(val, str) and '+' in val: return 'color: #ff4b4b' # 紅
                elif isinstance(val, str) and '-' in val: return 'color: #00c853' # 綠
                return ''
            
            st.dataframe(
                indices_df.style.applymap(color_change, subset=['漲跌', '幅度']),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("指數數據暫時無法取得")
            
        st.divider()

        # --- C. 個股智能分析層 ---
        if not full_df.empty:
            top_10, _ = analyzer.run_analysis(full_df)
            
            if not top_10.empty:
                st.success("✅ AI 數據包構建完成")
                
                # 1. 構建 JSON 數據包
                timestamp = datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M')
                
                # 準備宏觀數據字典
                macro_dict = {
                    "overview": {
                        "amount": total_amount,
                        "inst_net": total_inst
                    },
                    "indices": indices_df.to_dict(orient='records') if not indices_df.empty else []
                }
                
                # 生成 JSON
                json_payload = analyzer.generate_ai_json(market, timestamp, macro_dict, top_10)
                
                # 2. 顯示 AI 接口 (複製區塊)
                st.subheader("🤖 AI 介入接口 (複製 JSON 給 Gem)")
                st.caption("說明：此 JSON 包含完整的宏觀與個股技術數據，AI 可直接讀取進行深度推演。")
                st.code(json_payload, language="json")
                
                # 3. 顯示人類閱讀表格
                st.subheader("📊 關鍵標的指標")
                st.dataframe(top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Vol_Ratio', 'Predator_Tag', 'Score']], use_container_width=True)
                
            else:
                st.error("個股分析結果為空")
        else:
            st.error("無法獲取市場數據")
