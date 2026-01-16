# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json
import yfinance as yf
from datetime import datetime

def analyze_market_trend(indices_data, tw_inst_total):
    """生成宏觀分析短評"""
    try:
        twii = indices_data.get('^TWII', {})
        tw_change = float(twii.get('漲跌', 0)) if isinstance(twii.get('漲跌'), (int, float)) else 0
        tw_trend = "偏多" if tw_change > 0 else "偏空"
        fund_status = "流入" if "🔴" in str(tw_inst_total) else "流出"
        return f"台股結構{tw_trend}，資金呈現{fund_status}。"
    except:
        return "宏觀數據待更新"

def enrich_fundamentals(top_10_df):
    """補強 OPM/QoQ 基本面結構"""
    enriched_list = []
    # 確保輸入是 DataFrame
    if top_10_df is None or top_10_df.empty:
        return []

    for _, row in top_10_df.iterrows():
        # 強制轉為字典，避免 Series 索引問題
        row_dict = row.to_dict()
        symbol = row_dict.get('Symbol', 'Unknown')
        
        fund_data = {"OPM": 0, "QoQ": 0, "PE": 0, "Sector": "Unknown"}
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            fund_data["OPM"] = round(info.get('operatingMargins', 0) * 100, 2)
            fund_data["QoQ"] = round(info.get('revenueGrowth', 0) * 100, 2)
            fund_data["PE"] = round(info.get('trailingPE', 0), 2)
            fund_data["Sector"] = info.get('sector', 'Unknown')
        except: 
            pass
        
        row_dict['Fundamentals'] = fund_data
        enriched_list.append(row_dict)
    return enriched_list

def generate_ai_json(market, timestamp, macro_data, top_10_df):
    """V15.1: 產出結構化數據包 (防崩潰設計)"""
    enriched_stocks = enrich_fundamentals(top_10_df)
    final_stocks_list = []
    
    for item in enriched_stocks:
        # 使用 .get() 進行防禦性取值，避免 KeyError
        inst_status = item.get('Inst_Status', 'N/A')
        inst_type = "Estimated_Force" if "⚡" in str(inst_status) else "Real_Institutional"
        
        stock_data = {
            "Symbol": item.get('Symbol', 'Unknown'),
            "Price": item.get('Close', 0),
            "Technical": {
                "MA_Bias": round(item.get('MA_Bias', 0), 2),
                "Vol_Ratio": round(item.get('Vol_Ratio', 0), 2),
                "ERS": round(item.get('Score', 0), 1),
                "Tag": item.get('Predator_Tag', '○觀察')
            },
            "Institutional": {
                "Status": inst_status, 
                "Type": inst_type,
                "Net_Raw": item.get('Inst_Net_Raw', 0)
            },
            "Structure": item.get('Fundamentals', {})
        }
        final_stocks_list.append(stock_data)

    ai_data = {
        "meta": {
            "system": "Predator V15.1 (Stable)", 
            "market": market, 
            "timestamp": timestamp,
            "session": "EOD" if datetime.now().hour >= 14 else "INTRADAY"
        },
        "macro": macro_data,
        "stocks": final_stocks_list
    }
    return json.dumps(ai_data, ensure_ascii=False, indent=2)

def run_analysis(df):
    """核心過濾與排序邏輯 (V15.1 Stable)"""
    try:
        if df is None or df.empty: 
            return pd.DataFrame(), ""
        
        # 重置索引，確保數據乾淨
        df = df.reset_index(drop=True)
        results = []
        
        # 預先計算全市場權值門檻 (前50大)
        df['Amount'] = df['Close'] * df['Volume']
        # 處理空值避免報錯
        if len(df) > 50:
            top_50_amt = df.groupby('Symbol')['Amount'].last().nlargest(50).min()
        else:
            top_50_amt = 0

        for symbol, group in df.groupby('Symbol'):
            if len(group) < 20: continue
            
            # 取得最新一筆數據 (Series)
            latest_series = group.sort_values('Date').iloc[-1]
            
            # --- 關鍵修正：將 Series 轉為 Dict 進行操作，避免 Pandas 鎖定與賦值失效 ---
            latest = latest_series.to_dict()
            
            # 1. 基礎指標計算
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100 if ma20 else 0
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            
            # 2. Body_Power 力道
            h_l = latest['High'] - latest['Low']
            latest['Body_Power'] = (abs(latest['Close'] - latest['Open']) / h_l * 100) if h_l > 0 else 0
            
            # 3. Kill Switch (否決邏輯) - 直接跳過不加入 results
            if latest['MA_Bias'] > 20: continue 
            if latest['Body_Power'] < 20 and latest['Vol_Ratio'] > 2.5: continue 
            
            # 4. ERS 評分
            penalty = max(0, (latest['MA_Bias'] - 10) / 5) if latest['MA_Bias'] > 10 else 0
            ret = ((latest['Close'] - latest['Open']) / latest['Open']) * 100 if latest['Open'] else 0
            latest['Return'] = ret # 確保 Return 欄位存在
            latest['Score'] = ret * latest['Vol_Ratio'] * (1 - 0.5 * penalty)
            
            # 5. 籌碼狀態 (防禦性處理)
            inst_net = latest.get('Inst_Net', 0)
            if pd.isna(inst_net): inst_net = 0 # 轉 NaN 為 0
            
            latest['Inst_Net_Raw'] = inst_net
            val_k = round(inst_net/1000, 1)
            # 這裡明確寫入 Inst_Status
            latest['Inst_Status'] = f"🔴+{val_k}k" if inst_net > 0 else f"🔵{val_k}k"
            
            # 6. 標籤系統
            is_heavy = latest['Amount'] >= top_50_amt
            tags = []
            if 0 < latest['MA_Bias'] <= (8 if is_heavy else 12): tags.append("🟢起漲")
            if latest['Vol_Ratio'] >= (1.2 if is_heavy else 1.8): tags.append("🟡主力")
            if latest['Body_Power'] > 75: tags.append("🟣突破")
            
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            
            # 將處理好的 Dict 加入結果列表
            results.append(latest)

        if not results: 
            return pd.DataFrame(), ""
        
        # 從 Dict 列表建立 DataFrame，保證欄位與 Dict Key 一致
        full_df = pd.DataFrame(results)
        
        # 確保必要欄位存在 (防止空值錯誤)
        required_cols = ['Symbol', 'Close', 'Return', 'Vol_Ratio', 'Predator_Tag', 'Score', 'Inst_Status']
        for col in required_cols:
            if col not in full_df.columns:
                full_df[col] = 0 if col != 'Predator_Tag' else ''
                
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        return top_10, ""
        
    except Exception as e:
        # 捕捉所有錯誤並回傳空值，避免網頁崩潰
        print(f"DEBUG Error: {e}") # 在後台印出錯誤以便除錯
        return pd.DataFrame(), f"Analysis Error: {str(e)}"
