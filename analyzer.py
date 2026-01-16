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
    for _, row in top_10_df.iterrows():
        symbol = row['Symbol']
        fund_data = {"OPM": 0, "QoQ": 0, "PE": 0, "Sector": "Unknown"}
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            fund_data["OPM"] = round(info.get('operatingMargins', 0) * 100, 2)
            fund_data["QoQ"] = round(info.get('revenueGrowth', 0) * 100, 2)
            fund_data["PE"] = round(info.get('trailingPE', 0), 2)
            fund_data["Sector"] = info.get('sector', 'Unknown')
        except: pass
        
        row_dict = row.to_dict()
        row_dict['Fundamentals'] = fund_data
        enriched_list.append(row_dict)
    return enriched_list

def generate_ai_json(market, timestamp, macro_data, top_10_df):
    """V15.0: 產出結構化數據包"""
    enriched_stocks = enrich_fundamentals(top_10_df)
    final_stocks_list = []
    for item in enriched_stocks:
        inst_type = "Estimated_Force" if "⚡" in str(item.get('Inst_Status')) else "Real_Institutional"
        stock_data = {
            "Symbol": item['Symbol'],
            "Price": item['Close'],
            "Technical": {
                "MA_Bias": round(item['MA_Bias'], 2),
                "Vol_Ratio": round(item['Vol_Ratio'], 2),
                "ERS": round(item['Score'], 1),
                "Tag": item['Predator_Tag']
            },
            "Institutional": {"Status": item['Inst_Status'], "Type": inst_type},
            "Structure": item['Fundamentals']
        }
        final_stocks_list.append(stock_data)

    ai_data = {
        "meta": {"system": "Predator V15.0", "market": market, "timestamp": timestamp},
        "macro": macro_data,
        "stocks": final_stocks_list
    }
    return json.dumps(ai_data, ensure_ascii=False, indent=2)

def run_analysis(df):
    """核心過濾與排序邏輯 (V15.0 Standard)"""
    try:
        if df is None or df.empty: return pd.DataFrame(), ""
        df = df.reset_index()
        results = []
        
        # 動態定義權值股
        df['Amount'] = df['Close'] * df['Volume']
        top_50_amt = df.groupby('Symbol')['Amount'].transform('last').nlargest(50).min()

        for symbol, group in df.groupby('Symbol'):
            if len(group) < 20: continue
            latest = group.sort_values('Date').iloc[-1].copy()
            
            # --- 技術指標計算 ---
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            
            # --- Body_Power 力道判讀 ---
            h_l = latest['High'] - latest['Low']
            latest['Body_Power'] = (abs(latest['Close'] - latest['Open']) / h_l * 100) if h_l > 0 else 0
            
            # --- Kill Switch 否決邏輯 ---
            if latest['MA_Bias'] > 20: continue # 乖離極端
            if latest['Body_Power'] < 20 and latest['Vol_Ratio'] > 2.5: continue # 派貨陷阱
            
            # --- ERS 評分與懲罰 ---
            penalty = max(0, (latest['MA_Bias'] - 10) / 5) if latest['MA_Bias'] > 10 else 0
            ret = ((latest['Close'] - latest['Open']) / latest['Open']) * 100
            latest['Score'] = ret * latest['Vol_Ratio'] * (1 - 0.5 * penalty)
            
            # --- 紅綠燈標籤系統 ---
            is_heavy = latest['Amount'] >= top_50_amt
            tags = []
            if 0 < latest['MA_Bias'] <= (8 if is_heavy else 12): tags.append("🟢起漲")
            if latest['Vol_Ratio'] >= (1.2 if is_heavy else 1.8): tags.append("🟡主力")
            if latest['Body_Power'] > 75: tags.append("🟣突破")
            
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            results.append(latest)

        if not results: return pd.DataFrame(), ""
        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        return top_10, ""
    except Exception as e:
        return pd.DataFrame(), f"Analysis Error: {str(e)}"
