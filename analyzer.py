# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json
import yfinance as yf
from datetime import datetime

def analyze_market_trend(indices_data, tw_inst_total):
    """生成宏觀股市分析短評"""
    try:
        twii = indices_data.get('^TWII', {})
        tw_change = float(twii.get('漲跌', 0)) if isinstance(twii.get('漲跌'), (int, float)) else 0
        tw_trend = "偏多" if tw_change > 0 else "偏空"
        
        sox = indices_data.get('^SOX', {})
        sox_change = float(sox.get('漲跌', 0)) if isinstance(sox.get('漲跌'), (int, float)) else 0
        sox_status = "強勢" if sox_change > 0 else "疲軟"
        
        fund_status = "流入" if "🔴" in str(tw_inst_total) else "流出"
        
        return f"台股結構{tw_trend}，費半表現{sox_status}，整體資金呈現{fund_status}。"
    except:
        return "宏觀數據待更新"

def enrich_fundamentals(top_10_df):
    """針對 Top 10 進行 OPM/QoQ 掃描"""
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
    """V15.0: 產出結構化 JSON 供 Gem 判讀"""
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
                "Score": round(item['Score'], 1),
                "Body_Power": round(item.get('Body_Power', 0), 1),
                "Tag": item['Predator_Tag']
            },
            "Institutional": {
                "Status_Visual": item['Inst_Status'],
                "Net_Raw": item.get('Inst_Net_Raw', 0),
                "Type": inst_type
            },
            "Structure": item['Fundamentals']
        }
        final_stocks_list.append(stock_data)

    ai_data = {
        "meta": {
            "system": "Predator V15.0 (Final)",
            "market": market,
            "timestamp": timestamp,
            "session": "EOD" if datetime.now().hour >= 14 else "INTRADAY"
        },
        "macro": macro_data,
        "stocks": final_stocks_list
    }
    return json.dumps(ai_data, ensure_ascii=False, indent=2)

def run_analysis(df):
    """核心執行函數，整合 Kill Switch 與 ERS 排序"""
    try:
        if df is None or df.empty: return pd.DataFrame(), ""
        df = df.reset_index()
        results = []
        
        # 定義權值股門檻 (前50大成交額)
        df['Amount'] = df['Close'] * df['Volume']
        top_50_amt = df.groupby('Symbol')['Amount'].last().nlargest(50).min()

        for symbol, group in df.groupby('Symbol'):
            if len(group) < 20: continue
            group = group.sort_values('Date')
            latest = group.iloc[-1].copy()
            
            # --- 技術指標 ---
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            
            # --- Body_Power 力道 ---
            h_l = latest['High'] - latest['Low']
            latest['Body_Power'] = (abs(latest['Close'] - latest['Open']) / h_l * 100) if h_l > 0 else 0
            
            # --- 權值股判定與量能門檻 ---
            is_heavy = latest['Amount'] >= top_50_amt
            vol_qualified = latest['Vol_Ratio'] >= 1.2 if is_heavy else latest['Vol_Ratio'] >= 1.8
            
            # --- Kill Switch (否決邏輯) ---
            if latest['MA_Bias'] > 20: continue # 乖離極端
            if latest['Body_Power'] < 20 and latest['Vol_Ratio'] > 2.5: continue # 派貨陷阱
            
            # --- ERS 評分與懲罰 ---
            penalty = max(0, (latest['MA_Bias'] - 10) / 5) if latest['MA_Bias'] > 10 else 0
            latest['Return'] = ((latest['Close'] - latest['Open']) / latest['Open']) * 100
            latest['Score'] = latest['Return'] * latest['Vol_Ratio'] * (1 - 0.5 * penalty)
            
            # --- 籌碼與標籤 ---
            inst_net = latest.get('Inst_Net', 0)
            latest['Inst_Net_Raw'] = inst_net
            latest['Inst_Status'] = f"🔴+{round(inst_net/1000,1)}k" if inst_net > 0 else f"🔵{round(inst_net/1000,1)}k"
            
            tags = []
            if 0 < latest['MA_Bias'] <= (8 if is_heavy else 12): tags.append("🟢起漲")
