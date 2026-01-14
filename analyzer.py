# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json

# ... (保留 analyze_market_trend 函數不變) ...
def analyze_market_trend(indices_data, tw_inst_total):
    # ... (維持原樣) ...
    try:
        twii = indices_data.get('^TWII', {})
        tw_trend = "偏多" if twii.get('Change', 0) > 0 else "偏空"
        tw_pct = twii.get('Pct', 0)
        sox = indices_data.get('^SOX', {})
        sox_status = "強勢" if sox.get('Change', 0) > 0 else "疲軟"
        fund_status = "流入" if "🔴" in tw_inst_total else "流出"
        
        return f"台股{tw_trend}({tw_pct:.1f}%)，費半{sox_status}，法人資金{fund_status}。"
    except:
        return "數據不足"

# 🚀 新增：專門產給 AI 看的 JSON 生成器
def generate_ai_json(market, timestamp, macro_data, top_10_df):
    """
    將所有戰情數據打包成 JSON 格式
    """
    # 1. 處理個股數據
    stocks_list = []
    if not top_10_df.empty:
        # 將 DataFrame 轉為字典列表
        for _, row in top_10_df.iterrows():
            stocks_list.append({
                "Symbol": row['Symbol'],
                "Price": row['Close'],
                "MA_Bias": round(row['MA_Bias'], 2), # 保留數字格式供 AI 運算
                "Vol_Ratio": round(row['Vol_Ratio'], 2),
                "Inst_Net": row.get('Inst_Net', 0), # 保留原始張數/股數
                "Tag": row['Predator_Tag'],
                "Score": round(row['Score'], 1)
            })
    
    # 2. 組裝完整封包
    ai_data = {
        "meta": {
            "system": "Predator V14.0",
            "market": market,
            "timestamp": timestamp
        },
        "macro": macro_data, # 宏觀數據 (指數、大盤籌碼)
        "strategy": {
            "focus": "Top 10 Smart Selection",
            "criteria": "Volume + MA_Bias + Inst_Flow"
        },
        "stocks": stocks_list
    }
    
    # 轉為 JSON 字串 (ensure_ascii=False 讓中文正常顯示)
    return json.dumps(ai_data, ensure_ascii=False, indent=2)

def run_analysis(df):
    # ... (這部分邏輯維持原樣，負責計算指標) ...
    try:
        if df is None or df.empty: return pd.DataFrame(), ""
        df = df.reset_index()
        results = []
        for symbol, group in df.groupby('Symbol'):
            if len(group) < 25: continue
            group = group.sort_values('Date').tail(30)
            latest = group.iloc[-1].copy()
            
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            
            inst_net = latest.get('Inst_Net', 0)
            if inst_net == 0:
                h_l_range = latest['High'] - latest['Low']
                est_force = latest['Volume'] * ((latest['Close'] - latest['Open']) / h_l_range) * 0.5 if h_l_range > 0 else 0
                val_k = round(est_force / 1000, 1)
                latest['Inst_Status'] = f"⚡🔴+{val_k}k" if est_force > 0 else f"⚡🔵{val_k}k"
                score_feed = est_force
            else:
                val_k = round(inst_net / 1000, 1)
                latest['Inst_Status'] = f"🔴+{val_k}k" if inst_net > 0 else f"🔵{val_k}k"
                score_feed = inst_net

            chip_score = min(25, max(0, score_feed / 1000 * 5)) if score_feed > 0 else 0
            score = (min(latest['Vol_Ratio'] * 12, 40) + max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + chip_score)
            latest['Score'] = score
            
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -2.0 < latest['MA_Bias'] < 3.5: tags.append("🛡️起漲")
            if "⚡" not in latest['Inst_Status'] and inst_net > 0: tags.append("🏦法人")
            elif "⚡" in latest['Inst_Status'] and score_feed > 0: tags.append("⚡主力")
            
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            results.append(latest)

        if not results: return pd.DataFrame(), ""

        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        # 這裡只回傳 DataFrame，文字報告改由 main.py 呼叫 JSON 生成
        return top_10, "" 

    except Exception as e:
        return pd.DataFrame(), f"Error: {str(e)}"
