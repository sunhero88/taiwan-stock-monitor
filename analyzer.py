# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json

def analyze_market_trend(indices_data, tw_inst_total):
    """
    生成宏觀股市分析短評 (供 JSON 的 macro 欄位使用)
    """
    try:
        # 1. 解讀台股加權
        twii = indices_data.get('^TWII', {})
        tw_change = float(twii.get('漲跌', 0)) if isinstance(twii.get('漲跌'), (int, float)) else 0
        tw_trend = "偏多" if tw_change > 0 else "偏空"
        
        # 2. 解讀費半
        sox = indices_data.get('^SOX', {})
        sox_change = float(sox.get('漲跌', 0)) if isinstance(sox.get('漲跌'), (int, float)) else 0
        sox_status = "強勢" if sox_change > 0 else "疲軟"
        
        # 3. 資金面
        fund_status = "流入" if "🔴" in str(tw_inst_total) else "流出"
        
        return f"台股結構{tw_trend}，費半表現{sox_status}，整體資金呈現{fund_status}。"
    except:
        return "宏觀數據待更新"

def generate_ai_json(market, timestamp, macro_data, top_10_df):
    """
    將所有戰情數據打包成 JSON 格式 (AI 專用)
    """
    stocks_list = []
    if not top_10_df.empty:
        for _, row in top_10_df.iterrows():
            # 構建單檔股票數據
            stock_data = {
                "Symbol": row['Symbol'],
                "Price": row['Close'],
                "MA_Bias": round(row['MA_Bias'], 2),
                "Vol_Ratio": round(row['Vol_Ratio'], 2),
                "Inst_Status": row['Inst_Status'], # 保留視覺化狀態 (🔴/⚡)
                "Inst_Net_Raw": row.get('Inst_Net', 0), # 保留原始數值供運算
                "Tag": row['Predator_Tag'],
                "Score": round(row['Score'], 1)
            }
            stocks_list.append(stock_data)
    
    # 組裝完整封包
    ai_data = {
        "meta": {
            "system": "Predator V14.0",
            "market": market,
            "timestamp": timestamp,
            "mode": "JSON_API"
        },
        "macro": macro_data,
        "stocks": stocks_list
    }
    
    return json.dumps(ai_data, ensure_ascii=False, indent=2)

def run_analysis(df):
    """
    核心分析引擎：計算技術指標與籌碼評分
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame(), ""

        df = df.reset_index()
        results = []
        
        for symbol, group in df.groupby('Symbol'):
            if len(group) < 20: continue # 至少要有 20 天數據才能算 MA20
            
            group = group.sort_values('Date').tail(30)
            latest = group.iloc[-1].copy()
            
            # --- 技術指標 ---
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            
            # --- 籌碼/動能 雙模式判斷 ---
            inst_net = latest.get('Inst_Net', 0)
            
            # 判斷是否為 0 (盤中或無數據)，若是則啟動「⚡估算模式」
            if inst_net == 0:
                h_l_range = latest['High'] - latest['Low']
                # 估算買盤力道
                est_force = latest['Volume'] * ((latest['Close'] - latest['Open']) / h_l_range) * 0.5 if h_l_range > 0 else 0
                val_k = round(est_force / 1000, 1)
                latest['Inst_Status'] = f"⚡🔴+{val_k}k" if est_force > 0 else f"⚡🔵{val_k}k"
                score_feed = est_force
            else:
                # 真實盤後數據
                val_k = round(inst_net / 1000, 1)
                latest['Inst_Status'] = f"🔴+{val_k}k" if inst_net > 0 else f"🔵{val_k}k"
                score_feed = inst_net

            # --- 評分系統 ---
            chip_score = min(25, max(0, score_feed / 1000 * 5)) if score_feed > 0 else 0
            score = (min(latest['Vol_Ratio'] * 12, 40) + 
                     max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + 
                     chip_score)
            latest['Score'] = score
            
            # --- 戰術標籤 ---
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -2.0 < latest['MA_Bias'] < 3.5: tags.append("🛡️起漲")
            
            # 根據模式給予不同標籤
            if "⚡" not in latest['Inst_Status'] and inst_net > 0: 
                tags.append("🏦法人")
            elif "⚡" in latest['Inst_Status'] and score_feed > 0: 
                tags.append("⚡主力")
            
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            results.append(latest)

        if not results:
            return pd.DataFrame(), ""

        full_df = pd.DataFrame(results)
        # 排序取前 10
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        return top_10, "" # 不需要回傳純文字報告，改用 JSON

    except Exception as e:
        return pd.DataFrame(), f"Analysis Error: {str(e)}"
