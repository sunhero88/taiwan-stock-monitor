# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json
import yfinance as yf

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
    """
    針對篩選出的 Top 10 進行基本面結構掃描 (OPM, QoQ)
    注意：這會增加一點處理時間，但對 AI 判讀至關重要
    """
    enriched_list = []
    
    for _, row in top_10_df.iterrows():
        symbol = row['Symbol']
        # 預設值
        fund_data = {
            "OPM": None,      # 營業利益率 (Operating Margins)
            "QoQ": None,      # 營收成長率 (Revenue Growth)
            "PE": None,       # 本益比
            "Sector": "Unknown"
        }
        
        try:
            # 呼叫 yfinance info (需要連網)
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            fund_data["OPM"] = round(info.get('operatingMargins', 0) * 100, 2) if info.get('operatingMargins') else 0
            fund_data["QoQ"] = round(info.get('revenueGrowth', 0) * 100, 2) if info.get('revenueGrowth') else 0
            fund_data["PE"] = round(info.get('trailingPE', 0), 2) if info.get('trailingPE') else 0
            fund_data["Sector"] = info.get('sector', 'Unknown')
            
        except:
            pass # 抓不到就用預設值，不卡流程
            
        # 將基本面數據合併到 row
        row_dict = row.to_dict()
        row_dict['Fundamentals'] = fund_data
        enriched_list.append(row_dict)
        
    return enriched_list

def generate_ai_json(market, timestamp, macro_data, top_10_df):
    """
    V15.0: 包含結構面 (Structure) 與 修正後的籌碼數據
    """
    # 1. 進行基本面補強 (Enrichment)
    # 轉為列表字典
    enriched_stocks = enrich_fundamentals(top_10_df)
    
    final_stocks_list = []
    for item in enriched_stocks:
        
        # 判斷籌碼性質
        inst_type = "Estimated_Force" if "⚡" in str(item.get('Inst_Status')) else "Real_Institutional"
        
        stock_data = {
            "Symbol": item['Symbol'],
            "Price": item['Close'],
            "Technical": {
                "MA_Bias": round(item['MA_Bias'], 2),
                "Vol_Ratio": round(item['Vol_Ratio'], 2),
                "Score": round(item['Score'], 1),
                "Tag": item['Predator_Tag']
            },
            "Institutional": {
                "Status_Visual": item['Inst_Status'],
                "Net_Raw": item.get('Inst_Net_Raw', 0), # 修正：確保有數值
                "Type": inst_type, # 標註是估算還是真法人
                "Note": "Intraday breakdown (Foreign/Trust) unavail." if inst_type == "Estimated_Force" else "Official Data"
            },
            "Structure": item['Fundamentals'] # 新增：基本面結構
        }
        final_stocks_list.append(stock_data)
    
    # 組裝完整封包
    ai_data = {
        "meta": {
            "system": "Predator V15.0 (Structure Enhanced)",
            "market": market,
            "timestamp": timestamp,
            "mode": "JSON_API_FULL"
        },
        "macro": macro_data,
        "stocks": final_stocks_list
    }
    
    return json.dumps(ai_data, ensure_ascii=False, indent=2)

def run_analysis(df):
    try:
        if df is None or df.empty:
            return pd.DataFrame(), ""

        df = df.reset_index()
        results = []
        
        for symbol, group in df.groupby('Symbol'):
            if len(group) < 20: continue
            
            group = group.sort_values('Date').tail(30)
            latest = group.iloc[-1].copy()
            
            # --- 技術指標 ---
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            
            # --- 籌碼/動能 修正 ---
            inst_net = latest.get('Inst_Net', 0)
            
            if inst_net == 0:
                # ⚡ 盤中估算模式
                h_l_range = latest['High'] - latest['Low']
                est_force = latest['Volume'] * ((latest['Close'] - latest['Open']) / h_l_range) * 0.5 if h_l_range > 0 else 0
                
                # 關鍵修正：將估算值填入 Inst_Net_Raw，讓 AI 有數字可讀
                latest['Inst_Net_Raw'] = est_force 
                
                val_k = round(est_force / 1000, 1)
                latest['Inst_Status'] = f"⚡🔴+{val_k}k" if est_force > 0 else f"⚡🔵{val_k}k"
                score_feed = est_force
            else:
                # 盤後真實模式
                latest['Inst_Net_Raw'] = inst_net # 真實值
                
                val_k = round(inst_net / 1000, 1)
                latest['Inst_Status'] = f"🔴+{val_k}k" if inst_net > 0 else f"🔵{val_k}k"
                score_feed = inst_net

            # --- 評分 ---
            chip_score = min(25, max(0, score_feed / 1000 * 5)) if score_feed > 0 else 0
            score = (min(latest['Vol_Ratio'] * 12, 40) + 
                     max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + 
                     chip_score)
            latest['Score'] = score
            
            # --- 標籤 ---
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -2.0 < latest['MA_Bias'] < 3.5: tags.append("🛡️起漲")
            
            if "⚡" not in latest['Inst_Status'] and inst_net > 0: 
                tags.append("🏦法人")
            elif "⚡" in latest['Inst_Status'] and score_feed > 0: 
                tags.append("⚡主力")
            
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            results.append(latest)

        if not results: return pd.DataFrame(), ""

        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        return top_10, ""

    except Exception as e:
        return pd.DataFrame(), f"Analysis Error: {str(e)}"
