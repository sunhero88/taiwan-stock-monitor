# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def run_analysis(df):
    try:
        if df is None or df.empty:
            return pd.DataFrame(), "⚠️ 數據源中斷"

        df = df.reset_index()
        results = []
        
        for symbol, group in df.groupby('Symbol'):
            if len(group) < 25: continue
            
            group = group.sort_values('Date').tail(30)
            latest = group.iloc[-1].copy()
            
            # --- 技術指標 ---
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            latest['Return'] = (latest['Close'] / group['Close'].iloc[-2] - 1) * 100
            
            # --- 籌碼指標 (FinMind 來源) ---
            inst_net = latest.get('Inst_Net', 0)
            
            # 格式化顯示：滿 1000 股顯示 k，否則顯示張數
            # 處理 0 的狀況 (無數據或剛好為0)
            if inst_net == 0:
                 latest['Inst_Status'] = "⚪無/待更新"
            elif abs(inst_net) >= 1000:
                val_k = round(inst_net / 1000, 1) # 換算成千張 (k)
                latest['Inst_Status'] = f"🔴+{val_k}k" if inst_net > 0 else f"🔵{val_k}k"
            else:
                # 不足 1 張 (少見，但也處理)
                latest['Inst_Status'] = f"🔴+{int(inst_net)}" if inst_net > 0 else f"🔵{int(inst_net)}"
            
            # --- 智能評分 ---
            inst_score = min(25, max(0, inst_net / 1000 * 5)) if inst_net > 0 else 0
            
            score = (min(latest['Vol_Ratio'] * 12, 40) + 
                     max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + 
                     inst_score)
            latest['Score'] = score
            
            # --- 戰術標籤 ---
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -2.0 < latest['MA_Bias'] < 3.5: tags.append("🛡️起漲")
            if inst_net > 2000000: tags.append("🏦法人大買") # 2000張以上
            elif inst_net > 0: tags.append("🏦法人")
            
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            
            results.append(latest)

        if not results:
            return pd.DataFrame(), "❌ 無達標標的"

        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        report_df = top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Predator_Tag']].copy()
        report_df['MA_Bias'] = report_df['MA_Bias'].map('{:.1f}%'.format)
        report_text = report_df.to_string(index=False, justify='left')
        
        return top_10, report_text

    except Exception as e:
        return pd.DataFrame(), f"分析異常: {str(e)}"
