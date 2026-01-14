# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def run_analysis(df):
    """
    V14.0 Predator 智能分析引擎 - 盤中即時動能版
    """
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
            
            # --- 籌碼/動能指標 (自動切換) ---
            inst_net = latest.get('Inst_Net', 0)
            
            # 如果沒有法人數據 (盤中或被擋)，則計算「即時主力動能」
            # 公式：(收盤-開盤)/(最高-最低) * 成交量 * 0.3 (係數)
            if inst_net == 0:
                h_l_range = latest['High'] - latest['Low']
                if h_l_range > 0:
                    # 估算淨買盤 (Volume Force)
                    est_force = latest['Volume'] * ((latest['Close'] - latest['Open']) / h_l_range) * 0.5
                else:
                    est_force = 0
                
                # 標示為 ⚡ (估算)
                val_k = round(est_force / 1000, 1)
                latest['Inst_Status'] = f"⚡🔴+{val_k}k" if est_force > 0 else f"⚡🔵{val_k}k"
                score_feed = est_force
            else:
                # 使用真實法人數據
                val_k = round(inst_net / 1000, 1)
                latest['Inst_Status'] = f"🔴+{val_k}k" if inst_net > 0 else f"🔵{val_k}k"
                score_feed = inst_net

            # --- 智能評分 ---
            # 動能/籌碼加分
            chip_score = min(25, max(0, score_feed / 1000 * 5)) if score_feed > 0 else 0
            
            score = (min(latest['Vol_Ratio'] * 12, 40) + 
                     max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + 
                     chip_score)
            latest['Score'] = score
            
            # --- 戰術標籤 ---
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -2.0 < latest['MA_Bias'] < 3.5: tags.append("🛡️起漲")
            
            # 判斷是「法人」還是「預估主力」
            if inst_net != 0:
                if inst_net > 0: tags.append("🏦法人")
            else:
                if score_feed > 0: tags.append("⚡主力") # 盤中動能強
            
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
