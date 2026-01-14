# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def run_analysis(df):
    try:
        if df is None or df.empty:
            return pd.DataFrame(), "⚠️ 雲端數據源連線暫時中斷"

        # 1. 數據預處理 (解決 Groupby 可能產生的 MultiIndex 問題)
        df = df.reset_index()
        results = []
        
        for symbol, group in df.groupby('Symbol'):
            # 確保有足夠歷史數據計算指標 (至少 25 筆)
            if len(group) < 25: continue
            
            group = group.sort_values('Date').tail(30) # 只取最近 30 天
            latest = group.iloc[-1].copy()
            
            # 2. 技術指標核心計算
            close_series = group['Close']
            ma20 = close_series.rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            latest['Return'] = (latest['Close'] / group['Close'].iloc[-2] - 1) * 100
            
            # Body_Power: 判斷實體佔比 (過濾影線)
            k_range = latest['High'] - latest['Low']
            latest['Body_Power'] = (abs(latest['Close'] - latest['Open']) / k_range * 100) if k_range > 0 else 0
            
            # 3. 智能評分系統 (權重：量能 40% / 位階 30% / 力道 30%)
            latest['Score'] = (min(latest['Vol_Ratio'] * 10, 40) + 
                               max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + 
                               (latest['Body_Power'] * 0.3))
            
            # 4. 戰術標籤判定
            tags = []
            if latest['Vol_Ratio'] > 1.5 and latest['Return'] > 1.5: tags.append("🔥主力進攻")
            if -2.0 < latest['MA_Bias'] < 3.0: tags.append("🛡️低位起漲")
            if latest['Body_Power'] > 85 and latest['Return'] > 0: tags.append("⚡真突破")
            latest['Predator_Tag'] = " ".join(tags) if tags else "○ 觀察"
            results.append(latest)

        if not results:
            return pd.DataFrame(), "❌ 監控標的目前未達分析門檻"

        # 5. 挑選前 10 檔關鍵標的
        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        # 格式化輸出
        report_df = top_10[['Symbol', 'Close', 'MA_Bias', 'Vol_Ratio', 'Predator_Tag']]
        report_df['MA_Bias'] = report_df['MA_Bias'].map('{:.1f}%'.format)
        report_df['Vol_Ratio'] = report_df['Vol_Ratio'].map('{:.2f}x'.format)
        
        return top_10, report_df.to_string(index=False)

    except Exception as e:
        return pd.DataFrame(), f"分析系統錯誤: {str(e)}"
