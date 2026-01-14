# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def run_analysis(df):
    """
    V14.0 Predator 核心分析引擎 - 智能評分版
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame(), ""

        results = []
        # 按股票代碼群組處理
        for symbol, group in df.groupby('Symbol'):
            if len(group) < 20: continue
            
            group = group.sort_index()
            latest = group.iloc[-1].copy()
            
            # 1. 技術指標計算
            close = group['Close']
            ma20 = close.rolling(window=20).mean()
            vol_ma20 = group['Volume'].rolling(window=20).mean()
            
            latest['Return'] = group['Close'].pct_change().iloc[-1] * 100
            latest['MA_Bias'] = ((latest['Close'] - ma20.iloc[-1]) / ma20.iloc[-1]) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20.iloc[-1] if vol_ma20.iloc[-1] != 0 else 0
            
            # K線力道 (Body_Power)
            k_range = latest['High'] - latest['Low']
            latest['Body_Power'] = (abs(latest['Close'] - latest['Open']) / k_range * 100) if k_range != 0 else 0
            
            # 2. 智能評分邏輯 (Score)
            # 權重：量能(40%) + 位階安全(30%) + 力道(30%)
            score = 0
            score += min(latest['Vol_Ratio'] * 15, 40) # 量能爆發
            score += max(0, (10 - abs(latest['MA_Bias'])) * 3) # 位階越近月線分越高
            score += (latest['Body_Power'] * 0.3) # 收盤品質
            latest['Score'] = score
            
            # 3. 戰術標籤
            tags = []
            if latest['Vol_Ratio'] > 1.5 and latest['Return'] > 1.5: tags.append("🔥主力進攻")
            if -1.5 < latest['MA_Bias'] < 3.5: tags.append("🛡️低位起漲")
            if latest['Body_Power'] > 80 and latest['Return'] > 0: tags.append("⚡真突破")
            latest['Predator_Tag'] = " ".join(tags) if tags else "○ 觀察"
            
            results.append(latest)

        if not results:
            return pd.DataFrame(), "無有效數據"

        # 4. 智能篩選關鍵十股
        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        # 5. 格式化精簡指標報告
        report_df = top_10[['Symbol', 'Close', 'MA_Bias', 'Vol_Ratio', 'Predator_Tag']]
        # 格式化數字
        report_df['MA_Bias'] = report_df['MA_Bias'].map('{:.1f}%'.format)
        report_df['Vol_Ratio'] = report_df['Vol_Ratio'].map('{:.1f}x'.format)
        
        report_text = report_df.to_string(index=False)
        return top_10, report_text

    except Exception as e:
        return pd.DataFrame(), f"分析異常: {str(e)}"

