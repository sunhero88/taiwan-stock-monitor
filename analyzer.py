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
            
            # 技術指標
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / group['Volume'].rolling(window=20).mean().iloc[-1]
            
            # 🚀 籌碼標籤：🔴買進 🔵賣出
            inst_net = latest.get('Inst_Net', 0)
            if inst_net > 0:
                latest['Inst_Status'] = f"🔴+{int(inst_net)}"
            elif inst_net < 0:
                latest['Inst_Status'] = f"🔵{int(inst_net)}"
            else:
                latest['Inst_Status'] = "⚪0"
            
            # 智能評分
            score = (min(latest['Vol_Ratio'] * 12, 40) + 
                     max(0, (12 - abs(latest['MA_Bias'])) * 3) + 
                     (25 if inst_net > 0 else 0))
            latest['Score'] = score
            
            # 標籤
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -1.5 < latest['MA_Bias'] < 3.0: tags.append("🛡️起漲")
            if inst_net > 0: tags.append("🏦法人")
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            
            results.append(latest)

        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        # 精簡報告
        report_df = top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Predator_Tag']].copy()
        report_df['MA_Bias'] = report_df['MA_Bias'].map('{:.1f}%'.format)
        report_text = report_df.to_string(index=False, justify='left')
        
        return top_10, report_text
    except Exception as e:
        return pd.DataFrame(), f"異常: {str(e)}"
