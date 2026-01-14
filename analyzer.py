# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def run_analysis(df):
    """
    V14.0 Predator 核心分析引擎 - 籌碼與價量綜合版
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame(), "⚠️ 數據源連線暫時中斷"

        df = df.reset_index()
        results = []
        
        # 依股票代號分組處理
        for symbol, group in df.groupby('Symbol'):
            if len(group) < 25: continue
            
            group = group.sort_values('Date').tail(30)
            latest = group.iloc[-1].copy()
            
            # 1. 技術指標：乖離率與量能比
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            latest['Return'] = (latest['Close'] / group['Close'].iloc[-2] - 1) * 100
            
            # K線實體力道
            k_range = latest['High'] - latest['Low']
            latest['Body_Power'] = (abs(latest['Close'] - latest['Open']) / k_range * 100) if k_range > 0 else 0
            
            # 2. 籌碼面：三大法人買賣超
            inst_net = latest.get('Inst_Net', 0)
            # 格式化顯示：🔴買進 🔵賣出
            latest['Inst_Status'] = f"🔴+{int(inst_net)}" if inst_net > 0 else (f"🔵{int(inst_net)}" if inst_net < 0 else "⚪0")
            
            # 3. 智能評分系統 (量能40%+位階30%+籌碼30%)
            score = (min(latest['Vol_Ratio'] * 12, 40) + 
                     max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + 
                     (25 if inst_net > 0 else 0))
            latest['Score'] = score
            
            # 4. 戰術標籤
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -1.5 < latest['MA_Bias'] < 3.0: tags.append("🛡️起漲")
            if latest['Body_Power'] > 80: tags.append("⚡突破")
            if inst_net > 0: tags.append("🏦法人")
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            
            results.append(latest)

        if not results:
            return pd.DataFrame(), "❌ 目前無達標之標的"

        # 5. 篩選關鍵十股
        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        # 6. 生成精簡報告 (報告格式優化)
        report_df = top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Predator_Tag']].copy()
        report_df['MA_Bias'] = report_df['MA_Bias'].map('{:.1f}%'.format)
        report_text = report_df.to_string(index=False, justify='left')
        
        return top_10, report_text

    except Exception as e:
        return pd.DataFrame(), f"分析異常: {str(e)}"
