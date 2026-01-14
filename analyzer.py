# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path

def run(market_id):
    try:
        data_file = Path(f"raw_data_{market_id}.csv")
        if not data_file.exists():
            return None, None, {"Error": "數據檔案缺失"}
            
        df = pd.read_csv(data_file)
        
        # A. 計算技術指標
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        
        df['K_High_Low'] = df['High'] - df['Low']
        df['K_Real_Body'] = abs(df['Close'] - df['Open'])
        # 防止除以零
        df['Body_Power'] = (df['K_Real_Body'] / df['K_High_Low'].replace(0, np.inf)) * 100
        
        # B. 處理量能 (若當前無成交量則判定為盤前)
        latest_vol = df['Volume'].iloc[-1]
        if latest_vol == 0 or pd.isna(latest_vol):
            df['Vol_Ratio'] = 0
            is_premarket = True
        else:
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20']
            is_premarket = False

        df = df.fillna(0)

        # C. Predator 標籤邏輯
        def get_tag(row):
            if is_premarket: return "🕒 等待開盤"
            tags = []
            if row['Vol_Ratio'] > 1.5 and row['Return'] > 1.5: tags.append("🔥主力進攻")
            if row['MA_Bias'] < 3 and row['Return'] > 0: tags.append("🛡️低位起漲")
            if row['Body_Power'] > 75: tags.append("⚡真突破")
            elif row['Body_Power'] < 30 and row['Vol_Ratio'] > 2: tags.append("❌假突破")
            return " ".join(tags) if tags else "○ 觀察"

        df['Predator_Tag'] = df.apply(get_tag, axis=1)
        
        # D. 生成文字報告
        target_df = df.sort_values('Vol_Ratio' if not is_premarket else 'Return', ascending=False).head(10)
        report_text = {
            "📊 今日個股績效榜": target_df[['Symbol', 'Close', 'Return', 'Predator_Tag']].to_string(index=False),
            "FINAL_AI_REPORT": "盤前著重匯率與位階，盤中/盤後著重真突破與乖離。"
        }
        
        return [], df, report_text
    except Exception as e:
        return None, None, {"Error": str(e)}
