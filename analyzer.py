# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path

def run(market_id):
    try:
        data_file = Path(f"raw_data_{market_id}.csv")
        if not data_file.exists(): return None, None, {"Error": "檔案缺失"}
            
        df = pd.read_csv(data_file)
        
        # 技術指標計算
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        df['Body_Power'] = (abs(df['Close'] - df['Open']) / (df['High'] - df['Low']).replace(0, np.inf)) * 100
        
        # 處理盤前數據 (Volume=0)
        latest_vol = df['Volume'].iloc[-1]
        if latest_vol == 0 or pd.isna(latest_vol):
            df['Vol_Ratio'] = 0
            is_pre = True
        else:
            df['Vol_Ratio'] = df['Volume'] / df['Volume'].rolling(window=20).mean()
            is_pre = False

        # Predator 戰略標籤
        def get_tag(row):
            if is_pre: return "🕒 等待開盤"
            tags = []
            if row['Vol_Ratio'] > 1.5 and row['Return'] > 1.5: tags.append("🔥主力進攻")
            if row['MA_Bias'] < 3 and row['Return'] > 0: tags.append("🛡️低位起漲")
            if row['Body_Power'] > 75: tags.append("⚡真突破")
            return " ".join(tags) if tags else "○ 觀察"

        df['Predator_Tag'] = df.apply(get_tag, axis=1)
        
        # 產出摘要報告
        report_text = {
            "📊 今日個股績效榜": df.tail(10)[['Symbol', 'Close', 'Return', 'Predator_Tag']].to_string(index=False),
            "FINAL_AI_REPORT": "根據 V14.0 邏輯判讀：優先關注[🛡️低位起漲]且[⚡真突破]之標的。"
        }
        return [], df, report_text
    except Exception as e:
        return None, None, {"Error": str(e)}
