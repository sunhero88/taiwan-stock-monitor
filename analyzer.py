# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path

def run(market_id):
    try:
        data_file = Path(f"raw_data_{market_id}.csv")
        if not data_file.exists():
            return None, None, {"Error": "數據檔案生成中，請稍後再試。"}
            
        df = pd.read_csv(data_file)
        if df.empty:
            return None, None, {"Error": "數據檔案為空。"}

        # 基本處理
        df['Return'] = df['Close'].pct_change() * 100
        
        # 🛡️ 數據長度防禦邏輯
        if len(df) < 20:
            df['MA_Bias'] = 0
            df['Body_Power'] = 0
            df['Vol_Ratio'] = 0
            df['Predator_Tag'] = "🛡️ 數據採集中"
            is_pre = True
        else:
            # 正常計算指標
            df['MA20'] = df['Close'].rolling(window=20).mean()
            df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
            
            df['K_Range'] = df['High'] - df['Low']
            df['K_Body'] = abs(df['Close'] - df['Open'])
            df['Body_Power'] = (df['K_Body'] / df['K_Range'].replace(0, np.inf)) * 100
            
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20'].replace(0, np.inf)
            is_pre = False if df['Volume'].iloc[-1] > 0 else True

        # 戰略標籤判讀
        def get_tag(row):
            if row.get('Predator_Tag') == "🛡️ 數據採集中": return "🛡️ 數據採集中"
            if is_pre: return "🕒 待開盤"
            tags = []
            if row['Vol_Ratio'] > 1.5 and row['Return'] > 1.8: tags.append("🔥主力進攻")
            if -1.5 < row['MA_Bias'] < 3.5 and row['Return'] > 0: tags.append("🛡️低位起漲")
            if row['Body_Power'] > 80 and row['Return'] > 2: tags.append("⚡真突破")
            return " ".join(tags) if tags else "○ 觀察"

        df['Predator_Tag'] = df.apply(get_tag, axis=1)
        df = df.fillna(0) # 徹底消滅導致網頁崩潰的 NaN

        # 準備文字報告內容
        report_df = df.tail(15)[['Symbol', 'Close', 'Return', 'Predator_Tag']]
        report_text = {
            "📊 今日個股績效榜": report_df.to_string(index=False),
            "FINAL_AI_REPORT": "V14.0 數據介入成功。策略：關注[🛡️低位起漲]標的之右側轉強。"
        }
        
        return [], df, report_text

    except Exception as e:
        return None, None, {"Error": f"分析引擎異常: {str(e)}"}
