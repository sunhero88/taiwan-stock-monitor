# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def run_analysis(df):
    """
    V14.0 Predator 核心分析邏輯 (內存版)
    """
    try:
        if df is None or df.empty:
            return None, pd.DataFrame(), {"Error": "輸入數據為空"}

        # 技術指標計算
        df['Return'] = df['Close'].pct_change() * 100
        
        if len(df) >= 20:
            df['MA20'] = df['Close'].rolling(window=20).mean()
            df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
            df['Vol_Ratio'] = df['Volume'] / df['Volume'].rolling(window=20).mean().replace(0, np.inf)
            
            def get_tag(row):
                tags = []
                if row['Vol_Ratio'] > 1.5 and row['Return'] > 1.8: tags.append("🔥主力進攻")
                if -1.5 < row['MA_Bias'] < 3.5 and row['Return'] > 0: tags.append("🛡️低位起漲")
                if row.get('Body_Power', 0) > 80: tags.append("⚡真突破")
                return " ".join(tags) if tags else "○ 觀察"
            
            # K線力道計算
            df['Body_Power'] = (abs(df['Close'] - df['Open']) / (df['High'] - df['Low']).replace(0, np.inf)) * 100
            df['Predator_Tag'] = df.apply(get_tag, axis=1)
        else:
            df['Predator_Tag'] = "🛡️ 數據累積中"

        df = df.fillna(0)
        report_text = {"📊 今日個股績效榜": df.tail(15)[['Symbol', 'Close', 'Return', 'Predator_Tag']].to_string(index=False)}
        return [], df, report_text
    except Exception as e:
        return None, pd.DataFrame(), {"Error": str(e)}

# 為了兼容舊的 GitHub Action 模式，保留 run 函數
def run(market_id):
    from pathlib import Path
    data_file = Path(f"raw_data_{market_id}.csv")
    if data_file.exists():
        return run_analysis(pd.read_csv(data_file))
    return None, pd.DataFrame(), {"Error": "找不到檔案"}
