import pandas as pd
import numpy as np
from pathlib import Path

def run(market_id):
    try:
        data_file = Path(f"raw_data_{market_id}.csv")
        if not data_file.exists(): return None, None, {"Error": "檔案生成中..."}
        
        df = pd.read_csv(data_file)
        if df.empty: return None, None, {"Error": "數據空值"}

        df['Return'] = df['Close'].pct_change() * 100
        
        # 🛡️ 數據長度檢查，防止計算 MA20 崩潰
        if len(df) < 20:
            df['Predator_Tag'] = "🛡️ 累積數據中"
        else:
            df['MA20'] = df['Close'].rolling(window=20).mean()
            df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
            df['Vol_Ratio'] = df['Volume'] / df['Volume'].rolling(window=20).mean().replace(0, np.inf)
            
            def get_tag(row):
                tags = []
                if row['Vol_Ratio'] > 1.5 and row['Return'] > 1.8: tags.append("🔥主力進攻")
                if -1 < row['MA_Bias'] < 3.5: tags.append("🛡️低位起漲")
                return " ".join(tags) if tags else "○ 觀察"
            df['Predator_Tag'] = df.apply(get_tag, axis=1)

        df = df.fillna(0)
        report_text = {"📊 今日個股績效榜": df.tail(10)[['Symbol', 'Close', 'Return', 'Predator_Tag']].to_string(index=False)}
        return [], df, report_text
    except Exception as e:
        return None, None, {"Error": str(e)}
