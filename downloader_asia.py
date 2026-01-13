# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
from pathlib import Path

def download_asia_lead_data():
    # 亞太與匯率核心標的
    tickers = {
        "^N225": "Nikkei_225",   # 日經指數
        "JPY=X": "USD_JPY",       # 日圓匯率
        "TWD=X": "USD_TWD"        # 台幣匯率 (美金/台幣)
    }
    
    summary_data = []
    for symbol, name in tickers.items():
        try:
            df = yf.download(symbol, period="5d", interval="1d", progress=False)
            if not df.empty:
                # 處理 yfinance 回報多維度的情況
                c = df['Close'].values.flatten()
                change_pct = ((c[-1] - c[-2]) / c[-2]) * 100
                summary_data.append({
                    "Market": "ASIA", 
                    "Symbol": name, 
                    "Change": change_pct, 
                    "Value": round(c[-1], 2)
                })
        except: continue
            
    if summary_data:
        df_new = pd.DataFrame(summary_data)
        file_path = "global_market_summary.csv"
        # 如果美股已經寫入，則附加(append)，否則覆寫
        mode = 'a' if Path(file_path).exists() else 'w'
        header = not Path(file_path).exists()
        df_new.to_csv(file_path, mode=mode, index=False, header=header)

if __name__ == "__main__":
    download_asia_lead_data()
