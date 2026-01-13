# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
from pathlib import Path

def download_asia_lead_data():
    tickers = {
        "^N225": "Nikkei_225",
        "JPY=X": "USD_JPY",
        "TWD=X": "USD_TWD"
    }
    
    summary_data = []
    print("📥 正在獲取亞太指標與匯率...")
    
    for symbol, name in tickers.items():
        try:
            df = yf.download(symbol, period="5d", interval="1d", progress=False)
            if not df.empty:
                c = df['Close'].values.flatten()
                change_pct = ((c[-1] - c[-2]) / c[-2]) * 100
                # 統一為 4 個欄位：Market, Symbol, Change, Value
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
        # 採附加模式，且不再重寫標頭(header)，避免格式紊亂
        if Path(file_path).exists():
            df_new.to_csv(file_path, mode='a', index=False, header=False)
        else:
            df_new.to_csv(file_path, mode='w', index=False, header=True)
        print("✅ 亞太指標已成功整合")

if __name__ == "__main__":
    download_asia_lead_data()
