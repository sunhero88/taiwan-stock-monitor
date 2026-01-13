# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
from pathlib import Path

def download_asia_lead_data():
    tickers = {"^N225": "Nikkei_225", "JPY=X": "USD_JPY", "TWD=X": "USD_TWD"}
    summary_data = []
    print("📥 獲取亞太與台幣指標...")
    for symbol, name in tickers.items():
        try:
            df = yf.download(symbol, period="5d", interval="1d", progress=False)
            if not df.empty:
                c = df['Close'].values.flatten()
                change_pct = ((c[-1] - c[-2]) / c[-2]) * 100
                summary_data.append({"Market": "ASIA", "Symbol": name, "Change": change_pct, "Value": round(c[-1], 2)})
        except: continue
    if summary_data:
        df_new = pd.DataFrame(summary_data)
        file = "global_market_summary.csv"
        # 關鍵修正：若檔案存在則附加且不寫入標頭
        if Path(file).exists():
            df_new.to_csv(file, mode='a', index=False, header=False)
        else:
            df_new.to_csv(file, mode='w', index=False, header=True)

if __name__ == "__main__":
    download_asia_lead_data()
