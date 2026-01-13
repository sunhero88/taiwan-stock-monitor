# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
from pathlib import Path

def download_us_lead_data():
    tickers = {"^SOX": "SOX_Semi", "TSM": "TSM_ADR", "NVDA": "NVIDIA", "AAPL": "Apple"}
    summary_data = []
    print("📥 獲取美股指標...")
    for symbol, name in tickers.items():
        try:
            df = yf.download(symbol, period="5d", interval="1d", progress=False)
            if not df.empty:
                c = df['Close'].values.flatten()
                change_pct = ((c[-1] - c[-2]) / c[-2]) * 100
                summary_data.append({"Market": "US", "Symbol": name, "Change": change_pct, "Value": round(c[-1], 2)})
        except: continue
    if summary_data:
        pd.DataFrame(summary_data).to_csv("global_market_summary.csv", index=False)

if __name__ == "__main__":
    download_us_lead_data()
