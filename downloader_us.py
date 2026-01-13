# -*- coding: utf-8 -*-
import argparse
import yfinance as yf
from pathlib import Path
import pandas as pd

def download_us_lead_data():
    # 只挑選對台股有高度連動性的標的
    tickers = {
        "^SOX": "SOX_Semiconductor", # 費城半導體指數
        "TSM": "TSM_ADR",             # 台積電 ADR
        "NVDA": "NVIDIA",             # AI 領頭羊
        "AAPL": "Apple"               # 蘋概股指標
    }
    
    save_dir = Path("./data/us-lead")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print("📥 正在下載美股領先指標...")
    
    summary_data = []
    for symbol, name in tickers.items():
        try:
            # 抓取最近 5 天的數據以計算漲跌
            df = yf.download(symbol, period="5d", interval="1d", progress=False)
            if not df.empty:
                # 儲存 CSV 供 analyzer 使用
                df.to_csv(save_dir / f"{symbol}.csv")
                
                # 計算最新漲跌幅
                close_prices = df['Close'].values
                change_pct = ((close_prices[-1] - close_prices[-2]) / close_prices[-2]) * 100
                summary_data.append({"Symbol": name, "Change": change_pct})
        except Exception as e:
            print(f"❌ {symbol} 下載失敗: {e}")
    
    # 產出一個小型的摘要檔，讓 analyzer 快速讀取
    if summary_data:
        pd.DataFrame(summary_data).to_csv("us_market_summary.csv", index=False)

if __name__ == "__main__":
    download_us_lead_data()
