# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
from pathlib import Path

def download_us_lead_data():
    # 精選對台股有「實質引導力」的標的
    tickers = {
        "^SOX": "SOX_Semiconductor", # 費城半導體 (台股電子股天花板)
        "TSM": "TSM_ADR",             # 台積電 ADR (台股大盤權重指標)
        "NVDA": "NVIDIA",             # AI 伺服器族群領頭羊
        "AAPL": "Apple"               # 蘋概股族群指標
    }
    
    print("📥 正在獲取美股關鍵指標...")
    summary_data = []
    
    for symbol, name in tickers.items():
        try:
            # 抓取最近 5 天數據以確保計算漲跌無誤
            df = yf.download(symbol, period="5d", interval="1d", progress=False)
            if not df.empty:
                # 計算最新一天的漲跌幅
                close_prices = df['Close'].values
                change_pct = ((close_prices[-1] - close_prices[-2]) / close_prices[-2]) * 100
                summary_data.append({"Market": "US", "Symbol": name, "Change": change_pct})
        except Exception as e:
            print(f"❌ {symbol} 下載失敗: {e}")
            
    # 儲存為摘要檔供分析器讀取
    if summary_data:
        pd.DataFrame(summary_data).to_csv("global_market_summary.csv", index=False)
        print("✅ 美股摘要已產出 (global_market_summary.csv)")

if __name__ == "__main__":
    download_us_lead_data()
