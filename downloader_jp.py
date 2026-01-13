# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd

def download_jp_lead_data():
    # 只抓取對台股有實質參考意義的日股數據
    tickers = {
        "^N225": "Nikkei_225",       # 日經指數 (亞股聯動性)
        "JPY=X": "USD_JPY"           # 日圓匯率 (資金避險指標)
    }
    
    print("📥 正在獲取日股與匯率指標...")
    summary_data = []
    
    for symbol, name in tickers.items():
        try:
            df = yf.download(symbol, period="5d", interval="1d", progress=False)
            if not df.empty:
                close_prices = df['Close'].values
                change_pct = ((close_prices[-1] - close_prices[-2]) / close_prices[-2]) * 100
                summary_data.append({"Market": "JP", "Symbol": name, "Change": change_pct})
        except Exception as e:
            print(f"❌ {symbol} 下載失敗: {e}")
            
    # 以「附加(append)」模式寫入，與美股摘要合併
    if summary_data:
        df_new = pd.DataFrame(summary_data)
        if Path("global_market_summary.csv").exists():
            df_old = pd.read_csv("global_market_summary.csv")
            pd.concat([df_old, df_new]).to_csv("global_market_summary.csv", index=False)
        else:
            df_new.to_csv("global_market_summary.csv", index=False)
        print("✅ 日股摘要已整合至 global_market_summary.csv")

if __name__ == "__main__":
    download_jp_lead_data()
