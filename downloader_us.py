# -*- coding: utf-8 -*-
import argparse
import yfinance as yf
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def download_us_data(market_id):
    # 定義美股監控代號（範例：科技巨頭與指數 ETF）
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "QQQ", "SPY"]
    
    save_dir = Path(f"./data/{market_id}/dayK")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 開始下載 {market_id} 美股數據...")
    for t in tqdm(tickers):
        try:
            # 下載美股日 K 線
            df = yf.download(t, period="2y", interval="1d", progress=False)
            if not df.empty:
                # 存檔名稱格式：代號_名稱.csv (配合你 main.py 的解析邏輯)
                # 如果 main.py 只讀代號，改為 f"{t}.csv"
                df.to_csv(save_dir / f"{t}.csv")
        except Exception as e:
            print(f"❌ {t} 下載失敗: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    
    download_us_data(args.market)
    print("✅ 美股數據下載完成。")
