# -*- coding: utf-8 -*-
import argparse
import yfinance as yf
import pandas as pd
import os
from pathlib import Path
from tqdm import tqdm

def download_data(market_id):
    # 這裡設定您要監控的代號，例如台股前幾大權值股
    tickers = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW"]
    
    # 確保資料夾路徑正確
    save_dir = Path(f"./data/{market_id}/dayK")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 開始下載 {market_id} 數據...")
    for t in tqdm(tickers):
        try:
            # 下載兩年的日 K 線資料
            df = yf.download(t, period="2y", interval="1d", progress=False)
            if not df.empty:
                # 存檔名稱必須與 analyzer.py 預期的一致 (例如: 2330.TW.csv)
                df.to_csv(save_dir / f"{t}.csv")
        except Exception as e:
            print(f"❌ {t} 下載失敗: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    
    # 直接執行下載，不要再呼叫 main() 以免發生 RecursionError
    download_data(args.market)
    print("✅ 數據下載完成。")
