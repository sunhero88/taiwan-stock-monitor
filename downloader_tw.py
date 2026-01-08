# -*- coding: utf-8 -*-
import argparse
import yfinance as yf
import pandas as pd
import os
from pathlib import Path
from tqdm import tqdm

def download_market_data(market_id):
    # 這裡以台股為例，定義簡單的股票清單或讀取原本的列表檔案
    tickers = ["2330.TW", "2317.TW", "2454.TW"] # 您可以增加更多代號
    save_dir = Path(f"./data/{market_id}/dayK")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 開始下載 {market_id} 數據...")
    for t in tqdm(tickers):
        try:
            df = yf.download(t, period="2y", interval="1d", progress=False)
            if not df.empty:
                df.to_csv(save_dir / f"{t}.csv")
        except Exception as e:
            print(f"❌ {t} 下載失敗: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    
    # 執行下載，不要再次呼叫 main()
    download_market_data(args.market)
    print("✅ 數據下載完成。")
