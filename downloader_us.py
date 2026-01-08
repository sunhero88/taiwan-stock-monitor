# -*- coding: utf-8 -*-
import argparse
import yfinance as yf
from pathlib import Path
from tqdm import tqdm

def download_us_data(market_id):
    tickers = {
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "NVDA": "NVIDIA",
        "TSLA": "Tesla",
        "GOOGL": "Google"
    }
    save_dir = Path(f"./data/{market_id}/dayK")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 開始下載 {market_id} 數據...")
    for symbol, name in tqdm(tickers.items()):
        try:
            df = yf.download(symbol, period="2y", interval="1d", progress=False)
            if not df.empty:
                df.to_csv(save_dir / f"{symbol}_{name}.csv")
        except Exception as e:
            print(f"❌ {symbol} 下載失敗: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    download_us_data(args.market)
