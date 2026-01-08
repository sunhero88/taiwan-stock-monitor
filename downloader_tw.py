# -*- coding: utf-8 -*-
import argparse
import yfinance as yf
from pathlib import Path
from tqdm import tqdm

def download_data(market_id):
    # 定義代號與名稱（必須有底線分隔）
    tickers = {
        "2330.TW": "台積電",
        "2317.TW": "鴻海",
        "2454.TW": "聯發科",
        "2308.TW": "台達電",
        "2382.TW": "廣達"
    }
    save_dir = Path(f"./data/{market_id}/dayK")
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 開始下載 {market_id} 數據...")
    for symbol, name in tqdm(tickers.items()):
        try:
            df = yf.download(symbol, period="2y", interval="1d", progress=False)
            if not df.empty:
                # 這裡的檔名格式是關鍵：2330.TW_台積電.csv
                df.to_csv(save_dir / f"{symbol}_{name}.csv")
        except Exception as e:
            print(f"❌ {symbol} 下載失敗: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    download_data(args.market)
    print("✅ 台股數據下載完成。")
