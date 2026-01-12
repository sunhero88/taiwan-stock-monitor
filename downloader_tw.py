# -*- coding: utf-8 -*-
import argparse, yfinance as yf
from pathlib import Path
from tqdm import tqdm
import pandas as pd

def download_tw_data(market_id):
    tickers = {"2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電", "2382.TW": "廣達"}
    save_dir = Path(__file__).parent.absolute() / "data" / market_id / "dayK"
    save_dir.mkdir(parents=True, exist_ok=True)
    
    for symbol, name in tqdm(tickers.items()):
        try:
            # 💡 使用最保險的下載參數
            df = yf.download(symbol, period="2y", interval="1d", progress=False, auto_adjust=True)
            if not df.empty:
                df = df.reset_index()
                # 💡 修正點：改用更通用的方式平坦化表頭，避開 yf.utils.multi_index 報錯
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0] for c in df.columns]
                
                df.to_csv(save_dir / f"{symbol}_{name}.csv", index=False)
        except Exception as e:
            print(f"❌ {symbol} 下載失敗: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    download_tw_data(args.market)
