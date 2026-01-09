# -*- coding: utf-8 -*-
import argparse
import yfinance as yf
from pathlib import Path
from tqdm import tqdm

def download_tw_data(market_id):
    # 定義代號與名稱的對應
    tickers = {
        "2330.TW": "台積電",
        "2317.TW": "鴻海",
        "2454.TW": "聯發科",
        "2308.TW": "台達電",
        "2382.TW": "廣達"
    }
    # 💡 強制使用絕對路徑，解決路徑迷蹤問題
    save_dir = Path(__file__).parent.absolute() / "data" / market_id / "dayK"
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📥 開始下載 {market_id} 數據到: {save_dir}")
    for symbol, name in tqdm(tickers.items()):
        try:
            # 💡 增加 auto_adjust 確保獲取正確的收盤價
            df = yf.download(symbol, period="2y", interval="1d", progress=False, auto_adjust=True)
            if not df.empty:
                # 💡 修正 yfinance 多層標題問題，並重置索引讓 Date 變成一列
                df = df.reset_index()
                # 確保 CSV 格式是分析器最愛的標準格式
                df.to_csv(save_dir / f"{symbol}_{name}.csv", index=False)
        except Exception as e:
            print(f"❌ {symbol} 下載失敗: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    download_tw_data(args.market)
