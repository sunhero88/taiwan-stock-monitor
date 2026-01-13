# -*- coding: utf-8 -*-
import pandas as pd
import yfinance as yf
import argparse

def download_data(market_id):
    print(f"📡 正在從 Yahoo Finance 下載 {market_id} 數據與籌碼分析基礎...")
    
    # 這裡放您關注的台股核心標的（可自由增減）
    tickers = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"] 
    
    # 下載報價與成交量
    data = yf.download(tickers, period="1y", interval="1d", progress=False)
    
    # 格式轉換：Close 與 Volume 必須存在
    df_close = data['Close'].stack().reset_index()
    df_close.columns = ['Date', 'Symbol', 'Close']
    
    df_vol = data['Volume'].stack().reset_index()
    df_vol.columns = ['Date', 'Symbol', 'Volume']
    
    # 合併價格與成交量
    df = pd.merge(df_close, df_vol, on=['Date', 'Symbol'])
    
    # 確保數據完整後存檔
    output_file = f"data_{market_id}.csv"
    df.to_csv(output_file, index=False)
    print(f"✅ 台股數據已成功儲存至: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    download_data(args.market)
