# -*- coding: utf-8 -*-
import pandas as pd
import yfinance as yf
import argparse

def download_data(market_id):
    print(f"📡 正在從 Yahoo Finance 下載 {market_id} 數據...")
    # 這裡放您的下載邏輯，例如抓取台股 50 指數或特定清單
    # 範例：
    tickers = ["2330.TW", "2317.TW", "2308.TW", "2454.TW"] 
    data = yf.download(tickers, period="1y", interval="1d")
    
    # 將數據轉換為長表格式以便分析
    df = data['Close'].stack().reset_index()
    df.columns = ['Date', 'Symbol', 'Close']
    
    # 補上成交量 (如果需要爆量偵測)
    vol = data['Volume'].stack().reset_index()
    df['Volume'] = vol.iloc[:, 2]
    
    output_file = f"data_{market_id}.csv"
    df.to_csv(output_file, index=False)
    print(f"✅ 數據已成功儲存至: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    download_data(args.market)
