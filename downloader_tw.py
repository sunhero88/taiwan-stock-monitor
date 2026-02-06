# download_tw.py
import pandas as pd
import yfinance as yf
import argparse
import requests
import time

def repair_stock_gap(symbol):
    """備援方案：當 yfinance 批量失敗時使用單點抓取"""
    try:
        # 嘗試 1: yfinance 單點
        t = yf.Ticker(symbol)
        df = t.history(period="3d")
        if not df.empty:
            return df['Close'].iloc[-1], df['Volume'].iloc[-1]
        
        # 嘗試 2: Yahoo Query API (更底層)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5).json()
        price = r['chart']['result'][0]['meta']['regularMarketPrice']
        vol = r['chart']['result'][0]['indicators']['quote'][0]['volume'][0]
        return price, vol
    except:
        return None, None

def download_data(market_id):
    print(f"📡 正在從 Yahoo Finance 下載 {market_id} 數據...")
    tickers = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"] 
    
    # 執行批次下載
    data = yf.download(tickers, period="1y", interval="1d", progress=False)
    
    # 轉換與檢查
    df_list = []
    for symbol in tickers:
        try:
            s_close = data['Close'][symbol]
            s_vol = data['Volume'][symbol]
            
            # 偵測最新一筆是否缺失 (NaN)
            if pd.isna(s_close.iloc[-1]):
                print(f"⚠️ 偵測到 {symbol} 缺失，正在修復...")
                p, v = repair_stock_gap(symbol)
                if p:
                    # 使用 .at 或 .iloc 更新最後一筆
                    data.at[data.index[-1], ('Close', symbol)] = p
                    data.at[data.index[-1], ('Volume', symbol)] = v
                    s_close = data['Close'][symbol] # 重新選取
                    s_vol = data['Volume'][symbol]

            temp_df = pd.DataFrame({
                'Date': s_close.index,
                'Symbol': symbol,
                'Close': s_close.values,
                'Volume': s_vol.values
            })
            df_list.append(temp_df)
        except Exception as e:
            print(f"❌ {symbol} 處理失敗: {e}")

    final_df = pd.concat(df_list).dropna(subset=['Close'])
    output_file = f"data_{market_id}.csv"
    final_df.to_csv(output_file, index=False)
    print(f"✅ 數據已儲存: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    download_data(args.market)
