import pandas as pd
import yfinance as yf
import argparse
import requests
import os
import json
import logging
from datetime import datetime

# 設定日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CACHE_FILE = ".stock_data_cache.json"

def save_to_cache(symbol, price, volume):
    """將成功的數據存入本地快取"""
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
        except: pass
    
    cache[symbol] = {
        'price': price,
        'volume': volume,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)

def get_from_cache(symbol):
    """從快取讀取數據並給予保守權重"""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)
            if symbol in cache:
                data = cache[symbol]
                # 補償邏輯：成交量打 9 折，價格不變，確保系統能跑但保持警示
                return data['price'], data['volume'] * 0.9
    return None, None

def repair_stock_gap(symbol):
    """備援方案：多層級修復邏輯"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    try:
        # 嘗試 1: yfinance 單點
        t = yf.Ticker(symbol)
        df = t.history(period="3d")
        if not df.empty:
            p, v = df['Close'].iloc[-1], df['Volume'].iloc[-1]
            save_to_cache(symbol, p, v)
            return p, v
        
        # 嘗試 2: Yahoo Query API
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
        r = requests.get(url, headers=headers, timeout=10).json()
        result = r.get('chart', {}).get('result', [])
        if result:
            price = result[0]['meta']['regularMarketPrice']
            vol = result[0]['indicators']['quote'][0]['volume'][0]
            if price:
                save_to_cache(symbol, price, vol)
                return price, vol
    except Exception as e:
        logging.warning(f"⚠️ {symbol} 網路修復失敗: {e}")
    
    # 嘗試 3: 最後防線 - 快取補償
    p, v = get_from_cache(symbol)
    if p:
        logging.error(f"🚨 {symbol} 使用快取補償數據 (DEGRADED_MODE)")
        return p, v
        
    return None, None

def download_data(market_id):
    logging.info(f"📡 正在下載 {market_id} 數據 (Predator V16.3 核心)...")
    tickers = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"] 
    
    try:
        # 執行批次下載
        data = yf.download(tickers, period="1y", interval="1d", progress=False, group_by='column')
    except Exception as e:
        logging.error(f"❌ yfinance 批次下載崩潰: {e}")
        data = pd.DataFrame()

    df_list = []
    for symbol in tickers:
        try:
            # 取得該股數據，若不存在則建立空 Series
            s_close = data['Close'][symbol] if 'Close' in data and symbol in data['Close'] else pd.Series(dtype='float64')
            s_vol = data['Volume'][symbol] if 'Volume' in data and symbol in data['Volume'] else pd.Series(dtype='float64')
            
            # 偵測缺失 (空數據或最後一筆是 NaN)
            if s_close.empty or pd.isna(s_close.iloc[-1]):
                logging.warning(f"⚠️ {symbol} 數據缺失，啟動補償邏輯...")
                p, v = repair_stock_gap(symbol)
                
                if p is not None:
                    # 如果 yf 下載原本是空的，建立一個基礎 Index
                    if s_close.empty:
                        idx = pd.to_datetime([datetime.now().date()])
                        s_close = pd.Series([p], index=idx)
                        s_vol = pd.Series([v], index=idx)
                    else:
                        # 修正最後一筆數據
                        s_close.iloc[-1] = p
                        s_vol.iloc[-1] = v
                    save_to_cache(symbol, p, v)

            if not s_close.empty:
                temp_df = pd.DataFrame({
                    'Date': s_close.index,
                    'Symbol': symbol,
                    'Close': s_close.values,
                    'Volume': s_vol.values
                })
                df_list.append(temp_df)
                
        except Exception as e:
            logging.error(f"❌ {symbol} 處理失敗: {e}")

    if df_list:
        final_df = pd.concat(df_list).dropna(subset=['Close'])
        output_file = f"data_{market_id}.csv"
        final_df.to_csv(output_file, index=False)
        logging.info(f"✅ 數據更新完成: {output_file}")
    else:
        logging.critical("❌ 嚴重錯誤：完全無法取得任何數據！")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    download_data(args.market)
