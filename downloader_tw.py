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

def get_tpex_amount_official():
    """專門修復上櫃成交量抓取失敗的問題 (破解 REDIRECT_ERRORS)"""
    today = datetime.now()
    # 櫃買中心使用民國年格式: 115/02/06
    roc_date = f"{today.year - 1911}/{today.strftime('%m/%d')}"
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html'
    }
    params = {'l': 'zh-tw', 'd': roc_date, 'se': 'EW'}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=10, allow_redirects=False)
        if r.status_code == 200:
            data = r.json()
            amount = int(data.get('tse_amount', 0))
            if amount > 0:
                logging.info(f"✅ 成功透過官方 API 取得上櫃成交量: {amount}")
                return amount, "TPEX_OFFICIAL_OK"
    except Exception as e:
        logging.warning(f"⚠️ TPEX 官方 API 抓取失敗: {e}")

    # 備援 1: yfinance 櫃買指數
    try:
        tpex_idx = yf.Ticker("^TWOII")
        hist = tpex_idx.history(period="1d")
        if not hist.empty:
            est_amount = int(hist['Volume'].iloc[-1] * 0.8) # 簡易換算比例
            logging.info(f"💡 使用 yfinance 備援估算上櫃成交量: {est_amount}")
            return est_amount, "TPEX_YFINANCE_ESTIMATE"
    except: pass

    logging.error("🚨 TPEX 所有來源均失敗，使用保守歷史值")
    return 80000000000, "TPEX_FALLBACK_DEGRADED"

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
                return data['price'], data['volume'] * 0.9
    return None, None

def repair_stock_gap(symbol):
    """備援方案：多層級修復邏輯"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        t = yf.Ticker(symbol)
        df = t.history(period="3d")
        if not df.empty:
            p, v = df['Close'].iloc[-1], df['Volume'].iloc[-1]
            return p, v
    except Exception as e:
        logging.warning(f"⚠️ {symbol} 網路修復失敗: {e}")
    
    p, v = get_from_cache(symbol)
    if p: return p, v
    return None, None

def download_data(market_id):
    logging.info(f"📡 正在下載 {market_id} 數據 (Predator V16.3 核心)...")
    tickers = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"] 
    
    # 執行個股批次下載
    try:
        data = yf.download(tickers, period="1y", interval="1d", progress=False, group_by='column')
    except Exception as e:
        logging.error(f"❌ yfinance 批次下載崩潰: {e}")
        data = pd.DataFrame()

    df_list = []
    for symbol in tickers:
        try:
            s_close = data['Close'][symbol] if 'Close' in data and symbol in data['Close'] else pd.Series(dtype='float64')
            s_vol = data['Volume'][symbol] if 'Volume' in data and symbol in data['Volume'] else pd.Series(dtype='float64')
            
            if s_close.empty or pd.isna(s_close.iloc[-1]):
                p, v = repair_stock_gap(symbol)
                if p is not None:
                    if s_close.empty:
                        idx = pd.to_datetime([datetime.now().date()])
                        s_close = pd.Series([p], index=idx)
                        s_vol = pd.Series([v], index=idx)
                    else:
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

    # --- 關鍵整合：計算全市場總量並儲存 JSON ---
    tpex_amount, tpex_src = get_tpex_amount_official()
    # 假設上市成交量大約為這幾檔權值股成交額總和的 2.5 倍 (此處僅為邏輯示意，可依需求精確抓取)
    # 建議：Predator 還是要讀取這個產出的 market_amount.json
    market_status = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "tpex_amount": tpex_amount,
        "tpex_source": tpex_src,
        "status": "OK" if "OFFICIAL" in tpex_src else "DEGRADED"
    }
    with open("market_amount.json", "w") as f:
        json.dump(market_status, f, indent=4)

    if df_list:
        final_df = pd.concat(df_list).dropna(subset=['Close'])
        output_file = f"data_{market_id}.csv"
        final_df.to_csv(output_file, index=False)
        logging.info(f"✅ 數據與市場總量更新完成: {output_file}")
    else:
        logging.critical("❌ 嚴重錯誤：完全無法取得任何數據！")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    download_data(args.market)
