import pandas as pd
import yfinance as yf
import argparse
import requests
import os
import json
import logging
import time
from datetime import datetime, timedelta

# =========================
# 系統配置與日誌設定
# =========================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

CACHE_FILE = ".stock_data_cache.json"
MARKET_JSON = "market_amount.json"

# =========================
# 核心修復：跳過官網，直接抓取專業網站 API (鉅亨網優選)
# =========================
def get_tpex_amount_professional():
    """
    完全跳過官網，改用鉅亨網 (Anue) 專業接口抓取上市/上櫃成交量。
    這是目前在網路執行最穩定的方案，避開 403/Redirect 錯誤。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://invest.cnyes.com/twstock/market/TSE'
    }
    
    # 鉅亨網大盤統計 API (一次包含上市 TSE 與 上櫃 OTC)
    api_url = "https://market-api.api.cnyes.com/nexus/api/v2/mainland/index/quote"
    params = {"symbols": "TSE:TSE01:INDEX,OTC:OTC01:INDEX"}

    try:
        response = requests.get(api_url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            res_data = response.json()
            items = res_data.get('data', {}).get('items', [])
            
            tse_amount = 0
            otc_amount = 0
            
            for item in items:
                symbol = item.get('symbol')
                turnover = item.get('turnover', 0)
                
                if symbol == "OTC:OTC01:INDEX":
                    otc_amount = int(turnover)
                elif symbol == "TSE:TSE01:INDEX":
                    tse_amount = int(turnover)
            
            if otc_amount > 0:
                logging.info(f"✅ 取得數據 (鉅亨網 API) - 上市: {tse_amount:,}, 上櫃: {otc_amount:,}")
                # 我們將兩者存入，但回傳主要是針對你要求的上櫃數據
                return otc_amount, "TPEX_CNYES_OK"
                
    except Exception as e:
        logging.warning(f"⚠️ 鉅亨網 API 異常: {e}")

    # --- 備援方案：Yahoo Finance (當鉅亨網也掛掉時) ---
    try:
        otc_ticker = yf.Ticker("^TWO")
        df = otc_ticker.history(period="1d")
        if not df.empty:
            amount = int(df['Volume'].iloc[-1])
            logging.info(f"🚀 取得數據 (Yahoo 備援) - 上櫃成交量: {amount:,}")
            return amount, "TPEX_YAHOO_BACKUP"
    except Exception as e:
        logging.warning(f"⚠️ Yahoo 備援異常: {e}")

    # --- 最後防線：讀取歷史快取 ---
    if os.path.exists(MARKET_JSON):
        try:
            with open(MARKET_JSON, 'r') as f:
                old_data = json.load(f)
                logging.warning("🚨 使用歷史快取數據")
                return old_data.get('tpex_amount', 80000000000), "TPEX_FALLBACK_CACHE"
        except: pass

    return 80000000000, "TPEX_CRITICAL_DEGRADED"

# =========================
# 個股修復與快取邏輯
# =========================
def save_to_cache(symbol, price, volume):
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
        except: pass
    cache[symbol] = {'price': price, 'volume': volume, 'ts': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)

def get_from_cache(symbol):
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
                if symbol in cache:
                    d = cache[symbol]
                    return d['price'], d['volume'] * 0.9
        except: pass
    return None, None

def repair_stock_gap(symbol):
    try:
        time.sleep(1.0)
        t = yf.Ticker(symbol)
        df = t.history(period="5d")
        if not df.empty:
            return df['Close'].iloc[-1], df['Volume'].iloc[-1]
    except: pass
    return get_from_cache(symbol)

# =========================
# 主下載邏輯
# =========================
def download_data(market_id):
    logging.info(f"📡 Predator V16.3.6 (Net-Professional) 啟動：{market_id}")
    tickers = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"] 
    
    try:
        data = yf.download(tickers, period="1y", interval="1d", progress=False, group_by='column', timeout=25)
    except:
        data = pd.DataFrame()

    df_list = []
    for symbol in tickers:
        try:
            has_data = False
            s_close = pd.Series(dtype='float64')
            s_vol = pd.Series(dtype='float64')

            if not data.empty and 'Close' in data and symbol in data['Close']:
                s_close = data['Close'][symbol].dropna()
                s_vol = data['Volume'][symbol].dropna()
                if not s_close.empty: has_data = True

            if not has_data or pd.isna(s_close.iloc[-1]):
                p, v = repair_stock_gap(symbol)
                if p is not None:
                    idx = pd.to_datetime([datetime.now().strftime("%Y-%m-%d")])
                    s_close = pd.Series([p], index=idx)
                    s_vol = pd.Series([v], index=idx)
                    has_data = True
                    logging.info(f"🔧 {symbol} 資料缺口已修補")

            if has_data:
                save_to_cache(symbol, s_close.iloc[-1], s_vol.iloc[-1])
                temp_df = pd.DataFrame({'Date': s_close.index, 'Symbol': symbol, 'Close': s_close.values, 'Volume': s_vol.values})
                df_list.append(temp_df)
        except Exception as e:
            logging.error(f"❌ {symbol} 處理失敗: {e}")

    # 執行強化版專業網站抓取
    tpex_amt, tpex_src = get_tpex_amount_professional()
    
    market_status = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "tpex_amount": tpex_amt,
        "tpex_source": tpex_src,
        "status": "OK" if "OK" in tpex_src or "BACKUP" in tpex_src else "DEGRADED",
        "update_ts": datetime.now().strftime("%H:%M:%S")
    }
    
    with open(MARKET_JSON, "w", encoding="utf-8") as f:
        json.dump(market_status, f, indent=4, ensure_ascii=False)

    if df_list:
        final_df = pd.concat(df_list)
        os.makedirs("data", exist_ok=True)
        output_file = f"data_{market_id}.csv"
        final_df.to_csv(output_file, index=False)
        logging.info(f"✅ 完成：{output_file} (數據源: {tpex_src})")
    else:
        logging.critical("❌ 嚴重失敗：今日無有效數據")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    
    for attempt in range(3):
        try:
            download_data(args.market)
            break
        except Exception as e:
            logging.error(f"重試中... {e}")
            time.sleep(5)
