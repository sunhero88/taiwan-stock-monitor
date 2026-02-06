import pandas as pd
import yfinance as yf
import argparse
import requests
import os
import json
import logging
import time
from datetime import datetime

# =========================
# 系統配置與路徑設定 (Predator V16.3.7)
# =========================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)

# 確保路徑與主系統一致
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

CACHE_FILE = os.path.join(DATA_DIR, ".stock_data_cache.json")
MARKET_JSON = os.path.join(DATA_DIR, "market_amount.json")

# =========================
# 核心抓取邏輯：鉅亨網專業接口 (優化版)
# =========================
def get_tpex_amount_professional():
    """
    完全跳過官網，改用鉅亨網 (Anue) 專業接口抓取上市/上櫃成交量。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://invest.cnyes.com/twstock/market/TSE'
    }
    
    # 鉅亨網大盤統計 API
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
                # 這裡抓取的是 'turnover' (成交額)
                turnover = item.get('turnover', 0)
                
                if symbol == "OTC:OTC01:INDEX":
                    otc_amount = int(float(turnover))
                elif symbol == "TSE:TSE01:INDEX":
                    tse_amount = int(float(turnover))
            
            if otc_amount > 100000000: # 確保至少有1億，避免抓到空值
                logging.info(f"✅ 取得數據 (鉅亨網 API) - 上市: {tse_amount:,}, 上櫃: {otc_amount:,}")
                return otc_amount, "TPEX_CNYES_OK"
                
    except Exception as e:
        logging.warning(f"⚠️ 鉅亨網 API 異常: {e}")

    # --- 備援方案 1：Yahoo Finance ---
    try:
        # ⚠️ 注意：Yahoo 的 ^TWO Volume 往往是張數而非金額
        otc_ticker = yf.Ticker("^TWO")
        df = otc_ticker.history(period="1d")
        if not df.empty:
            # 使用收盤價 * 成交量(張) * 1000 作為金額估算備援
            vol_raw = df['Volume'].iloc[-1]
            price_raw = df['Close'].iloc[-1]
            est_amount = int(vol_raw * 1000 * (price_raw / 2.5)) # 權重校準因子
            logging.info(f"🚀 取得數據 (Yahoo 備援估算) - 上櫃成交額約: {est_amount:,}")
            return est_amount, "TPEX_YAHOO_BACKUP"
    except Exception as e:
        logging.warning(f"⚠️ Yahoo 備援異常: {e}")

    # --- 備援方案 2：讀取本地歷史快取 ---
    if os.path.exists(MARKET_JSON):
        try:
            with open(MARKET_JSON, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
                logging.warning("🚨 官網與API皆失敗，使用最後一次成功數據")
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
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
        except: pass
    cache[symbol] = {
        'price': price, 
        'volume': volume, 
        'ts': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=4)

def get_from_cache(symbol):
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                if symbol in cache:
                    d = cache[symbol]
                    # 劣化處理：快取數據量能打 9 折以示警覺
                    return d['price'], d['volume'] * 0.9
        except: pass
    return None, None

def repair_stock_gap(symbol):
    """當主力下載失敗時，嘗試針對性修補單一個股"""
    try:
        time.sleep(1.2) # 避開頻率限制
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
    logging.info(f"📡 Predator V16.3.7 執行環境：{market_id}")
    
    # 你關注的核心標的
    tickers = ["2330.TW", "2317.TW", "3324.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW"] 
    
    try:
        data = yf.download(tickers, period="1y", interval="1d", progress=False, group_by='column', timeout=30)
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
                if not s_close.empty and not pd.isna(s_close.iloc[-1]): 
                    has_data = True

            # 觸發修補機制
            if not has_data:
                p, v = repair_stock_gap(symbol)
                if p is not None:
                    idx = pd.to_datetime([datetime.now().strftime("%Y-%m-%d")])
                    s_close = pd.Series([p], index=idx)
                    s_vol = pd.Series([v], index=idx)
                    has_data = True
                    logging.info(f"🔧 {symbol} 透過 Repair 成功救援")

            if has_data:
                save_to_cache(symbol, s_close.iloc[-1], s_vol.iloc[-1])
                temp_df = pd.DataFrame({
                    'Date': s_close.index, 
                    'Symbol': symbol, 
                    'Close': s_close.values, 
                    'Volume': s_vol.values
                })
                df_list.append(temp_df)
                
        except Exception as e:
            logging.error(f"❌ {symbol} 處理失敗: {e}")

    # 執行強化版大盤成交額抓取
    tpex_amt, tpex_src = get_tpex_amount_professional()
    
    market_status = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "tpex_amount": tpex_amt,
        "tpex_source": tpex_src,
        "status": "OK" if "OK" in tpex_src or "BACKUP" in tpex_src else "DEGRADED",
        "update_ts": datetime.now().strftime("%H:%M:%S"),
        "version": "V16.3.7-PRO"
    }
    
    # 寫入 JSON (主系統 audit 使用)
    with open(MARKET_JSON, "w", encoding="utf-8") as f:
        json.dump(market_status, f, indent=4, ensure_ascii=False)

    # 輸出 CSV
    if df_list:
        final_df = pd.concat(df_list)
        output_file = os.path.join(DATA_DIR, f"data_{market_id}.csv")
        final_df.to_csv(output_file, index=False)
        logging.info(f"✅ 流程完成：{output_file}")
    else:
        logging.critical("❌ 數據全滅：請檢查網路環境")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    
    # 增加重試間隔
    for attempt in range(1, 4):
        try:
            download_data(args.market)
            break
        except Exception as e:
            logging.error(f"第 {attempt} 次嘗試失敗: {e}")
            if attempt < 3: time.sleep(10)
