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
# 市場總量補償邏輯 (TPEX Focus)
# =========================
def get_tpex_amount_official():
    """修復上櫃成交量抓取失敗的問題，並加入三層備援"""
    today = datetime.now()
    # 判斷是否為週末，週末抓取週五數據
    if today.weekday() >= 5:
        offset = today.weekday() - 4
        today = today - timedelta(days=offset)
        
    roc_date = f"{today.year - 1911}/{today.strftime('%m/%d')}"
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Referer': 'https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html'
    }
    params = {'l': 'zh-tw', 'd': roc_date, 'se': 'EW'}

    # 1. 官方 API (最高優先級)
    try:
        r = requests.get(url, params=params, headers=headers, timeout=12, allow_redirects=False)
        if r.status_code == 200:
            data = r.json()
            # 修正欄位抓取，部分 API 返回欄位為 tse_amount 或 amount
            amount = int(data.get('tse_amount', data.get('amount', 0)))
            if amount > 0:
                logging.info(f"✅ 取得上櫃成交量 (官方): {amount}")
                return amount, "TPEX_OFFICIAL_OK"
    except Exception as e:
        logging.warning(f"⚠️ 官方 API 異常: {e}")

    # 2. yfinance 指數成交量估算 (中級備援)
    try:
        tpex_idx = yf.Ticker("^TWOII")
        hist = tpex_idx.history(period="2d")
        if not hist.empty:
            # 取最近一個交易日
            v = hist['Volume'].iloc[-1]
            if v > 0:
                est_amount = int(v) 
                logging.info(f"💡 取得上櫃成交量 (yfinance 估算): {est_amount}")
                return est_amount, "TPEX_YFINANCE_ESTIMATE"
    except: pass

    # 3. 歷史快取或保守值 (最低優先級)
    if os.path.exists(MARKET_JSON):
        try:
            with open(MARKET_JSON, 'r') as f:
                old_data = json.load(f)
                return old_data.get('tpex_amount', 80000000000), "TPEX_FALLBACK_CACHE"
        except: pass

    logging.error("🚨 TPEX 所有來源均失敗，使用預設值")
    return 80000000000, "TPEX_FALLBACK_DEGRADED"

# =========================
# 個股數據修復邏輯
# =========================
def save_to_cache(symbol, price, volume):
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
        except: pass
    
    cache[symbol] = {
        'price': price,
        'volume': volume,
        'ts': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)

def get_from_cache(symbol):
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
                if symbol in cache:
                    d = cache[symbol]
                    # 補償邏輯：快取數據量縮 10% 以保持保守
                    return d['price'], d['volume'] * 0.9
        except: pass
    return None, None

def repair_stock_gap(symbol):
    """當批次下載失敗，針對單一個股進行深度抓取"""
    try:
        time.sleep(1.5) # 避開 API 頻率限制
        t = yf.Ticker(symbol)
        df = t.history(period="5d") # 抓 5 天確保有資料
        if not df.empty:
            return df['Close'].iloc[-1], df['Volume'].iloc[-1]
    except Exception as e:
        logging.warning(f"⚠️ {symbol} 深度抓取失敗: {e}")
    
    return get_from_cache(symbol)

# =========================
# 主下載邏輯
# =========================
def download_data(market_id):
    logging.info(f"📡 Predator V16.3.4 啟動：{market_id}")
    
    # 定義監控的核心權值股 (可擴充)
    tickers = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"] 
    
    # 1. 執行批次下載
    try:
        data = yf.download(tickers, period="1y", interval="1d", progress=False, group_by='column', timeout=20)
    except Exception as e:
        logging.error(f"❌ yfinance 服務異常: {e}")
        data = pd.DataFrame()

    df_list = []
    for symbol in tickers:
        try:
            # 安全讀取 Close/Volume
            has_data = False
            s_close = pd.Series(dtype='float64')
            s_vol = pd.Series(dtype='float64')

            if not data.empty and 'Close' in data and symbol in data['Close']:
                s_close = data['Close'][symbol].dropna()
                s_vol = data['Volume'][symbol].dropna()
                if not s_close.empty:
                    has_data = True

            # 2. 如果沒數據，啟動修復機制
            if not has_data or pd.isna(s_close.iloc[-1]):
                p, v = repair_stock_gap(symbol)
                if p is not None:
                    # 建立單日補償 Series
                    idx = pd.to_datetime([datetime.now().strftime("%Y-%m-%d")])
                    s_close = pd.Series([p], index=idx)
                    s_vol = pd.Series([v], index=idx)
                    has_data = True
                    logging.info(f"🔧 {symbol} 資料缺口已補償")

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

    # 3. 市場總量整合
    tpex_amt, tpex_src = get_tpex_amount_official()
    market_status = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "tpex_amount": tpex_amt,
        "tpex_source": tpex_src,
        "status": "OK" if "OK" in tpex_src else "DEGRADED",
        "update_ts": datetime.now().strftime("%H:%M:%S")
    }
    
    with open(MARKET_JSON, "w", encoding="utf-8") as f:
        json.dump(market_status, f, indent=4, ensure_ascii=False)

    # 4. 存檔與輸出
    if df_list:
        final_df = pd.concat(df_list)
        # 確保資料夾路徑存在
        os.makedirs("data", exist_ok=True)
        output_file = f"data_{market_id}.csv"
        final_df.to_csv(output_file, index=False)
        logging.info(f"✅ 完成：{output_file} (市場狀態: {tpex_src})")
    else:
        logging.critical("❌ 關鍵錯誤：今日無任何有效成交數據")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()
    
    # 增加重試機制
    for attempt in range(3):
        try:
            download_data(args.market)
            break
        except Exception as e:
            logging.error(f"第 {attempt+1} 次執行失敗，5秒後重試... ({e})")
            time.sleep(5)
