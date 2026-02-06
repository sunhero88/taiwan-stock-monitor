import pandas as pd
import yfinance as yf
import requests
import os
import json
import logging
import time
from datetime import datetime

# =========================
# 系統配置
# =========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
MARKET_JSON = os.path.join(DATA_DIR, "market_amount.json")

# =========================
# 1. 強化版大盤數據抓取 (鉅亨 API + Yahoo 備援)
# =========================
def fetch_market_amounts():
    """
    抓取上市與上櫃成交金額，支援三重救援。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    # --- 第一重：鉅亨網 API ---
    api_url = "https://market-api.api.cnyes.com/nexus/api/v2/mainland/index/quote"
    params = {"symbols": "TSE:TSE01:INDEX,OTC:OTC01:INDEX"}
    
    tse_amt, otc_amt = None, None
    source = "UNKNOWN"

    try:
        r = requests.get(api_url, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            items = r.json().get('data', {}).get('items', [])
            for item in items:
                sym = item.get('symbol', '')
                val = int(float(item.get('turnover', 0)))
                if "TSE" in sym: tse_amt = val
                if "OTC" in sym: otc_amt = val
            
            if tse_amt and otc_amt:
                logging.info(f"✅ 鉅亨 API 成功: 上櫃 {otc_amt:,}")
                return tse_amt, otc_amt, "CNYES_API"
    except Exception as e:
        logging.warning(f"⚠️ 鉅亨 API 異常: {e}")

    # --- 第二重：Yahoo Finance 指數備援 (強制執行) ---
    logging.info("📡 嘗試 Yahoo 指數備援抓取上櫃數據...")
    try:
        tse_idx = yf.Ticker("^TWII").history(period="1d")
        otc_idx = yf.Ticker("^TWO").history(period="1d")
        
        if not otc_idx.empty:
            # 上櫃成交額估算 (Volume * Close * 校準係數 0.45)
            # 因為 Yahoo Volume 有時是張數，有時是金額，這裡做保險估算
            y_otc_amt = int(otc_idx['Volume'].iloc[-1] * otc_idx['Close'].iloc[-1] * 0.45)
            y_tse_amt = int(tse_idx['Volume'].iloc[-1] * tse_idx['Close'].iloc[-1] * 0.45) if not tse_idx.empty else tse_amt
            
            return y_tse_amt, y_otc_amt, "YAHOO_INDEX_EST"
    except Exception as e:
        logging.error(f"❌ Yahoo 備援也失敗: {e}")

    return tse_amt or 0, otc_amt or 0, "CRITICAL_FAILURE"

# =========================
# 2. 個股修復邏輯 (專治 3324 等漏網之魚)
# =========================
def force_repair_stock(symbol):
    """針對特定失敗標的進行單點突破"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="2d")
        if not df.empty:
            return df['Close'].iloc[-1], df['Volume'].iloc[-1]
    except:
        return None, None

# =========================
# 3. 主程序
# =========================
def run_predator_update():
    tickers = ["2330.TW", "2317.TW", "2454.TW", "3324.TW", "2308.TW", "2382.TW", "3231.TW", "2603.TW"]
    
    # 執行大盤抓取
    tse, otc, src = fetch_market_amounts()
    total_amt = (tse or 0) + (otc or 0)
    
    # 下載個股
    logging.info("📥 開始下載個股數據...")
    try:
        data = yf.download(tickers, period="5d", progress=False)
    except:
        data = pd.DataFrame()

    results = []
    for s in tickers:
        p, v = None, None
        try:
            if not data.empty and ('Close', s) in data.columns:
                p = data['Close'][s].dropna().iloc[-1]
                v = data['Volume'][s].dropna().iloc[-1]
            
            # 檢查是否需要強行救援 (例如 3324.TW)
            if p is None or pd.isna(p):
                logging.info(f"🔧 觸發強行救援: {s}")
                p, v = force_repair_stock(s)
            
            if p:
                results.append({"Symbol": s, "Price": p, "Volume": v})
        except:
            continue

    # 輸出 Market JSON
    market_data = {
        "trade_date": datetime.now().strftime("%Y-%m-%d"),
        "amount_twse": tse,
        "amount_tpex": otc,
        "amount_total": total_amt,
        "source": src,
        "status": "OK" if otc and otc > 0 else "DEGRADED_PARTIAL",
        "integrity": {
            "tickers_count": len(results),
            "missing": [t for t in tickers if t not in [r['Symbol'] for r in results]]
        }
    }
    
    with open(MARKET_JSON, 'w', encoding='utf-8') as f:
        json.dump(market_data, f, indent=4, ensure_ascii=False)
    
    # 輸出 CSV
    if results:
        pd.DataFrame(results).to_csv(os.path.join(DATA_DIR, "data_tw-share.csv"), index=False)
        logging.info(f"✅ 成功! 數據源: {src}, 標的補完: {len(results)}/{len(tickers)}")
    else:
        logging.critical("🚨 數據全滅")

if __name__ == "__main__":
    run_predator_update()
