import os
import sys
import json
import time
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime

# ==========================================
# 1. 核心配置與全局變數
# ==========================================
CONFIG = {
    "CRITICAL_STOCKS": ["2330.TW", "2317.TW", "2454.TW", "3324.TW"],
    "RETRY_LIMIT": 3,
    "DATA_DIR": "data",
    "JSON_OUT": "macro.json",
    "CSV_OUT": "data/data_tw-share.csv"
}

if not os.path.exists(CONFIG["DATA_DIR"]):
    os.makedirs(CONFIG["DATA_DIR"])

def log(msg, level="INFO"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [{level}] {msg}")

# ==========================================
# 2. 強大抓取模組 (含 Redirect 與 備援邏輯)
# ==========================================
def get_market_amounts():
    """抓取上市櫃成交量，徹底修復 TPEX Redirect 問題"""
    results = {"twse": None, "tpex": None}
    
    # TWSE
    try:
        url = "https://www.twse.com.tw/exchangeReport/FMTQIK?response=json"
        res = requests.get(url, timeout=10)
        data = res.json()
        results["twse"] = float(data['data'][-1][2].replace(',', ''))
    except Exception as e:
        log(f"TWSE 抓取失敗: {e}", "ERROR")

    # TPEX (關鍵修復點)
    try:
        roc_date = f"{datetime.now().year - 1911}/{datetime.now().strftime('%m/%d')}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc_date}&se=EW"
        res = requests.get(url, timeout=10, allow_redirects=False) # 禁止重定向
        if res.status_code == 200:
            results["tpex"] = float(res.json().get("集合成交金額", 0))
    except Exception as e:
        log(f"TPEX 抓取失敗: {e}", "ERROR")
        
    return results

def download_stock_data():
    """下載個股數據並檢查關鍵標的完整性"""
    symbols = CONFIG["CRITICAL_STOCKS"] # 這裡可以擴展成讀取你的代碼清單
    try:
        df = yf.download(symbols, period="5d", interval="1d")['Close'].iloc[-1]
        # 轉成 DataFrame 格式以相容
        stock_df = df.reset_index()
        stock_df.columns = ['Symbol', 'Close']
        return stock_df
    except Exception as e:
        log(f"yfinance 下載失敗: {e}", "ERROR")
        return None

# ==========================================
# 3. 數據品質稽核與自動報警 (QC Layer)
# ==========================================
def run_quality_control(amounts, stocks):
    errors = []
    
    # 檢查量能
    if not amounts["twse"] or amounts["twse"] <= 0: errors.append("TWSE Amount Missing")
    if not amounts["tpex"] or amounts["tpex"] <= 0: errors.append("TPEX Amount Missing")
    
    # 檢查關鍵個股 (如 3324)
    if stocks is None or stocks.empty:
        errors.append("All Stock Data Missing")
    else:
        for s in CONFIG["CRITICAL_STOCKS"]:
            row = stocks[stocks['Symbol'] == s]
            if row.empty or pd.isna(row['Close'].values[0]):
                errors.append(f"Critical Stock Missing: {s}")

    if errors:
        log(f"❌ 數據完整性效驗失敗: {', '.join(errors)}", "CRITICAL")
        return False, errors
    
    return True, []

# ==========================================
# 4. 主執行邏輯 (Workflow Controller)
# ==========================================
def main():
    log("🚀 Predator V16.4 終極一體化任務啟動...")
    
    # 步驟 1: 抓取數據
    amounts = get_market_amounts()
    stocks = download_stock_data()
    
    # 步驟 2: 執行強效驗
    is_ok, error_list = run_quality_control(amounts, stocks)
    
    if not is_ok:
        # 如果失敗，直接停止，不更新 macro.json，不讓狀態變成 DEGRADED
        log("🛑 發現關鍵數據缺口，拒絕生成 macro.json。請檢查網路或 API 狀態。", "CRITICAL")
        sys.exit(1) # 回傳錯誤碼供網路伺服器監控
        
    # 步驟 3: 數據持久化與報表生成
    try:
        final_data = {
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "market_status": "OK",
            "macro": {
                "amount_twse": amounts["twse"],
                "amount_tpex": amounts["tpex"]
            },
            "stocks": stocks.to_dict(orient='records')
        }
        
        with open(CONFIG["JSON_OUT"], "w", encoding="utf-8") as f:
            json.dump(final_data, f, indent=4, ensure_ascii=False)
            
        stocks.to_csv(CONFIG["CSV_OUT"], index=False)
        
        log("✅ 任務圓滿完成！數據完整，market_status 已回歸 OK。")
    except Exception as e:
        log(f"存檔過程出錯: {e}", "ERROR")
        sys.exit(1)

if __name__ == "__main__":
    main()
