import os
import sys
import json
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime

# ==========================================
# 配置設定 (針對你的 GitHub 專案結構)
# ==========================================
CONFIG = {
    "CRITICAL_STOCKS": ["2330.TW", "2317.TW", "2454.TW", "3324.TW"],
    "DATA_DIR": "data",
    "JSON_OUT": "macro.json",
    "CSV_PATH": "data/data_tw-share.csv",
    "RETRY_LIMIT": 3
}

def log(msg, level="INFO"):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}")

# ==========================================
# 核心功能：數據抓取與修復
# ==========================================
def get_market_data():
    data = {"twse": None, "tpex": None}
    # TWSE 抓取
    try:
        url = "https://www.twse.com.tw/exchangeReport/FMTQIK?response=json"
        res = requests.get(url, timeout=15)
        raw = res.json()
        data["twse"] = float(raw['data'][-1][2].replace(',', ''))
    except Exception as e:
        log(f"TWSE 抓取失敗: {e}", "ERROR")

    # TPEX 抓取 (修復 Redirect 問題)
    try:
        # 取得當前民國日期格式
        now = datetime.now()
        roc_date = f"{now.year - 1911}/{now.strftime('%m/%d')}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc_date}&se=EW"
        res = requests.get(url, timeout=15, allow_redirects=False)
        if res.status_code == 200:
            data["tpex"] = float(res.json().get("集合成交金額", 0))
    except Exception as e:
        log(f"TPEX 抓取失敗: {e}", "ERROR")
    return data

def get_stock_prices():
    try:
        # 下載關鍵標的收盤價
        df = yf.download(CONFIG["CRITICAL_STOCKS"], period="5d", interval="1d", progress=False)['Close']
        latest = df.iloc[-1].reset_index()
        latest.columns = ['Symbol', 'Close']
        return latest
    except Exception as e:
        log(f"yfinance 數據缺失: {e}", "ERROR")
        return None

# ==========================================
# 核心功能：強效驗 (Quality Control)
# ==========================================
def validate_and_save(amounts, stocks):
    error_logs = []
    
    # 1. 驗證大盤量能
    if not amounts["twse"] or amounts["twse"] <= 0: error_logs.append("TWSE_AMOUNT_MISSING")
    if not amounts["tpex"] or amounts["tpex"] <= 0: error_logs.append("TPEX_AMOUNT_MISSING")
    
    # 2. 驗證關鍵權值 (例如雙鴻 3324)
    if stocks is None:
        error_logs.append("STOCK_DATA_TOTAL_MISSING")
    else:
        for symbol in CONFIG["CRITICAL_STOCKS"]:
            price = stocks[stocks['Symbol'] == symbol]['Close'].values
            if len(price) == 0 or pd.isna(price[0]):
                error_logs.append(f"CRITICAL_STOCK_MISSING_{symbol}")

    if error_logs:
        log(f"❌ 數據校驗失敗: {error_logs}", "CRITICAL")
        return False

    # 3. 存檔並回歸 OK 狀態
    try:
        output = {
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "market_status": "OK",
            "macro": amounts,
            "stocks": stocks.to_dict(orient='records')
        }
        with open(CONFIG["JSON_OUT"], "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4, ensure_ascii=False)
        log("✅ 數據完整，market_status 已回歸 OK。")
        return True
    except Exception as e:
        log(f"寫入 JSON 失敗: {e}", "ERROR")
        return False

if __name__ == "__main__":
    log("🚀 Predator 核心啟動 (網路自動化版)")
    
    amt = get_market_data()
    stk = get_stock_prices()
    
    if validate_and_save(amt, stk):
        sys.exit(0) # 成功，通知 GitHub Action 繼續執行
    else:
        sys.exit(1) # 失敗，強制中斷 GitHub Action，不更新錯誤數據
