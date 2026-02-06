import os, sys, json, requests
import pandas as pd
import yfinance as yf
from datetime import datetime

# 配置：確保路徑與你的 Repo 一致
CSV_PATH = "data/data_tw-share.csv"
JSON_OUT = "macro.json"
CRITICAL_STOCKS = ["2330.TW", "2317.TW", "2454.TW", "3324.TW"]

def get_data():
    results = {"twse": None, "tpex": None}
    try:
        # 上市金額
        res = requests.get("https://www.twse.com.tw/exchangeReport/FMTQIK?response=json", timeout=15)
        results["twse"] = float(res.json()['data'][-1][2].replace(',', ''))
        
        # 上櫃金額 (含 Redirect 修復)
        roc_date = f"{datetime.now().year - 1911}/{datetime.now().strftime('%m/%d')}"
        t_url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc_date}&se=EW"
        t_res = requests.get(t_url, timeout=15, allow_redirects=False)
        if t_res.status_code == 200:
            results["tpex"] = float(t_res.json().get("集合成交金額", 0))
    except: pass
    return results

def main():
    print("🔍 開始核心數據效驗...")
    amounts = get_data()
    
    # 強效驗：成交量缺失直接中斷
    if not amounts["twse"] or not amounts["tpex"]:
        print("❌ 錯誤：市場成交量數據缺失！中斷工作流。")
        sys.exit(1)

    # 檢查關鍵個股數據 (yfinance)
    try:
        df = yf.download(CRITICAL_STOCKS, period="2d", progress=False)['Close'].iloc[-1]
        if df.isnull().any():
            print(f"❌ 錯誤：關鍵個股數據含有 NaN！\n{df[df.isnull()]}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ 錯誤：yfinance 下載失敗: {e}")
        sys.exit(1)

    # 存檔：讓後續 main.py 能讀到最新的正確數據
    status_data = {
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_status": "OK",
        "macro": amounts
    }
    with open(JSON_OUT, 'w', encoding='utf-8') as f:
        json.dump(status_data, f, indent=4)
    
    print("✅ 校驗通過，狀態：OK")
    sys.exit(0)

if __name__ == "__main__":
    main()
