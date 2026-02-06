import pandas as pd
import requests
import json
import os
import sys
import logging
from datetime import datetime

# ==========================================
# 系統配置與日誌設定
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Predator_QC")

class DataIntegrityError(Exception):
    """當核心數據不完整時拋出的異常，用於強制熔斷系統"""
    pass

class MarketAmountManager:
    def __init__(self, trade_date=None):
        self.trade_date = trade_date or datetime.now().strftime("%Y%m%d")
        self.roc_date = f"{int(self.trade_date[:4]) - 1911}/{self.trade_date[4:6]}/{self.trade_date[6:]}"
        
        # 關鍵監控名單：這些標的數據缺失將觸發系統熔斷
        self.critical_symbols = ["2330.TW", "2317.TW", "2454.TW", "3324.TW", "3017.TW"]
        
        # Line Notify Token (選填，若有請填入)
        self.line_token = "YOUR_LINE_NOTIFY_TOKEN_HERE"

    # ------------------------------------------
    # 數據抓取模組 (含 Redirect 修復)
    # ------------------------------------------
    def fetch_twse_amount(self):
        """抓取上市成交金額"""
        url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
        params = {"response": "json", "date": self.trade_date}
        try:
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            if data.get("stat") == "OK":
                # 索引 3 通常是成交金額
                total_amount = sum(float(row[3].replace(',', '')) for row in data["data"])
                return total_amount
            return None
        except Exception as e:
            logger.error(f"TWSE 抓取失敗: {e}")
            return None

    def fetch_tpex_amount(self):
        """抓取上櫃成交金額 (修復 Redirect 問題)"""
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
        params = {"l": "zh-tw", "d": self.roc_date, "se": "EW"}
        try:
            # 關鍵：allow_redirects=False 防止被導向到錯誤頁面
            resp = requests.get(url, params=params, timeout=10, allow_redirects=False)
            if resp.status_code == 200:
                data = resp.json()
                return float(data.get("集合成交金額", 0))
            return None
        except Exception as e:
            logger.error(f"TPEX 抓取失敗: {e}")
            return None

    # ------------------------------------------
    # 強效驗與報警模組
    # ------------------------------------------
    def send_line_alert(self, message):
        """發送 Line 報警通知"""
        if self.line_token == "YOUR_LINE_NOTIFY_TOKEN_HERE":
            return
        url = "https://notify-api.line.me/api/notify"
        headers = {"Authorization": f"Bearer {self.line_token}"}
        data = {"message": f"\n🚨 Predator 數據熔斷警報\n{message}"}
        requests.post(url, headers=headers, data=data)

    def run_integrity_check(self, amount_twse, amount_tpex, stock_df):
        """核心品質稽核邏輯"""
        alerts = []
        is_fatal = False

        print(f"\n" + "="*50)
        print(f"🔍 數據品質稽核中... (交易日: {self.trade_date})")
        print("="*50)

        # 1. 檢查成交量數據
        if not amount_twse:
            alerts.append("❌ [致命] TWSE 成交金額缺失")
            is_fatal = True
        if not amount_tpex:
            alerts.append("❌ [致命] TPEX 成交金額缺失 (Redirect Error)")
            is_fatal = True

        # 2. 檢查 CSV 關鍵標的價格 (從 yfinance 下載的 stock_df)
        for symbol in self.critical_symbols:
            if symbol not in stock_df['Symbol'].values:
                alerts.append(f"⚠️ [缺失] 關鍵標的 {symbol} 不在 CSV 中")
                is_fatal = True
            else:
                price = stock_df.loc[stock_df['Symbol'] == symbol, 'Close'].iloc[0]
                if pd.isna(price) or price <= 0:
                    alerts.append(f"⚠️ [錯誤] 關鍵標的 {symbol} 價格異常: {price}")
                    is_fatal = True

        # 3. 處理稽核結果
        if alerts:
            alert_msg = "\n".join(alerts)
            for msg in alerts:
                logger.error(msg)
            
            if is_fatal:
                self.send_line_alert(alert_msg)
                print("\n" + "!"*50)
                print("🛑 偵測到核心數據缺口，系統執行強制熔斷！")
                print("!"*50 + "\n")
                raise DataIntegrityError("核心數據不完整，拒絕進入交易決策流程。")
        
        print("✅ 所有核心數據檢查通過，market_status: OK")
        return True

# ==========================================
# 主程式執行入口
# ==========================================
if __name__ == "__main__":
    # 範例情境：假設你已經有 stock_df (從 yfinance 讀取)
    # df_sample = pd.read_csv("data/data_tw-share.csv")
    
    manager = MarketAmountManager()
    
    try:
        # 1. 抓取數據
        amt_twse = manager.fetch_twse_amount()
        amt_tpex = manager.fetch_tpex_amount()
        
        # 2. 讀取目前的股票 CSV 進行稽核 (請確保路徑正確)
        if os.path.exists("data/data_tw-share.csv"):
            df_stocks = pd.read_csv("data/data_tw-share.csv")
            
            # 3. 執行強效驗 (若失敗會直接 raise Error)
            manager.run_integrity_check(amt_twse, amt_tpex, df_stocks)
            
            # 4. 若通過，則輸出最終 JSON (此處簡化邏輯)
            result = {
                "trade_date": manager.trade_date,
                "amount_twse": amt_twse,
                "amount_tpex": amt_tpex,
                "status": "OK"
            }
            with open("market_amount.json", "w") as f:
                json.dump(result, f, indent=4)
        else:
            logger.error("找不到股票數據 CSV，無法執行稽核。")
            sys.exit(1)

    except DataIntegrityError as de:
        # 攔截強效驗錯誤，優雅地停止程式
        sys.exit(1)
    except Exception as e:
        logger.error(f"系統運行發生未知錯誤: {e}")
        sys.exit(1)
