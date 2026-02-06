# health_checker.py
import json
import os
import subprocess
import time

# 設定路徑
REPORT_PATH = "latest_report.json"  # 您的 Predator 產出的 JSON 檔名
MARKET_SCRIPT = "market_amount.py"
DOWNLOAD_SCRIPT = "download_tw.py"

def run_repair():
    print("🛠️ 偵測到系統降級，啟動自動修復程序...")
    
    # 執行市場金額修復 (修正 TPEX 重導向)
    print("👉 正在修復市場成交額數據...")
    subprocess.run(["python", MARKET_SCRIPT], check=True)
    
    # 執行個股數據補完 (修復 2330, 3324 等 NaN 缺失)
    print("👉 正在修補個股缺失數據 (yfinance 備援機制)...")
    subprocess.run(["python", DOWNLOAD_SCRIPT, "--market", "tw-share"], check=True)
    
    print("✅ 修復指令執行完畢，等待系統重新生成報告。")

def check_health():
    if not os.path.exists(REPORT_PATH):
        print(f"❌ 找不到報告檔案: {REPORT_PATH}")
        return

    try:
        with open(REPORT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        status = data.get("meta", {}).get("market_status", "UNKNOWN")
        integrity = data.get("macro", {}).get("integrity", {})
        
        print(f"🔍 當前系統狀態: {status}")
        
        # 觸發修復的條件：
        # 1. 狀態為 DEGRADED
        # 2. 上櫃數據缺失 (amount_tpex 為 null)
        # 3. 核心個股缺失 (price_null > 0)
        should_repair = (
            status == "DEGRADED" or 
            data["macro"]["market_amount"]["amount_tpex"] is None or
            integrity.get("price_null", 0) > 0
        )

        if should_repair:
            run_repair()
        else:
            print("🌟 數據完整性良好，無需修復。")

    except Exception as e:
        print(f"❌ 檢查過程中發生錯誤: {e}")

if __name__ == "__main__":
    check_health()
