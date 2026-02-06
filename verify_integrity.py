import pandas as pd
import json
import os
from datetime import datetime

def check_data_health(json_path, csv_path):
    print(f"🔍 開始數據完整度稽核 - 執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)
    
    # 1. 載入 JSON 決策檔
    if not os.path.exists(json_path):
        print(f"❌ 錯誤: 找不到 JSON 檔案 ({json_path})")
        return
    
    with open(json_path, 'r', encoding='utf-8') as f:
        macro_data = json.load(f)
    
    # 2. 載入 CSV 數據源
    if not os.path.exists(csv_path):
        print(f"❌ 錯誤: 找不到 CSV 檔案 ({csv_path})")
        return
    
    df = pd.read_csv(csv_path)
    
    # --- 專項檢查 A: 市場成交額 (Market Amount) ---
    amount_twse = macro_data['macro']['market_amount']['amount_twse']
    amount_tpex = macro_data['macro']['market_amount']['amount_tpex']
    
    print(f"📈 [市場成交額檢查]")
    print(f"   - 上市 (TWSE): {amount_twse if amount_twse else '❌ MISSING'}")
    print(f"   - 上櫃 (TPEX): {amount_tpex if amount_tpex else '❌ MISSING'}")
    
    if amount_tpex is None:
        print("   ⚠️ 警報: TPEX 數據缺失，這將觸發 DEGRADED 狀態！")
    
    # --- 專項檢查 B: 個股數據完整度 (Stock Integrity) ---
    print(f"\n📋 [核心個股檢查]")
    stocks = macro_data.get('stocks', [])
    missing_stocks = [s['Name'] for s in stocks if s['Price'] is None]
    
    if missing_stocks:
        print(f"   ⚠️ 發現價格缺失個股: {', '.join(missing_stocks)}")
    else:
        print("   ✅ 所有核心個股價格正常。")

    # --- 專項檢查 C: CSV vs JSON 同步性 ---
    csv_symbols = set(df['Symbol'].unique())
    json_symbols = set([s['Symbol'] for s in stocks])
    
    diff = json_symbols - csv_symbols
    if diff:
        print(f"   ⚠️ 數據不同步: JSON 中的 {diff} 在 CSV 中找不到！")

    # --- 總結判定 ---
    is_ok = (amount_tpex is not None) and (len(missing_stocks) == 0)
    
    print("-" * 50)
    if is_ok:
        print("✅ 診斷結果: 數據完整，market_status 應可回歸 OK。")
    else:
        print("🚫 診斷結果: 數據不完整，請重新執行修復版 download_data.py。")

if __name__ == "__main__":
    # 請根據你的檔案路徑修改
    JSON_FILE = "macro.json" 
    CSV_FILE = "data/data_tw-share.csv"
    check_data_health(JSON_FILE, CSV_FILE)
