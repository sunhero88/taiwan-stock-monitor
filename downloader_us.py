# -*- coding: utf-8 -*-
import os
import time
import random
import requests
import pandas as pd
import yfinance as yf
import json
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數設定 ==========
MARKET_CODE = "us-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
# 🚀 新增：清單快取路徑
CACHE_LIST_PATH = os.path.join(BASE_DIR, "us_stock_list_cache.json")

MAX_WORKERS = 5 
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def classify_security(name: str, is_etf: bool) -> str:
    """過濾邏輯：僅保留高品質普通股"""
    if is_etf: return "Exclude"
    n_upper = name.upper()
    exclude_keywords = ["WARRANT", "RIGHTS", "UNIT", "PREFERRED", "DEPOSITARY", "ADR", "FOREIGN", "DEBENTURE"]
    if any(kw in n_upper for kw in exclude_keywords): return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """
    ⚡ 快取化清單獲取：
    若今日已抓過清單則直接讀取，不重複請求 Nasdaq 官網
    """
    if os.path.exists(CACHE_LIST_PATH):
        file_mtime = os.path.getmtime(CACHE_LIST_PATH)
        # 如果檔案是今天產生的，就直接用
        if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
            log("📦 偵測到今日已緩存美股清單，直接載入...")
            with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)

    log("📡 緩存失效，開始從官網獲取美股普通股清單...")
    all_rows = []

    # 1. NASDAQ
    try:
        r1 = requests.get("https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt", timeout=15)
        df1 = pd.read_csv(StringIO(r1.text), sep="|")
        df1 = df1[df1["Test Issue"] == "N"]
        df1["Category"] = df1.apply(lambda row: classify_security(row["Security Name"], row["ETF"] == "Y"), axis=1)
        f1 = df1[(df1["Market Category"].isin(["Q", "G"])) & (df1["Category"] == "Common Stock")]
        for _, row in f1.iterrows():
            all_rows.append(f"{str(row['Symbol']).strip().replace('$', '-')}&{str(row['Security Name']).strip()}")
    except Exception as e: log(f"⚠️ NASDAQ 失敗: {e}")

    # 2. NYSE/Other
    try:
        r2 = requests.get("https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt", timeout=15)
        df2 = pd.read_csv(StringIO(r2.text), sep="|")
        df2 = df2[df2["Test Issue"] == "N"]
        df2["Category"] = df2.apply(lambda row: classify_security(row["Security Name"], row["ETF"] == "Y"), axis=1)
        f2 = df2[(df2["Exchange"] == "N") & (df2["Category"] == "Common Stock")]
        for _, row in f2.iterrows():
            all_rows.append(f"{str(row['NASDAQ Symbol']).strip().replace('$', '-')}&{str(row['Security Name']).strip()}")
    except Exception as e: log(f"⚠️ NYSE 失敗: {e}")

    final_list = list(set(all_rows))
    
    # 儲存清單快取
    with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False)
        
    log(f"✅ 清單已更新並儲存，共 {len(final_list)} 檔。")
    return final_list

def download_stock_data(item):
    """
    ⚡ 檔案級快取：
    若硬碟已存在該代號 CSV 且大小正確，直接跳過下載
    """
    try:
        parts = item.split('&', 1)
        if len(parts) < 2: return {"status": "error"}
        yf_tkr, name = parts
        safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '_', '-')]).strip()
        out_path = os.path.join(DATA_DIR, f"{yf_tkr}_{safe_name}.csv")
        
        # ✅ 快取核心：檢查檔案是否存在
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": yf_tkr}

        # --- 若快取不存在才執行下載 ---
        time.sleep(random.uniform(0.4, 1.2))
        tk = yf.Ticker(yf_tkr)
        
        for attempt in range(2):
            try:
                hist = tk.history(period="2y", timeout=20)
                if hist is not None and not hist.empty:
                    hist.reset_index(inplace=True)
                    hist.columns = [c.lower() for c in hist.columns]
                    hist.to_csv(out_path, index=False, encoding='utf-8-sig')
                    return {"status": "success", "tkr": yf_tkr}
            except Exception as e:
                if "Rate limited" in str(e): time.sleep(random.uniform(20, 40))
            time.sleep(random.uniform(3, 6))

        return {"status": "empty", "tkr": yf_tkr}
    except: return {"status": "error"}

def main():
    items = get_full_stock_list()
    if not items: return log("❌ 無清單。")

    log(f"🚀 開始美股任務 (雙重快取啟動中)")
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="美股進度", unit="檔")
        for future in as_completed(futures):
            res = future.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
            # 只有在真正下載(success)時才需要長休眠，快取跳過時不需要
        pbar.close()
    
    log(f"📊 報告: 成功={stats['success']}, 跳過={stats['exists']}, 無資料={stats['empty']}, 失敗={stats['error']}")

if __name__ == "__main__":
    main()