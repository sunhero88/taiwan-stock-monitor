# -*- coding: utf-8 -*-
import os, io, re, time, random, json
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數與路徑 ==========
MARKET_CODE = "hk-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
CACHE_LIST_PATH = os.path.join(BASE_DIR, "hk_stock_list_cache.json")

MAX_WORKERS = 5
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 工具：代碼正規化 ==========
def normalize_code5(s: str) -> str:
    """確保為 5 位數補零格式 (用於檔名)"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-5:].zfill(5) if digits else ""

def to_symbol_yf(code: str) -> str:
    """轉換為 Yahoo Finance 格式 (4 位數.HK)"""
    digits = re.sub(r"\D", "", str(code or ""))
    return f"{digits[-4:].zfill(4)}.HK"

def classify_security(name: str) -> str:
    """過濾衍生品與非普通股"""
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    if any(kw in n for kw in bad_kw):
        return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """
    ⚡ 清單快取：從 HKEX 獲取清單並篩選
    """
    if os.path.exists(CACHE_LIST_PATH):
        file_mtime = os.path.getmtime(CACHE_LIST_PATH)
        if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
            log("📦 偵測到今日已緩存港股清單，直接載入...")
            with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)

    log("📡 緩存失效，從 HKEX 官網下載最新證券名單...")
    try:
        url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
        r = requests.get(url, timeout=30)
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 尋找表頭 (簡單定位)
        hdr_idx = 0
        for i in range(20):
            row_str = "".join([str(x) for x in df_raw.iloc[i]]).lower()
            if "stock code" in row_str and "short name" in row_str:
                hdr_idx = i
                break
        
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].tolist()
        
        col_code = [c for c in df.columns if "Stock Code" in str(c)][0]
        col_name = [c for c in df.columns if "Short Name" in str(c)][0]
        
        res = []
        for _, row in df.iterrows():
            raw_code = str(row[col_code])
            name = str(row[col_name])
            if classify_security(name) == "Common Stock":
                code5 = normalize_code5(raw_code)
                if code5 and int(code5) >= 1: # 確保是有效代碼
                    res.append(f"{code5}&{name}")
        
        final_list = list(set(res))
        with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False)
        log(f"✅ 港股清單更新完成，共 {len(final_list)} 檔普通股。")
        return final_list
        
    except Exception as e:
        log(f"❌ HKEX 清單獲取失敗: {e}")
        return []

def download_stock_data(item):
    """
    ⚡ 檔案級快取：下載 2 年 K 線
    """
    try:
        code5, name = item.split('&', 1)
        yf_sym = to_symbol_yf(code5)
        
        # 存檔命名：00700.HK.csv
        out_path = os.path.join(DATA_DIR, f"{code5}.HK.csv")
        
        # ✅ 快取檢查
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": code5}

        time.sleep(random.uniform(0.4, 1.2))
        tk = yf.Ticker(yf_sym)
        
        for attempt in range(2):
            try:
                hist = tk.history(period="2y", timeout=20)
                if hist is not None and not hist.empty:
                    hist.reset_index(inplace=True)
                    hist.columns = [c.lower() for c in hist.columns]
                    # 統一欄位名稱
                    hist = hist.rename(columns={'date':'date','open':'open','high':'high','low':'low','close':'close','volume':'volume'})
                    hist.to_csv(out_path, index=False, encoding='utf-8-sig')
                    return {"status": "success", "tkr": code5}
            except:
                time.sleep(random.uniform(5, 10))
            
        return {"status": "empty", "tkr": code5}
    except:
        return {"status": "error"}

def main():
    items = get_full_stock_list()
    if not items: return
    
    log(f"🚀 開始港股下載任務 (執行緒: {MAX_WORKERS})")
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="港股進度", unit="檔")
        
        for future in as_completed(futures):
            res = future.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
            
            if res.get("status") == "success" and stats["success"] % 100 == 0:
                time.sleep(random.uniform(5, 10))
                
        pbar.close()
    
    print("\n" + "="*50)
    log("📊 港股下載任務報告:")
    print(f"   - ✅ 成功: {stats['success']}")
    print(f"   - 📁 跳過 (已存在): {stats['exists']}")
    print(f"   - 🔍 無資料 (Empty): {stats['empty']}")
    print(f"   - ❌ 失敗 (Error): {stats['error']}")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
