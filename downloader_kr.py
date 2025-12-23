# -*- coding: utf-8 -*-
import os, sys, time, random, logging, warnings, subprocess, json
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import yfinance as yf

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        print(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

ensure_pkg("pykrx")
from pykrx import stock as krx

# ====== 降噪與環境設定 ======
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

MARKET_CODE = "kr-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# Checkpoint 檔案路徑
MANIFEST_CSV = Path(LIST_DIR) / "kr_manifest.csv"
LIST_ALL_CSV = Path(LIST_DIR) / "kr_list_all.csv"
THREADS = 4 

# 💡 核心新增：數據過期時間 (3600 秒 = 1 小時)
DATA_EXPIRY_SECONDS = 3600

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def map_symbol_kr(code: str, board: str) -> str:
    """轉換為 Yahoo Finance 格式"""
    suffix = ".KS" if board.upper() == "KS" else ".KQ"
    return f"{str(code).zfill(6)}{suffix}"

def standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    """將 yfinance 原始資料標準化"""
    if df is None or df.empty: return pd.DataFrame()
    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    if 'date' not in df.columns: return pd.DataFrame()
    
    # 移除時區
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
    req = ['date','open','high','low','close','volume']
    return df[req] if all(c in df.columns for c in req) else pd.DataFrame()

def get_kr_list():
    """從 KRX 獲取最新 KOSPI/KOSDAQ 清單"""
    threshold = 2200 
    max_retries = 3
    
    for i in range(max_retries):
        log(f"📡 正在從 pykrx 獲取韓股清單 (第 {i+1} 次嘗試)...")
        try:
            today = pd.Timestamp.today().strftime("%Y%m%d")
            lst = []
            for mk, bd in [("KOSPI","KS"), ("KOSDAQ","KQ")]:
                tickers = krx.get_market_ticker_list(today, market=mk)
                for t in tickers:
                    name = krx.get_market_ticker_name(t)
                    lst.append({"code": t, "name": name, "board": bd})
            
            df = pd.DataFrame(lst)
            if len(df) >= threshold:
                log(f"✅ 成功獲取 {len(df)} 檔韓股清單")
                df.to_csv(LIST_ALL_CSV, index=False, encoding='utf-8-sig')
                return df
        except Exception as e:
            log(f"❌ 獲取清單失敗: {e}")
        time.sleep(5)

    if LIST_ALL_CSV.exists():
        log("🔄 使用歷史清單快取作為備援...")
        return pd.read_csv(LIST_ALL_CSV)
    return pd.DataFrame([{"code":"005930","name":"三星電子","board":"KS"}])

def build_manifest(df_list):
    """建立續跑清單，偵測檔案是否存在且是否在有效期內"""
    # 如果 manifest 存在，讀取它，但我們會強制檢查檔案時效
    if MANIFEST_CSV.exists():
        mf = pd.read_csv(MANIFEST_CSV)
        # 確保新股入列
        new_items = df_list[~df_list['code'].astype(str).isin(mf['code'].astype(str))]
        if not new_items.empty:
            new_items = new_items.copy()
            new_items['status'] = 'pending'
            mf = pd.concat([mf, new_items], ignore_index=True)
    else:
        mf = df_list.copy()
        mf["status"] = "pending"

    # 💡 智慧檢查：遍歷檔案，若檔案太舊則標記為 pending 重新下載
    log("🔍 正在檢查數據時效性...")
    for idx, row in mf.iterrows():
        out_path = os.path.join(DATA_DIR, f"{row['code']}.{row['board']}.csv")
        if os.path.exists(out_path):
            file_age = time.time() - os.path.getmtime(out_path)
            if file_age < DATA_EXPIRY_SECONDS:
                mf.at[idx, "status"] = "done"
            else:
                mf.at[idx, "status"] = "pending" # 過期，需重抓
        else:
            mf.at[idx, "status"] = "pending"

    mf.to_csv(MANIFEST_CSV, index=False)
    return mf

def download_one(row_tuple):
    """單檔下載邏輯：強化版重試機制"""
    idx, row = row_tuple
    code, board = row['code'], row['board']
    symbol = map_symbol_kr(code, board)
    out_path = os.path.join(DATA_DIR, f"{code}.{board}.csv")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 隨機等待 0.4~1.0 秒以模擬真人
            time.sleep(random.uniform(0.4, 1.0))
            
            tk = yf.Ticker(symbol)
            df_raw = tk.history(period="2y", interval="1d", auto_adjust=True, timeout=20)
            df = standardize_df(df_raw)
            
            if not df.empty:
                df.to_csv(out_path, index=False)
                return idx, "done"
            
            if attempt == max_retries - 1: return idx, "empty"
        except Exception:
            if attempt == max_retries - 1: return idx, "failed"
            time.sleep(random.randint(2, 5))
            
    return idx, "failed"

def main():
    start_time = time.time()
    log("🇰🇷 啟動韓股下載引擎 (時效檢查模式)")
    
    # 1. 獲取與建立清單
    df_list = get_kr_list()
    mf = build_manifest(df_list)
    
    # 2. 篩選需要抓取的標的 (pending 或 failed)
    todo = mf[~mf["status"].isin(["done", "empty"])]
    
    if not todo.empty:
        log(f"📝 待處理標的：{len(todo)} 檔 (其餘 {len(mf)-len(todo)} 檔在有效期內)")
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {executor.submit(download_one, item): item for item in todo.iterrows()}
            pbar = tqdm(total=len(todo), desc="韓股下載進度")
            
            count = 0
            try:
                for f in as_completed(futures):
                    idx, status = f.result()
                    mf.at[idx, "status"] = status
                    count += 1
                    pbar.update(1)
                    if count % 100 == 0: mf.to_csv(MANIFEST_CSV, index=False)
            except KeyboardInterrupt:
                log("🛑 中斷下載，儲存進度...")
            finally:
                mf.to_csv(MANIFEST_CSV, index=False)
                pbar.close()
    else:
        log("✅ 所有韓股資料皆在 1 小時內更新過，直接進入分析。")

    # 📊 數據下載統計
    total_expected = len(mf)
    effective_success = len(mf[mf['status'] == 'done'])
    fail_count = total_expected - effective_success

    download_stats = {
        "total": total_expected,
        "success": effective_success,
        "fail": fail_count
    }

    duration = (time.time() - start_time) / 60
    log("="*30)
    log(f"🏁 韓股下載任務完成 (耗時 {duration:.1f} 分鐘)")
    log(f"   - 下載成功(含有效期內): {effective_success}")
    log(f"   - 數據完整度: {(effective_success/total_expected)*100:.2f}%")
    log("="*30)

    return download_stats

if __name__ == "__main__":
    main()
