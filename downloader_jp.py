# -*- coding: utf-8 -*-
import os, sys, time, random, logging, warnings, subprocess, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import yfinance as yf

# ====== 自動安裝/匯入必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ====== 降噪與環境設定 ======
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# 路徑定義
MARKET_CODE = "jp-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# 狀態管理檔案
MANIFEST_CSV = Path(LIST_DIR) / "jp_manifest.csv"
LIST_ALL_CSV = Path(LIST_DIR) / "jp_list_all.csv"
THREADS = 4 # 建議維持在 4-8 之間，避免並發太高被鎖 IP

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_tse_list():
    """獲取日股清單：具備門檻檢查與欄位識別"""
    threshold = 3000 # 日股 (TSE) 標的應在 3800 檔以上
    log("📡 正在讀取 tokyo-stock-exchange 套件資料...")
    try:
        df = pd.read_csv(tse.csv_file_path)
        
        # 尋找代碼與名稱欄位 (支援日文標頭)
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        name_col = next((c for c in ['銘柄名', 'Name', 'name', 'Company Name'] if c in df.columns), None)

        if not code_col:
            raise KeyError(f"無法定位代碼欄位。現有欄位: {list(df.columns)}")

        res = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            if len(code) >= 4 and code[:4].isdigit():
                res.append({
                    "code": code[:4], 
                    "name": str(row[name_col]) if name_col else code[:4], 
                    "board": "T"
                })
        
        final_df = pd.DataFrame(res).drop_duplicates(subset=['code'])
        
        # --- 🚀 防呆檢查：數量門檻 ---
        if len(final_df) < threshold:
            log(f"⚠️ 警告：獲取清單數量異常 ({len(final_df)} 檔)，低於門檻 {threshold}")
            # 若已有舊清單，則讀取舊清單作為備援
            if LIST_ALL_CSV.exists():
                log("🔄 使用歷史清單快取作為備援...")
                return pd.read_csv(LIST_ALL_CSV)
        else:
            final_df.to_csv(LIST_ALL_CSV, index=False, encoding='utf-8-sig')
            log(f"✅ 成功獲取 {len(final_df)} 檔日股清單")
            
        return final_df

    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        if LIST_ALL_CSV.exists():
            log("🔄 讀取歷史清單快取...")
            return pd.read_csv(LIST_ALL_CSV)
        return pd.DataFrame()

def build_manifest(df_list):
    """建立或載入續跑清單"""
    if df_list.empty: return pd.DataFrame()

    if MANIFEST_CSV.exists():
        return pd.read_csv(MANIFEST_CSV)
    
    df_list["status"] = "pending"
    existing_files = {f.split(".")[0] for f in os.listdir(DATA_DIR) if f.endswith(".T.csv")}
    if existing_files:
        df_list.loc[df_list['code'].astype(str).isin(existing_files), "status"] = "done"
    
    df_list.to_csv(MANIFEST_CSV, index=False)
    return df_list

def download_one(row_tuple):
    """單檔下載：加入隨機延遲保護"""
    idx, row = row_tuple
    code = str(row['code']).zfill(4)
    symbol = f"{code}.T"
    out_path = os.path.join(DATA_DIR, f"{code}.T.csv")
    
    try:
        # --- 🚀 關鍵修改：隨機等待防止限流 ---
        # 日本市場建議 0.3 ~ 0.8 秒
        time.sleep(random.uniform(0.3, 0.8)) 
        
        tk = yf.Ticker(symbol)
        df_raw = tk.history(period="2y", interval="1d", auto_adjust=False)
        
        if df_raw is not None and not df_raw.empty:
            df_raw.reset_index(inplace=True)
            df_raw.columns = [c.lower() for c in df_raw.columns]
            if 'date' in df_raw.columns:
                df_raw['date'] = pd.to_datetime(df_raw['date'], utc=True).dt.tz_localize(None)
            
            cols = ['date','open','high','low','close','volume']
            df_final = df_raw[[c for c in cols if c in df_raw.columns]]
            df_final.to_csv(out_path, index=False)
            return idx, "done"
        return idx, "empty"
    except Exception:
        return idx, "failed"

def main():
    log("🇯🇵 日本股市 K 線下載器啟動")
    df_list = get_tse_list()
    if df_list.empty: return

    mf = build_manifest(df_list)
    todo = mf[mf["status"] != "done"]
    if todo.empty:
        log("✅ 所有日股資料已是最新。")
        return

    log(f"📝 待處理標的數：{len(todo)} 檔")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(download_one, item): item for item in todo.iterrows()}
        pbar = tqdm(total=len(todo), desc="日股下載進度")
        
        count = 0
        for f in as_completed(futures):
            idx, status = f.result()
            mf.at[idx, "status"] = status
            count += 1
            pbar.update(1)
            
            if count % 50 == 0:
                mf.to_csv(MANIFEST_CSV, index=False)
        pbar.close()

    mf.to_csv(MANIFEST_CSV, index=False)
    log(f"🏁 任務結束。成功下載：{len(mf[mf['status'] == 'done'])} 檔")

if __name__ == "__main__":
    main()
