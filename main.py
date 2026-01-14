# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sys
import os
import datetime
import subprocess
from pathlib import Path

# 嘗試匯入自定義模組
try:
    import analyzer
    from notifier import StockNotifier
except ImportError:
    st.error("❌ 找不到 analyzer.py 或 notifier.py，請確保檔案在同一個資料夾。")

# 設定路徑
BASE_DIR = Path(__file__).parent.absolute()

def get_session_info():
    """判斷台北時間戰略時段"""
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    hour = now.hour
    if hour < 10:
        return "🌅 盤前戰略", "us"
    elif 10 <= hour < 14:
        return "⚡ 盤中監控", "tw-share"
    else:
        return "📊 盤後結算", "tw-share"

def auto_download_data(market_id):
    """防錯核心：如果找不到檔案，自動執行下載器"""
    # 轉換 ID 格式，例如 tw-share -> tw
    script_suffix = market_id.split('-')[0]
    downloader_script = f"downloader_{script_suffix}.py"
    target_csv = f"raw_data_{market_id}.csv"
    
    if not (BASE_DIR / target_csv).exists():
        st.warning(f"🔍 偵測到環境缺失數據：{target_csv}，正在啟動自動補完...")
        if (BASE_DIR / downloader_script).exists():
            try:
                # 執行下載指令
                subprocess.run([sys.executable, downloader_script], check=True)
                st.success(f"✅ 數據下載成功：{target_csv}")
                return True
            except Exception as e:
                st.error(f"❌ 自動下載失敗：{e}")
                return False
        else:
            st.error(f"❌ 找不到下載器檔案：{downloader_script}，請檢查檔案是否存在。")
            return False
    return True

# --- CLI 模式 (GitHub Actions 用) ---
def run_cli_mode():
    session_name, target_market = get_session_info()
    print(f"📡 模式：雲端 CLI | 時段：{session_name}")
    
    # 雲端執行通常會先跑 downloader 任務，但這裡加上防錯
    if auto_download_data(target_market):
        images, df_res, text_reports = analyzer.run(target_market)
        notifier = StockNotifier()
        notifier.send_stock_report
