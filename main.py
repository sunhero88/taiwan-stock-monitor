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
        notifier.send_stock_report(f"V14.0 - {session_name}", images, df_res, text_reports)
        print("✅ 報告已寄送。")

# --- Web 模式 (本地電腦 Streamlit 用) ---
def run_web_mode():
    st.set_page_config(page_title="Predator V14.0 指揮中心", layout="wide", page_icon="🦅")
    st.title("🦅 Predator 戰略指揮中心 V14.0")
    
    # 側邊欄控制
    market = st.sidebar.selectbox("切換監控市場", ["tw-share", "us", "asia"])
    st.sidebar.info(f"當前模式：本地實戰判讀\n檔案目錄：{BASE_DIR}")

    if st.button("🔥 啟動 V14.0 數據介入分析"):
        # 第一步：防錯檢查與自動補完
        if auto_download_data(market):
            # 第二步：啟動分析引擎
            with st.spinner(f"正在分析 {market} 技術指標 (MA20/K力道)..."):
                images, df_res, text_reports = analyzer.run(market)
                
                if df_res is not None:
                    # 顯示智能摘要
                    st.subheader("🤖 核心戰略判讀")
                    st.success(text_reports.get("FINAL_AI_REPORT", "分析完畢"))

                    # 一鍵複製區塊 (對接 Predator Gem)
                    st.subheader("📋 數據介入 (複製給 Gem)")
                    report_msg = f"【V14.0 數據介入】\n市場：{market}\n{text_reports.get('📊 今日個股績效榜', '')}"
                    st.code(report_msg, language="markdown")
                    
                    # 數據細節表格
                    with st.expander("查看原始技術指標數據"):
                        st.dataframe(df_res.tail(20), use_container_width=True)
                else:
                    st.error("🚨 分析引擎回傳空值，請檢查 CSV 內容是否正確。")

# --- 主程式進入點 ---
if __name__ == "__main__":
    if "--cli" in sys.argv:
        run_cli_mode()
    else:
        run_web_mode()
