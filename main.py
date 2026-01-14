# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sys
import argparse
from pathlib import Path
import analyzer
from notifier import StockNotifier

# 設定路徑
root_dir = Path(__file__).parent.absolute()

def run_core_logic(market_id="tw-share"):
    """核心執行邏輯：下載 -> 分析 -> 準備報告"""
    # 這裡呼叫你原本的 downloader 與 analyzer
    images, df_res, text_reports = analyzer.run(market_id)
    return images, df_res, text_reports

# --- CLI 模式 (給 GitHub Actions 運行) ---
def run_cli():
    print("🚀 啟動 V14.0 Predator 雲端分析...")
    images, df_res, text_reports = run_core_logic()
    # 執行郵件通知
    notifier = StockNotifier()
    notifier.send_stock_report("TW-SHARE", images, df_res, text_reports)
    print("✅ 分析完成並已發送報告！")

# --- Streamlit 網頁模式 (給本地電腦運行) ---
def run_web():
    st.set_page_config(page_title="Predator 戰略指揮中心 V14.0", layout="wide")
    st.title("🦅 Predator 戰略指揮中心 V14.0")
    
    market = st.sidebar.selectbox("選擇市場", ["tw-share", "us"])
    
    if st.button("🔥 生成即時分析報告"):
        with st.spinner("正在介入數據並判讀技術指標..."):
            images, df_res, text_reports = run_core_logic(market)
            
            # 顯示判讀標籤與位階
            st.subheader("🤖 Predator 智能判讀")
            st.code(text_reports.get("📊 今日個股績效榜", ""), language="markdown")
            
            if df_res is not None:
                st.dataframe(df_res.style.highlight_max(axis=0, subset=['Return']))
            
            # 提供一鍵複製給 Gem 的區塊
            st.subheader("📋 複製給 Predator Gem")
            copy_msg = f"市場：{market}\n數據報告：\n{text_reports.get('📊 今日個股績效榜', '')}"
            st.text_area("請將下方內容貼入 Gem", copy_msg, height=200)

# --- 主程式進入點 ---
if __name__ == "__main__":
    if "--cli" in sys.argv:
        run_cli()
    else:
        run_web()
