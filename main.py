# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sys
import datetime
from pathlib import Path
import analyzer
from notifier import StockNotifier

# 取得目前路徑
root_dir = Path(__file__).parent.absolute()

def get_session_info():
    """根據台北時間判斷目前的戰略時段"""
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    hour = now.hour
    if hour < 10:
        return "🌅 盤前戰略指引", "us" # 盤前看美股
    elif 10 <= hour < 14:
        return "⚡ 盤中實戰監控", "tw-share" # 盤中看台股爆量
    else:
        return "📊 盤後技術結算", "tw-share" # 盤後看完整指標

def execute_analysis(market_id):
    """執行分析核心"""
    return analyzer.run(market_id)

# --- CLI 模式 (GitHub Actions 用) ---
def run_cli_mode():
    session_name, target_market = get_session_info()
    print(f"📡 正在執行：{session_name}...")
    
    images, df_res, text_reports = execute_analysis(target_market)
    
    # 加入時間標籤以便 Gem 判讀
    text_reports['SESSION'] = session_name
    
    notifier = StockNotifier()
    notifier.send_stock_report(f"Predator V14.0 - {session_name}", images, df_res, text_reports)
    print(f"✅ {session_name} 發送成功！")

# --- Web 模式 (本地 Streamlit 用) ---
def run_web_mode():
    st.set_page_config(page_title="Predator V14.0 指揮中心", layout="wide")
    st.title("🦅 Predator 戰略指揮中心 V14.0")
    
    market = st.sidebar.selectbox("監控市場", ["tw-share", "us", "asia"])
    
    if st.button("🔥 啟動即時數據介入"):
        with st.spinner("正在介入數據並判讀技術指標..."):
            images, df_res, text_reports = execute_analysis(market)
            
            st.subheader(f"🤖 智能判讀結果")
            st.code(text_reports.get("📊 今日個股績效榜", ""), language="markdown")
            
            # 一鍵複製區塊
            st.subheader("📋 複製給 Predator Gem")
            copy_msg = f"【數據介入報告】\n{text_reports.get('📊 今日個股績效榜', '')}"
            st.text_area("複製以下文字到 Gem：", copy_msg, height=250)
            
            if df_res is not None:
                st.dataframe(df_res.tail(20))

if __name__ == "__main__":
    if "--cli" in sys.argv:
        run_cli_mode()
    else:
        run_web_mode()
