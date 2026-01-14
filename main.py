# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import sys
import datetime
from pathlib import Path
import analyzer
from notifier import StockNotifier

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

def execute_analysis(market_id):
    return analyzer.run(market_id)

if __name__ == "__main__":
    # GitHub Actions 模式
    if "--cli" in sys.argv:
        session_name, target_market = get_session_info()
        images, df_res, text_reports = execute_analysis(target_market)
        notifier = StockNotifier()
        notifier.send_stock_report(f"Predator V14.0 - {session_name}", images, df_res, text_reports)
    
    # 本地 Streamlit 模式
    else:
        st.set_page_config(page_title="Predator V14.0", layout="wide")
        st.title("🦅 Predator 戰略指揮中心 V14.0")
        market = st.sidebar.selectbox("監控市場", ["tw-share", "us", "asia"])
        
        if st.button("🔥 啟動即時數據介入"):
            images, df_res, text_reports = execute_analysis(market)
            st.subheader("📋 複製給 Predator Gem (數據介入)")
            copy_msg = f"【數據介入報告】\n{text_reports.get('📊 今日個股績效榜', '')}"
            st.code(copy_msg, language="markdown")
            if df_res is not None:
                st.dataframe(df_res.tail(20))
