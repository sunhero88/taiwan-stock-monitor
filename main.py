# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime
from pathlib import Path

# 1. 頁面配置
st.set_page_config(page_title="Predator 戰略指揮中心 V14.0", page_icon="🦅", layout="wide")

# 2. 側邊欄
st.sidebar.title("🦅 系統狀態")
st.sidebar.success(f"📡 數據即時同步中\n更新時間: {datetime.datetime.now().strftime('%H:%M:%S')}")

# 3. 數據引擎
def get_data():
    try:
        import analyzer
        # 取得分析結果
        results = analyzer.run('tw-share')
        if results and len(results) >= 3:
            return results[0], results[1], results[2]
    except Exception as e:
        st.error(f"數據抓取異常: {e}")
    return None, None, {}

images, df_res, text_reports = get_data()

# 4. 主畫面佈局
st.title("🦅 宇宙第一股市智能分析系統 V14.0")

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("🤖 Predator 智能戰略判讀")
    report = text_reports.get("FINAL_AI_REPORT", "正在初始化分析引擎...") if isinstance(text_reports, dict) else str(text_reports)
    st.info(report)
    
    # 數據介入區塊
    st.subheader("📋 複製給 Predator Gem (數據介入)")
    copy_text = f"【Predator 數據介入】\n時間：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n大盤點位：30105.04\n結論：{report[:100]}..."
    st.code(copy_text, language="markdown")

with col2:
    st.subheader("🎯 關鍵監控標的 (TOP 10)")
    # 強制檢查 df_res 是否具備個股特徵
    if df_res is not None and not df_res.empty:
        # 如果發現這是法人統計表，則嘗試尋找其他數據源或給予提示
        if '法人類別' in df_res.columns:
            st.warning("⚠️ 目前接收到的是【法人統計數據】，請檢查 analyzer.py 是否有篩選個股清單。")
            st.table(df_res)
        else:
            # 顯示個股清單
            st.dataframe(df_res.head(10), use_container_width=True)
    else:
        st.info("💡 正在等待個股篩選結果...")

# 5. 底部顯示法人統計 (若有)
if isinstance(text_reports, dict) and "三大法人買賣超" in str(text_reports):
    st.divider()
    st.subheader("📊 全球市場籌碼背景")
    st.write(text_reports.get("三大法人買賣超", "暫無法人細節"))
