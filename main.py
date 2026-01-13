# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime
import os
from pathlib import Path

# 1. 頁面配置
st.set_page_config(page_title="Predator 戰略指揮中心 V14.0", page_icon="🦅", layout="wide")

# 定義數據路徑
root_dir = Path(__file__).parent.absolute()
data_file = root_dir / "global_market_summary.csv"

# 2. 側邊欄
st.sidebar.title("🦅 系統狀態")
st.sidebar.success(f"📡 核心已就緒\n系統時間: {datetime.datetime.now().strftime('%H:%M:%S')}")

# 3. 主標題
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown("**多維數據驅動版** | 核心邏輯：Predator V14.0")

# 4. 數據引擎
def run_analysis_engine():
    images, df_res, text_reports = None, None, {}
    try:
        with st.status("正在執行 Predator 深度分析...", expanded=True) as status:
            import analyzer
            results = analyzer.run('tw-share')
            
            if results and len(results) >= 3:
                images, df_res, raw_reports = results[0], results[1], results[2]
                # 強制轉換報告為字典
                if isinstance(raw_reports, dict): text_reports = raw_reports
                else: text_reports = {"FINAL_AI_REPORT": str(raw_reports)}
                status.update(label="✅ 分析完成！", state="complete")
    except Exception as e:
        st.error(f"❌ 引擎異常: {str(e)}")
    
    return images, df_res, text_reports

images, df_res, text_reports = run_analysis_engine()

# 5. 畫面呈現
col1, col2 = st.columns([6, 4])

with col1:
    st.subheader("🤖 Predator 智能戰略判讀")
    report_content = text_reports.get("FINAL_AI_REPORT", "正在解析籌碼數據...")
    st.info(report_content)
    
    st.subheader("📋 複製給 Predator Gem (數據介入)")
    copy_text = f"""【Predator 數據介入】
時間：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
大盤點位：30105.04
主力動態：外資買超 60.43 億
戰略結論：{report_content[:150]}..."""
    st.code(copy_text, language="markdown")

with col2:
    st.subheader("🎯 關鍵監控標的 (數據列表)")
    if df_res is not None:
        # 自動存檔備份
        df_res.to_csv(data_file, index=False, encoding='utf-8-sig')
        
        # 邏輯判斷：如果表格包含 '法人類別'，代表是統計表；否則視為個股表
        if '法人類別' in df_res.columns:
            st.write("📊 三大法人買賣超統計")
            st.table(df_res) # 使用靜態表格呈現統計數據
        else:
            st.write("🚀 TOP 10 潛力個股名單")
            st.dataframe(df_res.head(10), use_container_width=True)
    else:
        st.warning("⚠️ 暫無個股篩選數據。")

# 顯示圖表
if images and len(images) > 0:
    st.image(images[0].get("path", ""), use_container_width=True)
