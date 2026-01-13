# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import subprocess
import sys
import os
from pathlib import Path
import time

# 1. 頁面基本設定
st.set_page_config(
    page_title="Predator 戰略指揮中心 V14.0",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 設定路徑
root_dir = Path(__file__).parent.absolute()

# 2. 注入自定義 CSS 樣式 (科技感外觀)
st.markdown("""
    <style>
    .report-box { background-color: #1e2630; padding: 20px; border-radius: 10px; border-left: 5px solid #ff4b4b; }
    .stButton>button { width: 100%; border-radius: 5px; height: 3em; background-color: #ff4b4b; color: white; }
    .stMetric { background-color: #0e1117; padding: 10px; border-radius: 5px; }
    </style>
    """, unsafe_allow_html=True)

# 3. 側邊欄：控制台
st.sidebar.title("🦅 Predator 戰略控制台")
market_options = {"tw-share": "台股上市櫃", "us": "美股指標", "asia": "亞太/匯率"}
market_id = st.sidebar.selectbox("選擇監控市場", list(market_options.keys()), format_func=lambda x: market_options[x])

st.sidebar.markdown("---")
st.sidebar.subheader("💰 帳戶資金概況")
con_asset = st.sidebar.number_input("保守帳戶資產", value=1200000)
adv_asset = st.sidebar.number_input("冒進帳戶資產", value=1650000)
st.sidebar.metric("總資產水位", f"{con_asset + adv_asset:,} 元")

# 4. 主頁面標題
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown(f"**目前監控對象：{market_options[market_id]}** | 數據來源：Yahoo Finance (原生數據)")

# 5. 核心功能按鈕區
col1, col2, col3 = st.columns(3)

def run_process(mode_name):
    """封裝執行邏輯"""
    try:
        # Step A: 清理舊摘要
        summary_file = root_dir / "global_market_summary.csv"
        if summary_file.exists(): os.remove(summary_file)
        
        # Step B: 抓取數據
        with st.status(f"🚀 正在執行 {mode_name} 流程...", expanded=True) as status:
            st.write("📡 正在同步美股與亞太領先指標...")
            subprocess.run([sys.executable, "downloader_us.py"], cwd=root_dir, check=False)
            subprocess.run([sys.executable, "downloader_asia.py"], cwd=root_dir, check=False)
            
            st.write(f"📊 正在下載 {market_id} 主市場數據...")
            downloader_tw = f"downloader_{market_id.split('-')[0]}.py"
            subprocess.run([sys.executable, downloader_tw, "--market", market_id], cwd=root_dir, check=True)
            
            st.write("🧠 啟動 V14.0 Predator 邏輯引擎進行判讀...")
            import analyzer
            images, df_res, text_reports = analyzer.run(market_id)
            
            status.update(label=f"✅ {mode_name} 報告生成完畢！", state="complete", expanded=False)
        return images, df_res, text_reports
    except Exception as e:
        st.error(f"❌ 系統執行異常: {e}")
        return None, None, None

# 執行按鈕
if col1.button("🔥 生成盤後/即時分析"):
    st.session_state.results = run_process("即時分析")

if col2.button("🌅 生成盤前策略指引"):
    st.session_state.results = run_process("盤前策略")

if col3.button("📤 發送報告至郵件"):
    if 'results' in st.session_state and st.session_state.results[1] is not None:
        from notifier import StockNotifier
        images, df_res, text_reports = st.session_state.results
        StockNotifier().send_stock_report(market_id.upper(), images, df_res, text_reports)
        st.toast("✅ 報告已送達您的信箱！")
    else:
        st.warning("請先生成報告後再發送。")

st.markdown("---")

# 6. 顯示分析結果區
if 'results' in st.session_state and st.session_state.results[1] is not None:
    images, df_res, text_reports = st.session_state.results
    
    # 建立兩欄佈局：左側文字與圖表，右側數據表
    left_col, right_col = st.columns([1, 1])
    
    with left_col:
        st.subheader("🤖 V14.0 Predator 智能判讀")
        # 顯示 FINAL_AI_REPORT (核心判讀)
        ai_msg = text_reports.get("FINAL_AI_REPORT", "無判讀數據")
        st.info(ai_msg)
        
        # 提供一鍵複製給 Gem 的區塊
        st.markdown("#### 📋 複製給 Predator Gem 進行深度對話")
        copy_text = f"【今日市場數據介入】\n{text_reports.get('00_全球市場背景', '')}\n{text_reports.get('📊 今日個股績效榜', '')}\n系統判讀：{ai_msg}"
        st.code(copy_text, language="markdown")
        
        if images:
            st.image(images[0]["path"], caption="市場情緒分佈圖", use_container_width=True)

    with right_col:
        st.subheader("🎯 關鍵標的監控")
        if df_res is not None:
            # 只顯示關鍵欄位
            display_df = df_res[['Symbol', 'Close', 'Return', 'Vol_Ratio']].sort_values('Return', ascending=False)
            st.dataframe(
                display_df.style.format({'Return': '{:+.2f}%', 'Vol_Ratio': '{:.2f}x'})
                .background_gradient(subset=['Return'], cmap='RdYlGn'),
                height=500
            )

# 7. 底部警報區
st.markdown("---")
st.subheader("🛡️ 系統防禦警報 (Red Flag)")
if 'results' in st.session_state:
    # 簡單邏輯判定：若有貶值警訊則顯示紅旗
    if "🔴 警訊" in st.session_state.results[2].get("FINAL_AI_REPORT", ""):
        st.error("🚩 紅旗觸發：資金外流壓力大，嚴禁過度槓桿，增加現金比重。")
    else:
        st.success("✅ 目前環境安全，依照 V14.0 指令動態追蹤。")
else:
    st.info("請執行分析以啟動防禦監控。")

# 側邊欄 Footer
st.sidebar.markdown("---")
st.sidebar.caption(f"系統運行中 | {time.strftime('%Y-%m-%d %H:%M:%S')}")
