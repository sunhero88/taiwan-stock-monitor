# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import subprocess
import sys
import os
from pathlib import Path
import datetime

# 1. 頁面配置
st.set_page_config(
    page_title="Predator 戰略指揮中心 V14.0",
    page_icon="🦅",
    layout="wide"
)

# 定義根目錄與數據檔案路徑
root_dir = Path(__file__).parent.absolute()
data_file = root_dir / "global_market_summary.csv"

# 2. 側邊欄：監控與資產
st.sidebar.title("🦅 系統監控")
if data_file.exists():
    mtime = datetime.datetime.fromtimestamp(data_file.stat().st_mtime)
    st.sidebar.success(f"📡 數據同步：{mtime.strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.sidebar.warning("📡 待同步：請執行分析或等待自動化任務")

st.sidebar.markdown("---")
st.sidebar.subheader("💰 帳戶資產")
con_asset = st.sidebar.number_input("保守帳戶 (TWD)", value=1200000)
adv_asset = st.sidebar.number_input("冒進帳戶 (TWD)", value=1650000)
st.sidebar.metric("總資產水位", f"{(con_asset + adv_asset):,}")

# 3. 主頁面標題
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown("**雲端自動化版** | 核心邏輯：Predator V14.0 (高盛策略分析師模式)")

# 4. 功能按鈕區
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("🔄 立即更新即時數據 (盤中)"):
        with st.status("正在抓取全球數據...", expanded=False) as status:
            subprocess.run([sys.executable, "downloader_us.py"], cwd=root_dir)
            subprocess.run([sys.executable, "downloader_asia.py"], cwd=root_dir)
            status.update(label="✅ 數據抓取完成", state="complete")
            st.rerun()

# 5. 數據呈現與 AI 橋樑
st.markdown("---")

try:
    # 呼叫您的分析模組
    import analyzer
    # 預設分析台股上市櫃
    images, df_res, text_reports = analyzer.run('tw-share')
    
    # 佈局分兩欄
    left_col, right_col = st.columns([6, 4])
    
    with left_col:
        st.subheader("🤖 Predator 智能戰略判讀")
        # 顯示分析報告文字
        ai_report = text_reports.get("FINAL_AI_REPORT", "分析引擎運算中...")
        st.info(ai_report)
        
        # --- 關鍵：數據介入區塊 (給 Gemini 讀取用) ---
        market_context = text_reports.get("00_全球市場背景", "未取得背景")
        top_stocks = text_reports.get("📊 今日個股績效榜", "未取得榜單")
        
        copy_text = f"""【Predator 數據介入報告】
時間：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
市場：台股上市櫃
領先指標：{market_context}
強勢標的：{top_stocks}
系統結論：{ai_report}"""
        
        st.subheader("📋 複製給 Predator Gem (數據介入)")
        st.code(copy_text, language="markdown")
        
        if images:
            st.image(images[0]["path"], use_container_width=True)

    with right_col:
        st.subheader("🎯 關鍵監控標的 (TOP 10)")
        if df_res is not None:
            # --- 🚀 關鍵修正：實體化儲存 CSV (供 GitHub Action 抓取) ---
            df_res.to_csv(data_file, index=False, encoding='utf-8-sig')
            
            # 格式化表格顯示
            display_df = df_res[['Symbol', 'Close', 'Return', 'Vol_Ratio']].head(10)
            st.dataframe(
                display_df.style.format({'Return': '{:+.2f}%', 'Vol_Ratio': '{:.2f}x'})
                .background_gradient(subset=['Return'], cmap='RdYlGn'),
                height=500
            )

except Exception as e:
    st.error(f"⚠️ 核心分析模組載入失敗：{e}")
    st.info("請確保 analyzer.py 與相關 downloader 檔案已上傳至 GitHub 同一目錄下。")

# 6. 自動化定時刷新 (維持網頁活躍狀態)
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=15 * 60 * 1000, key="auto_refresh") 

st.markdown("---")
st.caption(f"Predator V14.0 指令集已就緒 | 目前時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
