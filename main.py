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

# 2. 側邊欄：監控
st.sidebar.title("🦅 系統狀態")
update_time = datetime.datetime.now().strftime('%H:%M:%S')
st.sidebar.success(f"📡 核心已就緒\n當前時間: {update_time}")

# 3. 主標題
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown("**強制驅動版** | 核心邏輯：Predator V14.0")

# 4. 數據抓取引擎 (取消快取，強制重新載入)
def run_analysis():
    try:
        with st.status("正在啟動 Predator 引擎，抓取全球即時報價...", expanded=True) as status:
            st.write("📡 正在加載分析模組 (analyzer.py)...")
            import analyzer
            
            st.write("🔍 正在抓取台股上市櫃數據與全球指標...")
            # 強制執行分析
            results = analyzer.run('tw-share')
            
            if results and len(results) >= 3:
                status.update(label="✅ 數據抓取完成！", state="complete", expanded=False)
                return results[0], results[1], results[2]
            else:
                st.error("分析模組回傳格式錯誤。")
                return None, None, {}
    except Exception as e:
        st.error(f"❌ 引擎啟動失敗: {str(e)}")
        st.info("請檢查 analyzer.py 或其依賴的 downloader 檔案是否完整。")
        return None, None, {}

# 5. 執行分析
images, df_res, text_reports = run_analysis()

# 6. 戰略判讀呈現
if text_reports and isinstance(text_reports, dict):
    col1, col2 = st.columns([6, 4])
    
    with col1:
        st.subheader("🤖 Predator 智能戰略判讀")
        ai_report = text_reports.get("FINAL_AI_REPORT", "分析引擎運算中...")
        st.info(ai_report)
        
        # 給 Gemini 讀取的數據橋樑
        market_context = text_reports.get("00_全球市場背景", "未取得背景")
        top_stocks = text_reports.get("📊 今日個股績效榜", "未取得榜單")
        
        copy_text = f"""【Predator 數據介入報告】
時間：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
市場：台股上市櫃
數據：加權點位 30707.22 | 匯率 31.65
系統結論：{ai_report}"""
        
        st.subheader("📋 複製給 Predator Gem (數據介入)")
        st.code(copy_text, language="markdown")
        
        if images and isinstance(images, list) and len(images) > 0:
            image_path = images[0].get("path", "")
            if os.path.exists(image_path):
                st.image(image_path, use_container_width=True)

    with col2:
        st.subheader("🎯 關鍵監控標的 (TOP 10)")
        if df_res is not None and isinstance(df_res, pd.DataFrame):
            # 自動儲存最新 CSV
            df_res.to_csv(data_file, index=False, encoding='utf-8-sig')
            
            # 格式化顯示
            show_cols = [c for c in ['Symbol', 'Close', 'Return', 'Vol_Ratio'] if c in df_res.columns]
            display_df = df_res[show_cols].head(10)
            st.dataframe(
                display_df.style.format({
                    'Return': '{:+.2f}%', 
                    'Vol_Ratio': '{:.2f}x'
                } if 'Return' in display_df.columns else {})
                .background_gradient(subset=['Return'] if 'Return' in display_df.columns else [], cmap='RdYlGn'),
                height=500
            )
else:
    st.error("⚠️ 數據加載超時或分析邏輯中斷。請重新整理網頁再試一次。")
