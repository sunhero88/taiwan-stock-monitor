# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import datetime
from pathlib import Path

# 1. 頁面配置
st.set_page_config(page_title="Predator 戰略指揮中心 V14.0", page_icon="🦅", layout="wide")

# 定義數據路徑
root_dir = Path(__file__).parent.absolute()
data_file = root_dir / "global_market_summary.csv"

# 2. 自動更新邏輯 (核心：打開網頁就抓最新數據)
@st.cache_data(ttl=1800) # 每 30 分鐘自動失效，確保數據不會過舊
def get_latest_market_data():
    try:
        import analyzer
        # 這裡會自動去呼叫 downloader 抓數據並進行分析
        images, df_res, text_reports = analyzer.run('tw-share')
        return images, df_res, text_reports
    except Exception as e:
        st.error(f"即時數據抓取失敗: {e}")
        return None, None, None

# 執行分析
images, df_res, text_reports = get_latest_market_data()

# 3. 側邊欄顯示
st.sidebar.title("🦅 系統狀態")
st.sidebar.success(f"📡 數據即時同步中\n更新時間: {datetime.datetime.now().strftime('%H:%M:%S')}")

# 4. 主畫面與 AI 數據介入區
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown("**即時驅動版** | 核心邏輯：Predator V14.0")

if text_reports:
    col1, col2 = st.columns([6, 4])
    
    with col1:
        st.subheader("🤖 Predator 智能戰略判讀")
        ai_report = text_reports.get("FINAL_AI_REPORT", "分析引擎運算中...")
        st.info(ai_report)
        
        # --- 給 Gemini 讀取的數據橋樑 ---
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

    with col2:
        st.subheader("🎯 關鍵監控標的 (TOP 10)")
        if df_res is not None:
            # 存成 CSV 方便備查
            df_res.to_csv(data_file, index=False, encoding='utf-8-sig')
            
            display_df = df_res[['Symbol', 'Close', 'Return', 'Vol_Ratio']].head(10)
            st.dataframe(
                display_df.style.format({'Return': '{:+.2f}%', 'Vol_Ratio': '{:.2f}x'})
                .background_gradient(subset=['Return'], cmap='RdYlGn'),
                height=500
            )
else:
    st.warning("正在初始化數據模組，請稍候...")
