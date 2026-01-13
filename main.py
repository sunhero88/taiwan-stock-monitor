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

# 2. 自動更新邏輯 (網頁驅動版)
@st.cache_data(ttl=1800)
def get_latest_market_data():
    try:
        import analyzer
        # 執行分析模組
        results = analyzer.run('tw-share')
        
        # 嚴謹的結果解包與防呆處理
        if results and isinstance(results, (list, tuple)) and len(results) >= 3:
            images = results[0]
            df_res = results[1]
            text_reports = results[2]
            # 確保 text_reports 永遠是一個字典
            if text_reports is None:
                text_reports = {}
            return images, df_res, text_reports
        else:
            st.warning("分析模組回傳格式異常，請檢查 analyzer.py")
            return None, None, {}
    except Exception as e:
        st.error(f"即時數據抓取失敗: {e}")
        return None, None, {}

# 執行分析並確保變數皆有初始值
images, df_res, text_reports = get_latest_market_data()

# 3. 側邊欄顯示
st.sidebar.title("🦅 系統狀態")
st.sidebar.success(f"📡 數據即時同步中\n更新時間: {datetime.datetime.now().strftime('%H:%M:%S')}")

# 4. 主畫面與 AI 數據介入區
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown("**即時驅動版** | 核心邏輯：Predator V14.0")

# 5. 戰略判讀呈現 (加入內容檢查)
if text_reports and isinstance(text_reports, dict):
    col1, col2 = st.columns([6, 4])
    
    with col1:
        st.subheader("🤖 Predator 智能戰略判讀")
        ai_report = text_reports.get("FINAL_AI_REPORT", "分析引擎正在計算最新戰略判讀中...")
        st.info(ai_report)
        
        # --- 給 Gemini 讀取的數據橋樑 ---
        market_context = text_reports.get("00_全球市場背景", "未取得背景數據")
        top_stocks = text_reports.get("📊 今日個股績效榜", "未取得榜單數據")
        
        copy_text = f"""【Predator 數據介入報告】
時間：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
市場：台股上市櫃
領先指標：{market_context}
強勢標的：{top_stocks}
系統結論：{ai_report}"""
        
        st.subheader("📋 複製給 Predator Gem (數據介入)")
        st.code(copy_text, language="markdown")
        
        # 顯示圖片分析
        if images and isinstance(images, list) and len(images) > 0:
            image_path = images[0].get("path", "") if isinstance(images[0], dict) else ""
            if image_path:
                st.image(image_path, use_container_width=True)

    with col2:
        st.subheader("🎯 關鍵監控標的 (TOP 10)")
        if df_res is not None and isinstance(df_res, pd.DataFrame):
            # 自動儲存最新數據 CSV
            df_res.to_csv(data_file, index=False, encoding='utf-8-sig')
            
            # 顯示表格 (檢查欄位是否存在)
            show_cols = [c for c in ['Symbol', 'Close', 'Return', 'Vol_Ratio'] if c in df_res.columns]
            if show_cols:
                display_df = df_res[show_cols].head(10)
                st.dataframe(
                    display_df.style.format({
                        'Return': '{:+.2f}%', 
                        'Vol_Ratio': '{:.2f}x'
                    } if 'Return' in display_df.columns else {})
                    .background_gradient(
                        subset=['Return'] if 'Return' in display_df.columns else [], 
                        cmap='RdYlGn'
                    ),
                    height=500
                )
else:
    st.warning("📊 數據模組初始化中，請稍候... (若長時間未反應請確認網路連線)")
