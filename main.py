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

# 2. 側邊欄顯示
st.sidebar.title("🦅 系統狀態")
st.sidebar.success(f"📡 核心已就緒\n系統時間: {datetime.datetime.now().strftime('%H:%M:%S')}")

# 3. 主標題
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown("**終極兼容驅動版** | 核心邏輯：Predator V14.0")

# 4. 核心數據引擎 (強化類型校驗)
def run_analysis_engine():
    images, df_res, text_reports = None, None, {}
    try:
        with st.status("正在執行 Predator 深度分析...", expanded=True) as status:
            st.write("📡 啟動分析模組...")
            import analyzer
            
            st.write("🔍 抓取全球市場數據中 (預計 30-60 秒)...")
            results = analyzer.run('tw-share')
            
            # 嚴格解包並強制格式轉換
            if results and len(results) >= 3:
                images = results[0]
                df_res = results[1]
                
                # 強制轉換 text_reports 為字典，防止 tuple 導致的 .get() 報錯
                raw_reports = results[2]
                if isinstance(raw_reports, dict):
                    text_reports = raw_reports
                elif isinstance(raw_reports, (list, tuple)):
                    # 如果回傳是列表或元組，將其轉換為帶有索引的字典或提取第一個元素
                    text_reports = {"FINAL_AI_REPORT": str(raw_reports[0]) if len(raw_reports) > 0 else ""}
                else:
                    text_reports = {"FINAL_AI_REPORT": str(raw_reports)}
                
                status.update(label="✅ 實時數據分析完成！", state="complete")
            else:
                st.warning("⚠️ 分析結果格式不完整，嘗試讀取存檔...")
    except Exception as e:
        st.error(f"❌ 引擎異常: {str(e)}")
    
    # 備援邏輯：如果分析失敗但有舊檔案
    if df_res is None and data_file.exists():
        df_res = pd.read_csv(data_file)
        if not text_reports:
            text_reports = {"FINAL_AI_REPORT": "目前顯示最近一次成功的盤後備份數據。"}
            
    return images, df_res, text_reports

# 5. 執行分析
images, df_res, text_reports = run_analysis_engine()

# 6. 戰略判讀呈現
if df_res is not None or text_reports:
    col1, col2 = st.columns([6, 4])
    
    with col1:
        st.subheader("🤖 Predator 智能戰略判讀")
        # 安全獲取報告內容，確保 text_reports 是字典
        ai_report = text_reports.get("FINAL_AI_REPORT", "分析引擎正在計算中...") if isinstance(text_reports, dict) else str(text_reports)
        st.info(ai_report)
        
        # 數據介入快照
        st.code(f"【Predator 數據介入】\n時間：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n系統已校準至最新盤後數據", language="markdown")
        
        if images and len(images) > 0:
            img_path = images[0].get("path", "") if isinstance(images[0], dict) else ""
            if os.path.exists(img_path):
                st.image(img_path, use_container_width=True)

    with col2:
        st.subheader("🎯 關鍵監控標標的 (TOP 10)")
        if df_res is not None:
            df_res.to_csv(data_file, index=False, encoding='utf-8-sig')
            show_cols = [c for c in ['Symbol', 'Close', 'Return', 'Vol_Ratio'] if c in df_res.columns]
            st.dataframe(df_res[show_cols].head(10).style.background_gradient(cmap='RdYlGn'), height=500)
else:
    st.error("🚨 無法載入數據，請確認 analyzer.py 是否正確回傳數據列表。")
