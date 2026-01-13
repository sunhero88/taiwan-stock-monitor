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
update_time = datetime.datetime.now().strftime('%H:%M:%S')
st.sidebar.success(f"📡 核心已就緒\n系統時間: {update_time}")

# 3. 主標題
st.title("🦅 宇宙第一股市智能分析系統 V14.0")
st.markdown("**自動修復驅動版** | 核心邏輯：Predator V14.0")

# 4. 核心數據引擎
def run_analysis_engine():
    images, df_res, text_reports = None, None, {}
    try:
        with st.status("正在執行 Predator 深度分析...", expanded=True) as status:
            st.write("📡 啟動分析模組...")
            import analyzer
            
            st.write("🔍 抓取全球市場數據中 (預計 30-60 秒)...")
            results = analyzer.run('tw-share')
            
            # 嚴格校對回傳結果
            if results and isinstance(results, (list, tuple)) and len(results) >= 3:
                images = results[0]
                df_res = results[1]
                text_reports = results[2] if results[2] else {}
                status.update(label="✅ 實時數據分析完成！", state="complete")
            else:
                st.warning("⚠️ 實時抓取未回傳有效數據，嘗試加載歷史存檔...")
                if data_file.exists():
                    df_res = pd.read_csv(data_file)
                    text_reports = {"FINAL_AI_REPORT": "實時連接不穩定，目前顯示最近一次盤後備份數據。"}
                    status.update(label="⚠️ 使用備份數據呈現", state="complete")
    except Exception as e:
        st.error(f"❌ 引擎運作異常: {str(e)}")
        if data_file.exists():
            df_res = pd.read_csv(data_file)
            text_reports = {"FINAL_AI_REPORT": "引擎初始化失敗，已自動切換至歷史數據模式。"}
    
    return images, df_res, text_reports

# 5. 執行分析
images, df_res, text_reports = run_analysis_engine()

# 6. 戰略判讀呈現
if (text_reports and len(text_reports) > 0) or df_res is not None:
    col1, col2 = st.columns([6, 4])
    
    with col1:
        st.subheader("🤖 Predator 智能戰略判讀")
        # 確保 ai_report 永遠有文字內容
        ai_report = text_reports.get("FINAL_AI_REPORT", "分析引擎正在解析籌碼與位階數據...")
        st.info(ai_report)
        
        # 準備給 AI 的數據快照
        copy_text = f"""【Predator 數據介入】
時間：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
狀態：{ai_report[:100]}..."""
        st.code(copy_text, language="markdown")
        
        if images and len(images) > 0:
            img_path = images[0].get("path", "")
            if os.path.exists(img_path):
                st.image(img_path, use_container_width=True)

    with col2:
        st.subheader("🎯 關鍵監控標的 (TOP 10)")
        if df_res is not None:
            # 儲存本次成功結果
            df_res.to_csv(data_file, index=False, encoding='utf-8-sig')
            
            # 顯示表格
            cols = [c for c in ['Symbol', 'Close', 'Return', 'Vol_Ratio'] if c in df_res.columns]
            if cols:
                st.dataframe(
                    df_res[cols].head(10).style.format({
                        'Return': '{:+.2f}%', 'Vol_Ratio': '{:.2f}x'
                    } if 'Return' in df_res.columns else {}).background_gradient(cmap='RdYlGn'),
                    height=500
                )
else:
    st.error("🚨 嚴重錯誤：無法獲取實時數據或歷史備份，請檢查 analyzer.py 完整性。")
