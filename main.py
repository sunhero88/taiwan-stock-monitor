import streamlit as st
import pandas as pd
import sys, subprocess, datetime
from pathlib import Path
import analyzer

BASE_DIR = Path(__file__).parent.absolute()

def auto_fix_data(market_id):
    csv = f"raw_data_{market_id}.csv"
    dl = f"downloader_{market_id.split('-')[0]}.py"
    if not (BASE_DIR / csv).exists():
        st.warning(f"偵測到新電腦環境，自動補完數據中...")
        subprocess.run([sys.executable, dl], check=True)
    return True

if __name__ == "__main__":
    if "--cli" in sys.argv:
        # 雲端模式
        _, df_res, text_reports = analyzer.run("tw-share")
        # 此處呼叫 notifier 邏輯...
    else:
        # 本地網頁模式
        st.title("🦅 Predator 指揮中心 V14.0")
        m = st.sidebar.selectbox("市場", ["tw-share", "us"])
        if st.button("啟動數據介入"):
            if auto_fix_data(m):
                _, df, reports = analyzer.run(m)
                st.code(reports.get("📊 今日個股績效榜", ""))
                st.dataframe(df.tail(20))
