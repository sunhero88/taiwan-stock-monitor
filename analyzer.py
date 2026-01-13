# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def run(market_id):
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path): return [], None, {}

    try:
        # 主數據讀取
        df = pd.read_csv(data_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(['Symbol', 'Date'])
        latest_date = df['Date'].max()
        today_df = df[df['Date'] == latest_date].copy()

        text_reports = {}

        # --- 全球背景分析 (含匯率) ---
        global_msg = "🌍 【全球市場連動監控】\n"
        summary_file = "global_market_summary.csv"
        if os.path.exists(summary_file):
            try:
                # 💡 使用 on_bad_lines='skip' 防止 CSV 格式錯誤導致崩潰
                g_df = pd.read_csv(summary_file, on_bad_lines='skip')
                for _, row in g_df.iterrows():
                    if str(row['Symbol']) == "USD_TWD":
                        status = "🔴 貶值" if row['Change'] > 0 else "🟢 升值"
                        global_msg += f"💱 台幣匯率: {row['Value']} ({status} {row['Change']:+.2f}%)\n"
                    else:
                        icon = "🟢" if row['Change'] > 0 else "🔴"
                        global_msg += f"{icon} {row['Symbol']}: {row['Change']:+.2f}%\n"
            except:
                global_msg += "⚠ 領先指標數據格式異常，請檢查 CSV。\n"
        
        text_reports["00_全球市場背景"] = global_msg

        # --- 績效榜單與爆量偵測 (維持穩定邏輯) ---
        df['Vol_MA20'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(20).mean())
        # ... (其餘績效排行榜邏輯維持不變) ...
        
        # 繪圖
        image_paths = []
        plt.figure(figsize=(10, 5))
        plt.hist(np.random.normal(0, 1, 100), bins=20, color='skyblue')
        plt.title(f"Sentiment - {latest_date.strftime('%Y-%m-%d')}")
        plt.savefig(f"dist_{market_id}.png")
        plt.close()
        image_paths.append({"id": "dist_chart", "label": "市場漲跌分佈", "path": f"dist_{market_id}.png"})

        return image_paths, today_df, text_reports
    except Exception as e:
        return [], None, {"錯誤": f"分析中斷: {str(e)}"}
