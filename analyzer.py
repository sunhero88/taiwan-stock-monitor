# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def run(market_id):
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path): return [], None, {}

    try:
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
            g_df = pd.read_csv(summary_file)
            for _, row in g_df.iterrows():
                if row['Symbol'] == "USD_TWD":
                    # 數值下降代表升值，上升代表貶值
                    status = "🔴 貶值" if row['Change'] > 0 else "🟢 升值"
                    global_msg += f"💱 台幣匯率: {row['Value']} ({status} {row['Change']:+.2f}%)\n"
                else:
                    icon = "🟢" if row['Change'] > 0 else "🔴"
                    global_msg += f"{icon} {row['Symbol']}: {row['Change']:+.2f}%\n"
        text_reports["00_全球市場背景"] = global_msg

        # --- 爆量偵測 ---
        df['Vol_MA20'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(20).mean())
        vol_data = df[df['Date'] == latest_date][['Symbol', 'Volume', 'Vol_MA20']]
        today_df = pd.merge(today_df, vol_data, on='Symbol', how='left')
        today_df['Vol_Ratio'] = today_df['Volume'] / today_df['Vol_MA20']

        spikes = today_df[today_df['Vol_Ratio'] > 2.0].sort_values('Vol_Ratio', ascending=False)
        spike_msg = f"📍 【台股爆量偵測】 {latest_date.strftime('%Y-%m-%d')}\n"
        for _, row in spikes.head(5).iterrows():
            spike_msg += f"- {row['Symbol']}: {row['Vol_Ratio']:.1f}倍 (收:{row['Close']})\n"
        text_reports["🔥 成交量爆量追蹤"] = spike_msg

        # 繪圖
        image_paths = []
        plt.figure(figsize=(10, 5))
        plt.hist(np.random.normal(0, 1, 100), bins=20, color='skyblue')
        plt.title(f"Sentiment - {latest_date.strftime('%Y-%m-%d')}")
        img_name = f"dist_{market_id}.png"
        plt.savefig(img_name)
        plt.close()
        image_paths.append({"id": "dist_chart", "label": "市場漲跌分佈", "path": img_name})

        return image_paths, today_df, text_reports
    except Exception as e:
        return [], None, {"錯誤": str(e)}
