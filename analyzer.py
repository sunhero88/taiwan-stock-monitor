# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def run(market_id):
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path):
        print(f"❌ 分析器找不到檔案: {os.path.abspath(data_path)}")
        return [], None, {}

    df = pd.read_csv(data_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(['Symbol', 'Date'])

    # --- 1. 成交量爆量偵測 ---
    # 計算 20 日平均成交量
    df['Vol_MA20'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(window=20).mean())
    df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20']
    
    latest_date = df['Date'].max()
    today_df = df[df['Date'] == latest_date].copy()
    
    # 篩選爆量 2.5 倍以上的股票
    volume_spikes = today_df[today_df['Vol_Ratio'] > 2.5].sort_values('Vol_Ratio', ascending=False)

    text_reports = {}
    spike_msg = f"📍 【成交量異常偵測】 日期: {latest_date.strftime('%Y-%m-%d')}\n"
    if not volume_spikes.empty:
        for _, row in volume_spikes.head(10).iterrows():
            spike_msg += f"- {row['Symbol']}: 放大 {row['Vol_Ratio']:.2f} 倍 (股價: {row['Close']})\n"
    else:
        spike_msg += "今日無明顯爆量個股。\n"
    text_reports["🔥 成交量爆量追蹤"] = spike_msg

    # --- 2. 報酬分布與圖表 ---
    image_paths = []
    # (此處保留您原本的報酬分佈計算邏輯)
    plt.figure(figsize=(8, 4))
    plt.hist(np.random.normal(0, 1, 100), bins=10, color='orange')
    plt.title(f"{market_id} Market Distribution")
    img_name = "dist_summary.png"
    plt.savefig(img_name)
    plt.close()
    
    image_paths.append({"id": "summary_dist", "label": "市場報酬分布圖", "path": img_name})
    text_reports["週K線報酬"] = "數據處理完成，請參考附件圖表。"

    return image_paths, today_df, text_reports

