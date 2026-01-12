# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def run(market_id):
    # 1. 讀取數據 (假設 downloader 產出的 csv 包含：Date, Symbol, Close, Volume)
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path):
        print(f"❌ 找不到數據檔案: {data_path}")
        return [], None, {}

    df = pd.read_csv(data_path)
    df['Date'] = pd.to_datetime(df['Date'])
    
    # 確保按代碼和日期排序
    df = df.sort_values(['Symbol', 'Date'])

    # 2. 核心計算：成交量異常偵測 (Volume Spike)
    # 計算 20 日平均成交量 (MA20_Vol)
    df['Vol_MA20'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(window=20).mean())
    # 計算今日成交量是平均值的幾倍
    df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20']

    # 3. 獲取最新交易日的數據
    latest_date = df['Date'].max()
    today_df = df[df['Date'] == latest_date].copy()

    # 定義爆量標準：成交量大於 20 日平均的 2.5 倍
    volume_spikes = today_df[today_df['Vol_Ratio'] > 2.5].sort_values('Vol_Ratio', ascending=False)

    # 4. 準備文字報告
    text_reports = {}
    
    # 爆量個股文字摘要
    spike_text = f"📍 【成交量異常偵測】 基準日: {latest_date.strftime('%Y-%m-%d')}\n"
    if not volume_spikes.empty:
        spike_text += f"偵測到 {len(volume_spikes)} 檔個股成交量異常放大 (超過均量 2.5 倍)：\n"
        for _, row in volume_spikes.head(10).iterrows(): # 只取前 10 檔
            spike_text += f"- {row['Symbol']}: 放大 {row['Vol_Ratio']:.2f} 倍 (股價: {row['Close']})\n"
    else:
        spike_text += "今日無明顯爆量個股。\n"
    
    text_reports["🔥 成交量爆量追蹤"] = spike_text

    # 5. 原有的報酬分布計算 (簡化版)
    image_paths = []
    for label, days in {"週": 5, "月": 20, "年": 240}.items():
        # 計算回報率 (這裡假設您的 df 有備好計算回報的欄位)
        # 此處僅示意生成圖表
        plt.figure(figsize=(8, 5))
        plt.hist(np.random.normal(0, 1, 100), bins=15, color='orange', alpha=0.7)
        plt.title(f"{market_id} {label} Return Distribution")
        img_name = f"dist_{label}.png"
        plt.savefig(img_name)
        plt.close()
        image_paths.append({"id": f"img_{label}", "label": f"{label}報酬分佈", "path": img_name})
        
        # 模擬原本的報酬文字 (延用您之前的格式)
        text_reports[f"{label}K線明細"] = f"（此處為 {label} 報酬計算數據...）"

    return image_paths, today_df, text_reports
