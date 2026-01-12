# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime

def run(market_id):
    # 讀取數據 (假設 downloader 已產出 data.csv)
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path):
        return [], None, {}

    df = pd.read_csv(data_path)
    df['Date'] = pd.to_datetime(df['Date'])
    
    # 計算報酬率的邏輯
    # (此處簡化示意，實務上會根據您的數據結構做多週期計算)
    results = []
    text_reports = {}
    image_paths = []

    # 範例：計算週、月、年回報分布
    periods = {"週": 5, "月": 20, "年": 240}
    
    for label, days in periods.items():
        # 假設 df 已包含各股價，計算報酬
        # 此處會產出您在郵件中看到的 "報酬分布明細"
        report_text = f"--- {label} 報酬率分布 ---\n"
        report_text += "報酬區間 > 10%: 15 檔\n"
        report_text += "報酬區間 0~10%: 40 檔\n"
        # ... 這裡會放入您的計算邏輯 ...
        text_reports[f"{label}K線"] = report_text

        # 生成圖表
        plt.figure(figsize=(10, 6))
        # 模擬分佈圖
        plt.hist(np.random.normal(0, 1, 100), bins=20, color='skyblue', edgecolor='black')
        plt.title(f"{market_id} {label} Distribution")
        img_name = f"dist_{label}.png"
        plt.savefig(img_name)
        plt.close()
        image_paths.append({"id": f"img_{label}", "label": f"{label} 分佈圖", "path": img_name})

    return image_paths, df, text_reports
