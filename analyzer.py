# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def run(market_id):
    # 1. 讀取數據
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path):
        print(f"❌ 分析器找不到檔案: {os.path.abspath(data_path)}")
        return [], None, {}

    try:
        df = pd.read_csv(data_path)
        df['Date'] = pd.to_datetime(df['Date'])
        # 確保按代碼和日期排序，這對計算報酬至關重要
        df = df.sort_values(['Symbol', 'Date'])
        
        # 取得最新交易日與前一交易日
        all_dates = sorted(df['Date'].unique())
        if len(all_dates) < 2:
            return [], df, {"錯誤": "數據天數不足，無法計算報酬。"}
        
        latest_date = all_dates[-1]
        prev_date = all_dates[-2]
        
        # 2. 計算今日報酬率 (%)
        today_df = df[df['Date'] == latest_date].copy()
        prev_df = df[df['Date'] == prev_date][['Symbol', 'Close']].rename(columns={'Close': 'Prev_Close'})
        today_df = pd.merge(today_df, prev_df, on='Symbol', how='left')
        today_df['Daily_Return'] = (today_df['Close'] - today_df['Prev_Close']) / today_df['Prev_Close'] * 100

        # 3. 成交量爆量偵測 (Volume Spike)
        # 計算 20 日平均成交量
        df['Vol_MA20'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(window=20).mean())
        # 合併今日的成交量與均量
        vol_data = df[df['Date'] == latest_date][['Symbol', 'Volume', 'Vol_MA20']]
        today_df = pd.merge(today_df, vol_data, on='Symbol', how='left', suffixes=('', '_latest'))
        today_df['Vol_Ratio'] = today_df['Volume'] / today_df['Vol_MA20']

        text_reports = {}

        # --- A. 爆量追蹤報告 ---
        spikes = today_df[today_df['Vol_Ratio'] > 2.0].sort_values('Vol_Ratio', ascending=False)
        spike_msg = f"📍 【成交量異常偵測】 日期: {latest_date.strftime('%Y-%m-%d')}\n"
        if not spikes.empty:
            for _, row in spikes.head(8).iterrows():
                spike_msg += f"- {row['Symbol']}: 放大 {row['Vol_Ratio']:.2f} 倍 (收盤: {row['Close']})\n"
        else:
            spike_msg += "今日無明顯爆量個股 (門檻: 2.0x)。\n"
        text_reports["🔥 成交量爆量追蹤"] = spike_msg

        # --- B. 強勢股與弱勢股明細 ---
        top_gainers = today_df.sort_values('Daily_Return', ascending=False).head(5)
        top_losers = today_df.sort_values('Daily_Return', ascending=True).head(5)
        
        perf_msg = f"🚀 今日最強勢 (Top 5):\n"
        for _, row in top_gainers.iterrows():
            perf_msg += f"- {row['Symbol']}: {row['Daily_Return']:+.2f}%\n"
        
        perf_msg += f"\n📉 今日最弱勢 (Bottom 5):\n"
        for _, row in top_losers.iterrows():
            perf_msg += f"- {row['Symbol']}: {row['Daily_Return']:+.2f}%\n"
        text_reports["📊 今日個股績效榜"] = perf_msg

        # 4. 繪製報酬分布圖表
        image_paths = []
        plt.figure(figsize=(10, 6))
        # 移除 NaN 以免繪圖出錯
        returns = today_df['Daily_Return'].dropna()
        
        # 繪製直方圖
        n, bins, patches = plt.hist(returns, bins=30, color='gray', edgecolor='white', alpha=0.7)
        # 著色：正報酬綠色(或紅,依習慣), 負報酬紅色
        for i in range(len(patches)):
            if bins[i] < 0:
                patches[i].set_facecolor('#e74c3c') # 紅色 (跌)
            else:
                patches[i].set_facecolor('#2ecc71') # 綠色 (漲)

        plt.axvline(0, color='black', linestyle='--', linewidth=1)
        plt.title(f"{market_id} Daily Return Distribution ({latest_date.strftime('%Y-%m-%d')})")
        plt.xlabel("Return %")
        plt.ylabel("Number of Stocks")
        
        img_name = f"return_dist_{market_id}.png"
        plt.savefig(img_name, bbox_inches='tight')
        plt.close()
        
        image_paths.append({
            "id": "return_dist", 
            "label": "市場漲跌幅分布圖", 
            "path": img_name
        })

        return image_paths, today_df, text_reports

    except Exception as e:
        print(f"❌ 數據分析發生異常: {e}")
        return [], None, {"錯誤": str(e)}
