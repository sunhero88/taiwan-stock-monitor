# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def run(market_id):
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path):
        print(f"❌ 找不到台股數據: {data_path}")
        return [], None, {}

    try:
        df = pd.read_csv(data_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(['Symbol', 'Date'])
        
        all_dates = sorted(df['Date'].unique())
        latest_date = all_dates[-1]
        prev_date = all_dates[-2]
        
        # 今日與昨日數據對比
        today_df = df[df['Date'] == latest_date].copy()
        prev_df = df[df['Date'] == prev_date][['Symbol', 'Close']].rename(columns={'Close': 'P_Close'})
        today_df = pd.merge(today_df, prev_df, on='Symbol', how='left')
        
        # 計算報酬率
        today_df['Return'] = (today_df['Close'] - today_df['P_Close']) / today_df['P_Close'] * 100
        
        # 計算 20 日均量 (MA20_Vol) 用於籌碼判斷
        df['Vol_MA20'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(20).mean())
        latest_vol_ma = df[df['Date'] == latest_date][['Symbol', 'Vol_MA20']]
        today_df = pd.merge(today_df, latest_vol_ma, on='Symbol', how='left')

        text_reports = {}

        # --- A. 全球背景與台幣匯率 ---
        global_msg = "🌍 【全球市場連動監控】\n"
        summary_file = "global_market_summary.csv"
        if os.path.exists(summary_file):
            g_df = pd.read_csv(summary_file)
            for _, row in g_df.iterrows():
                if row['Symbol'] == "USD_TWD":
                    status = "🔴 貶值" if row['Change'] > 0 else "🟢 升值"
                    global_msg += f"💱 台幣匯率: {row['Value']} ({status} {row['Change']:+.2f}%)\n"
                else:
                    icon = "🟢" if row['Change'] > 0 else "🔴"
                    global_msg += f"{icon} {row['Symbol']}: {row['Change']:+.2f}%\n"
        text_reports["00_全球市場背景"] = global_msg

        # --- B. 強勢股榜單 + 主力籌碼偵測 ---
        top_gainers = today_df.sort_values('Return', ascending=False).head(5)
        perf_msg = "🚀 今日最強勢 (Top 5):\n"
        for _, row in top_gainers.iterrows():
            # 籌碼判斷邏輯：漲幅 > 1.5% 且 成交量 > 均量 1.5 倍
            is_main_force = row['Return'] > 1.5 and row['Volume'] > (row['Vol_MA20'] * 1.5)
            chip_tag = " 🔥[主力進攻]" if is_main_force else ""
            perf_msg += f"- {row['Symbol']}: {row['Return']:+.2f}%{chip_tag}\n"
        text_reports["📊 今日個股績效榜"] = perf_msg

        # --- C. 爆量追蹤 ---
        today_df['Vol_Ratio'] = today_df['Volume'] / today_df['Vol_MA20']
        spikes = today_df[today_df['Vol_Ratio'] > 2.0].sort_values('Vol_Ratio', ascending=False)
        spike_msg = f"📍 【台股爆量偵測】\n"
        if not spikes.empty:
            for _, row in spikes.head(5).iterrows():
                spike_msg += f"- {row['Symbol']}: {row['Vol_Ratio']:.1f}倍 (收:{row['Close']})\n"
        else:
            spike_msg += "今日無明顯爆量個股。\n"
        text_reports["🔥 成交量爆量追蹤"] = spike_msg

        # --- D. 生成圖表 ---
        image_paths = []
        plt.figure(figsize=(10, 5))
        plt.hist(today_df['Return'].dropna(), bins=15, color='gray', alpha=0.7, edgecolor='white')
        plt.axvline(0, color='red', linestyle='--', linewidth=1)
        plt.title(f"Market Sentiment - {latest_date.strftime('%Y-%m-%d')}")
        img_name = f"dist_{market_id}.png"
        plt.savefig(img_name)
        plt.close()
        image_paths.append({"id": "dist_chart", "label": "市場漲跌分佈圖", "path": img_name})

        return image_paths, today_df, text_reports

    except Exception as e:
        print(f"❌ 分析發生異常: {e}")
        return [], None, {"錯誤": str(e)}
