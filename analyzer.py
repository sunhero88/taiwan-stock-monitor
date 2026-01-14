# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from pathlib import Path

def run(market_id="tw-share"):
    """
    V14.0 Predator 核心分析引擎
    整合指標：量增比、20MA位階、K線實體力道
    """
    try:
        # 1. 讀取數據 (假設 downloader 產出名為 raw_data_{market}.csv)
        data_path = Path(f"raw_data_{market_id}.csv")
        if not data_path.exists():
            return None, None, {"Error": "找不到原始數據檔案，請先執行下載器。"}

        df = pd.read_csv(data_path)
        
        # 確保欄位名稱正確 (Open, High, Low, Close, Volume)
        # 計算技術指標
        
        # A. 計算報酬率與量能比
        df['Return'] = df['Close'].pct_change() * 100
        df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
        df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20']
        
        # B. 【新增】計算 20MA 位階 (乖離率)
        # 判斷股價相對於月線的位置
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        
        # C. 【新增】計算 K 線實體力道 (K-Body Ratio)
        # 判斷是真突破還是留長上影線的出貨
        df['K_High_Low'] = df['High'] - df['Low']
        df['K_Real_Body'] = abs(df['Close'] - df['Open'])
        # 避免除以零
        df['Body_Power'] = (df['K_Real_Body'] / df['K_High_Low']).replace([np.inf, -np.inf], 0).fillna(0) * 100

        # 2. 定義 Predator 標籤邏輯
        def get_predator_tag(row):
            tags = []
            # 量能核心：量增 1.5 倍且上漲
            if row['Vol_Ratio'] >= 1.5 and row['Return'] > 1.5:
                tags.append("🔥主力進攻")
            
            # 位階判斷
            if row['MA_Bias'] < 3 and row['Return'] > 0:
                tags.append("🛡️低位起漲")
            elif row['MA_Bias'] > 15:
                tags.append("⚠️高檔過熱")
                
            # 力道判斷
            if row['Body_Power'] > 70 and row['Return'] > 2:
                tags.append("⚡真突破")
            elif row['Body_Power'] < 30 and row['Vol_Ratio'] > 2:
                tags.append("❌假突破/壓力")
                
            return " ".join(tags) if tags else "○ 盤整"

        df['Predator_Tag'] = df.apply(get_predator_tag, axis=1)

        # 3. 篩選今日最新數據 (最後一列)
        latest_df = df.iloc[-50:].copy() # 取最近 50 筆做展示
        
        # 4. 產出文字報告 (餵入 Gem 用)
        top_picks = df[df['Vol_Ratio'] > 1.5].sort_values('Return', ascending=False).head(5)
        
        report_text = {
            "FINAL_AI_REPORT": f"偵測到 {len(top_picks)} 檔具備主力介入跡象。",
            "📊 今日個股績效榜": top_picks[['Symbol', 'Close', 'Return', 'Predator_Tag']].to_string(index=False),
            "💡 戰略建議": "優先關注 [🛡️低位起漲] 且 [🔥主力進攻] 雙標籤個股。"
        }

        # 5. 返回給 Streamlit (images 目前設為空，可自行加入 matplotlib 繪圖)
        return [], df, report_text

    except Exception as e:
        return None, None, {"Error": f"分析引擎中斷: {str(e)}"}
