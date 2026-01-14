# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from pathlib import Path

def run(market_id="tw-share"):
    """
    V14.0 Predator 戰略核心分析引擎
    微調重點：加入 20MA 乖離率 (位階) 與 K線實體佔比 (力道)
    """
    try:
        # 1. 讀取數據
        data_path = Path(f"raw_data_{market_id}.csv")
        if not data_path.exists():
            return None, None, {"Error": "找不到原始數據檔案，請先確認下載器已執行。"}

        df = pd.read_csv(data_path)
        
        # 2. 計算基礎指標 (報酬率與量比)
        df['Return'] = df['Close'].pct_change() * 100
        df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
        df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20']
        
        # 3. 【新增】技術位階指標：20MA 乖離率 (Bias)
        # 用來判斷當前價格是否偏離成本太遠，預防追高風險
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        
        # 4. 【新增】技術力道指標：K線實體百分比 (Body Power)
        # 用來過濾「留長上影線」的假突破，確保主力是「收高」真進攻
        df['K_High_Low'] = df['High'] - df['Low']
        df['K_Real_Body'] = abs(df['Close'] - df['Open'])
        # 處理平盤或無波動情況以避免除以零
        df['Body_Power'] = df.apply(
            lambda r: (r['K_Real_Body'] / r['K_High_Low'] * 100) if r['K_High_Low'] > 0 else 0, axis=1
        )

        # 5. 定義 Predator 多重標籤邏輯
        def get_predator_tag(row):
            tags = []
            
            # --- 量能與報酬 (核心) ---
            if row['Vol_Ratio'] >= 1.5 and row['Return'] > 1.5:
                tags.append("🔥主力進攻")
            
            # --- 位階判斷 (均線乖離) ---
            if row['MA_Bias'] < 3 and row['Return'] > 0:
                tags.append("🛡️低位起漲")
            elif row['MA_Bias'] > 15:
                tags.append("⚠️乖離過大")
                
            # --- 攻擊品質 (K線實體) ---
            if row['Body_Power'] > 75 and row['Return'] > 2:
                tags.append("⚡真突破")
            elif row['Body_Power'] < 35 and row['Vol_Ratio'] > 2:
                tags.append("❌假突破/壓力")
                
            return " ".join(tags) if tags else "○ 觀察"

        df['Predator_Tag'] = df.apply(get_predator_tag, axis=1)

        # 6. 整理回傳結果 (取 Top 10)
        top_picks = df[df['Vol_Ratio'] > 1.2].sort_values('Return', ascending=False).head(10)
        
        # 7. 格式化報告文字 (供 Streamlit 顯示與 Gem 複製)
        report_text = {
            "FINAL_AI_REPORT": f"偵測到 {len(top_picks[top_picks['Return'] > 0])} 檔具備攻擊特徵，需注意匯率對權值股的壓抑。",
            "📊 今日個股績效榜": top_picks[['Symbol', 'Close', 'Return', 'Vol_Ratio', 'Predator_Tag']].to_string(index=False),
            "💡 戰略分析建議": "優先選擇帶有 [🛡️低位起漲] 與 [⚡真突破] 雙標籤標的。"
        }

        return [], df, report_text

    except Exception as e:
        return None, None, {"Error": f"分析過程發生錯誤: {str(e)}"}
