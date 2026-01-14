# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from pathlib import Path

def run(market_id="tw-share"):
    """
    V14.0 Predator 戰略核心分析引擎
    微調重點：加入 20MA 乖離率 (位階判斷) 與 K線實體百分比 (力道識別)
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
        
        # 3. 【技術位階】20MA 乖離率 (MA_Bias)
        # 用於判斷個股是否過熱。乖離過大標註為風險，靠近均線則為安全起漲區。
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        
        # 4. 【技術力道】K線實體百分比 (Body_Power)
        # 區分「真突破」與「留長上影線的誘多」。實體佔比高代表買盤強勁且收高。
        df['K_High_Low'] = df['High'] - df['Low']
        df['K_Real_Body'] = abs(df['Close'] - df['Open'])
        df['Body_Power'] = df.apply(
            lambda r: (r['K_Real_Body'] / r['K_High_Low'] * 100) if r['K_High_Low'] > 0 else 0, axis=1
        )

        # 5. V14.0 Predator 多重戰略標籤邏輯
        def get_predator_tag(row):
            tags = []
            
            # --- 量能核心判定 ---
            if row['Vol_Ratio'] >= 1.5 and row['Return'] > 1.5:
                tags.append("🔥主力進攻")
            
            # --- 位階判定 (乖離率) ---
            if row['MA_Bias'] < 3 and row['Return'] > 0:
                tags.append("🛡️低位起漲")
            elif row['MA_Bias'] > 15:
                tags.append("⚠️乖離過大")
                
            # --- 攻擊品質判定 (K線實體) ---
            if row['Body_Power'] > 75 and row['Return'] > 2:
                tags.append("⚡真突破")
            elif row['Body_Power'] < 35 and row['Vol_Ratio'] > 2:
                tags.append("❌假突破/壓力")
                
            return " ".join(tags) if tags else "○ 觀察"

        df['Predator_Tag'] = df.apply(get_predator_tag, axis=1)

        # 6. 整理 Top 10 監控標的
        top_picks = df[df['Vol_Ratio'] > 1.2].sort_values('Return', ascending=False).head(10)
        
        # 7. 格式化輸出報告文字
        report_text = {
            "FINAL_AI_REPORT": f"系統偵測到 {len(top_picks[top_picks['Return'] > 0])} 檔攻擊標的。匯率波動目前控制中。",
            "📊 今日個股績效榜": top_picks[['Symbol', 'Close', 'Return', 'Vol_Ratio', 'Predator_Tag']].to_string(index=False),
            "💡 戰略分析指令": "建議鎖定帶有 [🛡️低位起漲] 與 [⚡真突破] 雙重標籤之個股，避開 [⚠️乖離過大] 者。"
        }

        return [], df, report_text

    except Exception as e:
        return None, None, {"Error": f"分析引擎異常: {str(e)}"}
