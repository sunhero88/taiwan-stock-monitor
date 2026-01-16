# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from pathlib import Path
from datetime import datetime

def run(market_id="tw-share"):
    """
    V15.0 Predator 戰略核心分析引擎
    整合：量能分級、ERS 加權排序、紅綠燈權限、Kill Switch 否決邏輯
    """
    try:
        # 1. 讀取數據
        data_path = Path(f"raw_data_{market_id}.csv")
        if not data_path.exists():
            return None, None, {"Error": "缺失 raw_data，請先執行下載器。"}

        df = pd.read_csv(data_path)
        
        # --- 數據預處理與權值股定義 ---
        # 計算成交額 (Amount) 用於動態定義權值股
        df['Amount'] = df['Close'] * df['Volume']
        # 取成交額前 50 名定義為權值股 (Heavyweight)
        top_50_threshold = df['Amount'].nlargest(50).min()
        df['Is_Heavyweight'] = df['Amount'] >= top_50_threshold

        # --- 第一層：量能門檻 (Volume Core) ---
        df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
        df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20']
        
        # 判定量能是否達標 (權值 1.2 / 中小 1.8)
        df['Vol_Qualified'] = df.apply(
            lambda r: r['Vol_Ratio'] >= 1.2 if r['Is_Heavyweight'] else r['Vol_Ratio'] >= 1.8, axis=1
        )

        # --- 第二層：排序指標 (Effective Return Score, ERS) ---
        df['Return'] = df['Close'].pct_change() * 100
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        
        # MA_Bias 懲罰函數 (10%-15% 線性扣分, >15% 加重扣分)
        def get_penalty(bias):
            if bias <= 10: return 0
            elif 10 < bias <= 15: return (bias - 10) / 5  # 線性 0~1
            else: return 1 + (bias - 15) * 0.2           # 加重扣分
        
        df['Penalty'] = df['MA_Bias'].apply(get_penalty)
        # ERS 公式：漲幅 * 量比 * (1 - 0.5 * 懲罰)
        df['ERS'] = df['Return'] * df['Vol_Ratio'] * (1 - 0.5 * df['Penalty'])

        # --- 第三層：指標判讀與標籤 (紅綠燈系統) ---
        # 計算 Body_Power (實體力道)
        df['K_High_Low'] = df['High'] - df['Low']
        df['K_Real_Body'] = abs(df['Close'] - df['Open'])
        df['Body_Power'] = df.apply(lambda r: (r['K_Real_Body'] / r['K_High_Low'] * 100) if r['K_High_Low'] > 0 else 0, axis=1)

        def get_tags(row):
            tags = []
            # 🟢 綠燈 (交易資格)：MA_Bias 在安全起漲區
            limit = 8 if row['Is_Heavyweight'] else 12
            if 0 < row['MA_Bias'] <= limit:
                tags.append("🟢起漲")
            
            # 🟡 黃燈 (動力確認)：量能達標 + 法人同步 (Net_Raw > 0)
            if row['Vol_Qualified'] and row.get('Net_Raw', 0) > 0:
                tags.append("🟡主力")
            
            # 🟣 紫燈 (攻擊加分)：強實體
            if row['Body_Power'] >= 75:
                tags.append("🟣突破")
                
            return " ".join(tags) if tags else "○觀察"

        df['Predator_Tag'] = df.apply(get_tags, axis=1)

        # --- 第四層：Kill Switch (結構否決) ---
        def apply_kill_switch(row):
            # 1. 派貨陷阱：Body_Power 極低且爆量 (Vol_Ratio > 2.5)
            if row['Body_Power'] < 20 and row['Vol_Ratio'] > 2.5:
                return True
            # 2. 結構惡化：QoQ < 0 (需有基本面資料)
            if row.get('QoQ', 0) < 0:
                return True
            # 3. 乖離極端：MA_Bias > 20
            if row['MA_Bias'] > 20:
                return True
            return False

        df['Is_Killed'] = df.apply(apply_kill_switch, axis=1)

        # --- 最終篩選與輸出 ---
        # 1. 必須量能達標 2. 未被 Kill Switch 否決
        final_candidates = df[(df['Vol_Qualified']) & (~df['Is_Killed'])].copy()
        
        # 依 ERS 評分排序取 Top 10
        top_10 = final_candidates.sort_values('ERS', ascending=False).head(10)
        
        # 判定 Session 狀態
        is_eod = datetime.now().hour >= 14
        session_tag = "【確認｜量能成立】" if is_eod else "【觀望｜量能預估】"

        report_text = {
            "FINAL_AI_REPORT": f"V15.0 系統掃描完畢。當前狀態：{session_tag}",
            "📊 10 關鍵監控標的 (ERS 排序)": top_10[['Symbol', 'Close', 'Return', 'ERS', 'Predator_Tag']].to_string(index=False),
            "🛡️ 戰略提醒": "嚴禁操作無「🟢起漲」標籤之個股。排除之標的已進入 Kill Switch 名單。"
        }

        return [], df, report_text

    except Exception as e:
        return None, None, {"Error": f"V15.0 引擎中斷: {str(e)}"}
