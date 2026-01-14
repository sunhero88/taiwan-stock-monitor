# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path

def run(market_id):
    """
    V14.0 Predator 核心分析模組
    功能：計算 MA20 乖離率、K線力道、並產出戰略標籤
    """
    try:
        # 1. 檔案定位
        data_file = Path(f"raw_data_{market_id}.csv")
        if not data_file.exists():
            return None, None, {"Error": f"找不到數據檔案: {data_file}，請點擊按鈕自動修復。"}
            
        # 2. 讀取數據
        df = pd.read_csv(data_file)
        if df.empty:
            return None, None, {"Error": "數據檔案為空，請重新執行下載器。"}

        # 3. 基礎指標計算 (確保排序正確)
        df = df.sort_values('Date' if 'Date' in df.columns else df.columns[0])
        df['Return'] = df['Close'].pct_change() * 100
        
        # --- 🛡️ 防錯：數據長度檢查 ---
        # 如果數據不足 20 筆，無法計算 MA20，將以基礎數據回傳
        if len(df) < 20:
            df['MA_Bias'] = 0
            df['Body_Power'] = 0
            df['Vol_Ratio'] = 1
            df['Predator_Tag'] = "🛡️ 數據累積中"
            
            latest_data = df.tail(10)
            report_text = {
                "📊 今日個股績效榜": latest_data[['Symbol', 'Close', 'Return']].to_string(index=False),
                "FINAL_AI_REPORT": "⚠️ 數據量不足(少於20日)，技術指標計算受限，僅提供基礎漲跌幅。"
            }
            return [], df, report_text

        # 4. 【核心技術指標】計算
        # A. 均線位階 (MA20 乖離率)
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        
        # B. K線實體力道 (Body Power)
        # 計算公式: |收盤-開盤| / (最高-最低)
        df['K_High_Low'] = df['High'] - df['Low']
        df['K_Real_Body'] = abs(df['Close'] - df['Open'])
        df['Body_Power'] = (df['K_Real_Body'] / df['K_High_Low'].replace(0, np.inf)) * 100
        
        # C. 量能比 (Vol_Ratio)
        # 處理無成交量(盤前)的狀況
        latest_vol = df['Volume'].iloc[-1]
        if latest_vol == 0 or pd.isna(latest_vol):
            df['Vol_Ratio'] = 0
            is_pre = True
        else:
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20'].replace(0, np.inf)
            is_pre = False

        # 5. 【Predator 戰略標籤】邏輯
        def get_tag(row):
            if is_pre: return "🕒 等待開盤"
            
            tags = []
            # 攻擊標籤
            if row['Vol_Ratio'] > 1.5 and row['Return'] > 1.5:
                tags.append("🔥主力進攻")
            # 位階標籤
            if -2 < row['MA_Bias'] < 3 and row['Return'] > 0:
                tags.append("🛡️低位起漲")
            elif row['MA_Bias'] > 15:
                tags.append("⚠️乖離過熱")
            # 力道標籤
            if row['Body_Power'] > 75 and row['Return'] > 2:
                tags.append("⚡真突破")
            elif row['Body_Power'] < 30 and row['Vol_Ratio'] > 2:
                tags.append("❌壓制/出貨")
                
            return " ".join(tags) if tags else "○ 觀察"

        df['Predator_Tag'] = df.apply(get_tag, axis=1)
        
        # 6. 整理報告文字 (給 Gem 數據介入用)
        # 優先篩選出今日有標籤的強勢股
        target_df = df.sort_values('Vol_Ratio', ascending=False).head(15)
        
        report_text = {
            "📊 今日個股績效榜": target_df[['Symbol', 'Close', 'Return', 'Predator_Tag']].to_string(index=False),
            "FINAL_AI_REPORT": f"V14.0 系統判讀：匯率對應{'盤前' if is_pre else '實戰'}模式。重點鎖定 [🛡️低位起漲] 之右側交易機會。"
        }
        
        # 處理數值空值 (避免網頁顯示 NaN)
        df = df.fillna(0)
        
        return [], df, report_text

    except Exception as e:
        import traceback
        error_msg = f"分析引擎崩潰: {str(e)}\n{traceback.format_exc()}"
        return None, None, {"Error": error_msg}
