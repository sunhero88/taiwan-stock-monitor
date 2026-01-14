# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path

def run(market_id):
    """
    V14.0 Predator 核心分析模組 (防護強化版)
    針對新環境自動補完後的空值問題進行徹底修正
    """
    try:
        # 1. 檔案路徑定位
        data_file = Path(f"raw_data_{market_id}.csv")
        if not data_file.exists():
            return None, None, {"Error": "數據檔案尚未就緒，請稍後再試。"}
            
        # 2. 讀取並清洗數據
        df = pd.read_csv(data_file)
        if df.empty or len(df) < 5:
            return None, None, {"Error": "初始數據量過低，無法建立有效判讀位階。"}

        # 基礎報酬率計算
        df['Return'] = df['Close'].pct_change() * 100
        
        # --- 🛡️ 智能降級邏輯：處理數據長度不足 ---
        if len(df) < 20:
            df['Predator_Tag'] = "🛡️ 數據採集中"
            latest = df.tail(10)
            return [], df, {
                "📊 今日個股績效榜": latest[['Symbol', 'Close', 'Return']].to_string(index=False),
                "FINAL_AI_REPORT": "⚠️ 偵測到新環境部署，數據累積中(目前 < 20日)，暫不提供乖離率判讀。"
            }

        # 3. 【V14.0 核心技術指標】計算
        # A. 均線位階：20MA 乖離率 (判斷是否追高)
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA_Bias'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
        
        # B. 攻擊品質：K線實體佔比 (判斷主力真誠度)
        df['K_Range'] = df['High'] - df['Low']
        df['K_Body'] = abs(df['Close'] - df['Open'])
        df['Body_Power'] = (df['K_Body'] / df['K_Range'].replace(0, np.inf)) * 100
        
        # C. 量能增幅 (相較於20日平均)
        df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
        df['Vol_Ratio'] = df['Volume'] / df['Vol_MA20'].replace(0, np.inf)

        # 4. 【Predator 智能標籤】判讀邏輯
        def get_tag(row):
            # 判斷是否為無成交量時段 (盤前)
            if row['Volume'] == 0: return "🕒 待開盤"
            
            tags = []
            # 攻擊標籤：量大且漲幅明確
            if row['Vol_Ratio'] > 1.5 and row['Return'] > 1.8:
                tags.append("🔥主力進攻")
            
            # 位階標籤：乖離率在安全區且收紅
            if -1 < row['MA_Bias'] < 4 and row['Return'] > 0:
                tags.append("🛡️低位起漲")
            elif row['MA_Bias'] > 15:
                tags.append("⚠️乖離過大")
            
            # 品質標籤：收盤價接近最高點
            if row['Body_Power'] > 80 and row['Return'] > 2:
                tags.append("⚡真突破")
                
            return " ".join(tags) if tags else "○ 盤整"

        df['Predator_Tag'] = df.apply(get_tag, axis=1)
        df = df.fillna(0) # 清除所有 NaN 避免網頁空白

        # 5. 輸出給 Gem 的數據介入格式
        top_active = df.sort_values('Vol_Ratio', ascending=False).head(12)
        report_text = {
            "📊 今日個股績效榜": top_active[['Symbol', 'Close', 'Return', 'Predator_Tag']].to_string(index=False),
            "FINAL_AI_REPORT": "匯率環境穩定，當前策略重點：優先鎖定[🛡️低位起漲]標的之右側轉強機會。"
        }
        
        return [], df, report_text

    except Exception as e:
        return None, None, {"Error": f"分析引擎異常: {str(e)}"}
