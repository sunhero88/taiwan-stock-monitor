# -*- coding: utf-8 -*-
import pandas as pd
import yfinance as yf
import datetime
import os

def run(market_type='tw-share'):
    """
    Predator V14.0 核心分析引擎
    回傳格式: (images, df_top_stocks, text_reports)
    """
    images = []
    text_reports = {}
    
    try:
        # --- 1. 抓取大盤與法人數據 (範例數據，實務上對接您的 downloader) ---
        # 這裡模擬您截圖中的 30105.04 點位數據
        market_idx = 30105.04
        change_pct = 2.57
        f_net, it_net, d_net = 60.43, 11.81, 1.55 # 單位: 億
        
        # 建立法人統計表
        df_institutional = pd.DataFrame({
            '法人類別': ['外資及陸資', '投信', '自營商', '合計'],
            '買賣超(億)': [f_net, it_net, d_net, f_net + it_net + d_net]
        })

        # --- 2. 執行 Predator 個股篩選邏輯 ---
        # 這裡我們模擬篩選出今日最強勢的 10 檔標的
        # 在您的實務代碼中，這裡應該是從 Yahoo Finance 抓取大量標的後進行排序
        stock_data = {
            'Symbol': ['2330.TW', '2454.TW', '2317.TW', '2308.TW', '2382.TW', 
                       '3231.TW', '6669.TW', '2376.TW', '3037.TW', '3711.TW'],
            'Name': ['台積電', '聯發科', '鴻海', '台達電', '廣達', 
                     '緯創', '緯穎', '技嘉', '欣興', '日月光'],
            'Close': [1105.0, 1280.0, 215.0, 412.0, 315.0, 125.0, 2450.0, 320.0, 185.0, 165.0],
            'Return': [3.5, 2.8, 1.2, 0.5, 4.2, 5.1, 6.8, 2.1, -0.5, 1.5],
            'Vol_Ratio': [1.2, 1.5, 0.8, 0.9, 2.5, 3.1, 4.2, 2.8, 1.1, 1.3]
        }
        df_top_stocks = pd.DataFrame(stock_data)
        
        # --- 3. 整合報告內容 ---
        text_reports["00_全球市場背景"] = f"加權指數: {market_idx} ({change_pct}%) | 匯率: 31.65"
        text_reports["三大法人買賣超"] = df_institutional # 存入報告中供網頁下方顯示
        text_reports["FINAL_AI_REPORT"] = f"""
        ### Predator V14.0 盤後總結
        今日大盤站穩 {market_idx} 點，多頭動能強勁。
        外資買超 {f_net} 億，資金集中在 AI 伺服器與半導體族群。
        個股量能指標 (Vol_Ratio) 以緯穎 (6669) 最為突出。
        """

        # --- 4. 圖片處理 (若有產生分析圖表) ---
        # 範例：images.append({"path": "momentum_chart.png", "caption": "動能趨勢圖"})

    except Exception as e:
        print(f"Error in analyzer: {e}")
        df_top_stocks = pd.DataFrame()
        text_reports["FINAL_AI_REPORT"] = f"分析失敗: {str(e)}"

    # 關鍵回傳修正：確保 df_top_stocks 是個股名單
    return images, df_top_stocks, text_reports

if __name__ == "__main__":
    # 本地測試
    img, df, txt = run()
    print(df)
