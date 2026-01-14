# -*- coding: utf-8 -*-
import pandas as pd
import yfinance as yf
import datetime
import numpy as np

def run(market_type='tw-share'):
    """
    Predator V14.0 終極分析引擎 (全球聯動補強版)
    回傳格式: (images, df_top_stocks, text_reports)
    """
    images = []
    text_reports = {}
    
    try:
        # --- 1. 全球風險訊號抓取 (Global Risk Signals) ---
        # 抓取台幣、日圓、VIX、費半、NVDA、台積電ADR
        tickers = {
            "TWD": "TWD=X",       # 台幣匯率
            "VIX": "^VIX",        # 美股恐慌指數
            "SOX": "^SOX",        # 費城半導體
            "NVDA": "NVDA",       # AI 領跑者
            "TSM_ADR": "TSM",     # 台積電 ADR
            "JPY": "JPY=X"        # 日圓 (流動性指標)
        }
        
        # 抓取過去 5 天數據以計算「速率 (Velocity)」
        global_data = yf.download(list(tickers.values()), period="5d", interval="1d")['Close']
        
        # --- 2. 核心指標計算 (依據 V14.0 守則) ---
        # (1) 台幣 3 日貶值速率 (Rule #8)
        twd_recent = global_data[tickers["TWD"]]
        twd_velocity = ((twd_recent.iloc[-1] - twd_recent.iloc[-3]) / twd_recent.iloc[-3]) * 100
        fx_status = "🔴 劇貶 (警告)" if twd_velocity > 1.0 else "🟢 穩定"

        # (2) VIX 單日斜率 (Risk Control)
        vix_recent = global_data[tickers["VIX"]]
        vix_slope = ((vix_recent.iloc[-1] - vix_recent.iloc[-2]) / vix_recent.iloc[-2]) * 100
        vix_status = "⚠️ 飆升" if vix_slope > 10.0 else "✅ 正常"

        # (3) AI 領先指標連動
        sox_change = ((global_data[tickers["SOX"]].iloc[-1] - global_data[tickers["SOX"]].iloc[-2]) / global_data[tickers["SOX"]].iloc[-2]) * 100
        nvda_change = ((global_data[tickers["NVDA"]].iloc[-1] - global_data[tickers["NVDA"]].iloc[-2]) / global_data[tickers["NVDA"]].iloc[-2]) * 100

        # --- 3. 建立全球背景報告 (Phase 0) ---
        global_report = pd.DataFrame({
            '監控指標': ['台幣匯率', 'VIX 指數', '費城半導體', 'NVIDIA', '台積電ADR'],
            '最新數值': [f"{twd_recent.iloc[-1]:.2f}", f"{vix_recent.iloc[-1]:.2f}", 
                         f"{global_data[tickers['SOX']].iloc[-1]:.0f}", f"{global_data[tickers['NVDA']].iloc[-1]:.1f}",
                         f"{global_data[tickers['TSM_ADR']].iloc[-1]:.1f}"],
            '動態斜率': [f"{twd_velocity:+.2f}% (3D)", f"{vix_slope:+.1f}% (1D)", 
                         f"{sox_change:+.1f}%", f"{nvda_change:+.1f}%", "-"],
            '風險狀態': [fx_status, vix_status, "-", "-", "-"]
        })
        text_reports["00_全球風險預警"] = global_report

        # --- 4. 建立法人統計表 (模擬數據，實務可對接證交所 API) ---
        # 假設今日數據：外資買超 60.43 億
        df_institutional = pd.DataFrame({
            '法人類別': ['外資及陸資', '投信', '自營商', '合計'],
            '買賣超(億)': [60.43, 11.81, 1.55, 73.79]
        })
        text_reports["三大法人買賣超"] = df_institutional

        # --- 5. 執行 Predator 個股篩選邏輯 ---
        # 這裡模擬篩選出的 TOP 10 (實務上這裡會是你的選股算法)
        stock_data = {
            'Symbol': ['6669.TW', '2330.TW', '3711.TW', '2454.TW', '2317.TW', '2382.TW', '3231.TW', '2308.TW', '3037.TW', '2376.TW'],
            'Name': ['緯穎', '台積電', '日月光', '聯發科', '鴻海', '廣達', '緯創', '台達電', '欣興', '技嘉'],
            'Close': [2450.0, 1105.0, 165.0, 1280.0, 215.0, 315.0, 125.0, 412.0, 185.0, 320.0],
            'Return': [6.8, 3.5, 1.5, 2.8, 1.2, 4.2, 5.1, 0.5, -0.5, 2.1],
            'Vol_Ratio': [4.2, 1.2, 1.3, 1.5, 0.8, 2.5, 3.1, 0.9, 1.1, 2.8]
        }
        df_top_stocks = pd.DataFrame(stock_data)

        # --- 6. AI 戰略總結內容 ---
        market_idx = 30105.04
        text_reports["FINAL_AI_REPORT"] = f"""
        ### Predator V14.0 戰略評估 (2026-01-14)
        1. **大盤位階**：站穩 {market_idx} 點，費半連動 {sox_change:+.1f}%。
        2. **匯率風險**：台幣 3 日變化 {twd_velocity:+.2f}%，{fx_status}。
        3. **量能核心**：緯穎 (6669) 出現 {df_top_stocks.iloc[0]['Vol_Ratio']}x 爆發量。
        4. **戰術建議**：符合 Rule #7 右側交易，但須監控 VIX {vix_status}。
        """

    except Exception as e:
        print(f"Error in analyzer: {e}")
        df_top_stocks = pd.DataFrame()
        text_reports["FINAL_AI_REPORT"] = f"分析引擎故障: {str(e)}"

    return images, df_top_stocks, text_reports
