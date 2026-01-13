# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def predator_logic_engine(today_df, g_df):
    """V14.0 Predator 核心邏輯引擎：將數據轉化為戰略判斷"""
    # 提取關鍵變數
    twd = g_df[g_df['Symbol'] == 'USD_TWD']
    twd_chg = twd['Change'].values[0] if not twd.empty else 0
    twd_val = twd['Value'].values[0] if not twd.empty else 0
    
    sox = g_df[g_df['Symbol'] == 'SOX_Semiconductor']
    sox_chg = sox['Change'].values[0] if not sox.empty else 0
    
    tsm = g_df[g_df['Symbol'] == 'TSM_ADR']
    tsm_chg = tsm['Change'].values[0] if not tsm.empty else 0

    # 開始 V14.0 核心判讀
    insight = "【V14.0 Predator 智能系統核心研判】\n"
    
    # 1. 宏觀資金流向分析
    if twd_chg > 0.1:
        insight += f"🔴 警訊：台幣匯率({twd_val})急貶，外資提款壓力劇增，慎防權值股虛拉掩護出貨。\n"
    elif twd_chg < -0.1:
        insight += f"🟢 強勢：台幣匯率({twd_val})強升，資金大舉匯入，大盤具備推升動能。\n"
    else:
        insight += f"⚪ 平穩：匯率維持 {twd_val} 高位震盪，當前為內資盤主導。\n"

    # 2. 跨市場連動背離偵測
    if sox_chg > 1.0 and tsm_chg > 1.0:
        insight += "📈 共振：美股半導體與台積電ADR強勢齊揚，今日電子族群具備攻擊力道。\n"
    elif sox_chg < -1.0 and tsm_chg < -1.0:
        insight += "📉 肅殺：美股指標集體走弱，今日建議嚴守停損，切勿盲目接刀。\n"
    
    # 3. 籌碼穿透判斷
    main_force_stocks = today_df[today_df['Vol_Ratio'] > 1.5]
    if len(main_force_stocks) >= 3:
        insight += f"🔥 籌碼：偵測到 {len(main_force_stocks)} 檔標的出現[主力進攻]信號，市場攻擊慾望強烈。\n"
    
    # 4. 終極策略建議
    insight += "\n🛡️ [Predator 策略執行指令]\n"
    if twd_chg > 0 and sox_chg < 0:
        insight += ">> 盤勢背離！執行「防禦性撤退」，持倉水位降至30%以下，專注現金流保護。"
    else:
        insight += ">> 趨勢確認。執行「動態追蹤止盈」，將防守位上移至 MA5，鎖定利潤。"
    
    return insight

def run(market_id):
    # 讀取數據
    data_path = f"data_{market_id}.csv"
    if not os.path.exists(data_path): return [], None, {}

    try:
        df = pd.read_csv(data_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(['Symbol', 'Date'])
        latest_date = df['Date'].max()
        prev_date = sorted(df['Date'].unique())[-2]
        
        today_df = df[df['Date'] == latest_date].copy()
        prev_df = df[df['Date'] == prev_date][['Symbol', 'Close']].rename(columns={'Close': 'P_Close'})
        today_df = pd.merge(today_df, prev_df, on='Symbol', how='left')
        today_df['Return'] = (today_df['Close'] - today_df['P_Close']) / today_df['P_Close'] * 100

        # 計算 MA20 與成交量比
        df['Vol_MA20'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(20).mean())
        latest_ma = df[df['Date'] == latest_date][['Symbol', 'Vol_MA20']]
        today_df = pd.merge(today_df, latest_ma, on='Symbol', how='left')
        today_df['Vol_Ratio'] = today_df['Volume'] / today_df['Vol_MA20']

        text_reports = {}

        # 讀取全球摘要並執行 V14.0 判讀
        summary_file = "global_market_summary.csv"
        if os.path.exists(summary_file):
            g_df = pd.read_csv(summary_file)
            # 💡 核心介入：生成智能判讀
            text_reports["FINAL_AI_REPORT"] = predator_logic_engine(today_df, g_df)
            
            # 格式化全球背景顯示
            global_msg = "🌍 【全球市場監控看板】\n"
            for _, row in g_df.iterrows():
                icon = "🟢" if row['Change'] > 0 else "🔴"
                global_msg += f"{icon} {row['Symbol']}: {row['Change']:+.2f}% (Val: {row['Value']})\n"
            text_reports["00_全球市場背景"] = global_msg

        # 績效榜單
        top_gainers = today_df.sort_values('Return', ascending=False).head(5)
        perf_msg = "🚀 今日最強勢 (Top 5):\n"
        for _, row in top_gainers.iterrows():
            tag = " 🔥[主力進攻]" if row['Return'] > 1.5 and row['Vol_Ratio'] > 1.5 else ""
            perf_msg += f"- {row['Symbol']}: {row['Return']:+.2f}%{tag}\n"
        text_reports["📊 今日個股績效榜"] = perf_msg

        # 圖表生成
        plt.figure(figsize=(10, 5))
        plt.hist(today_df['Return'].dropna(), bins=20, color='gray', alpha=0.7)
        plt.title(f"Sentiment Analysis - {latest_date.strftime('%Y-%m-%d')}")
        img_name = f"dist_{market_id}.png"
        plt.savefig(img_name)
        plt.close()

        return [{"id": "dist", "label": "市場情緒分佈", "path": img_name}], today_df, text_reports
    except Exception as e:
        return [], None, {"錯誤": str(e)}
