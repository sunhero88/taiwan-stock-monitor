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
        insight += ">> 盤勢背離！執行「防禦性撤退」，持倉水位降至30%
