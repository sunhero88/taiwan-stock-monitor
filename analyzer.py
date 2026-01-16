# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import json
import yfinance as yf
from datetime import datetime

# ======================================================
# 固定參數區（V15.2 策略鎖定）
# ======================================================

# 移除硬編碼 TW50_LIST，改用動態計算
VOL_THRESHOLD_WEIGHTED = 1.2
VOL_THRESHOLD_SMALL = 1.8

MA_BIAS_GREEN_WEIGHTED = (0, 8)
MA_BIAS_GREEN_SMALL = (0, 12)

MA_BIAS_PENALTY_START = 10
MA_BIAS_PENALTY_FULL = 15

BODY_POWER_STRONG = 75
BODY_POWER_DISTRIBUTE = 20
DISTRIBUTE_VOL_RATIO = 2.5

# ======================================================
# 工具函式
# ======================================================

def calc_body_power(row):
    """K棒實體力道（%）"""
    high_low = row['High'] - row['Low']
    if high_low <= 0:
        return 0
    return abs(row['Close'] - row['Open']) / high_low * 100

def calc_ma_bias_penalty(ma_bias):
    """MA_Bias 噴出懲罰（連續函數）"""
    if ma_bias <= MA_BIAS_PENALTY_START:
        return 0
    if ma_bias >= MA_BIAS_PENALTY_FULL:
        return 1
    return (ma_bias - MA_BIAS_PENALTY_START) / (
        MA_BIAS_PENALTY_FULL - MA_BIAS_PENALTY_START
    )

def enrich_fundamentals(symbol):
    """結構面補強（僅針對入圍者執行，節省流量）"""
    data = {"OPM": 0, "QoQ": 0, "PE": 0, "Sector": "Unknown"}
    try:
        # 設定 timeout 避免卡死
        ticker = yf.Ticker(symbol)
        info = ticker.info
        data["OPM"] = round(info.get("operatingMargins", 0) * 100, 2)
        data["QoQ"] = round(info.get("revenueGrowth", 0) * 100, 2)
        data["PE"] = round(info.get("trailingPE", 0), 2)
        data["Sector"] = info.get("sector", "Unknown")
    except:
        pass
    return data

# ======================================================
# 主分析流程
# ======================================================

def run_analysis(df: pd.DataFrame, session="INTRADAY"):
    if df is None or df.empty:
        return pd.DataFrame()
        
    df = df.reset_index(drop=True)
    results = []

    # --- 步驟 1: 動態定義權值股 (全市場成交額前 50 名) ---
    df['Amount'] = df['Close'] * df['Volume']
    if len(df) > 50:
        top_50_amt_threshold = df.groupby('Symbol')['Amount'].last().nlargest(50).min()
    else:
        top_50_amt_threshold = 0

    # --- 步驟 2: 技術面快篩 (本地運算，不連網) ---
    for symbol, group in df.groupby("Symbol"):
        if len(group) < 20: continue

        # 轉為 Dict 加速處理
        latest = group.sort_values("Date").iloc[-1].to_dict()
        
        # 基礎指標
        ma20 = group["Close"].rolling(20).mean().iloc[-1]
        vol_ma20 = group["Volume"].rolling(20).mean().iloc[-1]
        
        latest["MA_Bias"] = ((latest["Close"] - ma20) / ma20) * 100 if ma20 else 0
        latest["Vol_Ratio"] = latest["Volume"] / vol_ma20 if vol_ma20 > 0 else 0
        latest["Body_Power"] = calc_body_power(latest)

        # 權值股判斷 (動態)
        is_weighted = latest['Amount'] >= top_50_amt_threshold
        
        # Kill Switch I: 技術面否決 (派貨陷阱)
        if latest["Body_Power"] < BODY_POWER_DISTRIBUTE and \
           latest["Vol_Ratio"] > DISTRIBUTE_VOL_RATIO:
            continue # 直接剔除
            
        if latest["MA_Bias"] > 20: # 乖離過大
            continue

        # 評分計算 (ERS)
        penalty = calc_ma_bias_penalty(latest["MA_Bias"])
        ers = (
            latest["Vol_Ratio"] * 20
            + max(0, 15 - abs(latest["MA_Bias"])) * 2
        ) * (1 - 0.5 * penalty)
        latest["Score"] = round(ers, 2)
        
        # 暫存標記 (為了後續打標籤用)
        latest["_Is_Weighted"] = is_weighted
        
        results.append(latest)

    if not results: return pd.DataFrame()

    # --- 步驟 3: 選出準決賽名單 (前 15 名) ---
    # 先依技術分數排序，只對前段班抓基本面，大幅提升速度
    candidates_df = pd.DataFrame(results).sort_values("Score", ascending=False).head(15)
    
    final_list = []
    
    # --- 步驟 4: 基本面補強與最終過濾 ---
    # 這裡轉為 records 列表處理
    candidates = candidates_df.to_dict('records')
    
    for row in candidates:
        symbol = row["Symbol"]
        
        # 只有這裡才連網抓資料 (最多 15 次)
        fundamentals = enrich_fundamentals(symbol)
        row["Structure"] = fundamentals
        
        # Kill Switch II: 基本面否決 (結構惡化)
        # 若 QoQ < 0 (營收衰退)，剔除
        if fundamentals["QoQ"] is not None and fundamentals["QoQ"] < 0:
            continue
            
        # 打標籤 (Tags)
        weighted = row["_Is_Weighted"]
        vol_threshold = VOL_THRESHOLD_WEIGHTED if weighted else VOL_THRESHOLD_SMALL
        green_range = MA_BIAS_GREEN_WEIGHTED if weighted else MA_BIAS_GREEN_SMALL
        
        tags = []
        if green_range[0] < row["MA_Bias"] <= green_range[1]:
            tags.append("🛡️起漲")
        if row["Vol_Ratio"] >= vol_threshold:
            tags.append("🔥主力")
        if row["Body_Power"] >= BODY_POWER_STRONG:
            tags.append("⚡真突破")
            
        tag_suffix = "(觀望)" if session == "INTRADAY" else "(確認)"
        row["Predator_Tag"] = " ".join(tags) + tag_suffix if tags else "○觀察"
        
        final_list.append(row)

    # --- 步驟 5: 產出最終 Top 10 ---
    if not final_list: return pd.DataFrame()
    
    df_final = pd.DataFrame(final_list)
    # 再次確認排序 (因為可能有 Kill Switch 剔除)
    df_final = df_final.sort_values("Score", ascending=False).head(10)
    
    return df_final

# ======================================================
# JSON 輸出 (Gem 格式)
# ======================================================

def generate_ai_json(df_top10, market="tw-share", session="INTRADAY"):
    # 確保輸入有效
    if df_top10 is None or df_top10.empty:
        return json.dumps({"error": "No data"}, indent=2)

    stocks = []
    # 使用 records 避免索引問題
    records = df_top10.to_dict('records')

    for r in records:
        stocks.append({
            "Symbol": r.get("Symbol", "Unknown"),
            "Price": r.get("Close", 0),
            "Technical": {
                "MA_Bias": round(r.get("MA_Bias", 0), 2),
                "Vol_Ratio": round(r.get("Vol_Ratio", 0), 2),
                "Body_Power": round(r.get("Body_Power", 0), 1),
                "Score": round(r.get("Score", 0), 1),
                "Tag": r.get("Predator_Tag", "")
            },
            "Structure": r.get("Structure", {})
        })

    return json.dumps({
        "meta": {
            "system": "Predator V15.2 (High-Performance)",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "session": session
        },
        "stocks": stocks
    }, ensure_ascii=False, indent=2, default=str)
