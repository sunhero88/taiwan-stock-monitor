# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def analyze_market_trend(indices_data, tw_inst_total):
    """
    生成宏觀股市分析短評
    """
    try:
        # 1. 解讀台股加權 (TWII)
        twii = indices_data.get('^TWII', {})
        tw_trend = "偏多" if twii.get('Change', 0) > 0 else "偏空"
        tw_pct = twii.get('Pct', 0)
        
        # 2. 解讀費半 (SOX) - 影響台股最深
        sox = indices_data.get('^SOX', {})
        sox_status = "強勢" if sox.get('Change', 0) > 0 else "疲軟"
        
        # 3. 解讀大盤籌碼 (全市場)
        # 格式: 🔴+50.2億
        fund_status = "資金流入" if "🔴" in tw_inst_total else "資金流出"
        
        # 4. 生成短評
        comments = []
        if abs(tw_pct) < 0.3:
            comments.append(f"台股今日震盪整理({tw_trend})")
        else:
            action = "大漲" if tw_pct > 0 else "修正"
            comments.append(f"台股今日{action}{tw_pct:.1f}%")
            
        comments.append(f"，美股費半表現{sox_status}。")
        comments.append(f"外資與投信整體呈現{fund_status} ({tw_inst_total})。")
        
        # 綜合建議
        if tw_trend == "偏多" and "🔴" in tw_inst_total:
            comments.append("整體氣氛有利多頭，可積極關注下方【⚡真突破】標的。")
        elif tw_trend == "偏空" and "🔵" in tw_inst_total:
            comments.append("盤勢與籌碼雙弱，建議保守操作，僅關注高防禦個股。")
        else:
            comments.append("盤勢多空分歧，選股不選市，優先鎖定個股籌碼優勢者。")
            
        return "".join(comments)
    except:
        return "宏觀數據不足，暫無法生成分析。"

def run_analysis(df):
    # ... (保留原本的個股分析邏輯，完全不用變) ...
    try:
        if df is None or df.empty:
            return pd.DataFrame(), "⚠️ 數據源中斷"

        df = df.reset_index()
        results = []
        
        for symbol, group in df.groupby('Symbol'):
            if len(group) < 25: continue
            group = group.sort_values('Date').tail(30)
            latest = group.iloc[-1].copy()
            
            # 技術指標
            ma20 = group['Close'].rolling(window=20).mean().iloc[-1]
            vol_ma20 = group['Volume'].rolling(window=20).mean().iloc[-1]
            latest['MA_Bias'] = ((latest['Close'] - ma20) / ma20) * 100
            latest['Vol_Ratio'] = latest['Volume'] / vol_ma20 if vol_ma20 > 0 else 0
            
            # 籌碼/動能切換邏輯
            inst_net = latest.get('Inst_Net', 0)
            if inst_net == 0: # 盤中估算
                h_l_range = latest['High'] - latest['Low']
                est_force = latest['Volume'] * ((latest['Close'] - latest['Open']) / h_l_range) * 0.5 if h_l_range > 0 else 0
                val_k = round(est_force / 1000, 1)
                latest['Inst_Status'] = f"⚡🔴+{val_k}k" if est_force > 0 else f"⚡🔵{val_k}k"
                score_feed = est_force
            else: # 盤後真實
                val_k = round(inst_net / 1000, 1)
                latest['Inst_Status'] = f"🔴+{val_k}k" if inst_net > 0 else f"🔵{val_k}k"
                score_feed = inst_net

            # 評分
            chip_score = min(25, max(0, score_feed / 1000 * 5)) if score_feed > 0 else 0
            score = (min(latest['Vol_Ratio'] * 12, 40) + max(0, (12 - abs(latest['MA_Bias'])) * 2.5) + chip_score)
            latest['Score'] = score
            
            tags = []
            if latest['Vol_Ratio'] > 1.5: tags.append("🔥主力")
            if -2.0 < latest['MA_Bias'] < 3.5: tags.append("🛡️起漲")
            if "⚡" not in latest['Inst_Status'] and inst_net > 0: tags.append("🏦法人")
            elif "⚡" in latest['Inst_Status'] and score_feed > 0: tags.append("⚡主力")
            
            latest['Predator_Tag'] = " ".join(tags) if tags else "○觀察"
            results.append(latest)

        if not results: return pd.DataFrame(), "❌ 無達標標的"

        full_df = pd.DataFrame(results)
        top_10 = full_df.sort_values(by='Score', ascending=False).head(10)
        
        report_df = top_10[['Symbol', 'Close', 'MA_Bias', 'Inst_Status', 'Predator_Tag']].copy()
        report_df['MA_Bias'] = report_df['MA_Bias'].map('{:.1f}%'.format)
        report_text = report_df.to_string(index=False, justify='left')
        
        return top_10, report_text

    except Exception as e:
        return pd.DataFrame(), f"分析異常: {str(e)}"
