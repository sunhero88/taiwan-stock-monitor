# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
import matplotlib

# 強制使用 Agg 後端以確保穩定性
matplotlib.use('Agg')

# 字體設定
plt.rcParams['font.sans-serif'] = ['Noto Sans CJK TC', 'Noto Sans CJK JP', 'Microsoft JhengHei', 'Arial Unicode MS', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

# 基礎分箱設定
BIN_SIZE = 10.0
X_MIN, X_MAX = -100, 100
BINS = np.arange(X_MIN, X_MAX + 1, BIN_SIZE)

def get_market_url(market_id, ticker):
    """根據市場別生成對應的技術線圖連結"""
    if market_id == "us-share":
        # 美股連結：使用 StockCharts (支援大多數美股代號)
        return f"https://stockcharts.com/sc3/ui/?s={ticker}"
    else:
        # 台股連結：預設使用玩股網
        # 去除 .TW 或 .TWO 字尾以符合玩股網格式
        clean_ticker = ticker.split('.')[0]
        return f"https://www.wantgoo.com/stock/{clean_ticker}/technical-chart"

def build_company_list(arr_pct, codes, names, bins, market_id):
    """產出 HTML 格式的分箱清單，支援動態連結與全名稱顯示"""
    lines = [f"{'報酬區間':<12} | {'家數(比例)':<14} | 公司清單", "-"*80]
    total = len(arr_pct)
    
    # 內部函式：生成帶連結的股票標籤
    def make_link(i):
        url = get_market_url(market_id, codes[i])
        # 顯示格式：代號(名稱)
        return f'<a href="{url}" style="text-decoration:none; color:#0366d6;">{codes[i]}({names[i]})</a>'

    for lo in range(int(X_MIN), int(X_MAX), int(BIN_SIZE)):
        up = lo + 10
        lab = f"{lo}%~{up}%"
        mask = (arr_pct >= lo) & (arr_pct < up)
        cnt = int(mask.sum())
        if cnt == 0: continue
        
        picked_indices = np.where(mask)[0]
        # ✅ 修正：確保顯示完整代號與名稱
        links = [make_link(i) for i in picked_indices]
        lines.append(f"{lab:<12} | {cnt:>4} ({(cnt/total*100):5.1f}%) | {', '.join(links)}")

    # 解鎖天花板 (大於 100%)
    extreme_mask = (arr_pct >= 100)
    e_cnt = int(extreme_mask.sum())
    if e_cnt > 0:
        e_picked = np.where(extreme_mask)[0]
        sorted_e = sorted(e_picked, key=lambda i: arr_pct[i], reverse=True)
        # 飆股用紅色粗體標註具體漲幅
        e_links = []
        for i in sorted_e:
            url = get_market_url(market_id, codes[i])
            e_links.append(f'<a href="{url}" style="text-decoration:none; color:red; font-weight:bold;">{codes[i]}({names[i]}:{arr_pct[i]:.0f}%)</a>')
        
        lines.append(f"{' > 100%':<12} | {e_cnt:>4} ({(e_cnt/total*100):5.1f}%) | {', '.join(e_links)}")

    return "\n".join(lines)

def run_global_analysis(market_id="tw-share"):
    market_label = market_id.upper()
    print(f"📊 正在啟動 {market_label} 深度矩陣分析...")
    
    data_path = Path("./data") / market_id / "dayK"
    image_out_dir = Path("./output/images") / market_id
    image_out_dir.mkdir(parents=True, exist_ok=True)
    
    all_files = list(data_path.glob("*.csv"))
    if not all_files: return [], pd.DataFrame(), {}

    results = []
    for f in tqdm(all_files, desc=f"分析 {market_label} 數據"):
        try:
            df = pd.read_csv(f)
            if len(df) < 20: continue
            df.columns = [c.lower() for c in df.columns]
            close, high, low = df['close'].values, df['high'].values, df['low'].values
            
            # ✅ 關鍵修正：準確解析檔名中的代號與完整名稱
            # 假設檔名格式為：Ticker_FullName.csv
            stem = f.stem
            if '_' in stem:
                tkr, nm = stem.split('_', 1)
            else:
                tkr, nm = stem, stem
                
            row = {'Ticker': tkr, 'Full_Name': nm}
            
            # 定義週期：週(5), 月(20), 年(250)
            periods = [('Week', 5), ('Month', 20), ('Year', 250)]
            for p_name, days in periods:
                if len(close) <= days: continue
                prev_c = close[-(days+1)]
                if prev_c <= 0: continue
                row[f'{p_name}_High'] = (max(high[-days:]) - prev_c) / prev_c * 100
                row[f'{p_name}_Close'] = (close[-1] - prev_c) / prev_c * 100
                row[f'{p_name}_Low'] = (min(low[-days:]) - prev_c) / prev_c * 100
            results.append(row)
        except: continue

    df_res = pd.DataFrame(results)
    if df_res.empty: return [], df_res, {}

    # --- 繪圖邏輯 (保持不變，已優化 >100% 顯示) ---
    images = []
    color_map = {'High': '#28a745', 'Close': '#007bff', 'Low': '#dc3545'}
    EXTREME_COLOR = '#FF4500' 
    plot_bins = np.append(BINS, X_MAX + BIN_SIZE)

    for p_n, p_z in [('Week', '週'), ('Month', '月'), ('Year', '年')]:
        for t_n, t_z in [('High', '最高-進攻'), ('Close', '收盤-實質'), ('Low', '最低-防禦')]:
            col = f"{p_n}_{t_n}"
            if col not in df_res.columns: continue
            data = df_res[col].dropna()
            
            fig, ax = plt.subplots(figsize=(12, 7))
            clipped_data = np.clip(data.values, X_MIN, X_MAX + BIN_SIZE)
            counts, edges = np.histogram(clipped_data, bins=plot_bins)
            
            normal_counts = counts[:-1]
            extreme_count = counts[-1]
            
            bars = ax.bar(edges[:-2], normal_counts, width=9, align='edge', 
                          color=color_map[t_n], alpha=0.7, edgecolor='white')
            ex_bar = ax.bar(edges[-2], extreme_count, width=9, align='edge', 
                            color=EXTREME_COLOR, alpha=0.9, edgecolor='black', linewidth=1.5)
            
            all_bars = list(bars) + list(ex_bar)
            max_h = counts.max() if len(counts) > 0 else 1
            
            for i, bar in enumerate(all_bars):
                h = bar.get_height()
                if h > 0:
                    is_extreme = (i == len(all_bars) - 1)
                    text_color = 'red' if is_extreme else 'black'
                    ax.text(bar.get_x() + 4.5, h + (max_h * 0.02), f'{int(h)}\n({h/len(data)*100:.1f}%)', 
                            ha='center', va='bottom', fontsize=10, fontweight='bold', color=text_color)

            ax.set_ylim(0, max_h * 1.4) 
            ax.set_title(f"【{market_label}】{p_z}K {t_z} 報酬分布 (樣本:{len(data)})", fontsize=18, fontweight='bold')
            ax.set_xticks(plot_bins)
            x_labels = [f"{int(x)}%" for x in BINS] + [f">{int(X_MAX)}%"]
            ax.set_xticklabels(x_labels, rotation=45)
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            plt.tight_layout()
            
            img_path = image_out_dir / f"{col.lower()}.png"
            plt.savefig(img_path, dpi=120)
            plt.close()
            images.append({'id': col.lower(), 'path': str(img_path), 'label': f"【{market_label}】{p_z}K {t_z}"})

    # 生成文字報表清單
    text_reports = {}
    for p_n in ['Week', 'Month', 'Year']:
        col = f'{p_n}_High'
        if col in df_res.columns:
            # ✅ 傳入 market_id 以決定超連結目標
            text_reports[p_n] = build_company_list(
                df_res[col].values, 
                df_res['Ticker'].tolist(), 
                df_res['Full_Name'].tolist(), 
                BINS,
                market_id
            )
    
    return images, df_res, text_reports