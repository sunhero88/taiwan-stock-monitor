# -*- coding: utf-8 -*-
import os, argparse, subprocess, sys
from pathlib import Path

def main():
    # 0. 初始化參數與路徑
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True, help="例如: tw-share")
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()
    
    # 清除舊的摘要檔，確保數據不跨日誤用
    summary_file = root_dir / "global_market_summary.csv"
    if summary_file.exists():
        os.remove(summary_file)

    print(f"🌟 --- 啟動全球市場智能監控系統 ({market_id.upper()}) ---")

    # 1. 執行全球領先指標同步 (美股、日股)
    # 我們採用 check=False 避免美日股網路微恙時導致整個台股任務中斷
    try:
        if os.path.exists(root_dir / "downloader_us.py"):
            print("📡 [1/3] 同步美股領先指標 (SOX, TSM ADR, NVDA)...")
            subprocess.run([sys.executable, "downloader_us.py"], cwd=root_dir, check=False)
            
        if os.path.exists(root_dir / "downloader_jp.py"):
            print("📡 [2/3] 同步日股關鍵指標 (Nikkei 225, JPY)...")
            subprocess.run([sys.executable, "downloader_jp.py"], cwd=root_dir, check=False)
    except Exception as e:
        print(f"⚠️ 全球指標同步異常 (已跳過): {e}")

    # 2. 執行主市場數據下載 (台股)
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    print(f"📡 [3/3] 執行主市場下載: {downloader_script}")
    
    try:
        subprocess.run([sys.executable, downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ 主市場下載失敗，任務終止: {e}")
        return

    # 3. 執行核心分析 (analyzer.py)
    try:
        import analyzer
        print(f"📊 正在啟動數據矩陣運算與爆量偵測...")
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 💡 關鍵：設定 AI 區塊文字，取代舊有的 404 報錯
            text_reports["FINAL_AI_REPORT"] = (
                "📊 數據監控模式：AI 文字點評已停用以提升穩定性。\n"
                "💡 請優先對比「全球市場背景」與台股績效榜，觀察是否存在連動背
