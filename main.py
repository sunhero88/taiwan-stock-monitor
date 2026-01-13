# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    # 1. 如果是跑台股，先抓取昨晚美股關鍵數據作為背景
    if market_id == "tw-share":
        print("🌍 正在獲取美股領先指標 (SOX, NVDA, TSM)...")
        # 建立一個精簡清單，避免下載過多
        subprocess.run(["python", "downloader_us.py", "--market", "us-lead"], check=False)

    # 2. 執行主市場下載
    downloader_script = f"downloader_{market_id.split('-')[0]}.py"
    print(f"🚀 開始執行主市場下載: {downloader_script}")
    subprocess.run(["python", downloader_script, "--market", market_id], check=True)

    # 3. 執行分析
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 💡 這裡可以手動加入全球視野說明
            text_reports["FINAL_AI_REPORT"] = "📊 數據監控模式：已整合全球連動分析。請關注下方美股指標對今日台股之影響。"
            
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            print(f"✅ 報告已送達！")
    except Exception as e:
        print(f"❌ 流程出錯: {e}")

if __name__ == "__main__":
    main()
