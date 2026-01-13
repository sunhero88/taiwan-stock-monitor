# -*- coding: utf-8 -*-
import os, argparse, subprocess, sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()
    
    # 清理舊摘要檔
    if (root_dir / "global_market_summary.csv").exists():
        os.remove(root_dir / "global_market_summary.csv")

    print("🚀 啟動全球多市場分析系統...")

    # 1. 執行全球指標下載 (順序：美股 -> 亞太)
    subprocess.run([sys.executable, "downloader_us.py"], cwd=root_dir, check=False)
    subprocess.run([sys.executable, "downloader_asia.py"], cwd=root_dir, check=False)

    # 2. 執行主市場台股下載
    downloader_tw = f"downloader_{market_id.split('-')[0]}.py"
    subprocess.run([sys.executable, downloader_tw, "--market", market_id], cwd=root_dir, check=True)

    # 3. 分析與通知
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        if df_res is not None:
            text_reports["FINAL_AI_REPORT"] = "📊 數據連動模式：已整合美日股與台幣匯率數據，並啟動籌碼主力偵測。"
            from notifier import StockNotifier
            StockNotifier().send_stock_report(market_id.upper(), images, df_res, text_reports)
            print("✅ 報告發送完畢！")
    except Exception as e:
        print(f"❌ 流程執行異常: {e}")

if __name__ == "__main__":
    main()
