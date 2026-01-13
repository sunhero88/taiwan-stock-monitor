# -*- coding: utf-8 -*-
import os, argparse, subprocess, sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()
    
    # 1. 數據預處理：清理舊摘要
    summary_file = root_dir / "global_market_summary.csv"
    if summary_file.exists(): os.remove(summary_file)

    print("🚀 V14.0 Predator 智能監控系統啟動...")

    # 2. 全球數據介入 (美、日、台幣)
    subprocess.run([sys.executable, "downloader_us.py"], cwd=root_dir, check=False)
    subprocess.run([sys.executable, "downloader_asia.py"], cwd=root_dir, check=False)

    # 3. 主市場數據獲取
    downloader_tw = f"downloader_{market_id.split('-')[0]}.py"
    subprocess.run([sys.executable, downloader_tw, "--market", market_id], cwd=root_dir, check=True)

    # 4. 智能分析與自動寫入判讀
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None:
            # 這裡的 FINAL_AI_REPORT 已由 analyzer.predator_logic_engine 動態生成
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            success = notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            if success:
                print("✅ 智能投資報告已成功寫入並發送！")
    except Exception as e:
        print(f"❌ 智能分析中斷: {e}")

if __name__ == "__main__":
    main()
