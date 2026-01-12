# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    print(f"🚀 啟動數據監控任務: {market_id}")

    # 1. 執行下載器
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    try:
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"⚠️ 數據下載異常: {e}")

    # 2. 執行分析器 (帶有爆量偵測)
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None:
            # 💡 這裡手動插入一則溫馨提示，取代原本的 AI 區塊
            text_reports["FINAL_AI_REPORT"] = "📊 系統提示：AI 文字解讀已停用。請專注於下方「成交量爆量追蹤」與報酬分布數據。"
            
            # 3. 發送郵件
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            print(f"✅ {market_id} 任務執行完畢，郵件已發出。")
    except Exception as e:
        print(f"❌ 流程執行異常: {e}")

if __name__ == "__main__":
    main()
