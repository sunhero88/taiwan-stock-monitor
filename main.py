# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    print(f"🚀 啟動純數據任務: {market_id}")

    # 1. 下載數據
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    try:
        print(f"📡 執行下載器: {downloader_script}")
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"⚠️ 下載警告: {e}")

    # 2. 執行分析
    try:
        import analyzer
        print(f"📊 正在進行數據矩陣運算...")
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 💡 移除 AI 調用，改為靜態標題
            text_reports["FINAL_AI_REPORT"] = "📊 市場數據摘要已產出。請參考下方詳細回報率分布與個股明細。"
            
            # 3. 發送郵件
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            print(f"✅ {market_id} 數據報告發送成功！")
        else:
            print("❌ 分析失敗：數據結果為空。")
    except Exception as e:
        print(f"❌ 流程執行異常: {e}")

if __name__ == "__main__":
    main()
