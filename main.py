# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    
    # 1. 執行美股領先指標下載
    try:
        print("🚀 正在同步美股關鍵數據...")
        subprocess.run(["python", "downloader_us.py"], check=False)
    except:
        pass

    # 2. 執行台股下載
    downloader_script = f"downloader_{market_id.split('-')[0]}.py"
    print(f"🚀 執行台股下載: {downloader_script}")
    subprocess.run(["python", downloader_script, "--market", market_id], check=True)

    # 3. 分析與發信
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None:
            # 💡 置入 AI 區塊文字
            text_reports["FINAL_AI_REPORT"] = "📊 市場連動模式：美股先行指標已更新，請對比台股開盤強弱。"
            
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            print("✅ 報告發送完畢。")
    except Exception as e:
        print(f"❌ 流程異常: {e}")

if __name__ == "__main__":
    main()
