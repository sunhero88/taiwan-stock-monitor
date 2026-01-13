# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    # 1. 執行下載
    downloader_script = f"downloader_{market_id.split('-')[0]}.py"
    print(f"🚀 開始執行下載: {downloader_script}")
    subprocess.run(["python", downloader_script, "--market", market_id], check=True)

    # 2. 執行分析
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 💡 這裡是關鍵：將提示訊息放入 FINAL_AI_REPORT，避免 notifier.py 顯示預設值
            text_reports["FINAL_AI_REPORT"] = "📊 數據監控模式：AI 點評已跳過，請專注於下方「成交量爆量追蹤」清單。"
            
            # 3. 發送郵件 (呼叫您提供的 notifier.py)
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            success = notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            
            if success:
                print(f"✅ 報告已成功送達您的信箱！")
            else:
                print(f"❌ 郵件發送失敗，請檢查 RESEND_API_KEY。")
        else:
            print("❌ 分析失敗：找不到 CSV 檔案或數據為空。")
    except Exception as e:
        print(f"❌ 執行過程出錯: {e}")

if __name__ == "__main__":
    main()
