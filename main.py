import os
import argparse
import importlib
from openai import OpenAI
import pandas as pd

def get_ai_analysis(market_name, text_reports):
    """呼叫 OpenAI API 產出分析報告"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "（未提供 AI 分析報告：找不到金鑰）"
    
    # 組合一段摘要給 AI 看
    summary = ""
    for period, report in text_reports.items():
        summary += f"\n[{period} K線]\n{report[:1000]}\n"

    try:
        client = OpenAI(api_key=api_key)
        prompt = f"你是一位股市分析師，請針對以下 {market_name} 的數據摘要提供簡短繁體中文報告：\n{summary[:3000]}"
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"（AI 分析失敗: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 1. 執行下載 (對接 downloader_tw.py 等)
    download_stats = {}
    module_name = f"downloader_{market_id.split('-')[0]}"
    try:
        print(f"📡 步驟 1. 下載數據: {module_name}")
        downloader_mod = importlib.import_module(module_name)
        # 接收你的下載器回傳的統計字典 (total, success, fail)
        download_stats = downloader_mod.main()
    except Exception as e:
        print(f"⚠️ 下載警告: {e}")

    # 2. 執行分析 (對接你的 analyzer.py)
    try:
        print("📊 步驟 2. 執行分析...")
        import analyzer
        # 根據你的 analyzer.py，回傳 (images, df_res, text_reports)
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res.empty:
            print("⚠️ 數據為空，跳過發信。")
            return

        # 3. 執行 AI 智能分析
        ai_report = get_ai_analysis(market_id, text_reports)

        # 4. 執行發信 (對接你的 notifier.py)
        print("📧 步驟 3. 正在發送通知郵件...")
        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        
        # 💡 核心修正：將 AI 報告插入到 text_reports 的「年」報告後，讓它出現在郵件最後
        text_reports["AI 智能分析"] = ai_report
        
        # 💡 精確對接你的 send_stock_report(market_name, img_data, report_df, text_reports, stats)
        notifier_inst.send_stock_report(
            market_name=market_id.upper(),
            img_data=images,
            report_df=df_res,
            text_reports=text_reports,
            stats=download_stats
        )
        print(f"✅ {market_id} 任務執行完畢，郵件已寄送。")

    except Exception as e:
        print(f"❌ 分析或通知失敗: {e}")

if __name__ == "__main__":
    main()
if __name__ == "__main__":
    main()

