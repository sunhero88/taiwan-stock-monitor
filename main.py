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

    # 1. 下載數據
    download_stats = {}
    try:
        module_name = f"downloader_{market_id.split('-')[0]}"
        downloader_mod = importlib.import_module(module_name)
        download_stats = downloader_mod.main()
    except Exception as e:
        print(f"⚠️ 下載警告: {e}")

    # 2. 數據分析
    try:
        import analyzer
        # 根據您的 analyzer.py，回傳 (images, df_res, text_reports)
        images, df_res, text_reports = analyzer.run(market_id)
        
        # 3. AI 智能分析
        ai_report = get_ai_analysis(market_id, text_reports)
        # 將 AI 報告塞入 text_reports，以便 notifier 讀取
        text_reports["🤖 AI 智能分析報告"] = ai_report

        # 4. 發送通知
        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        notifier_inst.send_stock_report(
            market_name=market_id.upper(),
            img_data=images,
            report_df=df_res,
            text_reports=text_reports,
            stats=download_stats
        )
        print(f"✅ {market_id} 任務完成，郵件已寄送。")

    except Exception as e:
        print(f"❌ 失敗: {e}")

if __name__ == "__main__":
    main()
