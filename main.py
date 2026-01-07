import os
import argparse
import pandas as pd
from openai import OpenAI  # 💡 確保已在 Environment Setup 加入 openai
from downloader_tw import TaiwanStockDownloader
from downloader_us import USStockDownloader
from downloader_hk import HKStockDownloader
from downloader_cn import ChinaStockDownloader
from downloader_jp import JapanStockDownloader
from downloader_kr import KoreaStockDownloader
from analyzer import StockAnalyzer
from notifier import StockNotifier

def get_ai_analysis(market_name, summary_text):
    """
    呼叫 OpenAI API 針對市場數據進行智能分析
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("⚠️ 未偵測到 OPENAI_API_KEY，將跳過 AI 分析。")
        return "（未提供 AI 分析報告）"

    try:
        client = OpenAI(api_key=api_key)
        
        prompt = f"""
        你是一位專業的股市量化分析師。請根據以下 {market_name} 市場的分箱報酬數據進行深度解讀：
        {summary_text}
        
        請以繁體中文提供：
        1. 市場當前動能總結 (過熱/恐慌/盤整)。
        2. 異常警訊或潛在機會。
        3. 給投資者的 100 字短評。
        """

        print(f"🤖 正在向 OpenAI 請求 {market_name} 的智能分析...")
        response = client.chat.completions.create(
            model="gpt-4o",  # 使用最強的分析模型
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"❌ AI 分析過程中發生錯誤: {e}")
        return f"AI 分析失敗: {str(e)}"

def main():
    # 1. 處理市場參數
    parser = argparse.ArgumentParser(description='Global Market Monitor')
    parser.add_argument('--market', type=str, required=True, help='Market ID (e.g., tw-share)')
    args = parser.parse_args()
    market_id = args.market

    print(f"🚀 開始分析市場: {market_id}")

    # 2. 根據參數選擇對應的下載器
    downloaders = {
        "tw-share": TaiwanStockDownloader(),
        "us-share": USStockDownloader(),
        "hk-share": HKStockDownloader(),
        "cn-share": ChinaStockDownloader(),
        "jp-share": JapanStockDownloader(),
        "kr-share": KoreaStockDownloader()
    }

    downloader = downloaders.get(market_id)
    if not downloader:
        print(f"❌ 不支援的市場 ID: {market_id}")
        return

    # 3. 下載數據與分析
    df = downloader.get_data()
    analyzer = StockAnalyzer()
    matrix_data, summary_text = analyzer.run(df) # 假設 analyzer 會回傳統計文字

    # 4. 💡 執行 AI 智能分析
    ai_report = get_ai_analysis(market_id, summary_text)

    # 5. 發送通知 (將 AI 報告一併傳入)
    notifier = StockNotifier()
    notifier.send(market_id, matrix_data, ai_report)

    print(f"✅ {market_id} 市場監控任務完成！")

if __name__ == "__main__":
    main()

