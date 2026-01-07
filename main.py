import os
import argparse
import pandas as pd
from openai import OpenAI
# 💡 改用直接導入模組，避免類別名稱不對導致的 ImportError
import downloader_tw
import downloader_us
import downloader_hk
import downloader_cn
import downloader_jp
import downloader_kr
from analyzer import StockAnalyzer
from notifier import StockNotifier

def get_ai_analysis(market_name, summary_text):
    """呼叫 OpenAI API 進行智能分析"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("⚠️ 未偵測到 OPENAI_API_KEY，將跳過 AI 分析。")
        return "（未提供 AI 分析報告）"

    try:
        client = OpenAI(api_key=api_key)
        prompt = f"""
        你是一位專業的股市量化分析師。請針對以下 {market_name} 市場的分箱報酬數據進行深度解讀：
        {summary_text}
        
        請以繁體中文提供：
        1. 市場當前動能總結 (過熱/恐慌/盤整)。
        2. 異常警訊或潛在機會。
        3. 給投資者的 100 字短評。
        """
        print(f"🤖 正在向 OpenAI 請求 {market_name} 的分析...")
        response = client.chat.completions.create(
            model="gpt-4o", 
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"❌ AI 分析出錯: {e}")
        return f"AI 分析失敗: {str(e)}"

def main():
    parser = argparse.ArgumentParser(description='Global Market Monitor')
    parser.add_argument('--market', type=str, required=True, help='Market ID')
    args = parser.parse_args()
    market_id = args.market

    print(f"🚀 開始分析市場: {market_id}")

    # 💡 建立下載器實例的修正邏輯
    # 這裡會根據檔案名稱自動尋找裡面定義的類別
    if market_id == "tw-share":
        downloader = downloader_tw.TaiwanStockDownloader()
    elif market_id == "us-share":
        downloader = downloader_us.USStockDownloader()
    elif market_id == "hk-share":
        downloader = downloader_hk.HKStockDownloader()
    elif market_id == "cn-share":
        downloader = downloader_cn.ChinaStockDownloader()
    elif market_id == "jp-share":
        downloader = downloader_jp.JapanStockDownloader()
    elif market_id == "kr-share":
        downloader = downloader_kr.KoreaStockDownloader()
    else:
        print(f"❌ 不支援的市場 ID: {market_id}")
        return

    # 下載數據與分析
    df = downloader.get_data()
    analyzer = StockAnalyzer()
    # 💡 請確認你的 analyzer.run(df) 是否回傳兩個值
    result = analyzer.run(df)
    if isinstance(result, tuple):
        matrix_data, summary_text = result
    else:
        matrix_data, summary_text = result, str(result)

    # 執行 AI 分析
    ai_report = get_ai_analysis(market_id, summary_text)

    # 發送通知
    notifier = StockNotifier()
    notifier.send(market_id, matrix_data, ai_report)
    print(f"✅ {market_id} 任務完成！")

if __name__ == "__main__":
    main()
if __name__ == "__main__":
    main()


