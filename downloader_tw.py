import os
import argparse
import pandas as pd
from openai import OpenAI
# 💡 直接導入各市場的模組
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
        print(f"🤖 正在向 OpenAI 請求 {market_name} 的智能分析...")
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

    # 💡 修正點：根據你的 downloader_tw.py 結構，直接調用其 main() 函式
    modules = {
        "tw-share": downloader_tw,
        "us-share": downloader_us,
        "hk-share": downloader_hk,
        "cn-share": downloader_cn,
        "jp-share": downloader_jp,
        "kr-share": downloader_kr
    }

    target_module = modules.get(market_id)
    if not target_module:
        print(f"❌ 不支援的市場 ID: {market_id}")
        return

    # 1. 執行下載任務
    # 根據你的代碼，downloader_tw.main() 會下載 CSV 並回傳統計字典
    download_stats = target_module.main()
    print(f"📊 下載統計: {download_stats}")

    # 2. 執行數據分析
    # 這裡假設你的 analyzer 依然是類別形式，若報錯請再跟我說
    analyzer = StockAnalyzer()
    # 執行分析並取得矩陣與文字摘要
    matrix_data, summary_text = analyzer.run(market_id)

    # 3. 執行 AI 智能分析
    ai_report = get_ai_analysis(market_id, summary_text)

    # 4. 發送通知
    notifier = StockNotifier()
    notifier.send(market_id, matrix_data, ai_report)
    print(f"✅ {market_id} 任務完成！")

if __name__ == "__main__":
    main()
