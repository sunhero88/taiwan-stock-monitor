import os
import argparse
import importlib
from openai import OpenAI

def get_ai_analysis(market_name, summary_text):
    """呼叫 OpenAI API 進行智能分析"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("⚠️ 未偵測到 OpenAI API Key")
        return "（未提供 AI 分析報告）"

    try:
        client = OpenAI(api_key=api_key)
        prompt = f"你是一位股市分析師，請簡短分析 {market_name} 的數據：\n{summary_text}"
        print(f"🤖 正在為 {market_name} 請求 AI 分析...")
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"AI 分析出錯: {e}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 💡 關鍵修正：將市場 ID 轉換為模組名稱 (例如 tw-share -> downloader_tw)
    module_name = f"downloader_{market_id.split('-')[0]}"
    print(f"🚀 正在加載模組: {module_name}")

    try:
        # 動態加載模組，這樣就不會因為找不到特定類別而崩潰
        target_module = importlib.import_module(module_name)
        
        # 1. 執行下載 (根據你的 downloader_tw.py，它有一個 main 函式)
        download_stats = target_module.main()
        print(f"📊 下載統計: {download_stats}")

        # 2. 執行分析
        from analyzer import StockAnalyzer
        analyzer = StockAnalyzer()
        
        # 這裡根據你的 analyzer 結構獲取結果
        result = analyzer.run(market_id)
        
        # 判斷回傳值是單一物件還是 tuple (matrix, summary)
        if isinstance(result, tuple) and len(result) >= 2:
            matrix_data, summary_text = result[0], result[1]
        else:
            matrix_data, summary_text = result, str(result)

        # 3. AI 智能分析
        ai_report = get_ai_analysis(market_id, summary_text)

        # 4. 發送通知
        from notifier import StockNotifier
        notifier = StockNotifier()
        notifier.send(market_id, matrix_data, ai_report)
        print(f"✅ {market_id} 監控任務完成！")

    except Exception as e:
        print(f"❌ 執行過程中發生錯誤: {e}")
        # 如果失敗了，我們也回報錯誤訊息
        raise e

if __name__ == "__main__":
    main()



