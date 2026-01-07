import os
import argparse
import importlib
from openai import OpenAI

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
        print(f"🤖 正在為 {market_name} 請求 OpenAI 分析...")
        response = client.chat.completions.create(
            model="gpt-4o", 
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"❌ AI 分析出錯: {e}")
        return f"AI 分析失敗: {str(e)}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 💡 1. 加載下載器模組
    module_name = f"downloader_{market_id.split('-')[0]}"
    try:
        target_module = importlib.import_module(module_name)
        target_module.main()
    except Exception as e:
        print(f"⚠️ 下載過程警告: {e}")

    # 💡 2. 加載分析器與通知器 (改用 module 調用以避開 ImportError)
    try:
        import analyzer
        import notifier
        
        # 假設你的 analyzer.py 裡有一個 main() 函式或 run() 函式
        # 根據通用結構嘗試獲取數據
        result = analyzer.run(market_id)
        
        if isinstance(result, tuple) and len(result) >= 2:
            matrix_data, summary_text = result[0], result[1]
        else:
            matrix_data, summary_text = result, str(result)

        # 3. 執行 AI 智能分析
        ai_report = get_ai_analysis(market_id, summary_text)

        # 4. 發送通知
        # 嘗試將 AI 報告合併到原本的通知流程中
        notifier.send(market_id, matrix_data, ai_report)
        print(f"✅ {market_id} 任務完成！")

    except Exception as e:
        print(f"❌ 執行出錯: {e}")
        raise e

if __name__ == "__main__":
    main()




