import os
import argparse
import importlib
from openai import OpenAI

def get_ai_analysis(market_name, summary_text):
    """呼叫 OpenAI API 產出分析報告"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "（未提供 AI 分析報告：找不到金鑰）"
    try:
        client = OpenAI(api_key=api_key)
        prompt = f"你是一位股市分析師，請簡短分析 {market_name} 數據：\n{summary_text}"
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"（AI 分析出錯: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 1. 執行下載 (對接 downloader_tw.py 等)
    module_name = f"downloader_{market_id.split('-')[0]}"
    try:
        print(f"正在執行原始下載模組: {module_name}.py")
        downloader_mod = importlib.import_module(module_name)
        downloader_mod.main() 
    except Exception as e:
        print(f"下載失敗: {e}")

    # 2. 執行分析 (對接 analyzer.py)
    try:
        print("正在執行原始分析流程...")
        import analyzer
        # 直接使用模組內的 run 函式，避開類別導入錯誤
        result = analyzer.run(market_id)
        
        if isinstance(result, tuple):
            matrix_data, summary_text = result[0], result[1]
        else:
            matrix_data, summary_text = result, str(result)
    except Exception as e:
        print(f"分析失敗: {e}")
        return

    # 3. 執行 AI 分析
    ai_report = get_ai_analysis(market_id, summary_text)

    # 4. 執行發信 (對接 notifier.py)
    try:
        print("正在執行原始通知流程...")
        import notifier
        # 將 AI 報告與原始矩陣數據合併
        full_report = f"{matrix_data}\n\n🤖 AI 智能分析：\n{ai_report}"
        # 調用模組內的 send 函式
        notifier.send(market_id, full_report)
        print(f"✅ {market_id} 任務執行完畢，郵件已發送。")
    except Exception as e:
        print(f"通知發送失敗: {e}")

if __name__ == "__main__":
    main()
