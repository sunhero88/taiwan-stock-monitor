import os
import argparse
import importlib
from openai import OpenAI

def get_ai_analysis(market_name, summary_text):
    """呼叫 OpenAI API 進行分析"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "（未提供 AI 分析報告：找不到金鑰）"

    try:
        client = OpenAI(api_key=api_key)
        prompt = f"你是一位股市分析師，請針對以下 {market_name} 的數據提供簡短繁體中文報告：\n{summary_text}"
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"（AI 分析發生錯誤: {str(e)}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 1. 動態加載下載器 (對接 downloader_tw.py 等)
    module_name = f"downloader_{market_id.split('-')[0]}"
    try:
        downloader_mod = importlib.import_module(module_name)
        downloader_mod.main() 
    except Exception as e:
        print(f"下載階段警告: {e}")

    # 2. 執行分析器 (還原你原本正確的導入邏輯)
    from analyzer import run as run_analysis
    
    try:
        # 直接調用原本 analyzer.py 裡的 run 函式
        result = run_analysis(market_id)
        
        # 解析回傳結果
        if isinstance(result, tuple) and len(result) >= 2:
            matrix_data, summary_text = result[0], result[1]
        else:
            matrix_data, summary_text = result, str(result)
            
    except Exception as e:
        print(f"分析失敗: {e}")
        return

    # 3. 執行 AI 智能分析
    ai_report = get_ai_analysis(market_id, summary_text)

    # 4. 發送通知 (還原你原本正確的導入邏輯)
    from notifier import send as send_notification
    
    try:
        # 將 AI 報告與原始數據合併後發送
        full_report = f"{matrix_data}\n\n🤖 AI 智能分析：\n{ai_report}"
        send_notification(market_id, full_report)
        print(f"✅ {market_id} 任務執行完畢")
    except Exception as e:
        print(f"通知發送失敗: {e}")

if __name__ == "__main__":
    main()





