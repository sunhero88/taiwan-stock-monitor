import os
import argparse
import subprocess
from openai import OpenAI

def get_ai_analysis(market_name):
    """嘗試調用 OpenAI，失敗則回傳空字串"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return ""
    try:
        client = OpenAI(api_key=api_key)
        # 這裡我們不讀取變數，直接請 AI 根據市場名稱做一般性分析
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"請提供 100 字繁體中文的 {market_name} 今日股市短評。"}]
        )
        return f"\n\n🤖 AI 智能分析：\n{resp.choices[0].message.content}"
    except: return ""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 🚀 關鍵：直接執行你原本各市場的下載器腳本，確保「原本功能」不變
    module_name = f"downloader_{market_id.split('-')[0]}.py"
    print(f"正在執行原始下載模組: {module_name}")
    subprocess.run(["python", module_name])

    # 🚀 關鍵：執行你原本的分析與通知流程
    # 我們不再用 import 導入，而是直接運行腳本，這能避開所有 ImportError
    print("正在執行原始分析與通知流程...")
    subprocess.run(["python", "analyzer.py", "--market", market_id])
    
    # 最後，AI 分析僅作為控制台輸出參考
    ai_report = get_ai_analysis(market_id)
    if ai_report:
        print(ai_report)

    print(f"✅ {market_id} 任務執行完畢")

if __name__ == "__main__":
    main()
