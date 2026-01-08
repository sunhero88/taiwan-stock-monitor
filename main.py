import os
import argparse
import importlib
from pathlib import Path
from openai import OpenAI
import pandas as pd

def get_ai_analysis(market_name, summary_text):
    """呼叫 OpenAI API 產出分析報告"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "（未提供 AI 分析報告：找不到金鑰）"
    try:
        client = OpenAI(api_key=api_key)
        # 縮減摘要字數防止 Token 過長
        prompt = f"你是一位股市分析師，請針對以下 {market_name} 的數據摘要提供簡短繁體中文報告：\n{str(summary_text)[:2000]}"
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

    # 1. 執行下載
    module_name = f"downloader_{market_id.split('-')[0]}"
    try:
        print(f"📡 步驟 1. 下載數據: {module_name}")
        # 💡 使用更穩定的加載方式
        downloader_mod = importlib.import_module(module_name)
        if hasattr(downloader_mod, 'main'):
            downloader_mod.main()
        else:
            print(f"⚠️ {module_name} 沒有 main 函式，嘗試執行預設邏輯。")
    except Exception as e:
        print(f"❌ 下載失敗: {e}")

    # 2. 執行分析器 (對接你的 analyzer.py)
    summary_for_ai = ""
    images_data = []
    try:
        print("📊 步驟 2. 執行深度矩陣分析...")
        import analyzer
        # 根據你的原始碼，run 回傳 (images, df_res, text_reports)
        images, df_res, text_reports = analyzer.run(market_id)
        
        images_data = images
        # 取得文字摘要給 AI
        if text_reports:
            summary_for_ai = "\n".join([f"--- {k} ---\n{v[:500]}" for k, v in text_reports.items()])
    except Exception as e:
        print(f"❌ 分析階段崩潰: {e}")
        # 如果分析失敗，我們還是嘗試走完發信流程，避免完全沒收到信

    # 3. 執行 AI 智能分析
    ai_report = ""
    if summary_for_ai:
        ai_report = get_ai_analysis(market_id, summary_for_ai)
    else:
        ai_report = "由於分析數據為空，無法產出 AI 報告。"

    # 4. 執行發信 (對接 notifier.py)
    try:
        print("📧 步驟 3. 正在發送通知郵件...")
        import notifier
        # 💡 關鍵修正：將 AI 報告合併到郵件正文中
        # 假設你的 notifier.send 接受 (market_id, content, images) 或類似結構
        # 這裡採取最安全的合併方式
        email_content = f"🤖 AI 智能分析報告：\n{ai_report}\n\n"
        if summary_for_ai:
            email_content += f"📊 市場統計摘要：\n{summary_for_ai}"
        
        # 調用你的 notifier 發信函式
        # 注意：此處需確認你的 notifier.py 參數。如果原本只收兩個，請合併
        notifier.send(market_id, email_content)
        print(f"✅ {market_id} 任務執行完畢，郵件已發送。")
    except Exception as e:
        print(f"❌ 通知發送失敗: {e}")

if __name__ == "__main__":
    main()
