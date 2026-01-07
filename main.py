import os
import argparse
from openai import OpenAI  # 💡 新增這行
# ... (你原本其他的 import，例如 downloader, analyzer, notifier)

def get_ai_analysis(market_name, market_data):
    """
    呼叫 OpenAI API 進行股市智能分析
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("⚠️ 未找到 OPENAI_API_KEY，跳過 AI 分析步驟。")
        return "（未提供 AI 分析報告）"

    client = OpenAI(api_key=api_key)
    
    # 建立適合 AI 閱讀的 Prompt
    prompt = f"""
    你是一位專業的股市量化分析師。請針對以下 {market_name} 市場的分箱報酬數據進行分析：
    {market_data}
    
    請提供：
    1. 市場當前動能總結。
    2. 潛在的風險或機會提示。
    3. 給投資者的 100 字短評。
    請用繁體中文回覆。
    """

    try:
        print(f"🤖 正在為 {market_name} 生成 AI 分析報告...")
        response = client.chat.completions.create(
            model="gpt-4o",  # 或 gpt-4o-mini 以節省成本
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"❌ AI 分析出錯: {e}")
        return f"AI 分析失敗: {str(e)}"

def main():
    # 1. 處理參數 (例如 --market tw-share)
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, default='tw-share')
    args = parser.parse_args()
    
    market_id = args.market

    # 2. 執行原本的數據下載與分析 (假設你的變數名稱如下)
    # df = downloader.get_data(market_id)
    # matrix_data = analyzer.calculate_matrix(df)
    
    # --- 💡 這裡插入 AI 分析邏輯 ---
    # 假設你的數據總結在一個字串變數裡，如果沒有，就用 str(matrix_data)
    ai_report = get_ai_analysis(market_id, "這裡放你的數據摘要或矩陣文字內容")
    
    # 3. 發送通知 (將 AI 報告加入原本的通知內容)
    # notifier.send(market_id, matrix_data, ai_report)
    print("✅ 任務完成！")

if __name__ == "__main__":
    main()
