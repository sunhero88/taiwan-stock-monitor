import os
import argparse
import subprocess
from pathlib import Path
from openai import OpenAI

def get_ai_analysis(market_name, text_reports):
    """呼叫 OpenAI API 產出分析報告"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return "（未提供 AI 分析報告：找不到金鑰）"
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"你是一位股市分析師，請針對以下 {market_name} 數據摘要提供簡短繁體中文報告：\n{summary}"}]
        )
        return response.choices[0].message.content
    except Exception as e:
        if "insufficient_quota" in str(e):
            return "（AI 分析失敗：OpenAI API 額度已用盡，請至官網充值）"
        return f"（AI 分析失敗: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 💡 關鍵修復：手動建立絕對路徑資料夾
    work_dir = Path(__file__).parent.absolute()
    data_path = work_dir / "data" / market_id / "dayK"
    data_path.mkdir(parents=True, exist_ok=True)

    # 1. 執行下載 (確保傳遞絕對路徑作為參數，如果下載器支援)
    module_prefix = market_id.split('-')[0]
    module_name = f"downloader_{module_prefix}"
    print(f"📡 啟動下載器: {module_name}.py")
    
    try:
        # 強制在根目錄執行下載腳本
        subprocess.run(["python", f"{module_name}.py", "--market", market_id], cwd=work_dir, check=True)
    except Exception as e:
        print(f"⚠️ 下載警告: {e}")

    # 2. 執行分析
    try:
        import analyzer
        # 💡 在分析前列印路徑內容，確認檔案真的在那裡
        files = list(data_path.glob("*.csv"))
        print(f"🔍 路徑檢查: {data_path} 內有 {len(files)} 個檔案")
        
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is None or (hasattr(df_res, 'empty') and df_res.empty):
            print(f"❌ 警告: 即使下載成功，分析器仍讀取不到數據。")
            return

        # 3. AI 與發信
        ai_result = get_ai_analysis(market_id, text_reports)
        text_reports["🤖 AI 智能分析報告"] = ai_result

        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
        print(f"✅ {market_id} 任務全線完成！")
        
    except Exception as e:
        print(f"❌ 發生錯誤: {e}")

if __name__ == "__main__":
    main()
