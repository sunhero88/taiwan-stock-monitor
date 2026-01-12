# -*- coding: utf-8 -*-
import os, argparse, subprocess, importlib
from pathlib import Path
from openai import OpenAI

def get_ai_analysis(market_name, text_reports):
    """呼叫 OpenAI API，處理額度不足報錯"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return "（未提供 AI 金鑰，請檢查 Secrets）"
    
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": "你是一位專業股市分析師，請用繁體中文提供精簡報告。"},
                      {"role": "user", "content": f"分析以下 {market_name} 數據：\n{summary}"}]
        )
        return response.choices[0].message.content
    except Exception as e:
        if "insufficient_quota" in str(e):
            return "（⚠️ AI 分析失敗：OpenAI 帳戶餘額不足，請至官網充值）"
        return f"（AI 分析暫時不可用: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    # 1. 執行下載
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    print(f"📡 步驟 1. 啟動下載器: {downloader_script}")
    try:
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"❌ 數據下載失敗: {e}")

    # 2. 路徑檢查
    data_path = root_dir / "data" / market_id / "dayK"
    csv_files = list(data_path.glob("*.csv"))
    print(f"🔍 步驟 2. 路徑檢查: 在 {data_path} 發現 {len(csv_files)} 個 CSV 檔案")

    if not csv_files:
        print("❌ 錯誤：找不到數據，請檢查下載器是否正常存檔。")
        return

    # 3. 分析、AI 與寄信
    try:
        import analyzer
        print(f"📊 步驟 3. 執行 {market_id} 分析流程...")
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            ai_result = get_ai_analysis(market_id, text_reports)
            text_reports["🤖 AI 智能分析報告"] = ai_result
            
            from notifier import StockNotifier
            StockNotifier().send_stock_report(market_id.upper(), images, df_res, text_reports)
            print(f"✅ {market_id} 任務全線完成！")
        else:
            print("❌ 分析錯誤：生成的結果數據為空。")
    except Exception as e:
        print(f"❌ 流程執行中斷: {e}")

if __name__ == "__main__":
    main()
