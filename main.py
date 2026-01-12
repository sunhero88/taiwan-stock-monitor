# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path
import google.generativeai as genai

def get_gemini_analysis(market_name, text_reports):
    """
    修正 404 錯誤：確保模型名稱正確
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: return "（未配置 GEMINI_API_KEY）"
    
    # 彙整數據摘要，限制長度以符合 API 規範
    summary = "\n".join([f"[{k}]\n{v[:600]}" for k, v in text_reports.items()])
    
    try:
        genai.configure(api_key=api_key)
        # 💡 修正點：使用不帶 'models/' 前綴的名稱
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = f"""
        你是一位專業股市操盤手。請針對以下 {market_name} 數據摘要提供繁體中文分析。
        內容須包含：盤勢強弱判斷、核心權值股觀察、短期操作風險。
        
        數據內容：
        {summary}
        """
        
        response = model.generate_content(prompt)
        if response and response.text:
            return response.text
        return "（AI 產出內容為空）"
    except Exception as e:
        # 即使 AI 失敗，也回傳錯誤訊息，不要讓主程式崩潰
        return f"（智能解讀暫時不可用: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    print(f"🚀 啟動任務: {market_id}")

    # 1. 下載數據
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    try:
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"⚠️ 下載階段警告: {e}")

    # 2. 檢查數據
    data_path = root_dir / "data" / market_id / "dayK"
    csv_files = list(data_path.glob("*.csv"))
    if not csv_files:
        print("❌ 錯誤：找不到 CSV 檔案，終止流程。")
        return

    # 3. 執行分析與發信
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 獲取 AI 分析
            ai_result = get_gemini_analysis(market_id, text_reports)
            text_reports["🤖 Gemini 智能深度解讀"] = ai_result
            
            # 發送郵件
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            print(f"✅ {market_id} 任務全線完成！")
        else:
            print("❌ 分析失敗：數據結果為空。")
    except Exception as e:
        print(f"❌ 流程執行異常: {e}")

if __name__ == "__main__":
    main()
