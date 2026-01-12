# -*- coding: utf-8 -*-
import os
import argparse
import subprocess
from pathlib import Path
import google.generativeai as genai

def get_gemini_analysis(market_name, text_reports):
    """
    使用 Google Gemini API 進行智能分析
    修正了 image_69139a.png 中出現的 404 模型找不到問題
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return "（⚠️ 未配置 GEMINI_API_KEY，請檢查 GitHub Secrets）"
    
    # 彙整分析數據摘要
    summary = "\n".join([f"[{k}]\n{v[:600]}" for k, v in text_reports.items()])
    
    try:
        # 配置 Gemini SDK
        genai.configure(api_key=api_key)
        
        # 💡 使用最穩定的模型標識符，解決 404 錯誤
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = f"""
        你是一位專業的股市操盤手，擅長從數據中洞察趨勢。
        請針對以下 {market_name} 的數據摘要，提供一份精煉的繁體中文分析報告。
        
        報告要求：
        1. 總結當前盤勢的強弱結構。
        2. 針對核心權值股（如台積電、鴻海等）提供觀點。
        3. 給予短期操作的風險預警或機會提示。
        
        數據內容：
        {summary}
        """
        
        # 執行生成
        response = model.generate_content(prompt)
        
        if response and response.text:
            return response.text
        return "（AI 產出內容為空，請確認 API 狀態）"
        
    except Exception as e:
        print(f"⚠️ Gemini API 調用異常: {e}")
        # 如果是 404 錯誤，通常是模型名稱或 API 權限問題
        return f"（智能分析暫時不可用，錯誤代碼: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 獲取專案根目錄絕對路徑
    root_dir = Path(__file__).parent.absolute()
    
    print(f"🚀 開始執行 {market_id.upper()} 監控任務...")

    # 1. 執行數據下載 (呼叫 downloader_tw.py 等)
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    
    print(f"📡 步驟 1. 啟動下載器: {downloader_script}")
    try:
        # 確保在根目錄執行子程序
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"⚠️ 數據下載階段出現警告: {e}")

    # 2. 檢查數據路徑與 CSV 檔案
    data_path = root_dir / "data" / market_id / "dayK"
    csv_files = list(data_path.glob("*.csv"))
    print(f"🔍 步驟 2. 路徑檢查: 在 {data_path} 發現 {len(csv_files)} 個 CSV 檔案")

    if len(csv_files) == 0:
        print("❌ 錯誤：找不到數據檔案，請確認下載器是否正常運作。")
        return

    # 3. 執行分析器 (analyzer.py)
    try:
        import analyzer
        print(f"📊 步驟 3. 執行 {market_id} 深度矩陣分析...")
        # 取得統計圖表、結果 DataFrame 以及文字摘要
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is None or (hasattr(df_res, 'empty') and df_res.empty):
            print("❌ 錯誤：分析結果為空，停止後續流程。")
            return

        # 4. 獲取 AI 智能分析
        print("🤖 步驟 4. 請求 Gemini AI 進行盤勢解讀...")
        ai_result = get_gemini_analysis(market_id, text_reports)
        text_reports["🤖 Gemini 智能深度解讀"] = ai_result

        # 5. 發送報告 (notifier.py)
        print("📬 步驟 5. 封裝報告並寄送郵件...")
        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        notifier_inst.send_stock_report(
            market_name=market_id.upper(),
            img_data=images,
            report_df=df_res,
            text_reports=text_reports
        )
        print(f"✅ {market_id} 任務全線完成，請檢查您的信箱！")

    except Exception as e:
        print(f"❌ 流程執行異常: {e}")

if __name__ == "__main__":
    main()
