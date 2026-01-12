# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path
import google.generativeai as genai

def get_gemini_analysis(market_name, text_reports):
    """
    修正 Gemini 404 報錯並生成盤勢分析
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: 
        return "（⚠️ 未配置 GEMINI_API_KEY，請檢查 GitHub Secrets）"
    
    # 彙整數據摘要，限制長度以符合 API 規範
    summary = "\n".join([f"[{k}]\n{v[:600]}" for k, v in text_reports.items()])
    
    try:
        # 1. 配置 API
        genai.configure(api_key=api_key)
        
        # 2. 指定模型：💡 關鍵修正，直接寫名稱，不帶 models/ 前綴
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = f"""
        你是一位專業股市操盤手，擅長從數據中洞察趨勢。
        請針對以下 {market_name} 的數據摘要，提供一份精煉的繁體中文分析報告。
        
        報告要求：
        1. 總結當前盤勢的強弱結構。
        2. 針對核心權值股（如台積電、鴻海、台達電等）提供數據解讀。
        3. 給予短期操作的風險預警或機會提示。
        
        數據內容：
        {summary}
        """
        
        # 3. 執行生成
        response = model.generate_content(prompt)
        
        if response and response.text:
            return response.text
        return "（AI 產出內容為空，請檢查 API 額度或狀態）"
        
    except Exception as e:
        print(f"⚠️ Gemini API 調用異常: {e}")
        # 如果是 404 錯誤，通常是模型名稱字串格式問題
        return f"（智能解讀暫時不可用，錯誤訊息: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    
    # 獲取專案根目錄絕對路徑
    root_dir = Path(__file__).parent.absolute()
    
    print(f"🚀 啟動任務: {market_id}")

    # 1. 下載數據 (呼叫 downloader_tw.py 等)
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    
    print(f"📡 步驟 1. 執行下載器: {downloader_script}")
    try:
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"⚠️ 下載階段警告: {e}")

    # 2. 檢查數據路徑與 CSV 檔案
    data_path = root_dir / "data" / market_id / "dayK"
    csv_files = list(data_path.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ 錯誤：在 {data_path} 找不到 CSV 檔案，終止流程。")
        return

    # 3. 執行分析與發信
    try:
        import analyzer
        print(f"📊 步驟 2. 執行 {market_id} 深度分析...")
        # 取得圖表、結果 DataFrame 以及文字摘要
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 4. 獲取 AI 智能分析
            print("🤖 步驟 3. 請求 Gemini AI 進行盤勢解讀...")
            ai_result = get_gemini_analysis(market_id, text_reports)
            
            # 💡 關鍵修正：標籤名稱必須與 notifier.py 的 ai_report 變數對接
            text_reports["🤖 AI 智能分析報告"] = ai_result

            # 5. 發送報告 (notifier.py)
            print("📬 步驟 4. 封裝報告並寄送郵件...")
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(
                market_name=market_id.upper(),
                img_data=images,
                report_df=df_res,
                text_reports=text_reports
            )
            print(f"✅ {market_id} 任務全線完成！")
        else:
            print("❌ 分析失敗：數據結果為空。")

    except Exception as e:
        print(f"❌ 流程執行異常: {e}")

if __name__ == "__main__":
    main()
