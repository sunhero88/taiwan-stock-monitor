# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path
import google.generativeai as genai

def get_gemini_analysis(market_name, text_reports):
    """
    徹底修正 404 錯誤，確保不帶 models/ 前綴
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: return "（未配置 GEMINI_API_KEY）"
    
    # 彙整摘要
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    
    try:
        genai.configure(api_key=api_key)
        # 💡 核心修正：直接使用名稱，不帶任何路徑前綴
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = f"你是一位專業分析師。請針對以下 {market_name} 數據摘要提供繁體中文點評：\n{summary}"
        
        response = model.generate_content(prompt)
        if response and response.text:
            return response.text
        return "（AI 產出內容為空）"
    except Exception as e:
        # 如果 1.5-flash 持續失敗，嘗試降級到 gemini-pro
        try:
            model = genai.GenerativeModel('gemini-pro')
            response = model.generate_content(f"請分析數據：\n{summary}")
            return response.text
        except:
            return f"（智能解讀生成失敗: {e}）"

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
        print(f"⚠️ 下載警告: {e}")

    # 2. 執行分析
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 3. 獲取 AI 分析
            ai_result = get_gemini_analysis(market_id, text_reports)
            
            # 💡 重要：標籤必須與 notifier.py 絕對一致
            text_reports["FINAL_AI_REPORT"] = ai_result
            
            # 4. 發送郵件
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
