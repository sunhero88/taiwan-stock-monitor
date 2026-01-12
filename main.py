# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path
import google.generativeai as genai

def get_gemini_analysis(market_name, text_reports):
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: return "（未配置 GEMINI_API_KEY）"
    
    # 確保摘要內容不會過長導致 API 報錯
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"你是一位專業分析師。請針對以下 {market_name} 數據摘要提供繁體中文分析：\n{summary}"
        
        # 💡 增加安全檢查，確保 response 存在
        response = model.generate_content(prompt)
        if response and response.text:
            return response.text
        return "（AI 產出內容為空）"
    except Exception as e:
        print(f"⚠️ Gemini API 呼叫異常: {e}")
        return f"（AI 分析失敗: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    # 1. 下載與分析 (維持原本成功的邏輯)
    try:
        # 下載數據
        module_name = f"downloader_{market_id.split('-')[0]}"
        subprocess.run(["python", f"{module_name}.py", "--market", market_id], cwd=root_dir, check=True)
        
        # 執行分析器
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 💡 確保 AI 分析不會中斷主流程
            ai_result = get_gemini_analysis(market_id, text_reports)
            text_reports["🤖 Gemini 智能深度分析"] = ai_result
            
            # 💡 執行寄信
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            print(f"✅ {market_id} 任務全線完成，郵件已發出！")
        else:
            print("❌ 分析失敗：數據為空，無法進入發信程序。")
            
    except Exception as e:
        print(f"❌ 流程執行中斷: {e}")

if __name__ == "__main__":
    main()
