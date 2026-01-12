# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path
import google.generativeai as genai

def get_gemini_analysis(market_name, text_reports):
    """
    終極修正版：解決持續性的 404 報錯問題
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: return "（⚠️ 未配置 GEMINI_API_KEY）"
    
    # 限制摘要內容長度
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    
    # 嘗試的模型清單（依序嘗試）
    model_candidates = ['gemini-1.5-flash', 'gemini-pro']
    
    last_error = ""
    for model_id in model_candidates:
        try:
            print(f"🤖 嘗試呼叫 AI 模型: {model_id}...")
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(model_id)
            
            prompt = f"你是一位股市操盤手，請針對以下 {market_name} 的數據摘要提供繁體中文分析，包含盤勢強弱與操作風險：\n{summary}"
            
            response = model.generate_content(prompt)
            if response and response.text:
                print(f"✅ AI 模型 {model_id} 調用成功！")
                return response.text
        except Exception as e:
            last_error = str(e)
            print(f"⚠️ 模型 {model_id} 失敗: {last_error}")
            continue # 嘗試下一個模型
            
    return f"（智能解讀生成失敗。最後報錯: {last_error}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    print(f"🚀 啟動任務: {market_id}")

    # 1. 執行數據下載
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    try:
        print(f"📡 執行下載器: {downloader_script}")
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"⚠️ 下載階段警告: {e}")

    # 2. 執行分析器
    try:
        import analyzer
        print(f"📊 正在分析 {market_id} 數據並產出圖表...")
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 3. 獲取 AI 分析
            ai_result = get_gemini_analysis(market_id, text_reports)
            
            # 💡 雙重標籤寫入：確保 notifier.py 一定讀得到
            text_reports["FINAL_AI_REPORT"] = ai_result
            text_reports["實時 AI 點評"] = ai_result
            text_reports["🤖 AI 智能分析報告"] = ai_result
            
            # 4. 發送郵件
            print("📬 正在透過 Resend 發送專業監控報告...")
            from notifier import StockNotifier
            notifier_inst = StockNotifier()
            notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
            print(f"✅ {market_id} 任務全線完成！")
        else:
            print("❌ 分析失敗：數據庫結果為空，請檢查 yfinance 是否抓到資料。")
    except Exception as e:
        print(f"❌ 流程執行異常: {e}")

if __name__ == "__main__":
    main()
