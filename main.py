# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path
import google.generativeai as genai

def get_gemini_analysis(market_name, text_reports):
    """
    [AI-DEBUG] 此函數負責呼叫 Gemini 並回傳分析
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: 
        print("[AI-DEBUG] ❌ 錯誤：找不到 GEMINI_API_KEY 環境變數")
        return "（未配置 GEMINI_API_KEY）"
    
    # 限制內容長度，避免 Token 過長
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    
    try:
        # 1. 配置 API
        genai.configure(api_key=api_key)
        
        # 2. 指定模型（💡 移除 models/ 前綴）
        model_id = 'gemini-1.5-flash'
        print(f"[AI-DEBUG] 📡 正在準備呼叫模型: {model_id}")
        
        model = genai.GenerativeModel(model_id)
        
        prompt = f"""
        你是一位專業股市操盤手。請針對以下 {market_name} 數據摘要提供繁體中文分析。
        包含：盤勢強弱、核心權值股動態、操作建議。
        
        數據內容：
        {summary}
        """
        
        # 3. 呼叫生成
        response = model.generate_content(prompt)
        
        if response and response.text:
            print("[AI-DEBUG] ✅ AI 分析生成成功")
            return response.text
        return "（AI 產出內容為空）"
        
    except Exception as e:
        # 💡 這一行會在 GitHub 日誌中揭露 404 的真相
        error_msg = str(e)
        print(f"[AI-DEBUG] ❌ 呼叫失敗，錯誤詳情: {error_msg}")
        return f"（智能解讀生成失敗: {error_msg}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market
    root_dir = Path(__file__).parent.absolute()

    print(f"🚀 啟動任務: {market_id}")

    # 1. 執行下載器
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    try:
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"⚠️ 下載階段警告: {e}")

    # 2. 執行分析器
    try:
        import analyzer
        print(f"📊 正在分析數據...")
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is not None and not df_res.empty:
            # 3. 獲取 AI 分析
            print("🤖 進入 AI 分析環節...")
            ai_result = get_gemini_analysis(market_id, text_reports)
            
            # 💡 確保標籤名稱與 notifier.py 完全對接
            text_reports["實時 AI 點評"] = ai_result # 修改標籤為更簡單的名稱
            
            # 4. 發送郵件
            print("📬 正在準備發送郵件...")
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
