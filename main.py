# -*- coding: utf-8 -*-
import os
import argparse
import subprocess
from pathlib import Path
from openai import OpenAI

def get_ai_analysis(market_name, text_reports):
    """呼叫 OpenAI API，並針對額度問題做保護"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return "（未提供 AI 金鑰，請檢查 GitHub Secrets）"
    
    # 彙整分析數據給 AI
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": "你是一位專業股市分析師，請使用繁體中文提供精簡的盤勢解讀。"},
                      {"role": "user", "content": f"分析以下 {market_name} 數據：\n{summary}"}]
        )
        return response.choices[0].message.content
    except Exception as e:
        # 如果額度用光，回傳友善提示而非讓程式崩潰
        if "insufficient_quota" in str(e):
            return "（⚠️ AI 分析暫時失效：OpenAI 帳戶額度已用盡或點數不足，請至官網充值）"
        return f"（AI 分析失敗: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', required=True)
    args = parser.parse_args()
    market_id = args.market

    # 💡 獲取專案根目錄
    root_dir = Path(__file__).parent.absolute()
    
    # 1. 執行下載 (呼叫您剛剛修正的 downloader_tw.py)
    module_prefix = market_id.split('-')[0]
    downloader_script = f"downloader_{module_prefix}.py"
    
    print(f"📡 步驟 1. 啟動下載器: {downloader_script}")
    try:
        # 確保在根目錄執行，避免路徑錯亂
        subprocess.run(["python", downloader_script, "--market", market_id], cwd=root_dir, check=True)
    except Exception as e:
        print(f"❌ 下載失敗: {e}")

    # 2. 檢查數據路徑
    data_path = root_dir / "data" / market_id / "dayK"
    csv_files = list(data_path.glob("*.csv"))
    print(f"🔍 步驟 2. 路徑檢查: 在 {data_path} 掃描到 {len(csv_files)} 個 CSV 檔案")

    if len(csv_files) == 0:
        print("❌ 錯誤：找不到數據檔案，請檢查下載器存檔路徑是否正確。")
        return

    # 3. 執行分析器 (analyzer.py 不需要大改，只要路徑對了就能動)
    try:
        import analyzer
        print(f"📊 步驟 3. 執行 {market_id} 深度分析...")
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is None or (hasattr(df_res, 'empty') and df_res.empty):
            print("❌ 錯誤：分析結果為空。")
            return

        # 4. 獲取 AI 分析
        ai_result = get_ai_analysis(market_id, text_reports)
        text_reports["🤖 AI 智能分析報告"] = ai_result

        # 5. 寄送郵件 (notifier.py 不需要改，只要 Key 正確即可)
        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
        print(f"✅ {market_id} 任務全線完成，請查看信箱！")

    except Exception as e:
        print(f"❌ 流程執行
