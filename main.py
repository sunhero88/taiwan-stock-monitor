import os
import argparse
import subprocess
from pathlib import Path
from openai import OpenAI

def get_ai_analysis(market_name, text_reports):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return "（未提供 AI 金鑰）"
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"分析以下股市數據並給予簡短繁體中文報告：\n{summary}"}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"（AI 分析暫時失效，可能是額度問題：{e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 1. 執行下載
    module_prefix = market_id.split('-')[0]
    print(f"📡 執行下載器: downloader_{module_prefix}.py")
    try:
        # 強制傳遞參數給下載器
        subprocess.run(["python", f"downloader_{module_prefix}.py", "--market", market_id], check=True)
    except Exception as e:
        print(f"⚠️ 下載器執行異常: {e}")

    # 2. 【核心修補】掃描 CSV 檔案到底在哪裡
    # 如果分析器在 data/tw-share/dayK 找不到，我們就手動幫它對接
    target_path = Path(f"data/{market_id}/dayK")
    target_path.mkdir(parents=True, exist_ok=True)
    
    csv_files = list(Path(".").rglob("*.csv")) # 掃描整個目錄找 CSV
    print(f"🔍 掃描到 {len(csv_files)} 個 CSV 檔案")
    
    # 3. 執行分析
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        
        if df_res is None or (hasattr(df_res, 'empty') and df_res.empty):
            print("❌ 錯誤：分析數據仍為空。請確認下載器是否真的有抓到股票。")
            return

        # 4. AI 與發信
        ai_result = get_ai_analysis(market_id, text_reports)
        text_reports["🤖 AI 智能分析報告"] = ai_result

        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        notifier_inst.send_stock_report(market_id.upper(), images, df_res, text_reports)
        print(f"✅ {market_id} 任務完成！")
    except Exception as e:
        print(f"❌ 流程中斷: {e}")

if __name__ == "__main__":
    main()
