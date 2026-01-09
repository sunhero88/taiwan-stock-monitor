import os
import argparse
import importlib
import subprocess
from pathlib import Path
from openai import OpenAI

def get_ai_analysis(market_name, text_reports):
    """呼叫 OpenAI API 產出分析報告"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key: return "（未提供 AI 分析報告：找不到金鑰）"
    
    summary = "\n".join([f"[{k}]\n{v[:500]}" for k, v in text_reports.items()])
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"你是一位股市分析師，請針對以下 {market_name} 數據摘要提供簡短繁體中文報告：\n{summary}"}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"（AI 分析失敗: {e}）"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, required=True)
    args = parser.parse_args()
    market_id = args.market

    # 💡 強制建立資料夾，確保下載與分析的路徑一致
    Path(f"data/{market_id}/dayK").mkdir(parents=True, exist_ok=True)

    # 1. 執行數據下載
    module_prefix = market_id.split('-')[0]
    module_name = f"downloader_{module_prefix}"
    print(f"📡 正在準備下載 {market_id} 數據...")
    
    try:
        # 使用 subprocess 並傳遞市場參數
        subprocess.run(["python", f"{module_name}.py", "--market", market_id], check=True)
        print(f"✅ {market_id} 數據下載成功")
    except Exception as e:
        print(f"⚠️ 下載階段警告: {e}")

    # 2. 執行分析器
    try:
        import analyzer
        print(f"📊 正在啟動 {market_id.upper()} 深度矩陣分析...")
        # 調用分析器入口
        images, df_res, text_reports = analyzer.run(market_id)
        
        # 檢查數據內容
        if df_res is None or (hasattr(df_res, 'empty') and df_res.empty):
            print(f"⚠️ {market_id} 分析數據為空，請檢查 data/{market_id}/dayK 是否有 CSV 檔案。")
            return

        # 3. 獲取 AI 分析
        ai_result = get_ai_analysis(market_id, text_reports)
        text_reports["🤖 AI 智能分析報告"] = ai_result

        # 4. 發送郵件 (對接 StockNotifier)
        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        notifier_inst.send_stock_report(
            market_name=market_id.upper(),
            img_data=images,
            report_df=df_res,
            text_reports=text_reports
        )
        print(f"✅ {market_id} 監控報告處理完成！")
        
    except Exception as e:
        print(f"❌ 分析或寄送過程發生錯誤: {e}")

if __name__ == "__main__":
    main()
