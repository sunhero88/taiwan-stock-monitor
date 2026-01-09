import os
import argparse
import importlib
import subprocess
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

    # 1. 執行下載 (修正 AttributeError)
    module_name = f"downloader_{market_id.split('-')[0]}"
    try:
        print(f"📡 正在啟動下載模組: {module_name}")
        # 嘗試導入並尋找 main()，若失敗則直接用系統指令執行檔案
        try:
            mod = importlib.import_module(module_name)
            if hasattr(mod, 'main'):
                mod.main()
            else:
                subprocess.run(["python", f"{module_name}.py"], check=True)
        except:
            subprocess.run(["python", f"{module_name}.py"], check=True)
    except Exception as e:
        print(f"⚠️ 下載階段警告: {e}")

    # 2. 執行分析
    try:
        import analyzer
        images, df_res, text_reports = analyzer.run(market_id)
        if df_res.empty:
            print("⚠️ 分析數據為空，無法產出報告。")
            return

        # 3. 獲取 AI 分析
        ai_report = get_ai_analysis(market_id, text_reports)
        text_reports["🤖 AI 智能分析報告"] = ai_report

        # 4. 發信 (對接 StockNotifier)
        from notifier import StockNotifier
        notifier_inst = StockNotifier()
        notifier_inst.send_stock_report(
            market_name=market_id.upper(),
            img_data=images,
            report_df=df_res,
            text_reports=text_reports
        )
        print(f"✅ {market_id} 任務全線完成！")
    except Exception as e:
        print(f"❌ 分析或通知失敗: {e}")

if __name__ == "__main__":
    main()
