import subprocess
import sys
import time
from datetime import datetime

class PredatorWorkflow:
    def __init__(self):
        self.start_time = time.time()
        self.steps = [
            {"name": "數據下載 (download_data.py)", "script": "download_data.py"},
            {"name": "數據品質稽核與量能計算 (market_amount.py)", "script": "market_amount.py"},
            {"name": "決策引擎報表生成 (macro_generator.py)", "script": "macro_generator.py"}
        ]

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

    def run_step(self, step_name, script_name):
        self.log(f"▶️ 正在執行: {step_name}...")
        try:
            # 執行腳本並捕捉錯誤輸出
            result = subprocess.run(
                [sys.executable, script_name],
                check=True,
                capture_output=False, # 設為 False 讓子腳本的 print 直接顯示在終端
                text=True
            )
            self.log(f"✅ {step_name} 執行成功。")
            return True
        except subprocess.CalledProcessError as e:
            self.log(f"❌ 嚴重錯誤: {step_name} 失敗 (Exit Code: {e.returncode})")
            return False
        except Exception as e:
            self.log(f"❌ 未知異常: {str(e)}")
            return False

    def start(self):
        print("="*60)
        print(f"🐉 Predator V16.3.5 自動化工作流啟動")
        print("="*60)

        for step in self.steps:
            success = self.run_step(step["name"], step["script"])
            if not success:
                print("\n" + "!"*60)
                self.log("🛑 工作流偵測到數據完整性異常，已強制終止後續任務！")
                self.log("請檢查上方錯誤訊息並修復數據源。")
                print("!"*60 + "\n")
                sys.exit(1)

        total_time = round(time.time() - self.start_time, 2)
        print("="*60)
        self.log(f"🎉 全流程順序執行完成！總耗時: {total_time} 秒")
        self.log("market_status 已回歸 OK，可以進行交易決策。")
        print("="*60)

if __name__ == "__main__":
    workflow = PredatorWorkflow()
    workflow.start()
