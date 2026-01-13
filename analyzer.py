# analyzer.py
import pandas as pd
import yfinance as yf
import datetime
from pathlib import Path

def run(market):
    """
    生成 V12.3 智能分析報告
    回傳：images (list), df_res (DataFrame), text_reports (str), red_flags (list)
    """
    images = []  # 暫不生成圖片，未來可加 matplotlib

    # 抓取即時三大法人數據（模擬，未來可從 TWSE API 真抓）
    df_res = pd.DataFrame({
        '法人類別': ['外資及陸資', '投信', '自營商', '合計'],
        '買進(億)': [1620.5, 150.2, 250.8, '-'],
        '賣出(億)': [1625.7, 200.4, 150.3, '-'],
        '買賣超(億)': [60.43, 11.81, 1.82, 74.06]
    })

    # 模擬 V12.3 報告（未來可擴充真實邏輯）
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    text_reports = f"""
### 盤後終極戰報（V12.3） - {market.upper()} 市場 - {timestamp}

**今日大盤總覽**：加權指數收 30,105.04 點（漲 755.23 點，+2.57%），成交金額 7,669.32億元。

**三大法人買賣超**：
- 外資買超 60.43 億
- 投信買超 11.81 億
- 自營商買超 1.82 億

**主流族群 Top 3**：
- 電子零組件 +3.5%（動能 +18%）
- 半導體 +2.8%（台積電帶動）
- 電腦週邊 +2.2%（伺服器齊揚）

**操作建議**：
續抱現有部位，無重大紅旗觸發。

**保守帳戶**：總資產 1,200,000 元 (+20.00%)
**冒進帳戶**：總資產 1,650,000 元 (+65.00%)

**防禦警報**：目前現金比重正常，無需調整。
    """

    # 模擬紅旗（未來可從真實掃雷產生）
    red_flags = [
        "毛利率稀釋風險（N2製程預警）",
        "應收帳款回收天數惡化",
        "管理層對2026展望含糊"
    ]

    return images, df_res, text_reports, red_flags