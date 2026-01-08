# -*- coding: utf-8 -*-
import os
import resend
from datetime import datetime
from analyzer import StockAnalyzer  # 確保你的 analyzer.py 已經改好我上次給你的版本

def send_resend_email(report_html, market_name):
    """
    使用 Resend API 發送郵件
    """
    # 從 GitHub Secrets 讀取你貼上的 re_ 開頭字串
    api_key = os.environ.get('EMAIL_PASS') 
    if not api_key:
        print("❌ 錯誤：找不到 EMAIL_PASS (Resend API Key) 環境變數")
        return

    resend.api_key = api_key
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 寄件人必須是你驗證過的域名
    from_email = "Stock Monitor <report@twstock.cc>"
    # 收件人請改為你的 Gmail (或維持從環境變數讀取)
    to_email = os.environ.get('EMAIL_USER') 

    params = {
        "from": from_email,
        "to": [to_email],
        "subject": f"📈 {market_name} 股市分析報告 - {today}",
        "html": report_html
    }

    try:
        print(f"🚀 正在發送 {market_name} 報告至 {to_email}...")
        r = resend.Emails.send(params)
        print(f"✅ 郵件發送成功！ID: {r['id']}")
    except Exception as e:
        print(f"❌ 郵件發送失敗：{str(e)}")

def main():
    # 定義要分析的市場
    markets = {
        "tw-share": "台股",
        "us-share": "美股",
        "hk-share": "港股"
    }

    analyzer = StockAnalyzer()

    for m_id, m_name in markets.items():
        try:
            # 執行分析 (這會呼叫你 analyzer.py 中的邏輯)
            images, df_res, text_reports = analyzer.run(m_id)

            if df_res.empty:
                print(f"⚠️ {m_name} 無數據可分析，跳過。")
                continue

            # 組合簡單的 HTML 內容
            # 注意：Resend 免費版暫不支持直接傳送多張大圖附件，建議先發送文字報表
            report_content = f"<h2>{m_name} 今日行情總覽 ({datetime.now().strftime('%Y-%m-%d')})</h2>"
            for period, table in text_reports.items():
                report_content += f"<h3>{period} 區間分布</h3><pre>{table}</pre><hr>"

            # 執行發信
            send_resend_email(report_content, m_name)

        except Exception as e:
            print(f"❌ 處理 {m_name} 時發生崩潰: {str(e)}")

if __name__ == "__main__":
    main()

