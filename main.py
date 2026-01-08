# -*- coding: utf-8 -*-
import os
import resend
import argparse
from datetime import datetime
from analyzer import StockAnalyzer 

def send_resend_email(report_html, market_name):
    """ 使用 Resend API 發送郵件 """
    # 從 GitHub Secrets 傳遞進來的環境變數
    api_key = os.environ.get('EMAIL_PASS') 
    to_email = os.environ.get('EMAIL_USER') 

    if not api_key or not to_email:
        print(f"❌ 錯誤：找不到環境變數。Key存在: {bool(api_key)}, 收件人存在: {bool(to_email)}")
        return

    resend.api_key = api_key
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 寄件人必須是你驗證過的域名 @twstock.cc
    params = {
        "from": "Stock Monitor <report@twstock.cc>",
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', default='tw-share')
    args = parser.parse_args()

    market_map = {
        "tw-share": "台股", "us-share": "美股", "hk-share": "港股",
        "cn-share": "陸股", "jp-share": "日股", "kr-share": "韓股"
    }
    
    m_id = args.market
    m_name = market_map.get(m_id, m_id)

    analyzer = StockAnalyzer()
    try:
        # 執行分析
        images, df_res, text_reports = analyzer.run(m_id)

        if df_res.empty:
            print(f"⚠️ {m_name} 無數據，跳過發信。")
            return

        # 組合 HTML 報表
        report_content = f"<h2>{m_name} 市場分析報告 ({datetime.now().strftime('%Y-%m-%d')})</h2>"
        for period, table in text_reports.items():
            report_content += f"<h3>{period} 區間分布</h3><pre style='background:#f4f4f4;padding:10px;'>{table}</pre><hr>"

        send_resend_email(report_content, m_name)

    except Exception as e:
        print(f"❌ 處理 {m_id} 時發生錯誤: {str(e)}")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()


