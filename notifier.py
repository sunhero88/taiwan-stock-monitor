# -*- coding: utf-8 -*-
import os
import resend
from datetime import datetime

def send_stock_report(market_name, img_data, report_df, text_reports):
    """
    發送包含 9 張圖與技術圖表連結的電子郵件
    """
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key: return print("❌ 缺少 RESEND_API_KEY")
    resend.api_key = api_key

    now_str = datetime.now().strftime("%Y-%m-%d")
    
    # 建立 Top 50 連結區塊
    def get_top50_links(df, col_name):
        top50 = df.sort_values(by=col_name, ascending=False).head(50)
        return " | ".join([
            f'<a href="https://www.wantgoo.com/stock/{r["Ticker"]}/technical-chart" style="text-decoration:none; color:#0366d6;">{r["Ticker"]}({r["Full_ID"]})</a>'
            for _, r in top50.iterrows()
        ])

    # 組合 HTML 內容
    html_content = f"<h2>📈 {market_name} 全方位監控報表 ({now_str})</h2>"
    
    # 插入 9 張圖表 (垂直排列)
    for img in img_data:
        html_content += f"<h3>📍 {img['label']}</h3>"
        html_content += f'<img src="cid:{img["id"]}" style="width:100%; max-width:800px; margin-bottom:20px; border:1px solid #ddd;">'

    # 插入文字清單
    for period, report in text_reports.items():
        html_content += f"<h4>📊 {period} 最高價分箱清單</h4>"
        html_content += f"<pre style='background:#f9f9f9; padding:10px; font-size:12px;'>{report}</pre>"

    # 插入 Top 50 進攻標的
    html_content += "<hr><h4>🔥 週K漲幅前 50 名 (點擊看 K 線圖)</h4>"
    html_content += f"<p style='line-height:1.8;'>{get_top50_links(report_df, 'Week_High')}</p>"

    # 準備附件 (圖片嵌入)
    attachments = []
    for img in img_data:
        with open(img['path'], "rb") as f:
            attachments.append({
                "content": list(f.read()),
                "filename": f"{img['id']}.png",
                "content_id": img['id'],
                "disposition": "inline"
            })

    try:
        resend.Emails.send({
            "from": "StockMonitor <onboarding@resend.dev>",
            "to": ["grissomlin643@gmail.com"], # 這裡記得改！
            "subject": f"🚀 台股全方位監控報告 - {now_str}",
            "html": html_content,
            "attachments": attachments
        })
        print(f"✅ 郵件發送成功！({market_name})")
    except Exception as e:
        print(f"❌ 郵件發送失敗: {e}")

