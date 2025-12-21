# -*- coding: utf-8 -*-
import os
import resend
from datetime import datetime

def send_stock_report(market_name, img_data, report_df, text_reports):
    """
    發送包含 9 張圖與動態技術圖表連結的電子郵件
    """
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key: return print("❌ 缺少 RESEND_API_KEY")
    resend.api_key = api_key

    now_str = datetime.now().strftime("%Y-%m-%d")
    
    # 🕵️ 判斷市場別以決定超連結模板
    # 我們可以從 report_df 的 Ticker 範例來判斷，或者簡單用 market_name 判定
    is_us = "美國" in market_name or "US" in market_name.upper()

    # 建立 Top 50 連結區塊
    def get_top50_links(df, col_name):
        # 確保排序欄位存在
        if col_name not in df.columns: return "無數據"
        
        top50 = df.sort_values(by=col_name, ascending=False).head(50)
        links = []
        for _, r in top50.iterrows():
            ticker = r["Ticker"]
            # ✅ 動態超連結邏輯
            if is_us:
                url = f"https://stockcharts.com/sc3/ui/?s={ticker}"
            else:
                clean_tkr = ticker.split('.')[0]
                url = f"https://www.wantgoo.com/stock/{clean_tkr}/technical-chart"
            
            # 使用 Full_Name ( analyzer.py 傳過來的欄位 )
            name = r.get("Full_Name", r.get("Ticker"))
            links.append(f'<a href="{url}" style="text-decoration:none; color:#0366d6;">{ticker}({name})</a>')
        
        return " | ".join(links)

    # 組合 HTML 內容
    html_content = f"""
    <div style="font-family: sans-serif; color: #333; max-width: 900px; margin: auto;">
        <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">
            📈 {market_name} 全方位監控報表 ({now_str})
        </h2>
    """
    
    # 插入 9 張圖表 (垂直排列)
    for img in img_data:
        html_content += f"<h3 style='margin-top:40px; color:#2c3e50;'>📍 {img['label']}</h3>"
        html_content += f'<img src="cid:{img["id"]}" style="width:100%; max-width:850px; border:1px solid #eee; border-radius:8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">'

    # 插入文字清單
    html_content += "<div style='background:#f4f7f6; padding:20px; border-radius:10px; margin-top:40px;'>"
    for period, report in text_reports.items():
        p_zh = {"Week":"週", "Month":"月", "Year":"年"}.get(period, period)
        html_content += f"<h4 style='color:#16a085;'>📊 {p_zh}K 最高價分箱清單 (含 >100% 飆股)</h4>"
        # 使用 pre 標籤保留分箱間隔格式
        html_content += f"<pre style='background:#fff; padding:15px; border:1px solid #ddd; font-size:12px; overflow-x:auto; white-space: pre-wrap;'>{report}</pre>"
    html_content += "</div>"

    # 插入 Top 50 進攻標的
    html_content += f"<hr style='margin-top:40px;'><h4>🔥 本週最強動能前 50 名 (點擊代號跳轉 { 'StockCharts' if is_us else '玩股網' })</h4>"
    html_content += f"<p style='line-height:2; font-size:14px;'>{get_top50_links(report_df, 'Week_High')}</p>"
    html_content += "</div>"

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

    # 設定收件人 (建議從環境變數讀取，保護隱私)
    receiver_email = "grissomlin643@gmail.com"

    try:
        resend.Emails.send({
            "from": "StockMonitor <onboarding@resend.dev>",
            "to": [receiver_email],
            "subject": f"🚀 {market_name} 全方位監控報告 - {now_str}",
            "html": html_content,
            "attachments": attachments
        })
        print(f"✅ 郵件發送成功！({market_name})")
    except Exception as e:
        print(f"❌ 郵件發送失敗: {e}")