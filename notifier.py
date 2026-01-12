# -*- coding: utf-8 -*-
import os, requests, resend
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        if not self.tg_token or not self.tg_chat_id: return False
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {"chat_id": self.tg_chat_id, "text": message, "parse_mode": "HTML"}
        try:
            requests.post(url, json=payload, timeout=10)
            return True
        except: return False

    def send_stock_report(self, market_name, img_data, report_df, text_reports):
        if not self.resend_api_key: return False
        report_time = self.get_now_time_str()
        
        # 💡 重要：獲取 main.py 注入的 AI 內容
        ai_report = text_reports.get("🤖 AI 智能分析報告", "（AI 分析摘要生成中...）")
        
        # 構建 HTML (簡化結構確保不被 Spam 攔截)
        html_content = f"""
        <html><body style="font-family: sans-serif; color: #333;">
            <h2>📈 {market_name} 智能監控報告</h2>
            <p>生成時間: {report_time}</p>
            <div style="background: #e3f2fd; padding: 15px; border-left: 5px solid #1a73e8; margin: 20px 0;">
                <h3 style="margin-top:0;">🤖 AI 深度解讀</h3>
                <div style="white-space: pre-wrap;">{ai_report}</div>
            </div>
        """
        # 插入圖表
        for img in img_data:
            html_content += f'<h3>📍 {img["label"]}</h3><img src="cid:{img["id"]}" style="width:100%; max-width:700px;"><br>'

        # 插入數據明細
        for period, report in text_reports.items():
            if "AI" in period: continue
            html_content += f'<h4>📊 {period} 報酬分布明細</h4><pre style="background:#2d3436; color:#fff; padding:10px;">{report}</pre>'

        html_content += "</body></html>"

        # 處理附件
        attachments = []
        for img in img_data:
            if os.path.exists(img['path']):
                with open(img['path'], "rb") as f:
                    attachments.append({
                        "content": list(f.read()),
                        "filename": f"{img['id']}.png",
                        "content_id": img['id'],
                        "disposition": "inline"
                    })

        try:
            receiver_email = os.getenv("REPORT_RECEIVER_EMAIL", "sunhero88@gmail.com")
            resend.Emails.send({
                "from": "StockMonitor <onboarding@resend.dev>", # 💡 免費版建議先用此發信地址
                "to": receiver_email,
                "subject": f"🚀 {market_name} 監控報告 - {report_time}",
                "html": html_content,
                "attachments": attachments
            })
            self.send_telegram(f"📊 {market_name} 報告已寄送！")
            return True
        except Exception as e:
            print(f"❌ 寄送失敗: {e}")
            return False
