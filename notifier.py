# -*- coding: utf-8 -*-
import os, resend
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_stock_report(self, market_name, img_data, report_df, text_reports):
        if not self.resend_api_key: 
            print("❌ 未偵測到 API KEY，取消寄送。")
            return False
            
        report_time = self.get_now_time_str()
        ai_report = text_reports.get("FINAL_AI_REPORT", "（AI 摘要生成中...）")
        
        html_content = f"""
        <html><body style="font-family: sans-serif; color: #333;">
            <div style="max-width: 800px; margin: auto; border: 1px solid #ddd; padding: 20px; border-top: 8px solid #1a73e8;">
                <h2>📈 {market_name} 智能監控報告</h2>
                <p>時間: {report_time}</p>
                <div style="background: #f0f7ff; padding: 15px; border-left: 5px solid #1a73e8; margin: 20px 0;">
                    <h3 style="margin-top:0; color: #0d47a1;">🤖 AI 戰略標籤判讀</h3>
                    <div style="white-space: pre-wrap; font-size: 15px;">{ai_report}</div>
                </div>
        """
        
        # 插入績效清單
        for period, report in text_reports.items():
            if "FINAL" in period or "SESSION" in period: continue
            html_content += f'<h4>📊 {period}</h4><pre style="background:#2d3436; color:#fff; padding:10px; font-size:12px;">{report}</pre>'
        
        html_content += "</div></body></html>"

        try:
            receiver_email = os.getenv("REPORT_RECEIVER_EMAIL", "sunhero88@gmail.com")
            resend.Emails.send({
                "from": "PredatorSystem <onboarding@resend.dev>",
                "to": receiver_email,
                "subject": f"🚀 Predator V14.0 - {market_name}",
                "html": html_content
            })
            print(f"✅ 郵件報告已寄送至 {receiver_email}")
            return True
        except Exception as e:
            print(f"❌ 寄送失敗: {e}")
            return False
