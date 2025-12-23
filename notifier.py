# -*- coding: utf-8 -*-
import os
import requests
import resend
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        """
        初始化通知模組
        自動從 GitHub Secrets 或環境變數讀取金鑰
        """
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time(self):
        """獲取台北時間 (UTC+8)"""
        # GitHub Actions 預設是 UTC，手動加 8 小時
        return (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """發送即時訊息到 Telegram"""
        if not self.tg_token or not self.tg_chat_id:
            print("⚠️ 缺少 Telegram 設定，跳過發送。")
            return False
        
        # 加入時間戳記在訊息底部
        ts = self.get_now_time().split(" ")[1] # 取得 HH:MM:SS
        full_message = f"{message}\n\n🕒 <i>Sent at {ts} (UTC+8)</i>"
        
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id, 
            "text": full_message, 
            "parse_mode": "HTML"
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Telegram 發送失敗: {e}")
            return False

    def send_report(self, market, status, count, detail=""):
        """
        透過 Resend 發送 Email 專業報表
        """
        if not self.resend_api_key:
            print("⚠️ 缺少 Resend API Key，跳過發送。")
            return False

        report_time = self.get_now_time()
        market_name = market.upper()
        
        # 根據狀態決定顏色
        theme_color = "#28a745" if status == "Success" else "#dc3545"
        status_text = "更新成功" if status == "Success" else "更新失敗"

        subject = f"📊 {market_name} 股市矩陣監控報表 - {status_text}"
        
        html_content = f"""
        <html>
        <body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6;">
            <div style="max-width: 600px; margin: 20px auto; border: 1px solid #e0e0e0; border-top: 8px solid {theme_color}; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                <div style="padding: 20px; background-color: #f8f9fa;">
                    <h2 style="margin: 0; color: {theme_color};">{market_name} 全方位市場監控報表</h2>
                    <p style="margin: 5px 0; color: #666; font-size: 14px;">報告生成時間: {report_time} (UTC+8)</p>
                </div>
                
                <div style="padding: 20px;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 10px; border-bottom: 1px solid #eee; font-weight: bold;">市場區域</td>
                            <td style="padding: 10px; border-bottom: 1px solid #eee;">{market_name}</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; border-bottom: 1px solid #eee; font-weight: bold;">處理狀態</td>
                            <td style="padding: 10px; border-bottom: 1px solid #eee; color: {theme_color}; font-weight: bold;">{status_text}</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; border-bottom: 1px solid #eee; font-weight: bold;">成功同步數量</td>
                            <td style="padding: 10px; border-bottom: 1px solid #eee; font-size: 18px; font-weight: bold;">{count}</td>
                        </tr>
                    </table>
                    
                    <div style="margin-top: 20px; padding: 15px; background-color: #fff4f4; border-radius: 5px; font-size: 14px; border-left: 4px solid #ccc;">
                        <strong>詳情備註：</strong><br>
                        {detail}
                    </div>
                </div>
                
                <div style="padding: 15px; background-color: #f1f1f1; text-align: center; font-size: 12px; color: #999;">
                    本郵件由 GitHub Actions 全自動運作系統發送。<br>
                    如果您收到此郵件，代表您的資料倉儲已完成每日同步任務。
                </div>
            </div>
        </body>
        </html>
        """

        try:
            # 注意: 'from' 必須是 resend 驗證過的網域，或者預設的 onboarding@resend.dev
            resend.Emails.send({
                "from": "StockMonitor <onboarding@resend.dev>",
                "to": "your_email@example.com", # <--- 在這裡填入你的 Email
                "subject": subject,
                "html": html_content
            })
            print(f"📧 {market_name} 郵件報告發送成功")
            return True
        except Exception as e:
            print(f"❌ Email 發送失敗: {e}")
            return False
