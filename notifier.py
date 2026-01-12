# -*- coding: utf-8 -*-
import os
import requests
import resend
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        # 讀取環境變數
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        """獲取台北時間 (UTC+8)"""
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_stock_report(self, market_name, img_data, report_df, text_reports):
        """
        發送整合 AI 分析與圖表的專業郵件報告
        """
        if not self.resend_api_key:
            print("⚠️ 錯誤：找不到 RESEND_API_KEY，無法發送郵件。")
            return False

        report_time = self.get_now_time_str()
        
        # 💡 關鍵對接：嘗試從多個可能的標籤獲取 AI 內容，確保不落空
        ai_report = text_reports.get("實時 AI 點評", 
                    text_reports.get("🤖 AI 智能分析報告", 
                    "（AI 數據解讀生成中，請稍後...）"))

        # --- 構建響應式 HTML 內容 ---
        html_content = f"""
        <html>
        <body style="font-family: 'Microsoft JhengHei', sans-serif; color: #333; line-height: 1.6; max-width: 800px; margin: auto;">
            <div style="border: 1px solid #ddd; border-top: 8px solid #1a73e8; border-radius: 10px; padding: 20px;">
                <h2 style="color: #1a73e8; border-bottom: 2px solid #eee; padding-bottom: 10px;">📈 {market_name} 智能監控報告</h2>
                <p style="color: #666; font-size: 14px;">生成時間：{report_time} (台北時間)</p>

                <div style="background-color: #f0f7ff; border-left: 6px solid #1a73e8; padding: 15px; border-radius: 4px; margin: 20px 0;">
                    <h3 style="margin-top: 0; color: #0d47a1;">🤖 AI 專家深度解讀</h3>
                    <div style="white-space: pre-wrap; font-size: 15px; color: #1565c0;">{ai_report}</div>
                </div>

                <hr style="border: 0; border-top: 1px solid #eee; margin: 30px 0;">
        """

        # --- 插入分析圖表 (內嵌圖片) ---
        html_content += "<div style='text-align: center;'>"
        for img in img_data:
            html_content += f"""
            <div style="margin-bottom: 30px;">
                <h4 style="text-align: left; color: #2c3e50; border-left: 4px solid #3498db; padding-left: 10px;">📍 {img['label']}</h4>
                <img src="cid:{img['id']}" style="width: 100%; max-width: 700px; border-radius: 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
            </div>
            """
        html_content += "</div>"

        # --- 插入文字報酬明細 ---
        html_content += "<div style='margin-top: 30px;'>"
        for period, report in text_reports.items():
            # 跳過 AI 標籤，只印出 K線報酬明細
            if "AI" in period or "點評" in period:
                continue
            
            p_zh = {"Week": "週", "Month": "月", "Year": "年"}.get(period, period)
            html_content += f"""
            <div style="margin-bottom: 20px;">
                <h4 style="color: #16a085;">📊 {p_zh} K線報酬分布明細</h4>
                <pre style="background-color: #2d3436; color: #dfe6e9; padding: 15px; border-radius: 5px; font-size: 12px; white-space: pre-wrap; font-family: 'Courier New', monospace;">{report}</pre>
            </div>
            """
        html_content += "</div>"

        html_content += """
                <p style="font-size: 11px; color: #999; text-align: center; margin-top: 40px;">
                    此郵件由 Global Market Monitor 系統自動發送。數據僅供參考，不構成投資建議。
                </p>
            </div>
        </body>
        </html>
        """

        # --- 處理圖片附件 ---
        attachments = []
        for img in img_data:
            try:
                if os.path.exists(img['path']):
                    with open(img['path'], "rb") as f:
                        attachments.append({
                            "content": list(f.read()),
                            "filename": f"{img['id']}.png",
                            "content_id": img['id'],
                            "disposition": "inline"
                        })
            except Exception as e:
                print(f"⚠️ 處理圖表附件失敗 {img['id']}: {e}")

        # --- 執行發信 ---
        try:
            receiver_email = os.getenv("REPORT_RECEIVER_EMAIL", "sunhero88@gmail.com")
            resend.Emails.send({
                "from": "StockMonitor <onboarding@resend.dev>",
                "to": receiver_email,
                "subject": f"🚀 {market_name} 監控報告 - {report_time.split(' ')[0]}",
                "html": html_content,
                "attachments": attachments
            })
            print(f"✅ 郵件報告已成功寄送至: {receiver_email}")
            return True
        except Exception as e:
            print(f"❌ Resend 發信失敗: {e}")
            return False
