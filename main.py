# -*- coding: utf-8 -*-
import os, argparse, subprocess
from pathlib import Path
import google.generativeai as genai  # 使用 Google SDK

def get_gemini_analysis(market_name, text_reports):
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: return "（未配置 GEMINI_API_KEY）"
    
    # 彙整數據摘要
    summary = "\n".join([f"[{k}]\n{v[:600]}" for k, v in text_reports.items()])
    
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"你是一位資深股市分析師。請針對以下 {market_name} 數據摘要，提供繁體中文分析，包含盤勢結構觀察、權值股動向及風險預警：\n{summary}"
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"（AI 分析暫時失效: {e}）"

def main():
    # ... (其餘下載與分析邏輯與您目前成功的版本一致) ...
    # 在寄信前呼叫 AI 分析並存入 text_reports
    ai_result = get_gemini_analysis(market_id, text_reports)
    text_reports["🤖 Gemini 智能深度分析"] = ai_result
    
    # 發送郵件
    from notifier import StockNotifier
    StockNotifier().send_stock_report(market_id.upper(), images, df_res, text_reports)
