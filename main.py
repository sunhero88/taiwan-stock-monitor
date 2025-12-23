# -*- coding: utf-8 -*-
import os
import time
import argparse
from datetime import datetime, timedelta

# 導入自定義模組
import downloader_tw
import downloader_us
import downloader_hk
import downloader_cn
import downloader_jp
import downloader_kr
import analyzer
import notifier

def run_market_pipeline(market_id, market_name, emoji):
    """
    執行單一市場的完整管線：下載 -> 分析 -> 寄信
    """
    print("\n" + "="*60)
    print(f"{emoji} 啟動管線：{market_name} ({market_id})")
    print("="*60)

    # 初始化統計變數
    stats = {"total": 0, "success": 0, "fail": 0}
    
    # 建立通知器實例
    agent = notifier.StockNotifier()

    # --- Step 1: 數據獲取 ---
    print(f"【Step 1: 數據獲取】正在更新 {market_name} 原始 K 線資料...")
    try:
        # 根據市場 ID 呼叫對應的下載器
        if market_id == "tw-share":
            res = downloader_tw.main()
        elif market_id == "us-share":
            res = downloader_us.main()
        elif market_id == "hk-share":
            res = downloader_hk.main()
        elif market_id == "cn-share":
            res = downloader_cn.main()
        elif market_id == "jp-share":
            res = downloader_jp.main()
        elif market_id == "kr-share":
            res = downloader_kr.main()
        else:
            print(f"⚠️ 未知的市場 ID: {market_id}")
            return

        # ✨ 格式標準化：確保傳給 notifier 的一定是字典
        if isinstance(res, dict):
            stats = res
        elif hasattr(res, '__len__'):
            stats = {"total": len(res), "success": len(res), "fail": 0}

    except Exception as e:
        print(f"❌ {market_name} 數據下載過程發生異常: {e}")

    # --- Step 2: 數據分析 & 繪圖 ---
    print(f"\n【Step 2: 矩陣分析】正在計算 {market_name} 動能分布並生成圖表...")
    try:
        # 取得分析結果
        img_paths, report_df, text_reports = analyzer.run_global_analysis(market_id=market_id)
        
        if report_df is None or report_df.empty:
            print(f"⚠️ {market_name} 分析結果為空，跳過寄信步驟。")
            return
        
        print(f"✅ 分析完成！成功處理 {len(report_df)} 檔標的。")

        # --- Step 3: 報表發送 ---
        print(f"\n【Step 3: 報表發送】正在透過 Resend 傳送郵件...")
        
        # 呼叫 notifier.py 中的 send_stock_report 方法
        agent.send_stock_report(
            market_name=market_name,
            img_data=img_paths,
            report_df=report_df,
            text_reports=text_reports,
            stats=stats
        )
        print(f"✅ {market_name} 監控報告發送完畢。")

    except Exception as e:
        import traceback
        print(f"❌ {market_name} 分析或寄信過程出錯:\n{traceback.format_exc()}")

def main():
    parser = argparse.ArgumentParser(description="Global Stock Monitor Orchestrator")
    parser.add_argument('--market', type=str, default='all', 
                        choices=['tw-share', 'us-share', 'hk-share', 'cn-share', 'jp-share', 'kr-share', 'all'])
    args = parser.parse_args()

    start_time = time.time()
    # 顯示台北時間 (UTC+8) 供日誌對照
    now_utc8 = datetime.utcnow() + timedelta(hours=8)
    now_str = now_utc8.strftime("%Y-%m-%d %H:%M:%S")
    
    print("🚀 " + "="*50)
    print(f"🚀 全球股市監控系統啟動")
    print(f"🚀 當前時間: {now_str} (UTC+8)")
    print(f"🚀 執行模式: {args.market}")
    print("🚀 " + "="*50 + "\n")

    markets_config = {
        "tw-share": {"name": "台灣股市", "emoji": "🇹🇼"},
        "hk-share": {"name": "香港股市", "emoji": "🇭🇰"},
        "cn-share": {"name": "中國股市", "emoji": "🇨🇳"},
        "jp-share": {"name": "日本股市", "emoji": "🇯🇵"},
        "kr-share": {"name": "韓國股市", "emoji": "🇰🇷"},
        "us-share": {"name": "美國股市", "emoji": "🇺🇸"}
    }

    if args.market == 'all':
        for m_id, m_info in markets_config.items():
            run_market_pipeline(m_id, m_info["name"], m_info["emoji"])
    else:
        m_info = markets_config.get(args.market)
        if m_info:
            run_market_pipeline(args.market, m_info["name"], m_info["emoji"])
        else:
            print(f"❌ 找不到市場配置: {args.market}")

    end_time = time.time()
    total_duration = (end_time - start_time) / 60
    print("\n" + "="*60)
    print(f"🎉 任務全部達成！總耗時: {total_duration:.2f} 分鐘")
    print("="*60)

if __name__ == "__main__":
    main()
