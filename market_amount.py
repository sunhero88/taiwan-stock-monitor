import requests
import json
import pandas as pd
from datetime import datetime
import yfinance as yf

class MarketAmountProvider:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://www.tpex.org.tw/'
        }

    def _get_tpex_fallback_estimate(self, twse_amount):
        """當 TPEX 完全掛掉時，使用統計估計值 (緊急電源模式)"""
        if twse_amount:
            # 根據歷史統計，櫃買約佔上市成交額 20%~25%
            return twse_amount * 0.22
        return None

    def fetch_all(self):
        """執行完整抓取任務，確保不回傳 None"""
        trade_date = datetime.now().strftime("%Y-%m-%d")
        roc_date = f"{datetime.now().year - 1911}/{datetime.now().strftime('%m/%d')}"
        
        # 1. 抓取 TWSE (上市)
        twse_amount = None
        source_twse = "FAIL"
        try:
            url_twse = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={datetime.now().strftime('%Y%m%d')}"
            res = requests.get(url_twse, headers=self.headers, timeout=10)
            data = res.json()
            if data.get('data'):
                # 取得最新一筆成交金額 (元)
                twse_amount = float(str(data['data'][-1][2]).replace(',', ''))
                source_twse = "TWSE_OK"
        except Exception as e:
            print(f"❌ TWSE Fetch Error: {e}")

        # 2. 抓取 TPEX (上櫃) - 帶重試與多路徑邏輯
        tpex_amount = None
        source_tpex = "FAIL"
        
        # 路徑 A: 官方日行情 API
        try:
            url_tpex = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc_date}&se=EW"
            res = requests.get(url_tpex, headers=self.headers, timeout=10, allow_redirects=True)
            if res.status_code == 200:
                data = res.json()
                if "集合成交金額" in data:
                    tpex_amount = float(str(data["集合成交金額"]).replace(',', ''))
                    source_tpex = "TPEX_OFFICIAL_OK"
        except:
            pass

        # 路徑 B: 如果 A 失敗，嘗試 Yahoo Finance (^TWOO)
        if tpex_amount is None:
            try:
                ticker = yf.Ticker("^TWOO")
                df = ticker.history(period="1d")
                if not df.empty:
                    tpex_amount = df['Volume'].iloc[-1]
                    source_tpex = "TPEX_YAHOO_BACKUP"
            except:
                pass

        # 路徑 C: 如果 A, B 都失敗，啟動「估算模式」
        if tpex_amount is None and twse_amount:
            tpex_amount = self._get_tpex_fallback_estimate(twse_amount)
            source_tpex = "TPEX_ESTIMATED_WARNING"

        # 3. 整合結果 (保持原本 JSON 要求的欄位格式)
        total_amount = (twse_amount or 0) + (tpex_amount or 0)
        
        # 決定狀態
        market_status = "OK"
        if "FAIL" in [source_twse, source_tpex] or "WARNING" in source_tpex:
            market_status = "DEGRADED"

        return {
            "meta": {
                "trade_date": trade_date,
                "market_status": market_status,
                "audit_tag": "V16.3.4_AUTO_REPAIR"
            },
            "market_amount": {
                "amount_twse": twse_amount,
                "amount_tpex": tpex_amount,
                "amount_total": total_amount,
                "source_twse": source_twse,
                "source_tpex": source_tpex,
                "scope": "FULL_MARKET" if tpex_amount else "TWSE_ONLY"
            }
        }

# 為了相容於舊程式的呼叫方式
def get_market_amount():
    provider = MarketAmountProvider()
    return provider.fetch_all()

if __name__ == "__main__":
    report = get_market_amount()
    print(json.dumps(report, indent=4, ensure_ascii=False))
