import requests
import json
import pandas as pd
from datetime import datetime
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class MarketAmountProvider:
    def __init__(self):
        # 雲端網路環境必備：建立自動重試機制，應對不穩定的 Cross-border 網路
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.tpex.org.tw/'
        }

    def _get_tpex_fallback_estimate(self, twse_amount):
        """緊急電源模式：當網路完全無法觸達櫃買伺服器時啟動"""
        if twse_amount:
            return twse_amount * 0.22
        return None

    def fetch_all(self):
        trade_date = datetime.now().strftime("%Y-%m-%d")
        # 網路環境需考慮 UTC 與在地時間差，統一使用本地日期
        roc_date = f"{datetime.now().year - 1911}/{datetime.now().strftime('%m/%d')}"
        
        # 1. 抓取 TWSE (上市)
        twse_amount = None
        source_twse = "FAIL"
        try:
            url_twse = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={datetime.now().strftime('%Y%m%d')}"
            res = self.session.get(url_twse, headers=self.headers, timeout=15)
            data = res.json()
            if data.get('data'):
                twse_amount = float(str(data['data'][-1][2]).replace(',', ''))
                source_twse = "TWSE_OK"
        except Exception as e:
            print(f"⚠️ TWSE Fetch Error on Network: {e}")

        # 2. 抓取 TPEX (上櫃) - 針對 REDIRECT 特化處理
        tpex_amount = None
        source_tpex = "FAIL"
        
        try:
            # 加入 params 確保 URL 結構乾淨，避開部分 Redirect 邏輯
            tpex_params = {'l': 'zh-tw', 'd': roc_date, 'se': 'EW'}
            url_tpex = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
            
            res = self.session.get(url_tpex, params=tpex_params, headers=self.headers, timeout=15, allow_redirects=True)
            
            if res.status_code == 200:
                data = res.json()
                if "集合成交金額" in data:
                    tpex_amount = float(str(data["集合成交金額"]).replace(',', ''))
                    source_tpex = "TPEX_OFFICIAL_OK"
