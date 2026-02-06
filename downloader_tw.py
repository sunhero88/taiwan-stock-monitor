import pandas as pd
import yfinance as yf
import requests
import os
import json
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
MARKET_JSON = os.path.join(DATA_DIR, "market_amount.json")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Referer': 'https://www.tpex.org.tw/'
}

session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

def roc_date(dt):
    """轉 ROC 年/月/日 格式，如 115/02/06"""
    roc_year = dt.year - 1911
    return f"{roc_year:03d}/{dt.strftime('%m/%d')}"

# 第一重：TWSE 日總成交金額 (MI_INDEX 或 STOCK_DAY_ALL)
def get_twse_daily_amount():
    dt_str = datetime.now().strftime("%Y%m%d")
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={dt_str}&type=ALLBUT0999"
    try:
        r = session.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if 'data9' in data:  # MI_INDEX 有時用 data9 為總覽
                total_amt_str = data['data9'][0][2]  # 調整索引，視實際為總成交金額
                return int(float(total_amt_str.replace(',', ''))), "TWSE_MI_INDEX"
    except Exception as e:
        logging.warning(f"TWSE MI_INDEX 失敗: {e}")
    
    # 備援 STOCK_DAY_ALL
    url_all = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={dt_str}"
    try:
        r = session.get(url_all, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            items = r.json()
            total = sum(int(float(item.get('成交金額', 0))) for item in items if item.get('成交金額'))
            return total, "TWSE_STOCK_DAY_ALL"
    except:
        pass
    return 0, "TWSE_FAILED"

# 第二重：TPEX - 嘗試 st43_result.php 正確參數 + fallback HTML
def get_tpex_st43():
    roc_d = roc_date(datetime.now())
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc_d}&se=EW"
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200 and 'errors' not in r.url:
            data = r.json()
            if 'reportData' in data and data['reportData']:
                # 最後一列合計，第3欄 (索引2) 通常為成交金額
                total_str = data['reportData'][-1][2]
                amount = int(float(total_str.replace(',', '')) * 100000000)  # 若單位億元，轉元
                logging.info(f"✅ TPEX st43 成功: {amount:,}")
                return amount, "TPEX_ST43"
    except Exception as e:
        logging.warning(f"TPEX st43 失敗: {e} (可能 redirect)")
    return None, None

# 第三重：TPEX HTML parse (daily statistics page)
def get_tpex_html_parse():
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/statistics/daily.html"
    try:
        r = session.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        # 找總成交金額文字 (調整 selector 依實際 HTML)
        total_elem = soup.find(string=lambda t: "總成交金額" in str(t) if t else False)
        if total_elem:
            amt_text = total_elem.find_next('td').text.strip() if total_elem.find_next('td') else ""
            amt_text = amt_text.replace(',', '').replace('億元', '')
            amount = int(float(amt_text)) * 100000000  # 億元轉元
            return amount, "TPEX_HTML_PARSE"
        # 若無，試找表格最後合計
        table = soup.find('table', {'class': 'table'})  # 調整 class
        if table:
            rows = table.find_all('tr')
            if rows:
                last_row = rows[-1].find_all('td')
                if len(last_row) > 2:
                    amt_str = last_row[2].text.strip().replace(',', '')
                    amount = int(float(amt_str)) * 100000000
                    return amount, "TPEX_TABLE_PARSE"
    except Exception as e:
        logging.warning(f"TPEX HTML parse 失敗: {e}")
    return None, None

# 第四重：Yahoo 估算 (^TWO)
def get_yahoo_estimate():
    try:
        otc = yf.Ticker("^TWO").history(period="2d", prepost=False)
        if not otc.empty:
            vol = otc['Volume'].iloc[-1]
            close = otc['Close'].iloc[-1]
            est = int(vol * close * 0.58)  # 調到 0.58 更準 (歷史比對)
            logging.info(f"⚠️ Yahoo 估算上櫃: {est:,}")
            return est, "YAHOO_ESTIMATE"
    except Exception as e:
        logging.error(f"Yahoo 估算失敗: {e}")
    return 0, "FAILED"

# 主程序
def main():
    logging.info("🚀 Predator V16.3.9+ TPEX 修補版 執行中...")

    # TWSE
    tse_amount, tse_src = get_twse_daily_amount()

    # TPEX 多層
    otc_amount, otc_src = get_tpex_st43()
    if otc_amount is None:
        otc_amount, otc_src = get_tpex_html_parse()
    if otc_amount is None or otc_amount == 0:
        otc_amount, otc_src = get_yahoo_estimate()

    # 個股 + 雙鴻救援
    tickers = ["2330.TW", "2317.TW", "2454.TW", "3324.TW", "2308.TW", "2382.TW", "3231.TW", "3017.TW", "2603.TW"]
    data = yf.download(tickers, period="5d", group_by='ticker', threads=False, prepost=False)

    stock_list = []
    for s in tickers:
        try:
            df = data[s] if s in data else pd.DataFrame()
            if not df.empty:
                p = df['Close'].dropna().iloc[-1]
                v = df['Volume'].dropna().iloc[-1]
                stock_list.append({"Symbol": s, "Price": float(p), "Volume": int(v)})
                continue
        except:
            pass
        # 單檔救援
        logging.info(f"🔧 救援 {s}")
        fix = yf.Ticker(s).history(period="3d", prepost=False)
        if not fix.empty:
            stock_list.append({"Symbol": s, "Price": float(fix['Close'].iloc[-1]), "Volume": int(fix['Volume'].iloc[-1])})

    # 輸出
    market_output = {
        "trade_date": datetime.now().strftime("%Y-%m-%d"),
        "amount_twse": tse_amount,
        "amount_tpex": otc_amount,
        "amount_total": tse_amount + otc_amount,
        "source_twse": tse_src,
        "source_tpex": otc_src,
        "status": "OK" if otc_amount > 0 else "DEGRADED",
        "integrity": {
            "tickers_count": len(stock_list),
            "amount_partial": otc_amount == 0
        }
    }

    with open(MARKET_JSON, 'w', encoding='utf-8') as f:
        json.dump(market_output, f, indent=4, ensure_ascii=False)
    
    pd.DataFrame(stock_list).to_csv(os.path.join(DATA_DIR, "data_tw-share.csv"), index=False)
    logging.info(f"完成。上櫃源: {otc_src} | 總額: {market_output['amount_total']:,}")

if __name__ == "__main__":
    main()
