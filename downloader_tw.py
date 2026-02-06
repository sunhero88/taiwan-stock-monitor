import traceback  # 加這行

def init_tpex_session():
    """訪問主頁建立有效 session"""
    try:
        session.get("https://www.tpex.org.tw/zh-tw/", headers=HEADERS, timeout=10)
        session.get("https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php?l=zh-tw", headers=HEADERS, timeout=10)
        logging.info("✅ TPEX session 初始化成功")
        return True
    except Exception as e:
        logging.error(f"❌ TPEX session 初始化失敗: {e}")
        return False

def get_tpex_daily_amount_v2():
    """使用 TPEX 大盤統計 API"""
    roc_d = roc_date(datetime.now())
    url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
    params = {'l': 'zh-tw', 'd': roc_d, 'se': 'AL'}  # AL = 全部股票
    try:
        r = session.get(url, params=params, headers=HEADERS, timeout=15)
        if r.status_code == 200 and 'errors' not in r.url.lower():
            data = r.json()
            if 'aaData' in data:
                total = 0
                for row in data['aaData']:
                    try:
                        amt_str = row[7].replace(',', '')  # 調整為第8欄 (0-based index 7)
                        total += int(float(amt_str) * 1000)  # 千元轉元
                    except:
                        continue
                if total > 0:
                    logging.info(f"✅ TPEX stk_wn1430 成功: {total:,}")
                    return total, "TPEX_STK_WN1430"
    except Exception as e:
        logging.warning(f"TPEX stk_wn1430 失敗: {e}")
    return None, None

def get_tpex_st43_debug():
    """加強版：輸出完整診斷資訊"""
    roc_d = roc_date(datetime.now())
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    params = {'l': 'zh-tw', 'd': roc_d, 'se': 'EW'}
    
    logging.info(f"🔍 TPEX Request: {url}")
    logging.info(f"📅 ROC Date: {roc_d}")
    logging.info(f"📦 Params: {params}")
    
    try:
        r = session.get(url, params=params, headers=HEADERS, timeout=15, allow_redirects=False)
        logging.info(f"📡 Status Code: {r.status_code}")
        logging.info(f"🔗 Final URL: {r.url}")
        logging.info(f"📄 Response Headers: {dict(r.headers)}")
        
        if r.status_code in [301, 302, 303, 307, 308]:
            logging.warning(f"⚠️ 重導向到: {r.headers.get('Location')}")
            return None, "TPEX_REDIRECT"
        
        if r.status_code == 200:
            content_type = r.headers.get('Content-Type', '')
            if 'json' not in content_type.lower():
                logging.error(f"❌ 非 JSON 回應: {content_type}")
                logging.debug(f"內容前 500 字: {r.text[:500]}")
                return None, "TPEX_NOT_JSON"
            
            data = r.json()
            logging.info(f"📊 JSON Keys: {list(data.keys())}")
            
            if 'reportData' in data and data['reportData']:
                total_str = data['reportData'][-1][2]  # 合計列，第3欄
                amount = int(float(total_str.replace(',', '')) * 100000000)  # 億元轉元
                return amount, "TPEX_ST43"
    
    except Exception as e:
        logging.error(f"❌ TPEX 例外: {type(e).__name__} - {e}")
        logging.debug(traceback.format_exc())
    
    return None, None

def main():
    logging.info("🚀 Predator V16.3.10 TPEX 修補增強版")
    
    # 1. 初始化 TPEX session
    init_tpex_session()
    
    # 2. TWSE (你的原邏輯)
    tse_amount, tse_src = get_twse_daily_amount()
    
    # 3. TPEX 多層 fallback
    otc_amount, otc_src = get_tpex_daily_amount_v2()  # 新 API 優先
    if otc_amount is None:
        otc_amount, otc_src = get_tpex_st43_debug()  # Debug 版
    if otc_amount is None:
        otc_amount, otc_src = get_tpex_html_parse()  # 原 HTML
    if otc_amount is None or otc_amount == 0:
        otc_amount, otc_src = get_yahoo_estimate()  # 估算
    
    # 4. 如果仍失敗，記錄建議
    if otc_amount == 0:
        logging.error("🚨 所有 TPEX 方法均失敗！")
        logging.info("建議：1. 檢查網路/VPN 2. 確認 TPEX 網站正常 3. 手動訪問 URL 測試")
    
    # ... 其餘原程式碼 (個股抓取、輸出 JSON 等)

# roc_date 修正版 (你的方案1)
def roc_date(dt):
    """修正版：確保月日為兩位數"""
    roc_year = dt.year - 1911
    return f"{roc_year}/{dt.month:02d}/{dt.day:02d}"

if __name__ == "__main__":
    main()
