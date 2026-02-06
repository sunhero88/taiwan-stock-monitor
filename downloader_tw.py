import os
import json
import time
import logging
import re
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd
import requests
import yfinance as yf

# ===== (可選) Playwright：用真瀏覽器避開 /errors =====
USE_PLAYWRIGHT_FALLBACK = True
try:
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None
    USE_PLAYWRIGHT_FALLBACK = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
MARKET_JSON = os.path.join(DATA_DIR, "market_amount.json")
STOCK_CSV = os.path.join(DATA_DIR, "data_tw-share.csv")

TIMEOUT = 20
RETRY = 3
SLEEP_BETWEEN = 1.2

def _to_int_from_commas(s: str) -> Optional[int]:
    if s is None:
        return None
    s = str(s).strip().replace(",", "")
    if not s or s.lower() in ("--", "null", "none"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def _is_tpex_errors_url(url: str) -> bool:
    return bool(url) and ("tpex.org.tw/errors" in url)

def _parse_tpex_amount_from_html(html: str) -> Optional[int]:
    """
    保守解析：
    1) 找 '成交金額' 附近的第一個千分位數字
    2) 找不到就取頁面中「最大」的大數字（通常成交金額量級最大）
    """
    if not html:
        return None

    m = re.search(r"成交金額[^0-9]{0,80}([0-9][0-9,]{3,})", html)
    if m:
        v = _to_int_from_commas(m.group(1))
        if v and v > 0:
            return v

    nums = re.findall(r"\b[0-9][0-9,]{6,}\b", html)
    if not nums:
        return None
    cand = max((_to_int_from_commas(x) or 0) for x in nums)
    return cand if cand > 0 else None

# =========================
# TWSE：官方 STOCK_DAY_ALL 加總
# =========================
def fetch_twse_amount(trade_date_yyyymmdd: str) -> Tuple[Optional[int], Dict[str, Any]]:
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date_yyyymmdd}

    audit = {
        "market": "TWSE",
        "trade_date": f"{trade_date_yyyymmdd[:4]}-{trade_date_yyyymmdd[4:6]}-{trade_date_yyyymmdd[6:8]}",
        "url": url,
        "params": params,
        "status_code": None,
        "final_url": None,
        "rows": None,
        "missing_amount_rows": None,
        "amount_sum": None,
        "amount_col": "成交金額",
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.twse.com.tw/",
    }

    for k in range(1, RETRY + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            audit["status_code"] = r.status_code
            audit["final_url"] = r.url
            r.raise_for_status()

            js = r.json()
            data = js.get("data", [])
            fields = js.get("fields", [])

            if not isinstance(data, list) or not fields:
                raise ValueError("TWSE JSON 結構異常：缺少 data/fields")

            idx_amount = fields.index("成交金額")

            amount_sum = 0
            missing = 0
            for row in data:
                v = row[idx_amount] if idx_amount < len(row) else None
                vi = _to_int_from_commas(v)
                if vi is None:
                    missing += 1
                    continue
                amount_sum += vi

            audit["rows"] = len(data)
            audit["missing_amount_rows"] = missing
            audit["amount_sum"] = amount_sum
            return amount_sum, audit

        except Exception as e:
            logging.warning(f"TWSE 抓取失敗 (try {k}/{RETRY}): {e}")
            time.sleep(SLEEP_BETWEEN * k)

    return None, audit

# =========================
# TPEX：requests 嘗試（可能被導 /errors）
# =========================
def fetch_tpex_amount_requests(roc_date: str) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    roc_date 例：115/02/06
    """
    stock_pricing_url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html"
    prime_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php"
    result_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    se_candidates = ["EW", "AL", "ES"]

    # 更像瀏覽器的 header（有時會差這些）
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
    }

    audit: Dict[str, Any] = {
        "market": "TPEX",
        "roc_date": roc_date,
        "phase": [],
        "tries": [],
        "chosen": None,
        "status": "FAIL",
        "reason": None,
    }

    sess = requests.Session()
    sess.headers.update(headers)

    # Phase 1：先進入 stock-pricing.html（更符合實際使用流程）
    try:
        r1 = sess.get(stock_pricing_url, params={"code": ""}, timeout=TIMEOUT, allow_redirects=True)
        audit["phase"].append({"step": "stock_pricing", "status_code": r1.status_code, "final_url": r1.url})
    except Exception as e:
        audit["reason"] = f"STOCK_PRICING_FAIL: {e}"
        return None, audit

    # Phase 2：再 prime st43.php（拿可能需要的 cookie）
    try:
        r2 = sess.get(prime_url, params={"l": "zh-tw"}, timeout=TIMEOUT, allow_redirects=True)
        audit["phase"].append({"step": "st43_prime", "status_code": r2.status_code, "final_url": r2.url})
    except Exception as e:
        audit["reason"] = f"PRIME_FAIL: {e}"
        return None, audit

    # Phase 3：打 result
    for se in se_candidates:
        params = {"l": "zh-tw", "d": roc_date, "se": se}

        for k in range(1, RETRY + 1):
            one = {"se": se, "try": k, "status_code": None, "final_url": None, "hit_errors": None, "amount": None}
            try:
                rr = sess.get(result_url, params=params, timeout=TIMEOUT, allow_redirects=True)
                one["status_code"] = rr.status_code
                one["final_url"] = rr.url
                one["hit_errors"] = _is_tpex_errors_url(rr.url)

                if one["hit_errors"]:
                    audit["tries"].append(one)
                    raise RuntimeError("redirected_to_/errors")

                amount = _parse_tpex_amount_from_html(rr.text)
                if not amount:
                    audit["tries"].append(one)
                    raise ValueError("parse_failed")

                one["amount"] = amount
                audit["tries"].append(one)
                audit["chosen"] = {"se": se, "amount": amount}
                audit["status"] = "OK"
                audit["reason"] = "OK"
                return amount, audit

            except Exception as e:
                audit["tries"].append(one)
                logging.warning(f"TPEX(requests) se={se} (try {k}/{RETRY}) 失敗: {e}")
                time.sleep(SLEEP_BETWEEN * k)

    audit["reason"] = "ALL_SE_FAILED_or_WAF"
    return None, audit

# =========================
# TPEX：Playwright 真瀏覽器救援（可跑 JS）
# =========================
def fetch_tpex_amount_playwright(roc_date: str) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    用 headless chromium 走一遍真正瀏覽器流程，拿到 result HTML 再解析。
    """
    audit: Dict[str, Any] = {
        "market": "TPEX",
        "roc_date": roc_date,
        "engine": "playwright",
        "status": "FAIL",
        "reason": None,
        "steps": [],
        "chosen": None,
    }

    if sync_playwright is None:
        audit["reason"] = "PLAYWRIGHT_NOT_INSTALLED"
        return None, audit

    stock_pricing_url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html?code="
    result_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    se_candidates = ["EW", "AL", "ES"]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="zh-TW")
        page = context.new_page()

        # Step 1：先開主頁（讓站方把 cookie / session 建好）
        page.goto(stock_pricing_url, wait_until="domcontentloaded", timeout=TIMEOUT * 1000)
        audit["steps"].append({"step": "open_stock_pricing", "url": stock_pricing_url})

        for se in se_candidates:
            url = f"{result_url}?l=zh-tw&d={roc_date}&se={se}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT * 1000)
                final_url = page.url
                audit["steps"].append({"step": "open_result", "se": se, "final_url": final_url})

                if _is_tpex_errors_url(final_url):
                    continue

                html = page.content()
                amount = _parse_tpex_amount_from_html(html)
                if amount and amount > 0:
                    audit["status"] = "OK"
                    audit["reason"] = "OK"
                    audit["chosen"] = {"se": se, "amount": amount}
                    browser.close()
                    return amount, audit

            except Exception as e:
                audit["steps"].append({"step": "error", "se": se, "err": str(e)})

        browser.close()

    audit["reason"] = "PLAYWRIGHT_FAILED_or_WAF"
    return None, audit

# =========================
# 個股下載
# =========================
def download_stocks(tickers: List[str]) -> List[Dict[str, Any]]:
    logging.info(f"正在下載 {len(tickers)} 檔個股（yfinance, threads=False）...")
    raw = yf.download(tickers, period="10d", interval="1d", threads=False, progress=False)

    out: List[Dict[str, Any]] = []
    for sym in tickers:
        try:
            close_price = raw["Close"][sym].dropna().iloc[-1]
            volume = raw["Volume"][sym].dropna().iloc[-1]
            out.append({"Symbol": sym, "Price": float(close_price), "Volume": int(volume)})
        except Exception:
            logging.info(f"🔧 單獨救援 {sym} ...")
            single = yf.Ticker(sym).history(period="5d")
            if not single.empty and "Close" in single and "Volume" in single:
                out.append({"Symbol": sym, "Price": float(single["Close"].iloc[-1]), "Volume": int(single["Volume"].iloc[-1])})
            else:
                out.append({"Symbol": sym, "Price": None, "Volume": None})
    return out

# =========================
# 主程式
# =========================
def main():
    trade_date = datetime.now().strftime("%Y-%m-%d")
    yyyymmdd = trade_date.replace("-", "")
    roc_year = int(trade_date[:4]) - 1911
    roc_date = f"{roc_year}/{trade_date[5:7]}/{trade_date[8:10]}"

    # 1) TWSE
    twse_amount, twse_audit = fetch_twse_amount(yyyymmdd)
    if twse_amount is not None:
        logging.info(f"TWSE 成交額: {twse_amount:,}（約 {twse_amount/1e8:.1f} 億）")

    # 2) TPEX（requests → playwright）
    tpex_amount, tpex_audit = fetch_tpex_amount_requests(roc_date)

    # 若仍被導 /errors：啟動 playwright
    if (tpex_amount is None) and USE_PLAYWRIGHT_FALLBACK:
        logging.warning("TPEX requests 仍失敗，啟動 Playwright 真瀏覽器救援...")
        tpex_amount2, tpex_audit2 = fetch_tpex_amount_playwright(roc_date)
        if tpex_amount2 is not None:
            tpex_amount, tpex_audit = tpex_amount2, {"requests": tpex_audit, "playwright": tpex_audit2}
        else:
            tpex_audit = {"requests": tpex_audit, "playwright": tpex_audit2}

    if tpex_amount is not None:
        logging.info(f"TPEX 成交額: {tpex_amount:,}（約 {tpex_amount/1e8:.1f} 億）")
    else:
        logging.warning("TPEX 成交額仍為 null（WAF/導流未突破），維持降級。")

    # 3) 個股
    tickers = [
        "2330.TW","2317.TW","2454.TW","2308.TW","2382.TW","3231.TW","2376.TW","3017.TW","3324.TW","3661.TW",
        "2881.TW","2882.TW","2891.TW","2886.TW","2603.TW","2609.TW","1605.TW","1513.TW","1519.TW","2002.TW"
    ]
    stock_results = download_stocks(tickers)

    # 4) Integrity
    price_null = sum(1 for x in stock_results if x.get("Price") is None)
    vol_null = sum(1 for x in stock_results if x.get("Volume") is None)

    amount_total = (twse_amount or 0) + (tpex_amount or 0)
    status = "OK" if (twse_amount is not None and tpex_amount is not None) else "DEGRADED"
    amount_scope = "FULL" if (twse_amount is not None and tpex_amount is not None) else ("TWSE_ONLY" if twse_amount is not None else "NONE")

    market_output = {
        "trade_date": trade_date,
        "amount_twse": twse_amount,
        "amount_tpex": tpex_amount,
        "amount_total": amount_total if amount_total > 0 else None,
        "status": status,
        "amount_scope": amount_scope,
        "integrity": {
            "price_null": price_null,
            "volume_null": vol_null,
            "amount_partial": (amount_scope != "FULL"),
            "note": "TPEX 若為 null：代表 /errors 導流（反爬/WAF）。本程式不推估，維持降級。",
        },
        "audit": {
            "twse": twse_audit,
            "tpex": tpex_audit,
        }
    }

    with open(MARKET_JSON, "w", encoding="utf-8") as f:
        json.dump(market_output, f, indent=2, ensure_ascii=False)

    pd.DataFrame(stock_results).to_csv(STOCK_CSV, index=False, encoding="utf-8-sig")

    logging.info(f"輸出完成：{MARKET_JSON}, {STOCK_CSV}")
    logging.info(f"市場狀態={status} / amount_scope={amount_scope} / amount_total={(market_output['amount_total'] or 0):,}")

if __name__ == "__main__":
    main()
