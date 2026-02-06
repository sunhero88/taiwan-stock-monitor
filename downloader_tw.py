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

# =========================
# 基本設定
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

MARKET_JSON = os.path.join(DATA_DIR, "market_amount.json")
STOCK_CSV = os.path.join(DATA_DIR, "data_tw-share.csv")

TIMEOUT = 15
RETRY = 3
SLEEP_BETWEEN = 1.2

# =========================
# 工具：數字解析
# =========================
def _to_int_from_commas(s: str) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    s = s.replace(",", "")
    if not s or s in ("--", "null", "None"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def _is_tpex_errors_url(url: str) -> bool:
    if not url:
        return False
    return "tpex.org.tw/errors" in url

# =========================
# 1) TWSE 成交金額：官方 STOCK_DAY_ALL 加總
# =========================
def fetch_twse_amount(trade_date_yyyymmdd: str) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    回傳 (amount_twse, audit)
    """
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date_yyyymmdd}

    audit: Dict[str, Any] = {
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

            # 找到「成交金額」欄位索引
            try:
                idx_amount = fields.index("成交金額")
            except ValueError:
                # 有時欄位名稱可能不同（極少），保守處理
                raise ValueError(f"TWSE fields 找不到『成交金額』欄位，fields={fields}")

            amount_sum = 0
            missing = 0
            for row in data:
                v = row[idx_amount] if idx_amount < len(row) else None
                vi = _to_int_from_commas(str(v)) if v is not None else None
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
# 2) TPEX 成交金額：先 prime，再打 result，避免 /errors
# =========================
def fetch_tpex_amount(roc_date: str) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    roc_date 例：'115/02/06'
    回傳 (amount_tpex, audit)

    核心策略：
    - 先 GET st43.php prime（拿 cookie / session）
    - 再 GET st43_result.php
    - 補齊 User-Agent / Referer / Accept-Language
    - 若被導到 /errors → 視為失敗並換 se 參數重試
    """
    prime_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php"
    result_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

    # se 可能因站內切換而不同；常見值：AL / EW（你原本用 EW）
    se_candidates = ["EW", "AL", "ES"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
        "Connection": "keep-alive",
        "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html",
    }

    audit: Dict[str, Any] = {
        "market": "TPEX",
        "roc_date": roc_date,
        "prime": {"url": prime_url, "status_code": None, "final_url": None},
        "result": [],
        "chosen": None,
        "status": "FAIL",
        "reason": None,
    }

    sess = requests.Session()
    sess.headers.update(headers)

    # 先 prime（非常關鍵：很多情況不 prime 直接打 result 就會被丟 /errors）
    try:
        pr = sess.get(prime_url, params={"l": "zh-tw"}, timeout=TIMEOUT, allow_redirects=True)
        audit["prime"]["status_code"] = pr.status_code
        audit["prime"]["final_url"] = pr.url
    except Exception as e:
        audit["reason"] = f"PRIME_FAIL: {e}"
        return None, audit

    for se in se_candidates:
        params = {"l": "zh-tw", "d": roc_date, "se": se}
        one_try = {
            "url": result_url,
            "params": params,
            "status_code": None,
            "final_url": None,
            "hit_errors": None,
            "parsed_amount": None,
        }

        for k in range(1, RETRY + 1):
            try:
                rr = sess.get(result_url, params=params, timeout=TIMEOUT, allow_redirects=True)
                one_try["status_code"] = rr.status_code
                one_try["final_url"] = rr.url
                one_try["hit_errors"] = _is_tpex_errors_url(rr.url)

                if one_try["hit_errors"]:
                    raise RuntimeError("TPEX redirected to /errors")

                html = rr.text or ""
                # 常見頁面會出現「成交金額」欄位；用正規式抓該列數值（最保守寫法）
                # 由於版面可能變動，這裡採「先找『成交金額』附近的第一個千分位數字」
                m = re.search(r"成交金額[^0-9]{0,50}([0-9][0-9,]{3,})", html)
                if not m:
                    # 備援：抓整頁最大的一個像成交金額的數字（通常成交金額量級最大）
                    nums = re.findall(r"\b[0-9][0-9,]{6,}\b", html)  # 至少百萬等級
                    if not nums:
                        raise ValueError("TPEX HTML 找不到可解析數字")
                    # 取最大值
                    cand = max((_to_int_from_commas(x) or 0) for x in nums)
                    if cand <= 0:
                        raise ValueError("TPEX 解析結果為 0/無效")
                    amount = cand
                else:
                    amount = _to_int_from_commas(m.group(1))
                    if not amount or amount <= 0:
                        raise ValueError("TPEX 成交金額解析失敗/為 0")

                one_try["parsed_amount"] = amount
                audit["result"].append(one_try)

                audit["chosen"] = {"se": se, "amount": amount}
                audit["status"] = "OK"
                audit["reason"] = "OK"
                return amount, audit

            except Exception as e:
                logging.warning(f"TPEX 抓取失敗 se={se} (try {k}/{RETRY}): {e}")
                time.sleep(SLEEP_BETWEEN * k)

        audit["result"].append(one_try)

    audit["reason"] = "ALL_SE_FAILED_or_WAF"
    return None, audit

# =========================
# 3) 鉅亨 API（可選）：僅當作額外資訊，不作為唯一成交額來源
#    你原本的 endpoint 其實是「mainland/index/quote」，容易不對題
#    若你要留鉅亨，建議改成真正台股/指數對應的端點（此處先不硬寫推估）
# =========================
def try_cnyes_placeholder() -> Tuple[Optional[int], Optional[int], str]:
    # 這裡保留介面，但不再用「不確定端點」直接當成交額
    return None, None, "CNYES_DISABLED_NO_GUESS"

# =========================
# 4) 個股下載（維持你原本的穩定策略 + 單點救援）
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
# 5) 主程式：寫入 market_amount.json + data_tw-share.csv
# =========================
def main():
    # 以「今天」為預設；若你要用指定交易日，改這裡即可
    trade_date = datetime.now().strftime("%Y-%m-%d")
    yyyymmdd = trade_date.replace("-", "")

    # ROC 日期（民國）: 西元年-1911
    y = int(trade_date[:4]) - 1911
    roc_date = f"{y}/{trade_date[5:7]}/{trade_date[8:10]}"

    # (A) 官方 TWSE
    twse_amount, twse_audit = fetch_twse_amount(yyyymmdd)
    if twse_amount:
        logging.info(f"TWSE 成交額: {twse_amount:,}（約 {twse_amount/1e8:.1f} 億）")
    else:
        logging.warning("TWSE 成交額抓取失敗。")

    # (B) 官方 TPEX（prime + result）
    tpex_amount, tpex_audit = fetch_tpex_amount(roc_date)
    if tpex_amount:
        logging.info(f"TPEX 成交額: {tpex_amount:,}（約 {tpex_amount/1e8:.1f} 億）")
    else:
        logging.warning("TPEX 成交額仍抓不到（可能 WAF/導流）。")

    # (C) 個股
    tickers = [
        "2330.TW","2317.TW","2454.TW","2308.TW","2382.TW","3231.TW","2376.TW","3017.TW","3324.TW","3661.TW",
        "2881.TW","2882.TW","2891.TW","2886.TW","2603.TW","2609.TW","1605.TW","1513.TW","1519.TW","2002.TW"
    ]
    stock_results = download_stocks(tickers)

    # (D) Integrity
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
            "note": "TPEX 若為 null：代表被導到 /errors 或 HTML 結構無法解析；本程式不做推估，維持降級。",
        },
        "audit": {
            "twse": twse_audit,
            "tpex": tpex_audit,
        }
    }

    with open(MARKET_JSON, "w", encoding="utf-8") as f:
        json.dump(market_output, f, indent=2, ensure_ascii=False)

    df_stocks = pd.DataFrame(stock_results)
    df_stocks.to_csv(STOCK_CSV, index=False, encoding="utf-8-sig")

    logging.info(f"輸出完成：{MARKET_JSON}, {STOCK_CSV}")
    logging.info(f"市場狀態={status} / amount_scope={amount_scope} / amount_total={(market_output['amount_total'] or 0):,}")

if __name__ == "__main__":
    main()
