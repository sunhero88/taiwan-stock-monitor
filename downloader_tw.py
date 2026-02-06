import os
import json
import time
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests
import pandas as pd

# -------------------------
# Config
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DATA_DIR = "data"
AUDIT_DIR = os.path.join(DATA_DIR, "audit_market_amount")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)

MARKET_JSON = os.path.join(DATA_DIR, "market_amount.json")

TWSE_URL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
# 你目前用的 TPEX 舊端點（常見會被導到 /errors）
TPEX_URL = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
TPEX_PRIME = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php?l=zh-tw"

DEFAULT_TIMEOUT = 15
MAX_RETRY = 4

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

# -------------------------
# Helpers
# -------------------------
def now_ymd() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def ymd_to_yyyymmdd(ymd: str) -> str:
    # ymd: "2026-02-06" -> "20260206"
    return ymd.replace("-", "")

def ymd_to_roc(ymd: str) -> str:
    # ymd: "2026-02-06" -> ROC "115/02/06"
    y, m, d = ymd.split("-")
    roc_y = int(y) - 1911
    return f"{roc_y:03d}/{int(m):02d}/{int(d):02d}"

def save_text(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def save_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def sniff_is_error_page(final_url: str, text: str) -> bool:
    """
    TPEX 常見失敗：
    - final_url 變成 https://www.tpex.org.tw/errors
    - 或 HTML 中出現 errors / 系統忙碌 / 查無資料 等
    """
    if "tpex.org.tw/errors" in (final_url or ""):
        return True
    t = (text or "").lower()
    bad_keywords = [
        "tpex.org.tw/errors",
        "error",
        "系統忙碌",
        "查無資料",
        "請稍後再試",
        "access denied",
        "forbidden",
        "captcha",
        "cloudflare",
    ]
    return any(k.lower() in t for k in bad_keywords)

def make_headers(referer: Optional[str] = None) -> Dict[str, str]:
    ua = UA_POOL[int(time.time()) % len(UA_POOL)]
    h = {
        "User-Agent": ua,
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
    if referer:
        h["Referer"] = referer
        h["Origin"] = "https://www.tpex.org.tw"
    return h

# -------------------------
# TWSE (上市) 成交額：逐檔彙總
# -------------------------
def fetch_twse_amount(trade_date: str) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    使用 TWSE STOCK_DAY_ALL：
    - 下載全市場逐檔資料
    - 彙總「成交金額」
    - 稽核：raw/json/csv + rows + missing + sum
    """
    yyyymmdd = ymd_to_yyyymmdd(trade_date)
    params = {"response": "json", "date": yyyymmdd}

    meta: Dict[str, Any] = {
        "market": "TWSE",
        "trade_date": trade_date,
        "url": TWSE_URL,
        "params": params,
        "status_code": None,
        "final_url": None,
        "audit": None,
    }

    for i in range(1, MAX_RETRY + 1):
        try:
            r = requests.get(TWSE_URL, params=params, timeout=DEFAULT_TIMEOUT, headers=make_headers())
            meta["status_code"] = r.status_code
            meta["final_url"] = r.url

            raw_path = os.path.join(AUDIT_DIR, f"TWSE_{yyyymmdd}_raw.txt")
            save_text(raw_path, r.text)

            r.raise_for_status()
            j = r.json()

            json_path = os.path.join(AUDIT_DIR, f"TWSE_{yyyymmdd}_raw.json")
            save_json(json_path, j)

            data = j.get("data", [])
            fields = j.get("fields", [])
            if not data or not fields:
                raise ValueError("TWSE response missing data/fields")

            df = pd.DataFrame(data, columns=fields)

            # 依 TWSE 回傳欄位，成交金額欄位通常是「成交金額」
            if "成交金額" not in df.columns:
                raise ValueError(f"TWSE missing 成交金額 column. columns={list(df.columns)[:10]}...")

            # 轉數字：去除逗號/空白
            amt = (
                df["成交金額"]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
            )
            amt_num = pd.to_numeric(amt, errors="coerce")
            missing = int(amt_num.isna().sum())
            amount_sum = int(amt_num.fillna(0).sum())

            csv_path = os.path.join(AUDIT_DIR, f"TWSE_{yyyymmdd}_rows.csv")
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")

            audit = {
                "market": "TWSE",
                "trade_date": trade_date,
                "rows": int(df.shape[0]),
                "missing_amount_rows": missing,
                "amount_sum": amount_sum,
                "amount_col": "成交金額",
                "raw_saved": os.path.basename(raw_path),
                "json_saved": os.path.basename(json_path),
                "csv_saved": os.path.basename(csv_path),
            }
            meta["audit"] = audit
            return amount_sum, meta

        except Exception as e:
            logging.warning(f"[TWSE] retry {i}/{MAX_RETRY} failed: {e}")
            time.sleep(0.8 * i)

    return None, meta

# -------------------------
# TPEX (上櫃) 成交額：嘗試官方頁面/反爬處理
# -------------------------
def fetch_tpex_amount(trade_date: str) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    TPEX 端點容易導到 /errors。
    這裡做：
    1) 先 prime 一次拿 cookie
    2) 再打 result.php
    3) 偵測是否 /errors 或 error page
    4) 若回 JSON：試著找可彙總的成交金額欄位
       若回 HTML：用 read_html 嘗試抓表格並彙總成交金額
    """
    roc_date = ymd_to_roc(trade_date)
    params = {"l": "zh-tw", "d": roc_date, "se": "EW"}

    meta: Dict[str, Any] = {
        "market": "TPEX",
        "trade_date": trade_date,
        "roc_date": roc_date,
        "prime_url": TPEX_PRIME,
        "url": TPEX_URL,
        "params": params,
        "status_code": None,
        "final_url": None,
        "audit": None,
    }

    sess = requests.Session()

    for i in range(1, MAX_RETRY + 1):
        try:
            # prime：拿 cookie + 建立 session
            pr = sess.get(TPEX_PRIME, timeout=DEFAULT_TIMEOUT, headers=make_headers(), allow_redirects=True)

            # result：帶 referer / cookie
            headers = make_headers(referer=TPEX_PRIME)
            r = sess.get(TPEX_URL, params=params, timeout=DEFAULT_TIMEOUT, headers=headers, allow_redirects=True)

            meta["status_code"] = r.status_code
            meta["final_url"] = r.url

            yyyymmdd = ymd_to_yyyymmdd(trade_date)
            raw_path = os.path.join(AUDIT_DIR, f"TPEX_{yyyymmdd}_raw.txt")
            save_text(raw_path, r.text)

            # error page detect
            if sniff_is_error_page(r.url, r.text):
                raise ValueError(f"TPEX error page detected. final_url={r.url}")

            # 嘗試 JSON
            amount_sum: Optional[int] = None
            parsed_kind = None

            ct = (r.headers.get("Content-Type") or "").lower()
            if "application/json" in ct or r.text.strip().startswith("{"):
                j = r.json()
                json_path = os.path.join(AUDIT_DIR, f"TPEX_{yyyymmdd}_raw.json")
                save_json(json_path, j)

                # ⚠️ 不同時期 TPEX 回傳格式可能不同，你必須保守：
                # 只在確定找到「成交金額」欄位且可彙總時才回傳。
                # 常見 keys: "aaData" / "data" / "tables"
                candidates = []

                def collect_rows(obj):
                    if isinstance(obj, list):
                        return obj
                    if isinstance(obj, dict):
                        for k in ["aaData", "data", "tables", "result", "rows"]:
                            v = obj.get(k)
                            if isinstance(v, list):
                                return v
                    return None

                rows = collect_rows(j)
                if rows and isinstance(rows[0], (list, dict)):
                    candidates = rows

                # 若是 list-of-dict 且有 成交金額 欄位
                if candidates and isinstance(candidates[0], dict):
                    if "成交金額" in candidates[0]:
                        s = pd.Series([x.get("成交金額") for x in candidates])
                        s = s.astype(str).str.replace(",", "", regex=False).str.strip()
                        amt_num = pd.to_numeric(s, errors="coerce")
                        amount_sum = int(amt_num.fillna(0).sum())
                        parsed_kind = "JSON_DICT_SUM"

                # 若是 list-of-list：我們不知道哪個欄位是成交金額（不硬猜）
                if amount_sum is None and candidates and isinstance(candidates[0], list):
                    raise ValueError("TPEX JSON is list-of-list; column mapping unknown; refuse to guess.")

            else:
                # HTML：嘗試抓表格
                tables = pd.read_html(r.text)
                if not tables:
                    raise ValueError("TPEX HTML has no tables")

                # 尋找含「成交金額」欄位的表
                chosen = None
                for t in tables:
                    if any("成交金額" in str(c) for c in t.columns):
                        chosen = t
                        break
                if chosen is None:
                    raise ValueError("TPEX tables found but no 成交金額 column")

                col = None
                for c in chosen.columns:
                    if "成交金額" in str(c):
                        col = c
                        break
                if col is None:
                    raise ValueError("TPEX 成交金額 column not found after selection")

                s = chosen[col].astype(str).str.replace(",", "", regex=False).str.strip()
                amt_num = pd.to_numeric(s, errors="coerce")
                missing = int(amt_num.isna().sum())
                amount_sum = int(amt_num.fillna(0).sum())
                parsed_kind = "HTML_TABLE_SUM"

                # save csv audit
                csv_path = os.path.join(AUDIT_DIR, f"TPEX_{yyyymmdd}_rows.csv")
                chosen.to_csv(csv_path, index=False, encoding="utf-8-sig")

                meta["audit"] = {
                    "market": "TPEX",
                    "trade_date": trade_date,
                    "parsed_kind": parsed_kind,
                    "rows": int(chosen.shape[0]),
                    "missing_amount_rows": missing,
                    "amount_sum": amount_sum,
                    "amount_col": str(col),
                    "raw_saved": os.path.basename(raw_path),
                    "csv_saved": os.path.basename(csv_path),
                }
                return amount_sum, meta

            # JSON 成功才走到這
            if amount_sum is None:
                raise ValueError("TPEX parsed but amount_sum is None (refuse to guess)")

            meta["audit"] = {
                "market": "TPEX",
                "trade_date": trade_date,
                "parsed_kind": parsed_kind,
                "amount_sum": amount_sum,
                "raw_saved": os.path.basename(raw_path),
            }
            return amount_sum, meta

        except Exception as e:
            logging.warning(f"[TPEX] retry {i}/{MAX_RETRY} failed: {e}")
            time.sleep(1.0 * i)

    return None, meta

# -------------------------
# Main
# -------------------------
def build_market_amount(trade_date: str) -> Dict[str, Any]:
    twse_amt, twse_meta = fetch_twse_amount(trade_date)
    tpex_amt, tpex_meta = fetch_tpex_amount(trade_date)

    amount_total = (twse_amt or 0) + (tpex_amt or 0)

    scope = "FULL" if (twse_amt is not None and tpex_amt is not None) else "TWSE_ONLY"
    status = "OK" if scope == "FULL" else "DEGRADED"

    out = {
        "trade_date": trade_date,
        "amount_twse": twse_amt,
        "amount_tpex": tpex_amt,
        "amount_total": amount_total,
        "source_twse": "TWSE_OK:AUDIT_SUM" if twse_amt is not None else "TWSE_FAIL",
        "source_tpex": "TPEX_OK:AUDIT_SUM" if tpex_amt is not None else "TPEX_FAIL:REDIRECT_ERRORS",
        "scope": scope,
        "audit_dir": AUDIT_DIR.replace("\\", "/"),
        "meta": {
            "twse": twse_meta,
            "tpex": tpex_meta,
        },
        "integrity": {
            "amount_total_null": (twse_amt is None and tpex_amt is None),
            "amount_partial": (scope != "FULL"),
            "amount_scope": scope,
            "kill": False,  # 由你上層策略決定是否 kill
            "reason": "OK" if scope == "FULL" else f"DATA_MISSING amount_scope={scope}",
        },
    }
    return out

def main():
    # 你系統是用 trade_date 控制，不要用 datetime.now() 自己變日期
    trade_date = now_ymd()

    market_amount = build_market_amount(trade_date)
    save_json(MARKET_JSON, market_amount)

    logging.info(
        f"market_amount done | date={market_amount['trade_date']} "
        f"| TWSE={market_amount['amount_twse']} "
        f"| TPEX={market_amount['amount_tpex']} "
        f"| total={market_amount['amount_total']} "
        f"| scope={market_amount['scope']} status={('OK' if market_amount['scope']=='FULL' else 'DEGRADED')}"
    )
    logging.info(f"saved: {MARKET_JSON}")
    logging.info(f"audit_dir: {AUDIT_DIR}")

if __name__ == "__main__":
    main()
