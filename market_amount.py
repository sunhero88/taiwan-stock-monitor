# market_amount.py
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Tuple, List, Optional

import certifi
import requests

TZ_TAIPEI = ZoneInfo("Asia/Taipei")

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}

@dataclass
class MarketAmount:
    amount_twse: int
    amount_tpex: int
    amount_total: int
    source_twse: str
    source_tpex: str
    warnings: List[str]


def _get(url: str, timeout: int = 12, allow_insecure_ssl: bool = False) -> requests.Response:
    try:
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not allow_insecure_ssl:
            raise
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=False)


def _safe_head(text: str, n: int = 200) -> str:
    t = (text or "").replace("\n", " ").replace("\r", " ").strip()
    return t[:n]


def _as_yyyymmdd(dt: Optional[datetime] = None) -> str:
    d = dt.astimezone(TZ_TAIPEI) if dt else datetime.now(tz=TZ_TAIPEI)
    return d.strftime("%Y%m%d")


def _parse_int_any(x) -> int:
    try:
        s = str(x).replace(",", "").strip()
        if s in ("", "--", "None", "nan"):
            return 0
        return int(float(s))
    except Exception:
        return 0


def _fetch_twse_amount(allow_insecure_ssl: bool) -> Tuple[int, str, List[str]]:
    """
    TWSE 成交金額（上市）
    優先用 JSON：MI_INDEX（常見欄位含「成交金額」）
    """
    warnings: List[str] = []
    yyyymmdd = _as_yyyymmdd()

    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={yyyymmdd}&type=ALL&response=json"

    try:
        r = _get(url, timeout=12, allow_insecure_ssl=allow_insecure_ssl)
        if r.status_code != 200:
            warnings.append(f"TWSE_AMOUNT_HTTP_FAIL:{r.status_code}:{_safe_head(r.text)}")
            return 0, f"TWSE_HTTP_{r.status_code}", warnings

        j = r.json()

        # data9 / data8 在不同版本會變，做容錯掃描
        # 目標：找到市場總成交金額（通常在「大盤統計資訊」表）
        candidates = []
        for k in ("data9", "data8", "data7", "data6", "data5", "data4", "data3", "data2", "data1", "data"):
            v = j.get(k)
            if isinstance(v, list) and len(v) > 0:
                candidates.append((k, v))

        # 掃描內容，找包含「成交金額」的列
        for key, arr in candidates:
            for row in arr:
                # row 可能是 list
                if not isinstance(row, list) or len(row) < 2:
                    continue
                left = str(row[0])
                if "成交金額" in left:
                    amt = _parse_int_any(row[1])
                    if amt > 0:
                        return amt, f"TWSE_MI_INDEX:{key}:{yyyymmdd}", warnings

        # 若找不到，視為 parse fail（可稽核：吐前200字）
        warnings.append(f"TWSE_AMOUNT_PARSE_FAIL:{yyyymmdd}:{_safe_head(r.text)}")
        return 0, f"TWSE_PARSE_FAIL:{yyyymmdd}", warnings

    except requests.exceptions.SSLError as e:
        warnings.append(f"TWSE_AMOUNT_SSL_FAIL:{yyyymmdd}:{type(e).__name__}")
        return 0, "TWSE_SSL_FAIL", warnings
    except ValueError:
        warnings.append(f"TWSE_AMOUNT_JSON_FAIL:{yyyymmdd}:JSONDecodeError:{_safe_head(getattr(r,'text',''))}")
        return 0, "TWSE_JSON_FAIL", warnings
    except Exception as e:
        warnings.append(f"TWSE_AMOUNT_FATAL:{yyyymmdd}:{type(e).__name__}:{str(e)[:120]}")
        return 0, "TWSE_FATAL", warnings


def _fetch_tpex_amount(allow_insecure_ssl: bool) -> Tuple[int, str, List[str]]:
    """
    TPEx 成交金額（上櫃）
    優先用 JSON：每日交易資訊（欄位可能變動，所以以「文字掃描」找成交金額）
    """
    warnings: List[str] = []
    # TPEX 常用民國日期格式：YYY/MM/DD
    d = datetime.now(tz=TZ_TAIPEI)
    roc = d.year - 1911
    roc_date = f"{roc}/{d.strftime('%m/%d')}"

    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={roc_date}&o=json"

    try:
        r = _get(url, timeout=12, allow_insecure_ssl=allow_insecure_ssl)
        if r.status_code != 200:
            warnings.append(f"TPEX_AMOUNT_HTTP_FAIL:{r.status_code}:{_safe_head(r.text)}")
            return 0, f"TPEX_HTTP_{r.status_code}", warnings

        j = r.json()

        # 不同回傳格式差異大：先把所有值轉字串掃描「成交金額」關鍵
        text = r.text or ""
        if "成交金額" in text:
            # 嘗試抓出數字：最保守作法→掃描 JSON 中可能的欄位
            # 常見：j["aaData"] 為表格，或 j["tables"] 類型
            for key in ("aaData", "data", "tables", "result"):
                v = j.get(key)
                if isinstance(v, list):
                    # 逐列掃描
                    for row in v:
                        if isinstance(row, list) and len(row) >= 2:
                            if "成交金額" in str(row[0]):
                                amt = _parse_int_any(row[1])
                                if amt > 0:
                                    return amt, f"TPEX_STK_QUOTE:{roc_date}:{key}", warnings

        # 如果沒命中，視為 parse fail（吐前200字）
        warnings.append(f"TPEX_AMOUNT_PARSE_FAIL:{roc_date}:{_safe_head(r.text)}")
        return 0, f"TPEX_PARSE_FAIL:{roc_date}", warnings

    except requests.exceptions.SSLError as e:
        warnings.append(f"TPEX_AMOUNT_SSL_FAIL:{roc_date}:{type(e).__name__}")
        return 0, "TPEX_SSL_FAIL", warnings
    except ValueError:
        warnings.append(f"TPEX_AMOUNT_JSON_FAIL:{roc_date}:JSONDecodeError:{_safe_head(getattr(r,'text',''))}")
        return 0, "TPEX_JSON_FAIL", warnings
    except Exception as e:
        warnings.append(f"TPEX_AMOUNT_FATAL:{roc_date}:{type(e).__name__}:{str(e)[:120]}")
        return 0, "TPEX_FATAL", warnings


def fetch_amount_total(allow_insecure_ssl: bool = False) -> MarketAmount:
    """
    回傳：上市、上櫃、合計成交金額（元）
    allow_insecure_ssl=True 時允許 verify=False。
    """
    twse_amt, twse_src, w1 = _fetch_twse_amount(allow_insecure_ssl)
    tpex_amt, tpex_src, w2 = _fetch_tpex_amount(allow_insecure_ssl)
    total = int(twse_amt) + int(tpex_amt)
    warnings = (w1 or []) + (w2 or [])
    return MarketAmount(
        amount_twse=int(twse_amt),
        amount_tpex=int(tpex_amt),
        amount_total=int(total),
        source_twse=twse_src,
        source_tpex=tpex_src,
        warnings=warnings
    )
