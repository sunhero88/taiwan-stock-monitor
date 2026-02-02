# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone, timedelta
from typing import Optional, Tuple

import certifi
import requests
import urllib3

TZ_TAIPEI = timezone(timedelta(hours=8))

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

def _requests_get(url: str, timeout: int, allow_insecure_ssl: bool) -> requests.Response:
    try:
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not allow_insecure_ssl:
            raise
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return requests.get(url, headers=USER_AGENT, timeout=timeout, verify=False)

def _to_int_maybe(x) -> int:
    try:
        s = str(x).replace(",", "").strip()
        if s in ("", "--", "None", "nan"):
            return 0
        return int(float(s))
    except Exception:
        return 0

def _fetch_twse_amount(allow_insecure_ssl: bool) -> Tuple[int, str]:
    """
    TWSE 成交金額（當日）
    endpoint 常見：
    https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?type=ALLBUT0999&response=json
    但欄位會飄，這裡做 best-effort 解析。
    """
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?type=ALLBUT0999&response=json"
    r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
    r.raise_for_status()
    j = r.json()

    # 嘗試從「總計」或「合計」列找出成交金額
    # 不同版本可能在 data9 / data7 / 等位置
    candidates = []
    for k, v in j.items():
        if k.startswith("data") and isinstance(v, list):
            candidates.append(v)

    # 欄位常見包含：成交金額
    for data in candidates:
        for row in data:
            if not isinstance(row, list):
                continue
            # 嘗試掃描 row 裡看起來像「成交金額」的欄位（通常是大數字）
            # 取最大值當成交金額（best-effort）
            nums = [_to_int_maybe(x) for x in row]
            mx = max(nums) if nums else 0
            if mx > 10_000_000:  # 避免誤抓小數字
                return mx, "TWSE:MI_INDEX(best-effort)"

    raise ValueError("TWSE_AMOUNT_PARSE_FAIL")

def _fetch_tpex_amount(allow_insecure_ssl: bool) -> Tuple[int, str]:
    """
    TPEx 成交金額：公開資訊來源回傳格式常變。
    這裡用 r.text 容錯：先嘗試 json，失敗則改用粗略正則抓數字（避免 JSONDecodeError 直接炸）。
    """
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw"
    r = _requests_get(url, timeout=15, allow_insecure_ssl=allow_insecure_ssl)
    r.raise_for_status()

    # 1) 先嘗試 JSON
    try:
        j = r.json()
        # 常見欄位：aaData / data，內含成交金額欄位（但位置不固定）
        for key in ("aaData", "data"):
            if key in j and isinstance(j[key], list):
                # best-effort：掃整包找最大數字當成交金額
                mx = 0
                for row in j[key]:
                    if isinstance(row, list):
                        nums = [_to_int_maybe(x) for x in row]
                        if nums:
                            mx = max(mx, max(nums))
                if mx > 10_000_000:
                    return mx, "TPEX:st43(json best-effort)"
        raise ValueError("TPEX_JSON_PARSE_FAIL")
    except Exception:
        pass

    # 2) JSON 失敗：用文字粗略抓最大大數字
    import re
    nums = [int(n.replace(",", "")) for n in re.findall(r"\b\d{1,3}(?:,\d{3})+\b", r.text)]
    mx = max(nums) if nums else 0
    if mx > 10_000_000:
        return mx, "TPEX:st43(text best-effort)"

    raise ValueError("TPEX_AMOUNT_PARSE_FAIL")

def fetch_amount_total(allow_insecure_ssl: bool = False) -> MarketAmount:
    """
    回傳：上市、上櫃、合計成交金額（元）
    allow_insecure_ssl=True 時允許 verify=False 以繞過舊憑證/鏈問題。
    """
    twse_amt, twse_src = _fetch_twse_amount(allow_insecure_ssl)
    tpex_amt, tpex_src = _fetch_tpex_amount(allow_insecure_ssl)
    total = int(twse_amt) + int(tpex_amt)
    return MarketAmount(
        amount_twse=int(twse_amt),
        amount_tpex=int(tpex_amt),
        amount_total=int(total),
        source_twse=twse_src,
        source_tpex=tpex_src,
    )
