# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

import requests

TZ_TAIPEI = timezone(timedelta(hours=8))

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)
TRADING_MINUTES = 270  # 09:00~13:30

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def trading_progress(now: Optional[datetime] = None) -> float:
    """回傳盤中進度 0~1。盤外 clamp 到 0 或 1。"""
    now = now or _now_taipei()
    start_dt = now.replace(hour=TRADING_START.hour, minute=TRADING_START.minute, second=0, microsecond=0)
    end_dt = now.replace(hour=TRADING_END.hour, minute=TRADING_END.minute, second=0, microsecond=0)

    if now <= start_dt:
        return 0.0
    if now >= end_dt:
        return 1.0
    elapsed = (now - start_dt).total_seconds() / 60.0
    return max(0.0, min(1.0, elapsed / TRADING_MINUTES))


def progress_curve(p: float, alpha: float = 0.65) -> float:
    """用冪次曲線做『盤中累積量能』預期，避免早盤誤判 LOW。"""
    p = max(0.0, min(1.0, p))
    return p ** alpha


def classify_ratio(r: float) -> str:
    if r < 0.8:
        return "LOW"
    if r > 1.2:
        return "HIGH"
    return "NORMAL"


@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: Optional[str]
    source_tpex: Optional[str]
    error: Optional[str] = None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


# -----------------------------
# 免費來源（多路備援）
# 優先順序：
# 1) TWSE JSON / TPEx JSON（若成功 => 最接近官方）
# 2) Yahoo 股市首頁（同頁通常有 上市成交 / 上櫃成交）
# -----------------------------

def fetch_twse_amount_official(date_yyyymmdd: str, verify_ssl: bool = True) -> Tuple[int, str]:
    """
    TWSE 官方：MI_INDEX response=json
    注意：不同日期/版本 fields 可能變動，所以採「欄位名稱搜尋 + 加總」策略。
    """
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_yyyymmdd}&type=ALL"
    r = requests.get(url, headers=USER_AGENT, timeout=15, verify=verify_ssl)
    r.raise_for_status()
    j = r.json()

    # 可能在 j['data1'] 或 j['data']，欄位在 j['fields1'] 或 j['fields']
    fields = j.get("fields1") or j.get("fields") or []
    data = j.get("data1") or j.get("data") or []

    # 找「成交金額」欄位 index
    col_idx = None
    for i, f in enumerate(fields):
        if "成交金額" in str(f):
            col_idx = i
            break
    if col_idx is None:
        raise RuntimeError("TWSE JSON 找不到『成交金額』欄位")

    total = 0
    for row in data:
        if not isinstance(row, (list, tuple)) or len(row) <= col_idx:
            continue
        s = str(row[col_idx])
        s = re.sub(r"[^\d]", "", s)
        if s:
            total += int(s)

    if total <= 0:
        raise RuntimeError("TWSE JSON 成交金額加總為 0（可能欄位結構改版）")

    return total, "TWSE MI_INDEX (response=json) 成交金額欄位加總"


def fetch_tpex_amount_official(date_roc_slash: str, verify_ssl: bool = True) -> Tuple[int, str]:
    """
    TPEx 官方：stk_quote_result.php（每日行情摘要）
    date_roc_slash 例：114/01/28（民國/斜線）
    回傳資料常含總成交金額欄位（可能在 'aaData' or 'tables'）
    """
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={date_roc_slash}&_=0"
    r = requests.get(url, headers=USER_AGENT, timeout=15, verify=verify_ssl)
    r.raise_for_status()
    j = r.json()

    # 常見：j['aaData'] 為個股表；總成交金額有時在 j['reportDate'] 周邊或其他欄位
    # 這裡採較保守：從整包 JSON 轉字串找「總成交金額」或「成交金額」樣式數字
    txt = str(j)
    m = re.search(r"(總成交金額|成交金額).{0,20}?([0-9][0-9,]{5,})", txt)
    if not m:
        raise RuntimeError("TPEx JSON 找不到『總成交金額/成交金額』資訊（可能結構改版）")
    amt = re.sub(r"[^\d]", "", m.group(2))
    if not amt:
        raise RuntimeError("TPEx JSON 成交金額解析失敗")
    return int(amt), "TPEx stk_quote_result.php (JSON) 解析總成交金額"


def fetch_amount_from_yahoo_home(verify_ssl: bool = True) -> Tuple[int, int, str, str]:
    """
    Yahoo 股市首頁常同頁提供：
    - 上市 成交 xxxx.xx 億
    - 上櫃 成交 xxxx.xx 億
    若頁面改成 JS 動態，可能失敗，所以只做備援。
    """
    url = "https://tw.stock.yahoo.com/"
    r = requests.get(url, headers=USER_AGENT, timeout=15, verify=verify_ssl)
    r.raise_for_status()
    html = r.text

    # 嘗試抓「上市」區塊的成交（億）
    # 以你截圖的呈現，常見 "成交5714.84億" 這種格式
    m_twse = re.search(r"上市[\s\S]{0,300}?成交\s*([0-9]+(?:\.[0-9]+)?)\s*億", html)
    m_tpex = re.search(r"上櫃[\s\S]{0,300}?成交\s*([0-9]+(?:\.[0-9]+)?)\s*億", html)

    if not m_twse or not m_tpex:
        raise RuntimeError("Yahoo 首頁未找到 上市/上櫃 成交（可能改為動態載入）")

    twse_yi = float(m_twse.group(1))
    tpex_yi = float(m_tpex.group(1))

    twse_amt = int(twse_yi * 100_000_000)  # 1 億 = 1e8
    tpex_amt = int(tpex_yi * 100_000_000)

    return twse_amt, tpex_amt, "Yahoo 股市首頁（上市成交）", "Yahoo 股市首頁（上櫃成交）"


def fetch_amount_total(
    trade_date: Optional[datetime] = None,
    verify_ssl: bool = True,
) -> MarketAmount:
    """
    回傳 amount_twse / amount_tpex / amount_total
    - trade_date：預設取台北時間「今日」，但在開盤前你可能希望抓「昨日」=> 由 main.py 決定傳入哪天
    - verify_ssl：若 Streamlit Cloud 或中繼 SSL 出現奇怪憑證錯誤，可暫時設 False（模擬期可接受）
    """
    td = trade_date or _now_taipei()
    ymd = td.strftime("%Y%m%d")

    # TPEx 要民國格式：114/01/28
    roc_year = td.year - 1911
    roc = f"{roc_year:03d}/{td.month:02d}/{td.day:02d}"

    # 1) 官方優先（若 SSL 失敗，可由 main.py 改 verify_ssl=False）
    try:
        twse, s1 = fetch_twse_amount_official(ymd, verify_ssl=verify_ssl)
        tpex, s2 = fetch_tpex_amount_official(roc, verify_ssl=verify_ssl)
        return MarketAmount(
            amount_twse=twse,
            amount_tpex=tpex,
            amount_total=twse + tpex,
            source_twse=s1,
            source_tpex=s2,
            error=None,
        )
    except Exception as e1:
        # 2) Yahoo 備援
        try:
            twse2, tpex2, s1b, s2b = fetch_amount_from_yahoo_home(verify_ssl=verify_ssl)
            return MarketAmount(
                amount_twse=twse2,
                amount_tpex=tpex2,
                amount_total=twse2 + tpex2,
                source_twse=s1b,
                source_tpex=s2b,
                error=f"Official failed: {type(e1).__name__}: {str(e1)}",
            )
        except Exception as e2:
            return MarketAmount(
                amount_twse=None,
                amount_tpex=None,
                amount_total=None,
                source_twse=None,
                source_tpex=None,
                error=f"Official failed: {type(e1).__name__}: {str(e1)} | Yahoo failed: {type(e2).__name__}: {str(e2)}",
            )


def intraday_norm(
    amount_total_now: int,
    amount_total_prev: Optional[int],
    avg20_amount_total: Optional[int],
    now: Optional[datetime] = None,
    alpha: float = 0.65,
) -> Dict[str, Any]:
    """
    INTRADAY 量能正規化（V15.7）

    - 穩健型看：累積正規化 (amount_norm_cum_ratio)
    - 保守型看：切片正規化 (amount_norm_slice_ratio) => 需 prev
    - 試投型：忽略量能（由 arbiter 再決定）

    回傳：
    - progress：盤中進度 0~1
    - amount_norm_cum_ratio：累積比率
    - amount_norm_slice_ratio：切片比率
    - amount_norm_label：NORMAL/LOW/HIGH（以 cum_ratio 判定）
    """
    now = now or _now_taipei()
    p_now = trading_progress(now)
    p_prev = max(0.0, p_now - (5 / TRADING_MINUTES))

    out: Dict[str, Any] = {
        "progress": round(p_now, 4),
        "amount_norm_cum_ratio": None,
        "amount_norm_slice_ratio": None,
        "amount_norm_label": "UNKNOWN",
    }

    if not avg20_amount_total or avg20_amount_total <= 0:
        return out

    expected_cum = avg20_amount_total * progress_curve(p_now, alpha=alpha)
    cum_ratio = (amount_total_now / expected_cum) if expected_cum > 0 else None
    out["amount_norm_cum_ratio"] = None if cum_ratio is None else round(float(cum_ratio), 4)
    if cum_ratio is not None:
        out["amount_norm_label"] = classify_ratio(float(cum_ratio))

    if amount_total_prev is not None:
        slice_amount = max(0, amount_total_now - amount_total_prev)
        expected_slice = avg20_amount_total * (progress_curve(p_now, alpha=alpha) - progress_curve(p_prev, alpha=alpha))
        slice_ratio = (slice_amount / expected_slice) if expected_slice > 0 else None
        out["amount_norm_slice_ratio"] = None if slice_ratio is None else round(float(slice_ratio), 4)

    return out
