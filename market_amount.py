# market_amount.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import re
import warnings
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

import requests
import pandas as pd

# ====== Time / Trading Session ======
TZ_TAIPEI = timezone(timedelta(hours=8))

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)
TRADING_MINUTES = 270  # 09:00~13:30

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (compatible; SunheroStockBot/1.0; +https://github.com/)"
}


def _to_int_amount(x) -> int:
    """把 '775,402,495,419' 之類字串轉 int。非數字會被剔除。"""
    if x is None:
        return 0
    s = str(x)
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else 0


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def trading_progress(now: Optional[datetime] = None) -> float:
    """回傳盤中進度 0~1。盤外會 clamp 到 0 或 1。"""
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
    """
    盤中累積量能預期曲線（冪次曲線）。
    alpha < 1 代表早盤預期累積比率較高，避免「盤中動不動就 LOW」。
    """
    p = max(0.0, min(1.0, p))
    return p ** alpha


@dataclass
class MarketAmount:
    amount_twse: int
    amount_tpex: int
    amount_total: int
    source_twse: str
    source_tpex: str


# ====== Fetch TWSE / TPEx ======
def fetch_twse_amount(verify_ssl: bool = False) -> Tuple[int, str]:
    """
    上市成交金額（元）：抓 TWSE MI_INDEX(HTML) 的成交統計表，把各類別成交金額加總。

    注意（你在 Streamlit Cloud 已遇到）：
    - TWSE 憑證鏈可能造成 SSLError CERTIFICATE_VERIFY_FAILED
    - 模擬/免費階段：可接受只對 TWSE 關閉 verify（verify_ssl=False）
    """
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?date=&response=html"

    # 對 TWSE 關閉 verify 的工程妥協（避免 Cloud 憑證問題）
    # 並用 warning 明確標記（不隱瞞）
    if verify_ssl is False:
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    r = requests.get(url, headers=USER_AGENT, timeout=15, verify=verify_ssl)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    if not tables:
        raise RuntimeError("TWSE MI_INDEX 找不到可解析表格")

    # 嘗試找含「成交金額」的表；找不到就用第一張表 fallback
    target = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("成交金額" in c for c in cols):
            target = t
            break
    if target is None:
        target = tables[0]

    # 取出成交金額欄位
    amt_col = None
    for c in target.columns:
        if "成交金額" in str(c):
            amt_col = c
            break
    if amt_col is None:
        raise RuntimeError("TWSE 成交統計表找不到『成交金額』欄")

    amount = int(target[amt_col].apply(_to_int_amount).sum())

    src = "TWSE MI_INDEX(HTML) 成交統計各類別加總"
    if verify_ssl is False:
        src += "（TWSE verify=False；因雲端 SSL 鏈問題的模擬期妥協）"

    return amount, src


def fetch_tpex_amount() -> Tuple[int, str]:
    """
    上櫃成交金額（元）：
    - 優先 regex 抓 TPEx pricing.html 的「總成交金額」
    - 若 regex 失敗，再用 pd.read_html fallback（網頁結構變動時仍有機會救回）
    """
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
    r = requests.get(url, headers=USER_AGENT, timeout=15)
    r.raise_for_status()

    # regex：總成交金額: 175,152,956,339元
    m = re.search(r"總成交金額[:：]\s*([\d,]+)\s*元", r.text)
    if m:
        return _to_int_amount(m.group(1)), "TPEx pricing.html（regex：總成交金額）"

    # fallback 1：更寬鬆的 regex
    m2 = re.search(r"總成交金額.*?([\d,]+)\s*元", r.text)
    if m2:
        return _to_int_amount(m2.group(1)), "TPEx pricing.html（fallback regex：總成交金額）"

    # fallback 2：read_html（可能受版面影響，但保留救援）
    try:
        tables = pd.read_html(r.text)
        for t in tables:
            # 嘗試從表格裡找「總成交金額」
            flat = t.astype(str).values.flatten().tolist()
            for cell in flat:
                if "總成交金額" in cell:
                    # 把同列或同 cell 的數字抽出
                    m3 = re.search(r"([\d,]+)", cell)
                    if m3:
                        return _to_int_amount(m3.group(1)), "TPEx pricing.html（read_html fallback）"
        # 最後再整頁掃數字（風險較高，但至少不中斷）
        m4 = re.search(r"總成交金額[^0-9]*([\d,]+)", r.text)
        if m4:
            return _to_int_amount(m4.group(1)), "TPEx pricing.html（page-scan fallback）"
    except Exception:
        pass

    raise RuntimeError("TPEx pricing.html 找不到『總成交金額』")


def fetch_amount_total() -> MarketAmount:
    """
    合計成交金額：上市(TWSE) + 上櫃(TPEx)
    """
    twse, s1 = fetch_twse_amount(verify_ssl=False)  # Streamlit Cloud 實務
    tpex, s2 = fetch_tpex_amount()
    return MarketAmount(
        amount_twse=int(twse),
        amount_tpex=int(tpex),
        amount_total=int(twse) + int(tpex),
        source_twse=s1,
        source_tpex=s2,
    )


def fetch_amount_total_safe() -> Tuple[Optional[MarketAmount], Dict[str, Any]]:
    """
    安全版抓取：任何一段失敗都不中斷，回傳 error 給上層寫入 macro.amount_sources。
    回傳：
      - MarketAmount 或 None
      - sources dict：{"twse":..., "tpex":..., "error":...}
    """
    sources: Dict[str, Any] = {"twse": None, "tpex": None, "error": None}

    twse_amt = None
    tpex_amt = None
    src_twse = None
    src_tpex = None

    # TWSE
    try:
        twse_amt, src_twse = fetch_twse_amount(verify_ssl=False)
        sources["twse"] = src_twse
    except Exception as e:
        sources["error"] = f"TWSE {type(e).__name__}: {str(e)}"

    # TPEx
    try:
        tpex_amt, src_tpex = fetch_tpex_amount()
        sources["tpex"] = src_tpex
    except Exception as e:
        if sources["error"]:
            sources["error"] += f" | TPEx {type(e).__name__}: {str(e)}"
        else:
            sources["error"] = f"TPEx {type(e).__name__}: {str(e)}"

    if twse_amt is None or tpex_amt is None:
        return None, sources

    ma = MarketAmount(
        amount_twse=int(twse_amt),
        amount_tpex=int(tpex_amt),
        amount_total=int(twse_amt) + int(tpex_amt),
        source_twse=src_twse or "TWSE",
        source_tpex=src_tpex or "TPEx",
    )
    return ma, sources


# ====== INTRADAY Normalization ======
def classify_ratio(r: float) -> str:
    """
    量能分類（可調門檻）：
      <0.8  LOW
      0.8~1.2 NORMAL
      >1.2  HIGH
    """
    if r < 0.8:
        return "LOW"
    if r > 1.2:
        return "HIGH"
    return "NORMAL"


def intraday_norm(
    amount_total_now: int,
    amount_total_prev: Optional[int],
    avg20_amount_total: Optional[int],
    now: Optional[datetime] = None,
    alpha: float = 0.65,
) -> dict:
    """
    回傳（你要的盤中不再動不動 LOW）：
      - progress：盤中進度 0~1
      - amount_norm_cum_ratio：累積正規化比率（穩健型用）
      - amount_norm_slice_ratio：切片正規化比率（保守型用，需 prev）
      - amount_norm_label：LOW/NORMAL/HIGH/UNKNOWN（以 cum_ratio 判定）
    """
    now = now or _now_taipei()
    p_now = trading_progress(now)

    # 切片：預設 5 分鐘視窗（你 main.py 用 cache 每次 Run 更新）
    p_prev = max(0.0, p_now - (5 / TRADING_MINUTES))

    out = {
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

    # slice（需要前值）
    if amount_total_prev is not None:
        slice_amount = max(0, amount_total_now - amount_total_prev)
        expected_slice = avg20_amount_total * (progress_curve(p_now, alpha=alpha) - progress_curve(p_prev, alpha=alpha))
        slice_ratio = (slice_amount / expected_slice) if expected_slice > 0 else None
        out["amount_norm_slice_ratio"] = None if slice_ratio is None else round(float(slice_ratio), 4)

    return out
