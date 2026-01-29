# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import math
import inspect
from dataclasses import asdict
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import streamlit as st
import yfinance as yf

# 你目前 repo 有 market_amount.py（你貼過完整碼）
from market_amount import fetch_amount_total, intraday_norm, MarketAmount

TZ_TAIPEI = timezone(timedelta(hours=8))

APP_TITLE = "Sunhero｜股市智能超盤中控台"
APP_VERSION = "V15.7（免費/模擬期 Route A）"

# ----------------------------
# 0) 工具：時間/交易日
# ----------------------------
def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def is_weekend(d: date) -> bool:
    return d.weekday() >= 5

def prev_trading_day(d: date) -> date:
    # 免費/模擬期：先用「週末排除」當交易日近似（不引入付費行事曆）
    x = d
    while is_weekend(x):
        x = x - timedelta(days=1)
    return x

def resolve_trade_date(session: str, now: Optional[datetime] = None) -> date:
    """
    你的需求：開盤前要看到「昨日 EOD」。
    - 若現在時間 < 09:00：trade_date = 前一個交易日（週末排除）
    - 盤中/盤後：trade_date = 今天（週末排除）
    """
    now = now or now_taipei()
    today = now.date()
    today = prev_trading_day(today)

    if now.hour < 9:
        # 開盤前：看昨日
        y = today - timedelta(days=1)
        return prev_trading_day(y)

    # 盤中/盤後
    return today

# ----------------------------
# 1) 中文名稱：免費策略
# ----------------------------
DEFAULT_NAME_MAP_TW = {
    "2330.TW": "台積電",
    "2317.TW": "鴻海",
    "2454.TW": "聯發科",
    "2308.TW": "台達電",
    "2382.TW": "廣達",
    "3231.TW": "緯創",
    "2603.TW": "長榮",
    "2609.TW": "陽明",
}

def load_name_map() -> Dict[str, str]:
    """
    免費/穩定優先：
    1) configs/stock_name_map.csv（你可自行維護）
       欄位：Symbol,Name
    2) fallback：DEFAULT_NAME_MAP_TW
    """
    path = os.path.join("configs", "stock_name_map.csv")
    m = dict(DEFAULT_NAME_MAP_TW)
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            if "Symbol" in df.columns and "Name" in df.columns:
                for _, r in df.iterrows():
                    s = str(r["Symbol"]).strip()
                    n = str(r["Name"]).strip()
                    if s and n:
                        m[s] = n
        except Exception:
            pass
    return m

# ----------------------------
# 2) 全球市場摘要（免費）
# ----------------------------
GLOBAL_SYMBOLS = [
    ("US", "SOX_Semi", "^SOX"),
    ("US", "TSM_ADR", "TSM"),
    ("US", "NVIDIA", "NVDA"),
    ("US", "Apple", "AAPL"),
    ("ASIA", "Nikkei_225", "^N225"),
    ("ASIA", "USD_JPY", "JPY=X"),
    ("ASIA", "USD_TWD", "TWD=X"),  # yfinance 的匯率符號有時不穩；抓不到就顯示 NaN
]

@st.cache_data(ttl=300)
def fetch_global_summary() -> pd.DataFrame:
    rows = []
    for market, label, sym in GLOBAL_SYMBOLS:
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="5d", interval="1d", auto_adjust=False)
            if hist is None or hist.empty:
                rows.append((market, label, np.nan, np.nan))
                continue
            # 最新收盤 vs 前一日
            close = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else close
            chg_pct = (close / prev - 1.0) * 100.0 if prev != 0 else 0.0
            rows.append((market, label, round(chg_pct, 4), round(close, 4)))
        except Exception:
            rows.append((market, label, np.nan, np.nan))
    return pd.DataFrame(rows, columns=["Market", "Symbol", "Change(%)", "Value"])

# ----------------------------
# 3) 台股候選 Universe（Route A）
# ----------------------------
DEFAULT_UNIVERSE_TW = [
    "2330.TW", "2317.TW", "2454.TW", "2308.TW",
    "2382.TW", "3231.TW", "2603.TW", "2609.TW",
]

def parse_holdings(raw: str) -> List[str]:
    """
    允許輸入：
    - 2330 / 2317 → 轉成 2330.TW / 2317.TW
    - 2330.TW → 原樣
    以逗號/空白分隔
    """
    if not raw:
        return []
    tokens = []
    for part in raw.replace(" ", ",").split(","):
        s = part.strip()
        if not s:
            continue
        if s.isdigit() and len(s) == 4:
            s = f"{s}.TW"
        tokens.append(s)
    # 去重但保持順序
    out = []
    seen = set()
    for s in tokens:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

# ----------------------------
# 4) 指標計算（免費：yfinance）
# ----------------------------
@st.cache_data(ttl=300)
def download_prices(symbols: List[str], period: str = "6mo") -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    data = yf.download(
        tickers=symbols,
        period=period,
        interval="1d",
        group_by="column",
        auto_adjust=False,
        progress=False,
        threads=True
    )
    return data

def compute_features(data: pd.DataFrame, symbols: List[str], trade_date: date) -> pd.DataFrame:
    """
    回傳欄位：
    Symbol, Name, Date, Close, Volume, MA20, MA60, MA_Bias(%), Vol_Ratio, Score, Predator_Tag
    """
    if data is None or data.empty:
        return pd.DataFrame(columns=[
            "Symbol","Name","Date","Close","Volume","MA_Bias","Vol_Ratio","Score","Predator_Tag"
        ])

    name_map = load_name_map()
    rows = []

    # yfinance download 回來是 MultiIndex 欄位：('Close', '2330.TW') 或相反格式
    # 這裡用 data["Close"][sym] 兼容常見格式
    for sym in symbols:
        try:
            close = data["Close"][sym].dropna()
            vol = data["Volume"][sym].dropna() if "Volume" in data else pd.Series(dtype=float)

            if close.empty:
                continue

            # 找到 <= trade_date 的最後一筆（避免你看到 3 天前）
            close_idx = close.index.date
            mask = [d <= trade_date for d in close_idx]
            close_use = close[mask]
            if close_use.empty:
                continue

            last_dt = close_use.index[-1]
            last_close = float(close_use.iloc[-1])

            # volume 同步到同一天（可能缺）
            last_vol = float(vol.loc[last_dt]) if (not vol.empty and last_dt in vol.index) else np.nan

            ma20 = float(close_use.rolling(20).mean().iloc[-1]) if len(close_use) >= 20 else float(close_use.mean())
            ma60 = float(close_use.rolling(60).mean().iloc[-1]) if len(close_use) >= 60 else float(close_use.mean())
            ma_ref = ma20 if ma20 and not math.isnan(ma20) else ma60
            ma_bias = ((last_close / ma_ref) - 1.0) * 100.0 if ma_ref else 0.0

            # 20D 平均量
            if not vol.empty:
                vol_use = vol.loc[close_use.index]
                v20 = float(vol_use.rolling(20).mean().iloc[-1]) if len(vol_use) >= 20 else float(vol_use.mean())
                vol_ratio = (last_vol / v20) if (v20 and not math.isnan(last_vol)) else np.nan
            else:
                vol_ratio = np.nan

            # 免費期 Score：以趨勢 + 量比做簡化
            # 你要的是「能運作、可解釋、避免亂 LOW」
            score = (ma_bias * 1.0) + (0.0 if math.isnan(vol_ratio) else (vol_ratio - 1.0) * 10.0)

            # Tag（可在你後續 Arbiter 進一步嚴格化）
            if ma_bias > 2 and (not math.isnan(vol_ratio) and vol_ratio > 1.2):
                tag = "🟢起漲(確認)"
            elif ma_bias > 0:
                tag = "🟢起漲(觀望)"
            else:
                tag = "○觀察(觀望)"

            rows.append({
                "Symbol": sym,
                "Name": name_map.get(sym, ""),
                "Date": last_dt.date().isoformat(),
                "Close": round(last_close, 4),
                "Volume": (None if math.isnan(last_vol) else int(last_vol)),
                "MA_Bias": round(ma_bias, 4),
                "Vol_Ratio": (None if math.isnan(vol_ratio) else round(float(vol_ratio), 4)),
                "Score": round(float(score), 2),
                "Predator_Tag": tag,
            })
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)
    df["Rank"] = np.arange(1, len(df) + 1)
    return df

# ----------------------------
# 5) 安全呼叫 fetch_amount_total（防止參數不一致）
# ----------------------------
def safe_fetch_amount_total(trade_date: date, verify_ssl: bool) -> Dict:
    """
    兼容你 market_amount.py 可能是：
    - fetch_amount_total() 無參數
    - 或 fetch_amount_total(trade_date=..., verify_ssl=...)
    - 或回傳 MarketAmount dataclass / dict
    """
    try:
        sig = inspect.signature(fetch_amount_total)
        kwargs = {}
        if "trade_date" in sig.parameters:
            kwargs["trade_date"] = trade_date
        if "verify_ssl" in sig.parameters:
            kwargs["verify_ssl"] = verify_ssl

        raw = fetch_amount_total(**kwargs)

        # 統一成 dict
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, MarketAmount):
            d = asdict(raw)
            d["error"] = None
            return d

        # object 兼容
        return {
            "amount_twse": getattr(raw, "amount_twse", None),
            "amount_tpex": getattr(raw, "amount_tpex", None),
            "amount_total": getattr(raw, "amount_total", None),
            "source_twse": getattr(raw, "source_twse", None),
            "source_tpex": getattr(raw, "source_tpex", None),
            "error": getattr(raw, "error", None),
        }
    except Exception as e:
        return {
            "amount_twse": None,
            "amount_tpex": None,
            "amount_total": None,
            "source_twse": None,
            "source_tpex": None,
            "error": f"{type(e).__name__}: {e}",
        }

# ----------------------------
# 6) UI
# ----------------------------
def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption(f"{APP_VERSION}｜資料來源：yfinance +（可用時）TWSE/TPEx｜FinMind（付費則停用）")

    # Sidebar
    st.sidebar.header("控制台")
    market = st.sidebar.selectbox("Market", ["tw-share"], index=0)
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=0)

    holdings_raw = st.sidebar.text_input(
        "持倉股（逗號分隔，例如：2330,2317 或 2330.TW）",
        value=""
    )
    holdings = parse_holdings(holdings_raw)

    verify_ssl = st.sidebar.checkbox("SSL 驗證（若官方資料抓不到可先關閉）", value=True)

    run = st.sidebar.button("Run", type="primary")

    # --- 每次 Run 才刷新 cache（避免你一直看到舊資料）
    if run:
        st.cache_data.clear()

    # --- trade_date：開盤前看昨日
    trade_d = resolve_trade_date(session=session, now=now_taipei())

    # 顯示當前時間與 trade_date
    info_bar = st.container()
    with info_bar:
        st.info(
            f"目前台北時間：{now_taipei().strftime('%Y-%m-%d %H:%M')}｜"
            f"顯示交易日：{trade_d.isoformat()}（開盤前自動顯示昨日 EOD）"
        )

    # ----------------------------
    # A) 全球市場摘要
    # ----------------------------
    st.subheader("全球市場摘要（美股/日經/匯率）")
    gdf = fetch_global_summary()
    st.dataframe(gdf, use_container_width=True)

    # ----------------------------
    # B) 市場成交金額（上市 + 上櫃）
    # ----------------------------
    st.subheader("市場成交金額（上市 + 上櫃 = amount_total）")

    ma = safe_fetch_amount_total(trade_date=trade_d, verify_ssl=verify_ssl)

    c1, c2, c3, c4 = st.columns(4)
    def _fmt_amt(v):
        if v is None or (isinstance(v, float) and math.isnan(v)) or v == 0:
            return "待更新"
        return f"{int(v)/100_000_000:,.0f} 億"

    c1.metric("TWSE 上市", _fmt_amt(ma.get("amount_twse")))
    c2.metric("TPEx 上櫃", _fmt_amt(ma.get("amount_tpex")))
    c3.metric("Total 合計", _fmt_amt(ma.get("amount_total")))

    # 免費期：沒有可靠免費的 20D median（除非你把歷史 amount 落地）
    c4.metric("20D Median(代理)", "None")

    st.caption(
        f"來源/錯誤：{{'twse': {ma.get('source_twse')}, 'tpex': {ma.get('source_tpex')}, 'error': {ma.get('error')}}}"
    )

    # ----------------------------
    # C) INTRADAY 量能正規化（你要避免早盤動不動 LOW）
    # ----------------------------
    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")

    # 免費期：若你沒有把 20D amount_total 的歷史存成檔案/DB，就只能 UNKNOWN
    # 先設 None（不會炸），你日後可把 avg20_amount_total_median 接回來
    avg20_amount_total_median = None
    amount_prev = None  # 若你做 5 分鐘輪詢可帶入 prev

    norm = intraday_norm(
        amount_total_now=ma.get("amount_total") or 0,
        amount_total_prev=amount_prev,
        avg20_amount_total=avg20_amount_total_median,
        alpha=0.65,
    )
    st.json({
        "progress": norm.get("progress"),
        "cum_ratio(穩健型用)": norm.get("amount_norm_cum_ratio"),
        "slice_ratio(保守型用)": norm.get("amount_norm_slice_ratio"),
        "label": norm.get("amount_norm_label"),
    })

    # ----------------------------
    # D) Top20（Route A：免費/模擬期推薦）
    # ----------------------------
    st.subheader("Top List（Route A：Universe + 持倉 → 動態排名）")

    # Universe + 持倉（你問過 20+1 的問題：這裡就是 20 + 持倉）
    universe = list(DEFAULT_UNIVERSE_TW)
    for s in holdings:
        if s not in universe:
            universe.append(s)

    data = download_prices(universe, period="6mo")
    df = compute_features(data, universe, trade_date=trade_d)

    # Top20 + 持倉全部展示
    # 你若要只顯示 Top20，可以 df.head(20)，但持倉仍要顯示，所以用標記
    holdings_set = set(holdings)

    if not df.empty:
        df["Is_Holding"] = df["Symbol"].apply(lambda x: x in holdings_set)
        # Top20 旗標：免費 Route A 的「Top20」指的是「此 Universe 的 Top20」
        df["Top20_Flag"] = df["Rank"].apply(lambda r: r <= 20)
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("無法取得候選股價格資料（可能是 yfinance 暫時失效或網路問題）。")

    # ----------------------------
    # E) Arbiter Input（你問：AI JSON 內有哪些數據）
    # ----------------------------
    st.subheader("Arbiter Input（JSON）")

    # inst_status：免費期（FinMind 付費 402）→ UNAVAILABLE
    inst_status = "UNAVAILABLE"
    inst_dates_3d = []

    # degraded_mode：資料黑洞防線
    amount_ok = (ma.get("amount_total") is not None and (ma.get("amount_total") or 0) > 0)
    amount_label = norm.get("amount_norm_label", "UNKNOWN")

    # 你的裁決要求：成交金額缺失 OR label UNKNOWN → 禁止 BUY/TRIAL
    degraded_mode = (not amount_ok) or (amount_label == "UNKNOWN") or (inst_status != "READY")

    market_comment = []
    if not amount_ok:
        market_comment.append("成交金額待更新")
    if amount_label == "UNKNOWN":
        market_comment.append("量能正規化標籤=UNKNOWN（缺 20D 代理）")
    if inst_status != "READY":
        market_comment.append("法人資料不可用（免費期）")

    if degraded_mode:
        market_comment.append("裁決層進入資料降級：禁止 BUY/TRIAL")

    arbiter = {
        "meta": {
            "system": "Predator V15.7 (Route A Free/Sim)",
            "market": market,
            "timestamp": now_taipei().strftime("%Y-%m-%d %H:%M"),
            "session": session,
        },
        "macro": {
            "overview": {
                "amount_twse": ma.get("amount_twse"),
                "amount_tpex": ma.get("amount_tpex"),
                "amount_total": ma.get("amount_total"),
                "amount_sources": {
                    "twse": ma.get("source_twse"),
                    "tpex": ma.get("source_tpex"),
                    "error": ma.get("error"),
                },
                "avg20_amount_total_median": avg20_amount_total_median,
                "progress": norm.get("progress"),
                "amount_norm_cum_ratio": norm.get("amount_norm_cum_ratio"),
                "amount_norm_slice_ratio": norm.get("amount_norm_slice_ratio"),
                "amount_norm_label": amount_label,
                "trade_date": trade_d.isoformat(),
                "inst_status": inst_status,
                "inst_dates_3d": inst_dates_3d,
                "kill_switch": False,
                "v14_watch": False,
                "degraded_mode": degraded_mode,
                "data_mode": session,
                "market_comment": "；".join(market_comment) if market_comment else "資料正常",
            }
        },
        "toplist": [] if df.empty else df.head(20).to_dict(orient="records"),
        "holdings": holdings,
        "global_summary": gdf.to_dict(orient="records"),
    }

    st.json(arbiter)

    # 最後提示：你要的「昨日台股 + 最新美股」的可靠性關鍵
    st.caption(
        "說明：開盤前顯示昨日 EOD（trade_date 自動回退）；"
        "全球摘要採 yfinance 最新收盤；"
        "成交金額若官方抓不到將顯示待更新但不影響 UI。"
    )

if __name__ == "__main__":
    app()
