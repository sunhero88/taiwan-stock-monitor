# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, date, timedelta, timezone, time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

# 你 repo 內的模組（請確保 market_amount.py 已套用我給的完整覆蓋版）
from market_amount import TZ_TAIPEI, fetch_amount_total_latest, intraday_norm


# =========================
# 基本設定
# =========================
APP_TITLE = "Sunhero｜股市智能超盤中控台"
SYSTEM_NAME = "Predator V15.7（Free/Sim）"

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"
REPORTS_DIR = REPO_ROOT / "reports"
PLOTS_DIR = REPO_ROOT / "plots"

TRADING_START = time(9, 0)
TRADING_END = time(13, 30)


# =========================
# 小工具
# =========================
def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def is_tw_market_open(now: Optional[datetime] = None) -> bool:
    now = now or now_taipei()
    t = now.timetz().replace(tzinfo=None)
    return TRADING_START <= t <= TRADING_END


def safe_float(x, default=np.nan) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0) -> int:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return default
        return int(float(x))
    except Exception:
        return default


def fmt_bn_twd(amount: Optional[int]) -> str:
    if amount is None:
        return "待更新"
    if amount <= 0:
        return "待更新"
    # 元 → 億
    bn = amount / 1e8
    return f"{bn:,.0f} 億"


def fmt_pct(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{x:.2f}%"


def fmt_num(x: float, nd=2) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{x:,.{nd}f}"


def normalize_symbol(s: str) -> str:
    s = (s or "").strip()
    return s


def parse_holdings_input(text: str) -> List[str]:
    if not text:
        return []
    parts = []
    for token in text.replace("\n", ",").split(","):
        token = token.strip()
        if token:
            parts.append(token)
    # 去重保序
    out = []
    seen = set()
    for p in parts:
        p2 = normalize_symbol(p)
        if p2 and p2 not in seen:
            seen.add(p2)
            out.append(p2)
    return out


# =========================
# 台股中文名稱（免費/模擬期：用內建小字典 + yfinance fallback）
# 你可自行擴充或改為讀檔（例如 data/tw_name_map.csv）
# =========================
TW_NAME_MAP = {
    "2330.TW": "台積電",
    "2317.TW": "鴻海",
    "2382.TW": "廣達",
    "2454.TW": "聯發科",
    "2603.TW": "長榮",
    "2609.TW": "陽明",
    "2308.TW": "台達電",
    "3231.TW": "緯創",
    "0050.TW": "元大台灣50",
}


@st.cache_data(ttl=3600)
def yf_short_name(symbol: str) -> str:
    # 盡量不要呼叫太重；cache 1 小時
    try:
        t = yf.Ticker(symbol)
        info = getattr(t, "fast_info", None)
        # fast_info 沒 name
        # 用 info（可能比較慢）
        d = t.info or {}
        name = d.get("shortName") or d.get("longName") or ""
        return str(name) if name else ""
    except Exception:
        return ""


def get_stock_name(symbol: str) -> str:
    symbol = normalize_symbol(symbol)
    if symbol in TW_NAME_MAP:
        return TW_NAME_MAP[symbol]
    # fallback：yfinance（可能是英文/拼音）
    n = yf_short_name(symbol)
    return n if n else ""


# =========================
# 讀取 TopList 資料（多路徑容錯）
# =========================
def candidate_market_csv_paths(market: str) -> List[Path]:
    m1 = market.replace("-", "_")
    candidates = [
        REPO_ROOT / f"data_{market}.csv",
        REPO_ROOT / f"data_{m1}.csv",
        DATA_DIR / f"data_{market}.csv",
        DATA_DIR / f"data_{m1}.csv",
        DATA_DIR / f"{market}.csv",
        DATA_DIR / f"{m1}.csv",
        DATA_DIR / f"top_{market}.csv",
        DATA_DIR / f"top_{m1}.csv",
        DATA_DIR / f"toplist_{market}.csv",
        DATA_DIR / f"toplist_{m1}.csv",
        DATA_DIR / f"ranking_{market}.csv",
        DATA_DIR / f"ranking_{m1}.csv",
        DATA_DIR / f"{market}_toplist.csv",
        DATA_DIR / f"{m1}_toplist.csv",
    ]
    # 去重保序
    out = []
    seen = set()
    for p in candidates:
        s = str(p)
        if s not in seen:
            seen.add(s)
            out.append(p)
    return out


def load_market_toplist_csv(market: str) -> Tuple[pd.DataFrame, str]:
    last_err = None
    for p in candidate_market_csv_paths(market):
        try:
            if p.exists():
                df = pd.read_csv(p)
                return df, str(p.relative_to(REPO_ROOT))
        except Exception as e:
            last_err = e
    raise FileNotFoundError(
        f"找不到 {market} 的資料檔（已嘗試多個候選路徑）。最後錯誤：{last_err}"
    )


def ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    # 允許 Date/date/Datetime 等
    cols = {c.lower(): c for c in df.columns}
    if "date" in cols:
        c = cols["date"]
        df = df.copy()
        df["date"] = pd.to_datetime(df[c], errors="coerce")
        return df
    if "datetime" in cols:
        c = cols["datetime"]
        df = df.copy()
        df["date"] = pd.to_datetime(df[c], errors="coerce")
        return df
    # 沒日期欄：建立空
    df = df.copy()
    df["date"] = pd.NaT
    return df


def pick_latest_trade_date_from_df(df: pd.DataFrame) -> Optional[date]:
    if "date" not in df.columns:
        return None
    s = pd.to_datetime(df["date"], errors="coerce")
    if s.notna().sum() == 0:
        return None
    dmax = s.max()
    if pd.isna(dmax):
        return None
    return dmax.date()


def build_top20_with_holdings(
    df: pd.DataFrame,
    holdings: List[str],
    top_n: int = 20,
) -> pd.DataFrame:
    """
    Top20 定義（Route A / 免費模擬）：
    - 以資料檔中 score 由大到小排序，取前 N 名
    - 若沒有 score 欄位，改用 rank（由小到大）
    - 加入持倉（若不在 Top20 內，附加在表尾）→ 變成 20 + H
    """
    df = df.copy()

    # 標準化欄位名
    cols_lower = {c.lower(): c for c in df.columns}

    # symbol 欄
    sym_col = cols_lower.get("symbol") or cols_lower.get("ticker") or cols_lower.get("code")
    if not sym_col:
        raise RuntimeError("TopList CSV 缺少 symbol/ticker/code 欄位，無法建立候選清單")

    # score 或 rank
    score_col = cols_lower.get("score")
    rank_col = cols_lower.get("rank")

    if score_col:
        df["_sort"] = pd.to_numeric(df[score_col], errors="coerce")
        df = df.sort_values(["_sort"], ascending=False)
    elif rank_col:
        df["_sort"] = pd.to_numeric(df[rank_col], errors="coerce")
        df = df.sort_values(["_sort"], ascending=True)
    else:
        # 最後 fallback：成交量 volume 由大到小
        vol_col = cols_lower.get("volume")
        if not vol_col:
            df["_sort"] = np.arange(len(df))
            df = df.sort_values(["_sort"], ascending=True)
        else:
            df["_sort"] = pd.to_numeric(df[vol_col], errors="coerce")
            df = df.sort_values(["_sort"], ascending=False)

    top = df.head(top_n).copy()

    # 持倉加入：若持倉不在 top，從原 df 找到該列；找不到就建空列
    top_syms = set(top[sym_col].astype(str))
    extra_rows = []
    for h in holdings:
        if h in top_syms:
            continue
        # 從 df 找第一筆
        m = df[df[sym_col].astype(str) == h]
        if len(m) > 0:
            extra_rows.append(m.iloc[0].to_dict())
        else:
            extra_rows.append({sym_col: h})

    if extra_rows:
        extra_df = pd.DataFrame(extra_rows)
        # 補日期欄對齊
        for c in top.columns:
            if c not in extra_df.columns:
                extra_df[c] = np.nan
        extra_df = extra_df[top.columns]
        top = pd.concat([top, extra_df], axis=0, ignore_index=True)

    # 清理
    if "_sort" in top.columns:
        top.drop(columns=["_sort"], inplace=True, errors="ignore")

    # 加上 name（中文/英文）
    top["name"] = [get_stock_name(str(x)) for x in top[sym_col].astype(str)]

    return top


# =========================
# 全球摘要：優先讀 data/global_market_summary.csv，沒有就 yfinance 即時抓
# =========================
GLOBAL_FALLBACK_TICKERS = [
    ("US", "S&P500", "^GSPC"),
    ("US", "NASDAQ", "^IXIC"),
    ("US", "DOW", "^DJI"),
    ("US", "SOX", "^SOX"),
    ("US", "VIX", "^VIX"),
    ("ASIA", "Nikkei_225", "^N225"),
    ("FX", "USD_JPY", "JPY=X"),
    ("FX", "USD_TWD", "TWD=X"),
]


@st.cache_data(ttl=900)
def fetch_global_summary_yf() -> pd.DataFrame:
    rows = []
    for market, name, symbol in GLOBAL_FALLBACK_TICKERS:
        try:
            h = yf.download(symbol, period="10d", interval="1d", progress=False, auto_adjust=False)
            if h is None or len(h) == 0:
                continue
            h = h.dropna()
            if len(h) == 0:
                continue
            last = h.iloc[-1]
            prev = h.iloc[-2] if len(h) >= 2 else last
            close = safe_float(last.get("Close"))
            prev_close = safe_float(prev.get("Close"))
            chg_pct = ((close / prev_close) - 1) * 100 if prev_close and prev_close > 0 else np.nan
            rows.append(
                {
                    "Market": market,
                    "Name": name,
                    "Symbol": symbol,
                    "Date": str(h.index[-1].date()),
                    "Close": close,
                    "Chg%": chg_pct,
                }
            )
        except Exception:
            continue
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return df
    return df


def load_global_market_summary() -> Tuple[pd.DataFrame, str]:
    p = DATA_DIR / "global_market_summary.csv"
    if p.exists():
        try:
            df = pd.read_csv(p)
            return df, "data/global_market_summary.csv"
        except Exception:
            pass
    # fallback yfinance
    df = fetch_global_summary_yf()
    return df, "yfinance(fallback)"


# =========================
# Arbiter Input（免費模擬：法人資料關閉）
# =========================
def build_arbiter_input(
    market: str,
    session: str,
    display_trade_date: Optional[date],
    amount_pack: Dict[str, Any],
    toplist: pd.DataFrame,
) -> Dict[str, Any]:
    # 判定資料降級（免費模擬：法人資料一律 UNAVAILABLE）
    inst_status = "UNAVAILABLE"
    degraded_mode = True

    # 成交金額資料是否完整（TWSE/TPEx 任一缺失都視為「宏觀不完整」→ Degraded Mode）
    twse_ok = isinstance(amount_pack.get("amount_twse"), int) and amount_pack.get("amount_twse", 0) > 0
    tpex_ok = isinstance(amount_pack.get("amount_tpex"), int) and amount_pack.get("amount_tpex", 0) > 0

    # 這裡採「絕對防線」：任一缺失 → degraded_mode=true（禁止 BUY/TRIAL）
    if twse_ok and tpex_ok:
        degraded_mode = True  # 仍然因法人不可用而降級（免費期）
    else:
        degraded_mode = True

    # Toplist 欄位映射
    cols_lower = {c.lower(): c for c in toplist.columns}
    sym_col = cols_lower.get("symbol") or cols_lower.get("ticker") or cols_lower.get("code")

    def getv(row, key, default=None):
        c = cols_lower.get(key.lower())
        return row.get(c, default) if c else default

    stocks = []
    for _, r in toplist.iterrows():
        sym = str(r.get(sym_col, "")).strip() if sym_col else ""
        if not sym:
            continue
        stocks.append(
            {
                "Symbol": sym,
                "Name": str(r.get("name", "")) if "name" in r else get_stock_name(sym),
                "Price": safe_float(getv(r, "close", np.nan), np.nan),
                "ranking": {
                    "symbol": sym,
                    "rank": safe_int(getv(r, "rank", np.nan), 0),
                    "tier": str(getv(r, "tier", "")) if "tier" in cols_lower else "",
                    "top20_flag": True,
                },
                "Technical": {
                    "MA_Bias": safe_float(getv(r, "ma_bias", getv(r, "ma_bias_pct", np.nan)), np.nan),
                    "Vol_Ratio": safe_float(getv(r, "vol_ratio", np.nan), np.nan),
                    "Body_Power": safe_float(getv(r, "body_power", np.nan), np.nan),
                    "Score": safe_float(getv(r, "score", np.nan), np.nan),
                    "Tag": str(getv(r, "predator_tag", getv(r, "tag", "")) or ""),
                },
                "Institutional": {
                    "Inst_Visual": "PENDING",
                    "Inst_Net_3d": 0.0,
                    "Inst_Streak3": 0,
                    "Inst_Dir3": "PENDING",
                    "Inst_Status": "PENDING",
                },
                "Structure": {
                    "OPM": safe_float(getv(r, "opm", np.nan), np.nan),
                    "Rev_Growth": safe_float(getv(r, "rev_growth", np.nan), np.nan),
                    "PE": safe_float(getv(r, "pe", np.nan), np.nan),
                    "Sector": str(getv(r, "sector", "")) if "sector" in cols_lower else "Unknown",
                    "Rev_Growth_Source": str(getv(r, "rev_growth_source", "")) if "rev_growth_source" in cols_lower else "",
                },
                "risk": {
                    "position_pct_max": 12,
                    "risk_per_trade_max": 1.0,
                    "trial_flag": True,
                },
                "orphan_holding": False,
                "weaken_flags": {
                    "technical_weaken": False,
                    "structure_weaken": False,
                },
            }
        )

    overview = {
        "trade_date": str(display_trade_date) if display_trade_date else None,
        "amount_twse": amount_pack.get("amount_twse"),
        "amount_tpex": amount_pack.get("amount_tpex"),
        "amount_total": amount_pack.get("amount_total"),
        "amount_sources": amount_pack.get("sources", {}),
        "inst_status": inst_status,
        "degraded_mode": degraded_mode,
        "data_mode": session,
        "market_comment": (
            "免費模擬期：法人資料(FinMind)停用；成交金額採官方口徑回溯最近交易日。"
        ),
    }

    out = {
        "meta": {
            "system": SYSTEM_NAME,
            "market": market,
            "timestamp": now_taipei().strftime("%Y-%m-%d %H:%M"),
            "session": session,
        },
        "macro": {"overview": overview},
        "stocks": stocks,
    }
    return out


# =========================
# UI
# =========================
def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    now = now_taipei()

    # -------- Sidebar --------
    st.sidebar.header("設定")

    market = st.sidebar.selectbox(
        "Market",
        options=["tw-share", "us", "hk", "cn", "jp", "kr"],
        index=0,
    )

    # 盤前/盤中/盤後（你要的選項）
    session_ui = st.sidebar.selectbox(
        "Session",
        options=["盤前(顯示昨日/最近可用EOD)", "盤中(INTRADAY)", "盤後(EOD)"],
        index=0,
    )

    # 盤前強制顯示 EOD（避免 08:17 還在用 INTRADAY）
    if session_ui.startswith("盤前"):
        session = "EOD"
        show_mode_note = "盤前：顯示『最近可用交易日收盤』做全市場參考。"
    elif "INTRADAY" in session_ui:
        session = "INTRADAY"
        show_mode_note = "盤中：顯示盤中候選（若資料可用）。"
    else:
        session = "EOD"
        show_mode_note = "盤後：顯示當日收盤候選。"

    verify_ssl = st.sidebar.checkbox("SSL 驗證（官方資料）", value=True)
    lookback = st.sidebar.slider("官方資料回溯天數", min_value=3, max_value=20, value=10, step=1)

    st.sidebar.divider()

    # 持倉（會納入追蹤 → 20 + H）
    st.sidebar.subheader("持倉（會納入追蹤）")
    holdings_text = st.sidebar.text_area("輸入代碼（逗號分隔）", value="2330.TW", height=100)
    holdings = parse_holdings_input(holdings_text)
    if st.sidebar.button("保存持倉"):
        st.session_state["holdings"] = holdings
        st.sidebar.success("已保存持倉")

    # 若 session_state 有保存過，就優先使用
    if "holdings" in st.session_state and isinstance(st.session_state["holdings"], list):
        holdings = st.session_state["holdings"]

    st.sidebar.caption("免費模擬期：法人資料（FinMind）停用，避免 402 付費門檻。")

    run_btn = st.sidebar.button("Run")

    # -------- Header info --------
    st.info(
        f"目前台北時間：{now.strftime('%Y-%m-%d %H:%M')}｜模式：{session_ui}｜{show_mode_note}"
    )

    if not run_btn:
        st.caption("按下 Run 以更新畫面。")
        return

    # =========================
    # 1) 全球市場摘要（美股/日經/匯率）
    # =========================
    gdf, gsrc = load_global_market_summary()
    st.subheader("全球市場摘要（美股/日經/匯率）— 最新可用交易日")
    if gdf is None or len(gdf) == 0:
        st.warning("全球摘要目前無資料（global_market_summary.csv 不存在且 yfinance fallback 失敗）。")
    else:
        # 統一欄位顯示
        # 允許使用者 csv 欄位不同：盡量映射
        gl = {c.lower(): c for c in gdf.columns}
        def col(name):
            return gl.get(name.lower())

        out = pd.DataFrame()
        if col("Market"):
            out["Market"] = gdf[col("Market")]
        elif col("market"):
            out["Market"] = gdf[col("market")]
        else:
            out["Market"] = ""

        # Name
        if col("Name"):
            out["Name"] = gdf[col("Name")]
        elif col("name"):
            out["Name"] = gdf[col("name")]
        else:
            out["Name"] = ""

        # Symbol
        if col("Symbol"):
            out["Symbol"] = gdf[col("Symbol")]
        elif col("symbol"):
            out["Symbol"] = gdf[col("symbol")]
        else:
            out["Symbol"] = ""

        # Date
        if col("Date"):
            out["Date"] = gdf[col("Date")]
        elif col("date"):
            out["Date"] = gdf[col("date")]
        else:
            out["Date"] = ""

        # Close / Value
        if col("Close"):
            out["Close"] = gdf[col("Close")]
        elif col("Value"):
            out["Close"] = gdf[col("Value")]
        elif col("close"):
            out["Close"] = gdf[col("close")]
        else:
            out["Close"] = np.nan

        # Chg%
        if col("Chg%"):
            out["Chg%"] = gdf[col("Chg%")]
        elif col("change"):
            out["Chg%"] = gdf[col("change")]
        elif col("chg%"):
            out["Chg%"] = gdf[col("chg%")]
        else:
            out["Chg%"] = np.nan

        st.dataframe(out, use_container_width=True, hide_index=True)
        st.caption(f"資料來源：{gsrc}")

    st.divider()

    # =========================
    # 2) 官方成交金額（TWSE + TPEx）
    # =========================
    st.subheader("市場成交金額（官方口徑：TWSE + TPEx = amount_total）")

    amount_pack: Dict[str, Any] = {
        "trade_date": None,
        "amount_twse": None,
        "amount_tpex": None,
        "amount_total": None,
        "sources": {"twse": None, "tpex": None},
        "warning": None,
        "error": None,
        "debug": None,
    }

    try:
        ma, debug = fetch_amount_total_latest(
            lookback_days=int(lookback),
            verify_ssl=bool(verify_ssl),
        )
        amount_pack.update(
            {
                "trade_date": str(ma.trade_date),
                "amount_twse": int(ma.amount_twse),
                "amount_tpex": int(ma.amount_tpex),
                "amount_total": int(ma.amount_total),
                "sources": {"twse": ma.source_twse, "tpex": ma.source_tpex},
                "debug": debug,
            }
        )
    except Exception as e:
        amount_pack["error"] = str(e)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", fmt_bn_twd(amount_pack.get("amount_twse")))
    c2.metric("TPEx 上櫃", fmt_bn_twd(amount_pack.get("amount_tpex")))
    c3.metric("Total 合計", fmt_bn_twd(amount_pack.get("amount_total")))
    c4.metric("官方交易日", amount_pack.get("trade_date") or "未知")

    st.caption(f"來源/錯誤：TWSE={amount_pack['sources'].get('twse')}｜TPEx={amount_pack['sources'].get('tpex')}")
    if amount_pack.get("error"):
        st.warning(f"官方抓取失敗：{amount_pack['error']}")

    # 盤中量能正規化（僅在 INTRADAY 且 amount_total 有值才有意義）
    st.markdown("### INTRADAY 量能正規化（避免早盤誤判 LOW）")
    norm_box = {"progress": None, "cum_ratio(穩健型用)": None, "slice_ratio(保守型用)": None, "label": "UNKNOWN"}

    if session == "INTRADAY" and isinstance(amount_pack.get("amount_total"), int) and amount_pack.get("amount_total", 0) > 0:
        # 免費模擬：沒有 20D median（你可自行接 sqlite 或 csv）
        avg20 = None
        res = intraday_norm(
            amount_total_now=int(amount_pack["amount_total"]),
            amount_total_prev=None,
            avg20_amount_total=avg20,
            now=now,
            alpha=0.65,
        )
        norm_box = {
            "progress": res.get("progress"),
            "cum_ratio(穩健型用)": res.get("amount_norm_cum_ratio"),
            "slice_ratio(保守型用)": res.get("amount_norm_slice_ratio"),
            "label": res.get("amount_norm_label"),
        }

    st.json(norm_box)

    st.divider()

    # =========================
    # 3) TopList（Route A：score 排名）+ 持倉追加
    # =========================
    st.subheader("Top List（Route A：Universe + 持倉，動能排名）")

    try:
        mdf_raw, msrc = load_market_toplist_csv(market)
        mdf_raw = ensure_date_column(mdf_raw)
        display_trade_date = pick_latest_trade_date_from_df(mdf_raw)

        # 若盤前：一定用「資料檔內最後日期」(避免顯示 3 天前)
        # 若盤中：仍優先用資料檔內最後日期（你的資料管線如果盤中更新，日期會是今天）
        if display_trade_date is None:
            st.warning("TopList CSV 沒有可解析日期欄（Date/date）。將顯示全表（可能失真）。")

        # 取最新日期資料（如果 date 欄可用）
        mdf = mdf_raw.copy()
        if display_trade_date is not None and mdf["date"].notna().sum() > 0:
            mdf = mdf[mdf["date"].dt.date == display_trade_date].copy()

        top = build_top20_with_holdings(mdf, holdings=holdings, top_n=20)

        # 盡量把常用欄位呈現出來（你截圖那些）
        cols_lower = {c.lower(): c for c in top.columns}
        def pick(*names):
            for n in names:
                c = cols_lower.get(n.lower())
                if c:
                    return c
            return None

        sym_col = pick("symbol", "ticker", "code")
        show_cols = []
        for want in ["symbol", "name", "date", "close", "ret20_pct", "vol_ratio", "ma_bias_pct", "volume", "score", "rank", "predator_tag"]:
            c = pick(want)
            if c and c not in show_cols:
                show_cols.append(c)

        # 如果 pred/tag 欄不同名，也嘗試
        if not pick("predator_tag"):
            ctag = pick("tag")
            if ctag and ctag not in show_cols:
                show_cols.append(ctag)

        show_df = top[show_cols].copy() if show_cols else top.copy()
        st.dataframe(show_df, use_container_width=True, hide_index=True)

        st.caption(f"TopList 來源：{msrc}｜顯示交易日：{str(display_trade_date) if display_trade_date else '未知'}｜持倉追加：{len(holdings)} 檔 → 20+H")

    except Exception as e:
        st.error(f"TopList 載入失敗：{e}")
        return

    st.divider()

    # =========================
    # 4) AI JSON（Arbiter Input）— st.code 提供「可複製」
    # =========================
    st.subheader("AI JSON（Arbiter Input）— 可回溯（模擬期免費）")

    arbiter_input = build_arbiter_input(
        market=market,
        session=session,
        display_trade_date=display_trade_date,
        amount_pack=amount_pack,
        toplist=top,
    )

    json_text = json.dumps(arbiter_input, ensure_ascii=False, indent=2)

    # st.code 右上角自帶 copy（你說缺的「複製鍵」）
    st.code(json_text, language="json")

    st.download_button(
        label="下載 Arbiter Input JSON",
        data=json_text.encode("utf-8"),
        file_name=f"arbiter_input_{market}_{now.strftime('%Y%m%d_%H%M')}.json",
        mime="application/json",
    )

    # Debug（可選）
    with st.expander("官方成交金額 Debug（回溯嘗試紀錄）"):
        st.json(amount_pack.get("debug"))


if __name__ == "__main__":
    app()
