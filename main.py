# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, time
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf

from market_amount import TZ_TAIPEI, fetch_amount_total_latest, MarketAmount


APP_TITLE = "Sunhero｜股市智能超盤中控台"
TRADING_START = time(9, 0)
TRADING_END = time(13, 30)

DEFAULT_UNIVERSE = [
    "2330.TW", "2317.TW", "2382.TW", "2454.TW", "2308.TW",
    "3231.TW", "2603.TW", "2609.TW", "2881.TW", "2882.TW",
    "1301.TW", "1303.TW", "3711.TW", "3034.TW", "6415.TW",
    "1101.TW", "1102.TW", "2408.TW", "2357.TW", "0050.TW",
]

US_SYMBOLS = {
    "S&P500": "^GSPC",
    "NASDAQ": "^IXIC",
    "DOW": "^DJI",
    "SOX": "^SOX",
    "VIX": "^VIX",
}

FX_SYMBOLS = {
    "USD/JPY": "JPY=X",
    "USD/TWD": "TWD=X",
}

HOLDINGS_PATH = "data/holdings.json"


def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def is_tw_market_open(now: Optional[datetime] = None) -> bool:
    now = now or now_taipei()
    t = now.time()
    return TRADING_START <= t <= TRADING_END


def resolve_session(mode: str, now: datetime) -> str:
    """
    mode:
      - AUTO: 盤中 -> INTRADAY；盤外 -> EOD（盤前/盤後一律顯示昨日/最近可用EOD）
      - INTRADAY: 強制盤中
      - EOD: 強制盤後（最新可用交易日收盤）
    """
    mode = (mode or "AUTO").upper()
    if mode == "INTRADAY":
        return "INTRADAY"
    if mode == "EOD":
        return "EOD"
    # AUTO
    return "INTRADAY" if is_tw_market_open(now) else "EOD"


def load_universe() -> List[str]:
    path = "configs/universe_tw.csv"
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            col = "symbol" if "symbol" in df.columns else df.columns[0]
            syms = [str(x).strip() for x in df[col].dropna().tolist()]
            syms = [s for s in syms if s.endswith(".TW") or s.endswith(".TWO")]
            return syms[:300] if len(syms) > 300 else syms
        except Exception:
            pass
    return DEFAULT_UNIVERSE


def load_name_map() -> Dict[str, str]:
    nm_path = "configs/name_map_tw.csv"
    if os.path.exists(nm_path):
        try:
            nm = pd.read_csv(nm_path)
            if "symbol" in nm.columns and "name" in nm.columns:
                return dict(zip(nm["symbol"].astype(str), nm["name"].astype(str)))
        except Exception:
            pass
    return {}


def load_holdings() -> List[str]:
    try:
        with open(HOLDINGS_PATH, "r", encoding="utf-8") as f:
            js = json.load(f)
        if isinstance(js, dict) and "symbols" in js:
            return [str(x).strip() for x in js["symbols"] if str(x).strip()]
        if isinstance(js, list):
            return [str(x).strip() for x in js if str(x).strip()]
    except Exception:
        return []
    return []


def save_holdings(symbols: List[str]) -> None:
    try:
        os.makedirs("data", exist_ok=True)
        with open(HOLDINGS_PATH, "w", encoding="utf-8") as f:
            json.dump({"symbols": symbols}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def yf_last_close(symbol: str, period: str = "10d") -> Dict[str, Any]:
    df = yf.download(symbol, period=period, interval="1d", progress=False, auto_adjust=False)
    if df is None or df.empty:
        return {"symbol": symbol, "date": None, "close": None, "change_pct": None}

    df = df.dropna()
    if df.empty:
        return {"symbol": symbol, "date": None, "close": None, "change_pct": None}

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None

    close = float(last["Close"])
    d = df.index[-1].strftime("%Y-%m-%d")

    chg = None
    if prev is not None and float(prev["Close"]) != 0:
        chg = (close / float(prev["Close"]) - 1.0) * 100.0

    return {"symbol": symbol, "date": d, "close": close, "change_pct": chg}


def yf_tw_snapshot(symbols: List[str], period: str = "90d") -> pd.DataFrame:
    df = yf.download(symbols, period=period, interval="1d", group_by="ticker", progress=False, auto_adjust=False)
    rows = []

    for s in symbols:
        try:
            sub = df[s].dropna()
            if sub.empty:
                continue

            ret20 = (sub["Close"].iloc[-1] / sub["Close"].iloc[-21] - 1) * 100 if len(sub) >= 21 else np.nan
            v20 = sub["Volume"].tail(20).mean() if len(sub) >= 20 else np.nan
            vr = (sub["Volume"].iloc[-1] / v20) if (v20 and v20 > 0) else np.nan
            ma20 = sub["Close"].tail(20).mean() if len(sub) >= 20 else np.nan
            bias = (sub["Close"].iloc[-1] / ma20 - 1) * 100 if (ma20 and ma20 > 0) else np.nan

            rows.append({
                "symbol": s,
                "date": sub.index[-1].strftime("%Y-%m-%d"),
                "close": float(sub["Close"].iloc[-1]),
                "ret20_pct": float(ret20) if np.isfinite(ret20) else None,
                "vol_ratio": float(vr) if np.isfinite(vr) else None,
                "ma_bias_pct": float(bias) if np.isfinite(bias) else None,
                "volume": int(sub["Volume"].iloc[-1]) if np.isfinite(sub["Volume"].iloc[-1]) else None,
            })
        except Exception:
            continue

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # Route A（免費模擬期）：ret20 + vol_ratio 簡化排序
    out["score"] = out["ret20_pct"].fillna(0) * 0.7 + (out["vol_ratio"].fillna(1) - 1) * 10 * 0.3
    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    out["rank"] = np.arange(1, len(out) + 1)
    return out


def format_amount_100m(n: Optional[int]) -> str:
    if n is None:
        return "待更新"
    return f"{n/1e8:,.2f} 億"


def build_arbiter_input(
    amount: MarketAmount,
    us_df: pd.DataFrame,
    fx_df: pd.DataFrame,
    top_df: pd.DataFrame,
    holdings: List[str],
    session: str,
) -> Dict[str, Any]:
    inst_status = "UNAVAILABLE"  # 免費模擬期：不接 FinMind
    degraded_mode = (not (amount.amount_total and amount.amount_total > 0)) or bool(amount.warning)

    top_syms = top_df["symbol"].tolist()[:20] if not top_df.empty else []
    merged: List[str] = []
    seen = set()

    def push(sym: str):
        if sym in seen:
            return
        seen.add(sym)
        merged.append(sym)

    for s in top_syms:
        push(s)
    for s in holdings:
        push(s)

    m = top_df.set_index("symbol").to_dict(orient="index") if not top_df.empty else {}
    watchlist = []
    for sym in merged:
        row = m.get(sym, {})
        watchlist.append({
            "symbol": sym,
            "date": row.get("date"),
            "close": row.get("close"),
            "score": row.get("score"),
            "rank": row.get("rank"),
            "ret20_pct": row.get("ret20_pct"),
            "vol_ratio": row.get("vol_ratio"),
            "ma_bias_pct": row.get("ma_bias_pct"),
            "is_holding": sym in holdings,
        })

    return {
        "meta": {
            "system": "Predator V15.7 (Free/Sim)",
            "timestamp": now_taipei().strftime("%Y-%m-%d %H:%M"),
            "session": session,
            "market": "tw-share",
        },
        "macro": {
            "overview": {
                "trade_date": amount.trade_date or None,
                "amount_twse": amount.amount_twse,
                "amount_tpex": amount.amount_tpex,
                "amount_total": amount.amount_total,
                "amount_twse_100m": format_amount_100m(amount.amount_twse),
                "amount_tpex_100m": format_amount_100m(amount.amount_tpex),
                "amount_total_100m": format_amount_100m(amount.amount_total),
                "amount_sources": {
                    "twse": amount.source_twse,
                    "tpex": amount.source_tpex,
                    "warning": amount.warning,
                },
                "inst_status": inst_status,
                "degraded_mode": degraded_mode,
                "data_mode": session,
            },
            "global": {
                "us": us_df.to_dict(orient="records"),
                "fx": fx_df.to_dict(orient="records"),
            }
        },
        "watchlist": watchlist,
    }


def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    now = now_taipei()

    with st.sidebar:
        st.subheader("設定")

        session_mode = st.selectbox(
            "Session",
            ["AUTO", "INTRADAY", "EOD"],
            index=0,
            help="AUTO：盤中/盤外自動判斷；EOD：強制顯示昨日/最近可用收盤；INTRADAY：強制盤中"
        )

        session = resolve_session(session_mode, now)

        verify_ssl = st.checkbox(
            "SSL 驗證（官方資料）",
            value=True,
            help="Streamlit Cloud 若遇證書問題可暫時關閉（僅模擬期）"
        )
        lookback = st.slider("官方資料回溯天數", min_value=3, max_value=20, value=10, step=1)
        st.divider()

        st.subheader("持倉（會納入追蹤）")
        holdings_raw = st.text_area("輸入代碼（逗號分隔）", value="2330.TW", height=80)
        holdings = [x.strip() for x in holdings_raw.replace("\n", ",").split(",") if x.strip()]

        if st.button("保存持倉"):
            save_holdings(holdings)
            st.success("已保存持倉")

        st.caption("免費模擬期：法人資料（FinMind）不使用，避免 402 付費門檻。")

    mode_text = "盤中" if session == "INTRADAY" else "盤後(EOD)/盤前顯示EOD"
    st.info(f"目前台北時間：{now.strftime('%Y-%m-%d %H:%M')}｜Session：{session}（{mode_text}）")

    # 全球：美股/匯率（EOD為主）
    colA, colB = st.columns([2, 2])

    with colA:
        st.subheader("全球市場摘要（美股）— 最新可用交易日收盤")
        us_rows = []
        for name, sym in US_SYMBOLS.items():
            snap = yf_last_close(sym)
            us_rows.append({
                "Name": name,
                "Symbol": sym,
                "Date": snap["date"],
                "Close": snap["close"],
                "Chg%": None if snap["change_pct"] is None else round(float(snap["change_pct"]), 2),
            })
        us_df = pd.DataFrame(us_rows)
        st.dataframe(us_df, use_container_width=True, hide_index=True)

    with colB:
        st.subheader("匯率（參考）— 最新可用交易日")
        fx_rows = []
        for name, sym in FX_SYMBOLS.items():
            snap = yf_last_close(sym)
            fx_rows.append({
                "Name": name,
                "Symbol": sym,
                "Date": snap["date"],
                "Close": snap["close"],
                "Chg%": None if snap["change_pct"] is None else round(float(snap["change_pct"]), 2),
            })
        fx_df = pd.DataFrame(fx_rows)
        st.dataframe(fx_df, use_container_width=True, hide_index=True)

    st.divider()

    # 台股：官方成交金額（回溯 + 快取）
    st.subheader("市場成交金額（官方口徑：TWSE + TPEx = amount_total）")

    amount = fetch_amount_total_latest(
        base_date=now.date(),
        lookback_days=lookback,
        verify_ssl=verify_ssl,
        timeout=15,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", format_amount_100m(amount.amount_twse))
    c2.metric("TPEx 上櫃", format_amount_100m(amount.amount_tpex))
    c3.metric("Total 合計", format_amount_100m(amount.amount_total))
    c4.metric("官方交易日", amount.trade_date or "未知")

    st.caption(f"來源：TWSE={amount.source_twse}｜TPEx={amount.source_tpex}")
    if amount.warning:
        st.warning(amount.warning)

    st.divider()

    # 台股 Top20（Route A）+ 持倉納入
    st.subheader("台股 Top List（Route A：Universe + 動能排序｜模擬期免費）")

    universe = load_universe()
    symbols = list(dict.fromkeys((universe[:200] + holdings)))  # 去重保序，最多200+持倉
    top_df = yf_tw_snapshot(symbols, period="90d")

    if top_df.empty:
        st.error("無法取得台股行情（yfinance 可能暫時不可用或被限流）")
    else:
        name_map = load_name_map()
        show = top_df.head(20).copy()
        show.insert(1, "name", show["symbol"].map(lambda x: name_map.get(x, "")))

        for col in ["ret20_pct", "vol_ratio", "ma_bias_pct", "score"]:
            show[col] = show[col].map(lambda x: None if x is None else round(float(x), 2))

        st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("AI JSON（Arbiter Input）— 可回溯（模擬期免費）")
    arbiter_input = build_arbiter_input(
        amount=amount,
        us_df=us_df,
        fx_df=fx_df,
        top_df=top_df,
        holdings=holdings,
        session=session,
    )
    st.json(arbiter_input)


if __name__ == "__main__":
    app()
