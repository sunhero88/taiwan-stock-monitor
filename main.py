# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone, time, date
from typing import Dict, Any, List, Optional

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf

from market_amount import TZ_TAIPEI, fetch_amount_total_latest, MarketAmount


# -----------------------------
# 基本設定
# -----------------------------
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
    # 先只用時間判斷（免費版不做交易所假日行事曆）
    return (t >= TRADING_START) and (t <= TRADING_END)


def load_universe() -> List[str]:
    # 允許你放 configs/universe_tw.csv（欄位：symbol）
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
    if prev is not None and float(prev["Close"]) != 0:
        chg = (close / float(prev["Close"]) - 1.0) * 100.0
    else:
        chg = None

    return {"symbol": symbol, "date": d, "close": close, "change_pct": chg}


def yf_tw_snapshot(symbols: List[str], period: str = "60d") -> pd.DataFrame:
    df = yf.download(symbols, period=period, interval="1d", group_by="ticker", progress=False, auto_adjust=False)
    rows = []
    for s in symbols:
        try:
            sub = df[s].dropna()
            if sub.empty:
                continue
            sub = sub.copy()
            # 指標：20D 報酬、20D 均量、最新量能比
            sub["ret1"] = sub["Close"].pct_change()
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

    # 簡化 Top20：ret20 + vol_ratio 共同排序（可再換回你的 arbiter）
    out["score"] = (
        out["ret20_pct"].fillna(0) * 0.7 +
        (out["vol_ratio"].fillna(1) - 1) * 10 * 0.3
    )
    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    out["rank"] = np.arange(1, len(out) + 1)
    return out


def format_amount_100m(n: Optional[int]) -> str:
    # 元 -> 億
    if n is None:
        return "待更新"
    return f"{n/1e8:,.2f} 億"


def build_arbiter_input(
    amount: MarketAmount,
    us_summary: List[Dict[str, Any]],
    fx_summary: List[Dict[str, Any]],
    top_df: pd.DataFrame,
    holdings: List[str],
) -> Dict[str, Any]:

    # 法人資料：免費版一律 UNAVAILABLE（不阻止盤前看昨日）
    inst_status = "UNAVAILABLE"

    # degraded_mode：只針對「交易動作」的安全鎖
    # 模擬期規則：若 amount_total 缺失 或來源不完整 -> degraded_mode = True
    degraded_mode = not (amount.amount_total and amount.amount_total > 0) or bool(amount.warning)

    macro = {
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
            "data_mode": "EOD" if not is_tw_market_open() else "INTRADAY",
            "market_comment": (
                "盤前：顯示最近可用交易日（通常為昨日收盤）。"
                if not is_tw_market_open()
                else "盤中：顯示最新可用交易日（可能為今日），若官方延遲則回溯。"
            ),
        },
        "global": {
            "us": us_summary,
            "fx": fx_summary,
        }
    }

    # Top list + holdings 合併（避免「買了台積電但明天沒入選」）
    top_syms = top_df["symbol"].tolist()[:20] if not top_df.empty else []
    merged = []
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

    watch = []
    if not top_df.empty:
        m = top_df.set_index("symbol").to_dict(orient="index")
        for sym in merged:
            row = m.get(sym, {})
            watch.append({
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
    else:
        for sym in merged:
            watch.append({"symbol": sym, "is_holding": sym in holdings})

    return {
        "meta": {
            "system": "Predator V15.7 (Free/Sim)",
            "timestamp": now_taipei().strftime("%Y-%m-%d %H:%M"),
            "session": "PREOPEN" if not is_tw_market_open() else "INTRADAY",
            "market": "tw-share",
        },
        "macro": macro,
        "watchlist": watch,
    }


# -----------------------------
# UI
# -----------------------------
def app():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    with st.sidebar:
        st.subheader("設定")
        verify_ssl = st.checkbox("SSL 驗證（官方資料）", value=True, help="若 Streamlit Cloud 遇到證書問題可暫時關閉（僅模擬期）")
        lookback = st.slider("官方資料回溯天數", min_value=3, max_value=20, value=10, step=1)
        st.divider()

        st.subheader("持倉（會覆蓋到 data/holdings.json）")
        holdings_raw = st.text_area("輸入代碼（逗號分隔）", value="2330.TW", height=80)
        holdings = [x.strip() for x in holdings_raw.replace("\n", ",").split(",") if x.strip()]
        if st.button("保存持倉"):
            save_holdings(holdings)
            st.success("已保存持倉")

        st.divider()
        st.caption("模擬期：法人資料（FinMind）不使用，以避免 402 付費門檻。")

    holdings = load_holdings()
    if holdings_raw:
        # 讓 UI 輸入也能立即生效（不按保存也能看）
        holdings = [x.strip() for x in holdings_raw.replace("\n", ",").split(",") if x.strip()]

    now = now_taipei()
    st.info(f"目前台北時間：{now.strftime('%Y-%m-%d %H:%M')}｜模式：{'盤前(顯示昨日EOD)' if not is_tw_market_open(now) else '盤中'}")

    # --- 全球（美股/匯率）---
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

    # --- 台股成交金額（官方裁決用）---
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

    # --- 台股 Top20（免費路線 A：Universe + 排序）---
    st.subheader("台股 Top List（Route A：Universe + 動能排序｜模擬期免費）")
    universe = load_universe()
    # holdings 一定納入（解決你說的：買了台積電但明天沒入選）
    symbols = list(dict.fromkeys((universe[:200] + holdings)))  # 去重保序
    top_df = yf_tw_snapshot(symbols, period="90d")

    if top_df.empty:
        st.error("無法取得台股行情（yfinance 可能暫時不可用或被限流）")
    else:
        # 顯示中文名稱：免費版不做外部資料表（避免再多一個不穩來源）
        # 但你可自行在 configs/name_map_tw.csv 放 symbol,name，這裡支援讀入
        name_map = {}
        nm_path = "configs/name_map_tw.csv"
        if os.path.exists(nm_path):
            try:
                nm = pd.read_csv(nm_path)
                if "symbol" in nm.columns and "name" in nm.columns:
                    name_map = dict(zip(nm["symbol"].astype(str), nm["name"].astype(str)))
            except Exception:
                pass

        show = top_df.head(20).copy()
        show.insert(1, "name", show["symbol"].map(lambda x: name_map.get(x, "")))
        show["ret20_pct"] = show["ret20_pct"].map(lambda x: None if x is None else round(float(x), 2))
        show["vol_ratio"] = show["vol_ratio"].map(lambda x: None if x is None else round(float(x), 2))
        show["ma_bias_pct"] = show["ma_bias_pct"].map(lambda x: None if x is None else round(float(x), 2))
        show["score"] = show["score"].map(lambda x: None if x is None else round(float(x), 2))

        st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()

    # --- Arbiter Input JSON（你要餵 AI 的資料包）---
    st.subheader("AI JSON（Arbiter Input）— 穩定可回溯（模擬期免費）")
    arbiter_input = build_arbiter_input(
        amount=amount,
        us_summary=us_df.to_dict(orient="records"),
        fx_summary=fx_df.to_dict(orient="records"),
        top_df=top_df,
        holdings=holdings,
    )
    st.json(arbiter_input)

    st.caption(
        "注意：此版本把『資料正確/最新』當作第一優先。"
        "若官方成交金額缺失，會明確標示 warning，並把 degraded_mode 打開（只針對交易動作）。"
    )


if __name__ == "__main__":
    app()
