# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime, timezone, timedelta, time

import pandas as pd
import streamlit as st
import yfinance as yf

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from institutional_utils import calc_inst_3d
from finmind_institutional import fetch_finmind_institutional

from market_amount import (
    fetch_amount_total,
    intraday_norm,
    yfinance_amount_proxy,
)

TZ_TAIPEI = timezone(timedelta(hours=8))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# 你的 demo 追蹤池（模擬期免費）
DEFAULT_TICKERS = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"]

# 中文名（先用靜態表，避免每次 yfinance info 太慢）
NAME_ZH_MAP = {
    "2330.TW": "台積電",
    "2317.TW": "鴻海",
    "2308.TW": "台達電",
    "2454.TW": "聯發科",
    "2382.TW": "廣達",
    "3231.TW": "緯創",
    "2603.TW": "長榮",
    "2609.TW": "陽明",
}

def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)

def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return _now_taipei().strftime("%Y-%m-%d")

def _ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def _candidate_paths(filename: str):
    # 依序嘗試：根目錄、data/ 目錄
    return [
        os.path.join(BASE_DIR, filename),
        os.path.join(DATA_DIR, filename),
    ]

def _load_market_csv(market: str) -> pd.DataFrame:
    """
    先找 data_{market}.csv（支援 root / data/）
    找不到就用 yfinance 以 DEFAULT_TICKERS 產生（免費/模擬期保底）
    """
    _ensure_dirs()

    fname = f"data_{market}.csv"
    # 兼容舊命名
    fallbacks = [fname, "data_tw-share.csv", "data_tw.csv"]

    for fb in fallbacks:
        for p in _candidate_paths(fb):
            if os.path.exists(p):
                df = pd.read_csv(p)
                return df

    # 找不到 → 免費保底：用 yfinance 產生近一年日線
    symbols = DEFAULT_TICKERS if market in ("tw-share", "tw") else DEFAULT_TICKERS
    data = yf.download(symbols, period="1y", interval="1d", progress=False, auto_adjust=False, threads=True)
    if data is None or data.empty:
        raise FileNotFoundError(f"找不到資料檔（{fallbacks}），且 yfinance 無法下載：market={market}")

    df_close = data["Close"].stack().reset_index()
    df_close.columns = ["Date", "Symbol", "Close"]

    df_vol = data["Volume"].stack().reset_index()
    df_vol.columns = ["Date", "Symbol", "Volume"]

    df_open = data["Open"].stack().reset_index()
    df_open.columns = ["Date", "Symbol", "Open"]

    df_high = data["High"].stack().reset_index()
    df_high.columns = ["Date", "Symbol", "High"]

    df_low = data["Low"].stack().reset_index()
    df_low.columns = ["Date", "Symbol", "Low"]

    df = df_close.merge(df_vol, on=["Date", "Symbol"], how="left")
    df = df.merge(df_open, on=["Date", "Symbol"], how="left")
    df = df.merge(df_high, on=["Date", "Symbol"], how="left")
    df = df.merge(df_low, on=["Date", "Symbol"], how="left")

    outpath = os.path.join(DATA_DIR, fname)
    df.to_csv(outpath, index=False, encoding="utf-8")
    return df

def _decide_preopen_session(now: datetime) -> str:
    """
    台北時間 < 09:00 → 顯示昨日 EOD（避免 08:17 還在用 INTRADAY 導致日期/量能失真）
    """
    if now.time() < time(9, 0):
        return SESSION_EOD
    return SESSION_INTRADAY

def _load_global_summary() -> pd.DataFrame:
    """
    讀 data/global_market_summary.csv
    若沒有 → 用 yfinance 生成（免費保底）
    """
    _ensure_dirs()
    p = os.path.join(DATA_DIR, "global_market_summary.csv")
    if os.path.exists(p):
        try:
            return pd.read_csv(p)
        except Exception:
            pass

    # 免費保底：抓你表上的符號
    # SOX(半導體)用 ^SOX，TSM ADR 用 TSM，NVIDIA=NVDA，Apple=AAPL，日經 ^N225，USDJPY=X? 走 JPY=X，USDTWD=X? 常不穩 → 用 TWD=X
    items = [
        ("US", "SOX_Semi", "^SOX"),
        ("US", "TSM_ADR", "TSM"),
        ("US", "NVIDIA", "NVDA"),
        ("US", "Apple", "AAPL"),
        ("ASIA", "Nikkei_225", "^N225"),
        ("ASIA", "USD_JPY", "JPY=X"),
        ("ASIA", "USD_TWD", "TWD=X"),
    ]

    rows = []
    for mkt, sym, yf_sym in items:
        try:
            hist = yf.download(yf_sym, period="5d", interval="1d", progress=False)
            if hist is None or hist.empty:
                continue
            c = float(hist["Close"].iloc[-1])
            p0 = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else c
            chg = (c / p0 - 1.0) * 100.0 if p0 else 0.0
            rows.append({"Market": mkt, "Symbol": sym, "Change": round(chg, 4), "Value": round(c, 4)})
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if not df.empty:
        df.to_csv(p, index=False, encoding="utf-8")
    return df

def _merge_institutional_into_df_top(df_top: pd.DataFrame, inst_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    df_out = df_top.copy()
    inst_records = []
    for _, r in df_out.iterrows():
        symbol = str(r.get("Symbol", ""))
        inst_calc = calc_inst_3d(inst_df, symbol=symbol, trade_date=trade_date)
        inst_records.append(
            {
                "Symbol": symbol,
                "Institutional": {
                    "Inst_Visual": inst_calc.get("Inst_Status", "PENDING"),
                    "Inst_Net_3d": float(inst_calc.get("Inst_Net_3d", 0.0)),
                    "Inst_Streak3": int(inst_calc.get("Inst_Streak3", 0)),
                    "Inst_Dir3": inst_calc.get("Inst_Dir3", "PENDING"),
                    "Inst_Status": inst_calc.get("Inst_Status", "PENDING"),
                },
            }
        )
    inst_map = {x["Symbol"]: x["Institutional"] for x in inst_records}
    df_out["Institutional"] = df_out["Symbol"].map(inst_map)
    return df_out

def _decide_inst_status(inst_df: pd.DataFrame, symbols: list[str], trade_date: str, inst_fetch_error: str | None) -> tuple[str, list[str], str | None]:
    """
    inst_status：READY / PENDING / UNAVAILABLE
    - 402 Payment Required → UNAVAILABLE（免費期）
    - 其他錯誤 → UNAVAILABLE（避免假 READY）
    - 無錯誤但資料不足 → PENDING
    """
    if inst_fetch_error:
        return "UNAVAILABLE", [], None

    ready_any = False
    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True

    dates_3d = []
    data_date_finmind = None
    try:
        if not inst_df.empty and "date" in inst_df.columns:
            dates = sorted(inst_df["date"].astype(str).unique().tolist())
            dates_3d = dates[-3:]
            data_date_finmind = dates[-1] if dates else None
    except Exception:
        dates_3d = []
        data_date_finmind = None

    return ("READY" if ready_any else "PENDING"), dates_3d, data_date_finmind

def _market_comment(m: dict) -> str:
    # 明確、可回溯的裁決文字（避免空泛）
    amt_total = m.get("amount_total")
    amt_label = m.get("amount_norm_label", "UNKNOWN")
    inst_status = m.get("inst_status", "UNAVAILABLE")
    degraded = bool(m.get("degraded_mode", False))

    parts = []

    if amt_total in (None, "待更新"):
        parts.append("成交金額待更新")
    else:
        try:
            v = int(str(amt_total).replace(",", ""))
            parts.append(f"成交金額約 {v/1e8:,.0f} 億")
        except Exception:
            parts.append("成交金額已取得")

    parts.append(f"量能標籤={amt_label}")

    if inst_status == "READY":
        parts.append("法人資料可用")
    elif inst_status == "PENDING":
        parts.append("法人資料不足")
    else:
        parts.append("法人資料不可用")

    if degraded:
        parts.append("裁決層：資料降級 → 禁止 BUY/TRIAL")
    else:
        parts.append("裁決層：允許依規則執行")

    return "；".join(parts) + "。"

def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    now = _now_taipei()

    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)

    # 開盤前強制顯示昨日 EOD（你 08:17 的需求）
    default_session = _decide_preopen_session(now)
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0 if default_session == SESSION_INTRADAY else 1)

    tickers_text = st.sidebar.text_area("追蹤清單（逗號分隔，模擬期）", value=",".join(DEFAULT_TICKERS), height=80)
    tickers = [x.strip() for x in tickers_text.split(",") if x.strip()]

    run_btn = st.sidebar.button("Run")
    if not run_btn:
        st.info("按左側 Run 產生 Top 清單與 JSON。")
        return

    # 1) Load market data
    df = _load_market_csv(market)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    # 2) Run analyzer
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 2.1) 中文名補上（至少你監控池一定有）
    if "Name" not in df_top.columns:
        df_top["Name"] = df_top["Symbol"].map(NAME_ZH_MAP).fillna("")

    # 3) Global summary（美股/日經/匯率）
    st.subheader
    st.subheader("全球市場摘要（美股/日經/匯率）")
    gsum = _load_global_summary()
    if gsum is None or gsum.empty:
        st.warning("全球摘要資料缺失（global_market_summary.csv 不存在，且 yfinance 補抓失敗）")
    else:
        st.dataframe(gsum, use_container_width=True)

    # 4) Market amount: TWSE + TPEx（抓不到就用 yfinance proxy（昨日保底））
    amt = fetch_amount_total(trade_date)
    amount_sources = {"twse": amt.source_twse, "tpex": amt.source_tpex, "error": amt.error}

    amount_twse = amt.amount_twse
    amount_tpex = amt.amount_tpex
    amount_total = amt.amount_total

    # 開盤前/昨日模式：若官方抓不到 → 用 yfinance proxy（至少昨日有數字）
    if amount_total is None:
        proxy, proxy_src = yfinance_amount_proxy(tickers, trade_date=trade_date)
        if proxy is not None:
            amount_total = proxy
            amount_sources["proxy"] = proxy_src

    # 5) intraday normalization（只有在 INTRADAY 且 amount_total 有值才算）
    norm = {"progress": None, "amount_norm_cum_ratio": None, "amount_norm_slice_ratio": None, "amount_norm_label": "UNKNOWN"}
    if session == SESSION_INTRADAY and isinstance(amount_total, int):
        # 模擬期：先用 20D 代理為 None（你之後可接真正 avg20）
        avg20_amount_total_median = None
        norm = intraday_norm(amount_total_now=amount_total, amount_total_prev=None, avg20_amount_total=avg20_amount_total_median)

    # 6) Institutional (FinMind) — 免費期遇到 402 就 UNAVAILABLE
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    end_date = trade_date
    symbols = df_top["Symbol"].astype(str).tolist()

    inst_fetch_error = None
    inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])
    try:
        inst_df = fetch_finmind_institutional(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            token=os.getenv("FINMIND_TOKEN", None),
        )
    except Exception as e:
        inst_fetch_error = f"{type(e).__name__}: {str(e)}"
        st.warning(f"個股法人資料抓取失敗：{inst_fetch_error}")

    # 7) inst_status & degraded_mode（V15.7 裁決）
    inst_status, inst_dates_3d, data_date_finmind = _decide_inst_status(inst_df, symbols, trade_date, inst_fetch_error)

    # ✅ 資料降級防線（你指定：任一關鍵資料缺失 → 禁止 BUY/TRIAL）
    amount_ok = isinstance(amount_total, int) and amount_total > 0
    inst_ok = (inst_status == "READY")  # 免費期如果不可用，寧願降級
    degraded_mode = (not amount_ok) or (not inst_ok) or (norm.get("amount_norm_label") == "UNKNOWN" and session == SESSION_INTRADAY)

    # 8) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 9) Macro overview
    macro_overview = {
        "amount_twse": "待更新" if amount_twse is None else f"{amount_twse:,}",
        "amount_tpex": "待更新" if amount_tpex is None else f"{amount_tpex:,}",
        "amount_total": "待更新" if not amount_ok else f"{amount_total:,}",
        "amount_sources": amount_sources,
        "avg20_amount_total_median": None,
        "progress": norm.get("progress"),
        "amount_norm_cum_ratio": norm.get("amount_norm_cum_ratio"),
        "amount_norm_slice_ratio": norm.get("amount_norm_slice_ratio"),
        "amount_norm_label": norm.get("amount_norm_label", "UNKNOWN"),
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": data_date_finmind,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
    }
    macro_overview["market_comment"] = _market_comment(macro_overview)

    macro_data = {"overview": macro_overview, "indices": []}

    # 10) Generate JSON
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # ---------------- UI ----------------
    st.subheader("今日市場狀態判斷（V15.7 裁決）")
    st.info(macro_overview["market_comment"])

    st.subheader("市場成交金額（上市 + 上櫃 = amount_total）")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", macro_overview["amount_twse"])
    c2.metric("TPEx 上櫃", macro_overview["amount_tpex"])
    c3.metric("Total 合計", macro_overview["amount_total"])
    c4.metric("20D Median(代理)", str(macro_overview["avg20_amount_total_median"]))

    st.caption(f"來源/錯誤：{json.dumps(amount_sources, ensure_ascii=False)}")

    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")
    st.code(
        json.dumps(
            {
                "progress": macro_overview["progress"],
                "cum_ratio(穩健型用)": macro_overview["amount_norm_cum_ratio"],
                "slice_ratio(保守型用)": macro_overview["amount_norm_slice_ratio"],
                "label": macro_overview["amount_norm_label"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        language="json",
    )

    st.subheader("Top List")
    # 顯示：代碼 + 中文名
    if "Name" in df_top2.columns:
        df_top2["Name"] = df_top2["Symbol"].map(NAME_ZH_MAP).fillna(df_top2["Name"].fillna(""))
    else:
        df_top2["Name"] = df_top2["Symbol"].map(NAME_ZH_MAP).fillna("")
    st.dataframe(df_top2, use_container_width=True)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    # Save output (Streamlit Cloud 容器可寫，但會重置；這裡仍保留給你下載/除錯)
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    try:
        with open(os.path.join(DATA_DIR, outname), "w", encoding="utf-8") as f:
            f.write(json_text)
        st.success(f"JSON 已輸出：data/{outname}")
    except Exception as e:
        st.warning(f"JSON 寫檔失敗（不影響畫面）：{type(e).__name__}: {e}")

if __name__ == "__main__":
    app()
