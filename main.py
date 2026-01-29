# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone, time
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import streamlit as st
import yfinance as yf

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from institutional_utils import calc_inst_3d

# 付費牆/不可用時會降級，不讓整個 app 爆
try:
    from finmind_institutional import fetch_finmind_institutional
except Exception:
    fetch_finmind_institutional = None

from market_amount import fetch_amount_total, intraday_norm

TZ_TAIPEI = timezone(timedelta(hours=8))
TRADING_START = time(9, 0)
TRADING_END = time(13, 30)


# =========================
# 0) 基本工具
# =========================
def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return _now_taipei().strftime("%Y-%m-%d")


def _find_market_csv(market: str) -> str:
    """
    同時支援：
    - root: data_tw-share.csv / data_tw.csv
    - data/: data/data_tw-share.csv / data/data_tw.csv
    """
    candidates = [
        f"data_{market}.csv",
        os.path.join("data", f"data_{market}.csv"),
        "data_tw-share.csv",
        os.path.join("data", "data_tw-share.csv"),
        "data_tw.csv",
        os.path.join("data", "data_tw.csv"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"找不到資料檔：{candidates}")


def _load_market_csv(market: str) -> pd.DataFrame:
    path = _find_market_csv(market)
    df = pd.read_csv(path)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Close"] = pd.to_numeric(df.get("Close"), errors="coerce")
    df["Volume"] = pd.to_numeric(df.get("Volume"), errors="coerce")
    return df


def _get_latest_trade_date(df: pd.DataFrame) -> datetime:
    d = df.dropna(subset=["Date"]).copy()
    if d.empty:
        return _now_taipei()
    return pd.to_datetime(d["Date"].max()).to_pydatetime()


# =========================
# 1) 全球市場摘要（美股/半導體/匯率/日經）
# =========================
def _load_global_market_summary() -> pd.DataFrame:
    """
    優先讀 data/global_market_summary.csv
    若不存在 => 即時用 yfinance 抓（免費）
    """
    p = os.path.join("data", "global_market_summary.csv")
    if os.path.exists(p):
        g = pd.read_csv(p)
        return g

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
    for mkt, name, sym in items:
        try:
            t = yf.Ticker(sym)
            info = t.fast_info
            last = float(info.get("last_price") or 0.0)
            prev = float(info.get("previous_close") or 0.0)
            chg = ((last - prev) / prev * 100.0) if prev > 0 else 0.0
            rows.append({"Market": mkt, "Symbol": name, "Change": round(chg, 4), "Value": round(last, 4)})
        except Exception:
            rows.append({"Market": mkt, "Symbol": name, "Change": None, "Value": None})
    return pd.DataFrame(rows)


# =========================
# 2) 中文名稱（免費：yfinance shortName）
# =========================
@st.cache_data(ttl=60 * 60 * 12)
def _fetch_name_zh(symbol: str) -> str:
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}
        name = info.get("shortName") or info.get("longName") or ""
        return str(name) if name else ""
    except Exception:
        return ""


def _ensure_names(df_top: pd.DataFrame) -> pd.DataFrame:
    out = df_top.copy()
    if "Name" not in out.columns:
        out["Name"] = ""
    for i, r in out.iterrows():
        if not str(r.get("Name", "")).strip():
            sym = str(r.get("Symbol", "")).strip()
            out.at[i, "Name"] = _fetch_name_zh(sym)
    return out


# =========================
# 3) 持倉強制納入（20 + N）
# =========================
def _parse_holdings(text: str) -> List[str]:
    if not text:
        return []
    parts = [x.strip() for x in text.replace("，", ",").split(",")]
    out = []
    for p in parts:
        if not p:
            continue
        if p.isdigit():
            out.append(f"{p}.TW")
        else:
            out.append(p)
    seen = set()
    dedup = []
    for s in out:
        if s not in seen:
            seen.add(s)
            dedup.append(s)
    return dedup


def _append_holdings_rows(df_top: pd.DataFrame, df_all: pd.DataFrame, holdings: List[str]) -> pd.DataFrame:
    if not holdings:
        return df_top

    out = df_top.copy()
    if "orphan_holding" not in out.columns:
        out["orphan_holding"] = False

    top_syms = set(out["Symbol"].astype(str).tolist())

    latest_dt = df_all["Date"].max()
    d = df_all[df_all["Date"] == latest_dt].copy()

    for sym in holdings:
        if sym in top_syms:
            continue

        one = d[d["Symbol"].astype(str) == sym].copy()
        if one.empty:
            row = {"Symbol": sym, "Name": _fetch_name_zh(sym), "orphan_holding": True}
            out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
            continue

        close = float(one["Close"].iloc[0]) if pd.notna(one["Close"].iloc[0]) else None
        vol = float(one["Volume"].iloc[0]) if pd.notna(one["Volume"].iloc[0]) else None
        row = {
            "Symbol": sym,
            "Name": _fetch_name_zh(sym),
            "Close": close,
            "Volume": vol,
            "orphan_holding": True,
        }
        out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)

    return out


# =========================
# 4) 法人狀態（免費/模擬期：付費牆則 UNAVAILABLE）
# =========================
def _decide_inst_status(inst_df: pd.DataFrame, symbols: List[str], trade_date: str) -> Tuple[str, List[str], Optional[str]]:
    if inst_df is None or inst_df.empty:
        return "UNAVAILABLE", [], None

    ready_any = False
    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True
            break

    dates_3d = []
    try:
        dates_3d = sorted(inst_df["date"].astype(str).unique().tolist())[-3:]
    except Exception:
        dates_3d = []

    return ("READY" if ready_any else "PENDING"), dates_3d, (max(dates_3d) if dates_3d else None)


def _merge_institutional_into_df(df_top: pd.DataFrame, inst_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    out = df_top.copy()

    if inst_df is None or inst_df.empty:
        out["Institutional"] = [{"Inst_Status": "PENDING"} for _ in range(len(out))]
        return out

    inst_map: Dict[str, Any] = {}
    for _, r in out.iterrows():
        sym = str(r.get("Symbol", ""))
        calc = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        inst_map[sym] = {
            "Inst_Visual": calc.get("Inst_Status", "PENDING"),
            "Inst_Net_3d": float(calc.get("Inst_Net_3d", 0.0)),
            "Inst_Streak3": int(calc.get("Inst_Streak3", 0)),
            "Inst_Dir3": calc.get("Inst_Dir3", "PENDING"),
            "Inst_Status": calc.get("Inst_Status", "PENDING"),
        }

    out["Institutional"] = out["Symbol"].astype(str).map(inst_map).fillna({"Inst_Status": "PENDING"})
    return out


# =========================
# 5) 市場一句話（可讀版）
# =========================
def generate_market_comment(macro_overview: dict) -> str:
    amount_total = macro_overview.get("amount_total")
    amount_label = macro_overview.get("amount_norm_label", "UNKNOWN")
    inst_status = macro_overview.get("inst_status", "UNAVAILABLE")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))

    parts = []

    if amount_total and str(amount_total).isdigit():
        yi = int(amount_total) / 100_000_000
        parts.append(f"成交金額（上市+上櫃）約 {yi:,.0f} 億；量能判定 {amount_label}。")
    else:
        parts.append("成交金額待更新。")

    if inst_status == "READY":
        parts.append("法人資料可用。")
    elif inst_status == "PENDING":
        parts.append("法人資料不足（未滿 3 日）。")
    else:
        parts.append("法人資料不可用（免費模擬期常見：付費牆/來源中斷）。")

    if degraded_mode:
        parts.append("裁決層已進入資料降級：禁止 BUY/TRIAL。")
    else:
        parts.append("裁決層允許依模式評估進場（仍以風控條件為準）。")

    return " ".join(parts)


# =========================
# 6) 主程式
# =========================
def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")
    st.caption("V15.7（免費/模擬期）｜開盤前顯示昨日 EOD + 最新全球市場摘要")

    # Sidebar
    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)

    holdings_text = st.sidebar.text_input("持倉股（逗號分隔，例：2330,2317 或 2330.TW）", value="")
    holdings = _parse_holdings(holdings_text)

    verify_ssl = st.sidebar.checkbox("SSL 驗證（若抓官方資料出現奇怪憑證錯誤可關閉）", value=True)
    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("按左側 Run 產生昨日/最新資料、Top20(+持倉) 與 AI JSON。")
        return

    # 1) Load market data
    df = _load_market_csv(market)

    # 2) 顯示交易日（以資料檔最新日期為準：開盤前就是昨日 EOD）
    effective_dt = _get_latest_trade_date(df)
    trade_date = _fmt_date(effective_dt)
    st.info(f"目前台北時間：{_now_taipei().strftime('%Y-%m-%d %H:%M')}｜顯示交易日：{trade_date}")

    # 3) 全球市場摘要
    st.subheader("全球市場摘要（美股/日經/匯率）")
    g = _load_global_market_summary()
    st.dataframe(g, use_container_width=True)

    # 4) 市場成交金額（上市+上櫃）
    st.subheader("市場成交金額（上市 + 上櫃 = amount_total）")
    ma = fetch_amount_total(
        trade_date=pd.to_datetime(trade_date).to_pydatetime().replace(tzinfo=TZ_TAIPEI),
        verify_ssl=verify_ssl,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", "待更新" if ma.amount_twse is None else f"{ma.amount_twse/100_000_000:,.0f} 億")
    c2.metric("TPEx 上櫃", "待更新" if ma.amount_tpex is None else f"{ma.amount_tpex/100_000_000:,.0f} 億")
    c3.metric("Total 合計", "待更新" if ma.amount_total is None else f"{ma.amount_total/100_000_000:,.0f} 億")
    c4.metric("20D Median(代理)", "None")

    amount_sources = {"twse": ma.source_twse, "tpex": ma.source_tpex, "error": ma.error}
    st.caption(f"來源/錯誤：{amount_sources}")

    # 5) INTRADAY 量能正規化（模擬期：無 20D 代理值 => UNKNOWN）
    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")
    norm = {"progress": None, "amount_norm_cum_ratio": None, "amount_norm_slice_ratio": None, "amount_norm_label": "UNKNOWN"}
    st.code(json.dumps({
        "progress": norm["progress"],
        "cum_ratio(穩健型用)": norm["amount_norm_cum_ratio"],
        "slice_ratio(保守型用)": norm["amount_norm_slice_ratio"],
        "label": norm["amount_norm_label"],
    }, ensure_ascii=False, indent=2), language="json")

    # 6) Analyzer：產生 Top 清單
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 7) 補中文名
    df_top = _ensure_names(df_top)

    # 8) Top20 + 持倉（20+N）
    df_top = _append_holdings_rows(df_top, df_all=df, holdings=holdings)

    # 9) 法人（FinMind 402 付費牆 => UNAVAILABLE，不崩）
    symbols = df_top["Symbol"].astype(str).tolist()
    inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])
    inst_fetch_error = None

    if fetch_finmind_institutional is None:
        inst_fetch_error = "FinMind module import failed"
    else:
        try:
            start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
            end_date = trade_date
            inst_df = fetch_finmind_institutional(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                token=os.getenv("FINMIND_TOKEN", None),
            )
        except Exception as e:
            inst_fetch_error = f"{type(e).__name__}: {str(e)}"

    inst_status = "UNAVAILABLE"
    inst_dates_3d: List[str] = []
    data_date_finmind: Optional[str] = None

    if inst_fetch_error:
        inst_status = "UNAVAILABLE"
    else:
        inst_status, inst_dates_3d, data_date_finmind = _decide_inst_status(inst_df, symbols, trade_date)

    df_top2 = _merge_institutional_into_df(df_top, inst_df, trade_date=trade_date)

    # 10) V15.7 裁決層：degraded_mode 絕對防線
    amount_ok = (ma.amount_total is not None and ma.amount_total > 0)

    degraded_mode = False
    if not amount_ok:
        degraded_mode = True
    if inst_status == "UNAVAILABLE":
        degraded_mode = True

    macro_overview = {
        "amount_twse": "待更新" if ma.amount_twse is None else str(ma.amount_twse),
        "amount_tpex": "待更新" if ma.amount_tpex is None else str(ma.amount_tpex),
        "amount_total": "待更新" if ma.amount_total is None else str(ma.amount_total),
        "amount_sources": amount_sources,
        "avg20_amount_total_median": None,
        "progress": norm["progress"],
        "amount_norm_cum_ratio": norm["amount_norm_cum_ratio"],
        "amount_norm_slice_ratio": norm["amount_norm_slice_ratio"],
        "amount_norm_label": norm["amount_norm_label"],
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": data_date_finmind,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
        "amount": "待更新" if ma.amount_total is None else str(ma.amount_total),
    }

    macro_overview["market_comment"] = generate_market_comment(macro_overview)

    st.subheader("今日市場狀態判斷（V15.7 裁決）")
    st.info(macro_overview["market_comment"])

    if inst_fetch_error:
        st.warning(f"個股法人資料抓取失敗（模擬期可接受）：{inst_fetch_error}")

    # 11) Assemble JSON for Arbiter
    macro_data = {"overview": macro_overview, "indices": []}
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # 12) Top List（含中文名）
    st.subheader("Top List（Top20 + 持倉 20+N）")
    st.dataframe(df_top2, use_container_width=True)

    st.subheader("AI JSON（Arbiter Input）")
    st.code(json_text, language="json")

    # 13) save（雲端可能限制寫入，失敗也不致命）
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    try:
        with open(outname, "w", encoding="utf-8") as f:
            f.write(json_text)
        st.success(f"JSON 已輸出：{outname}")
    except Exception as e:
        st.warning(f"JSON 寫檔失敗（雲端環境可能限制寫入）：{type(e).__name__}: {str(e)}")


if __name__ == "__main__":
    app()
