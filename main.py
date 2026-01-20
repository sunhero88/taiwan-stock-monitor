# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import argparse
from datetime import datetime
from functools import lru_cache

import pandas as pd
import streamlit as st
import yfinance as yf
import requests

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d


# -----------------------------
# Utilities
# -----------------------------
def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _load_market_csv(market: str) -> pd.DataFrame:
    fname = f"data_{market}.csv"
    if not os.path.exists(fname):
        if os.path.exists("data_tw-share.csv"):
            fname = "data_tw-share.csv"
        elif os.path.exists("data_tw.csv"):
            fname = "data_tw.csv"
        else:
            raise FileNotFoundError(f"找不到資料檔：{fname} / data_tw-share.csv / data_tw.csv")

    df = pd.read_csv(fname)
    return df


def _normalize_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    把 Date 欄位轉成「不帶時區」的 datetime（避免 00:00/UTC 造成困擾）
    """
    d = df.copy()
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")

    # 若未帶 tz，tz_localize 會噴錯，故用 try
    try:
        # 若是 tz-aware，移除 tz
        if getattr(d["Date"].dt, "tz", None) is not None:
            d["Date"] = d["Date"].dt.tz_convert(None)
    except Exception:
        pass

    # 進一步：把時間歸零（只保留日期），避免出現 00:00 誤會
    try:
        d["Date"] = d["Date"].dt.normalize()
    except Exception:
        pass

    return d


def _compute_market_amount_today(df: pd.DataFrame, latest_date) -> str:
    d = df.copy()
    d = _normalize_date_column(d)
    d = d[d["Date"] == latest_date].copy()
    if d.empty:
        return "待更新"

    d["Close"] = pd.to_numeric(d.get("Close"), errors="coerce").fillna(0)
    d["Volume"] = pd.to_numeric(d.get("Volume"), errors="coerce").fillna(0)
    amt = float((d["Close"] * d["Volume"]).sum())
    return f"{amt:,.0f}"


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


def _decide_inst_status(inst_df: pd.DataFrame, symbols: list[str], trade_date: str) -> tuple[str, list[str]]:
    ready_any = False
    dates_3d = []

    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True

    try:
        if not inst_df.empty:
            dates_3d = sorted(inst_df["date"].astype(str).unique().tolist())[-3:]
    except Exception:
        dates_3d = []

    return ("READY" if ready_any else "PENDING"), dates_3d


# -----------------------------
# Stock name (寫入 JSON / UI)
# -----------------------------
@lru_cache(maxsize=512)
def _get_stock_name(symbol: str) -> str:
    """
    以 yfinance Ticker.info 取 shortName/longName
    取不到就回傳 symbol
    """
    try:
        info = yf.Ticker(symbol).info or {}
        name = info.get("shortName") or info.get("longName") or ""
        name = str(name).strip()
        return name if name else symbol
    except Exception:
        return symbol


def _attach_stock_names(df_top: pd.DataFrame) -> pd.DataFrame:
    d = df_top.copy()
    d["Name"] = d["Symbol"].astype(str).map(_get_stock_name)
    return d


# -----------------------------
# Indices (TW + US)
# -----------------------------
def fetch_indices_snapshot(trade_date: str) -> list[dict]:
    """
    用 yfinance 抓指數快照（近 5 日，取最後兩天算漲跌）
    回傳 list[dict]，寫入 macro.indices
    """
    index_map = {
        "^TWII": "台股加權指數",
        "^DJI": "道瓊工業指數",
        "^IXIC": "那斯達克指數",
    }

    out = []
    for ticker, name in index_map.items():
        try:
            df = yf.download(ticker, period="7d", interval="1d", progress=False)
            if df is None or df.empty:
                continue

            df = df.dropna()
            last_close = float(df["Close"].iloc[-1])
            prev_close = float(df["Close"].iloc[-2]) if len(df) >= 2 else last_close
            chg = last_close - prev_close
            chg_pct = (chg / prev_close * 100.0) if prev_close != 0 else 0.0

            out.append(
                {
                    "symbol": ticker,
                    "name": name,
                    "close": round(last_close, 2),
                    "chg": round(chg, 2),
                    "chg_pct": round(chg_pct, 2),
                    "asof": str(df.index[-1].date()),
                }
            )
        except Exception:
            continue

    return out


# -----------------------------
# Market Institutional A/B (三大法人 / 外資)
# -----------------------------
def fetch_twse_market_inst_net_ab(trade_date: str) -> dict:
    """
    嘗試用 TWSE 公開 rwd 端點抓「三大法人買賣超」合計。
    抓不到就回 A=0, B=0（不讓流程失敗）

    A = 外資 + 投信 + 自營商（含避險）合計
    B = 外資
    """
    yyyymmdd = trade_date.replace("-", "")
    urls = [
        # 常見 TWSE rwd 端點（zh）
        f"https://www.twse.com.tw/rwd/zh/fund/T86?date={yyyymmdd}&selectType=ALL&response=json",
        # 有些環境會用 en
        f"https://www.twse.com.tw/rwd/en/fund/T86?date={yyyymmdd}&selectType=ALL&response=json",
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            js = r.json()

            # js 常見欄位：fields / data
            data = js.get("data") or []
            fields = js.get("fields") or []

            if not data or not fields:
                continue

            # 嘗試找「外資」「投信」「自營商」相關欄位
            # 這裡採「寬鬆解析」：先把每列轉成 dict，再用關鍵字判斷
            a_sum = 0.0
            b_sum = 0.0

            for row in data:
                d = {fields[i]: row[i] for i in range(min(len(fields), len(row)))}
                name = str(d.get("單位名稱") or d.get("Institutional investors") or d.get("name") or "").strip()

                # 淨額欄位名稱在不同語系可能不同，這裡用多候選
                net_raw = (
                    d.get("買賣超股數")
                    or d.get("Net buy/sell")
                    or d.get("net")
                    or d.get("Buy/Sell")
                    or 0
                )

                # 轉數字（可能帶逗號）
                try:
                    net = float(str(net_raw).replace(",", "").replace(" ", ""))
                except Exception:
                    net = 0.0

                # 外資
                if ("外資" in name) or ("Foreign" in name):
                    b_sum += net
                    a_sum += net
                # 投信
                elif ("投信" in name) or ("Investment Trust" in name):
                    a_sum += net
                # 自營商（含避險）
                elif ("自營商" in name) or ("Dealer" in name):
                    a_sum += net

            return {"A": float(a_sum), "B": float(b_sum)}
        except Exception:
            continue

    return {"A": 0.0, "B": 0.0}


def _fmt_inst_net_ab_text(ab: dict) -> str:
    """
    以「億元」顯示更直觀：A/B 以 100,000,000 換算
    """
    try:
        a = float(ab.get("A", 0.0))
        b = float(ab.get("B", 0.0))
        a_yi = a / 100_000_000
        b_yi = b / 100_000_000
        return f"A:{a_yi:.2f}億 | B:{b_yi:.2f}億"
    except Exception:
        return "A:0 | B:0"


# -----------------------------
# Market comment (一般投資人版)
# -----------------------------
def generate_market_comment_retail(macro_overview: dict) -> str:
    amount = macro_overview.get("amount")
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))
    inst_net = str(macro_overview.get("inst_net", "") or "")

    if kill_switch or v14_watch:
        return "今日市場風險指標觸發保護機制，系統以資金保全為優先：不建議進場加碼，僅處理必要的持倉風控。"

    liquidity_ok = False
    try:
        if amount not in (None, "", "待更新"):
            liquidity_ok = float(str(amount).replace(",", "")) > 300_000_000_000  # 3,000 億
    except Exception:
        liquidity_ok = False

    liquidity_text = "成交金額在常態區間，" if liquidity_ok else "成交金額偏低，"

    # 法人資訊可用性
    if inst_status in ("UNAVAILABLE", "PENDING"):
        inst_text = "法人資料目前不可用或不足（三大法人無法可靠判讀），建議以觀察或小額試單為主。"
    elif inst_status == "READY":
        inst_text = f"法人資料可用（{inst_net}），可搭配個股條件做較積極的倉位調整。"
    else:
        inst_text = "法人資料狀態不完整，建議以風控為先。"

    strategy_text = "整體策略以保守為主。" if (degraded_mode and inst_status != "READY") else "倉位可依個股訊號彈性調整。"
    return liquidity_text + inst_text + strategy_text


# -----------------------------
# Core build payload (shared by UI/CLI)
# -----------------------------
def build_payload(market: str, session: str) -> tuple[pd.DataFrame, dict, str]:
    df = _load_market_csv(market)
    df = _normalize_date_column(df)

    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    df_top, err = run_analysis(df, session=session)
    if err:
        raise RuntimeError(f"Analyzer error: {err}")

    # 股票名稱
    df_top = _attach_stock_names(df_top)

    # 法人（FinMind：若 402 就標 UNAVAILABLE）
    symbols = df_top["Symbol"].astype(str).tolist()

    inst_fetch_error = None
    inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])
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

    inst_status, inst_dates_3d = _decide_inst_status(inst_df, symbols, trade_date)

    if inst_fetch_error and ("402" in inst_fetch_error or "Payment Required" in inst_fetch_error):
        inst_status = "UNAVAILABLE"
        inst_dates_3d = []

    # 合併個股法人（就算空 DF 也不會壞）
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 成交金額
    amount_str = _compute_market_amount_today(df, latest_date)

    # 指數快照（TW + US）
    indices = fetch_indices_snapshot(trade_date)

    # 市場三大法人 A/B（抓不到也不影響）
    ab = fetch_twse_market_inst_net_ab(trade_date)
    inst_net_text = _fmt_inst_net_ab_text(ab)

    # degraded_mode：法人 UNAVAILABLE 不等於「系統降級」，交給 Arbiter 的 NA 規則即可；
    # PENDING 才視作暫時不足
    degraded_mode = (inst_status == "PENDING")

    macro_overview = {
        "amount": amount_str,
        "inst_net": inst_net_text,
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
    }

    market_comment = generate_market_comment_retail(macro_overview)
    macro_overview["market_comment"] = market_comment

    macro_data = {
        "overview": macro_overview,
        "indices": indices,
    }

    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)
    payload = json.loads(json_text)

    # ✅ 把股票名稱也寫入 JSON（每檔 stock 加上 Name）
    # analyzer.generate_ai_json 現在未必帶 Name，這裡保底寫入
    name_map = {r["Symbol"]: r.get("Name", r["Symbol"]) for r in df_top2.to_dict("records")}
    for s in payload.get("stocks", []):
        sym = s.get("Symbol")
        s["Name"] = name_map.get(sym, sym)

    json_text2 = json.dumps(payload, ensure_ascii=False, indent=2)

    return df_top2, macro_data, json_text2


# -----------------------------
# Streamlit App
# -----------------------------
def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)
    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("按左側 Run 產生 Top 清單與 JSON。")
        return

    try:
        df_top2, macro_data, json_text = build_payload(market=market, session=session)
    except Exception as e:
        st.error(f"{type(e).__name__}: {str(e)}")
        return

    macro_overview = (macro_data.get("overview") or {})
    market_comment = macro_overview.get("market_comment", "")

    st.subheader("今日市場狀態判斷（一般投資人版）")
    st.info(market_comment)

    st.subheader("Macro 指數快照")
    st.dataframe(pd.DataFrame(macro_data.get("indices", [])))

    st.subheader("Top List（含股票名稱）")
    st.dataframe(df_top2)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    trade_date = macro_overview.get("trade_date", datetime.now().strftime("%Y-%m-%d"))
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


# -----------------------------
# CLI
# -----------------------------
def cli_run(market: str, session: str):
    df_top2, macro_data, json_text = build_payload(market=market, session=session)
    trade_date = (macro_data.get("overview") or {}).get("trade_date", datetime.now().strftime("%Y-%m-%d"))
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)

    # CLI 版：印出一句市場判斷 + 檔名
    print((macro_data.get("overview") or {}).get("market_comment", ""))
    print(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", action="store_true", help="Run in CLI mode (for GitHub Actions)")
    parser.add_argument("--market", default="tw-share", help="tw-share / tw")
    parser.add_argument("--session", default=SESSION_INTRADAY, help="INTRADAY / EOD")
    args = parser.parse_args()

    if args.cli:
        cli_run(market=args.market, session=args.session)
    else:
        app()
