# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime

import pandas as pd
import streamlit as st
import requests  # 用於辨識 FinMind 402

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import (
    fetch_finmind_institutional,
    fetch_finmind_market_inst_net_ab,
)
from institutional_utils import calc_inst_3d
from arbiter import arbitrate


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
    return pd.read_csv(fname)


def _compute_market_amount_today(df: pd.DataFrame, latest_date) -> str:
    d = df.copy()
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    d = d[d["Date"] == latest_date].copy()
    if d.empty:
        return "待更新"
    d["Close"] = pd.to_numeric(d.get("Close"), errors="coerce").fillna(0)
    d["Volume"] = pd.to_numeric(d.get("Volume"), errors="coerce").fillna(0)
    amt = float((d["Close"] * d["Volume"]).sum())
    return f"{amt:,.0f}"


def _merge_institutional_into_df_top(df_top: pd.DataFrame, inst_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    """
    把 calc_inst_3d 的結果塞回 df_top 的 Institutional 欄位（dict）
    """
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
    """
    給 macro.overview.inst_status + inst_dates_3d
    規則：只要有任何一檔能滿足「三日資料齊全」→ READY
    否則 PENDING
    """
    ready_any = False
    dates_3d: list[str] = []

    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True

    try:
        if not inst_df.empty and "date" in inst_df.columns:
            dates_3d = sorted(inst_df["date"].astype(str).unique().tolist())[-3:]
    except Exception:
        dates_3d = []

    return ("READY" if ready_any else "PENDING"), dates_3d


def _apply_arbiter_to_payload(payload: dict) -> dict:
    """
    在 payload["stocks"] 逐檔寫入 FinalDecision（Conservative/Aggressive）
    """
    macro_overview = (payload.get("macro", {}) or {}).get("overview", {}) or {}
    stocks = payload.get("stocks", []) or []

    for s in stocks:
        s["FinalDecision"] = {
            "Conservative": arbitrate(s, macro_overview, account="Conservative"),
            "Aggressive": arbitrate(s, macro_overview, account="Aggressive"),
        }

    payload["stocks"] = stocks
    return payload


def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)

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

    symbols = df_top["Symbol"].astype(str).tolist()
    finmind_token = os.getenv("FINMIND_TOKEN", None)

    # 3) Fetch institutional (FinMind) - 402 自動降級，不中斷
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    end_date = trade_date

    inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])
    inst_status = "PENDING"  # READY / PENDING / UNAVAILABLE
    inst_dates_3d: list[str] = []
    market_inst_ab = {"A": 0.0, "B": 0.0}

    # --- 個股法人
    try:
        inst_df = fetch_finmind_institutional(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            token=finmind_token,
        )
        inst_status, inst_dates_3d = _decide_inst_status(inst_df, symbols, trade_date)
    except requests.exceptions.HTTPError as e:
        status_code = getattr(e.response, "status_code", None)
        if status_code == 402:
            inst_status = "UNAVAILABLE"
            inst_dates_3d = []
            st.warning("個股法人資料抓取失敗：FinMind 402（付費牆）。系統將進入「法人缺失模式」繼續運行。")
        else:
            inst_status = "PENDING"
            inst_dates_3d = []
            st.warning(f"個股法人資料抓取失敗：HTTPError {status_code}。系統將進入「法人缺失模式」繼續運行。")
    except Exception as e:
        inst_status = "PENDING"
        inst_dates_3d = []
        st.warning(f"個股法人資料抓取失敗：{type(e).__name__}: {str(e)}。系統將進入「法人缺失模式」繼續運行。")

    # --- 市場法人 A/B（你指定：inst_net 用 A三大法人合計 / B外資）
    try:
        market_inst_ab = fetch_finmind_market_inst_net_ab(
            trade_date=trade_date,
            start_date=trade_date,
            end_date=trade_date,
            token=finmind_token,
        )
    except requests.exceptions.HTTPError as e:
        status_code = getattr(e.response, "status_code", None)
        if status_code == 402:
            st.warning("市場整體法人 A/B 抓取失敗：FinMind 402（付費牆）。本次 inst_net 將以 0 顯示。")
        else:
            st.warning(f"市場整體法人 A/B 抓取失敗：HTTPError {status_code}。本次 inst_net 將以 0 顯示。")
    except Exception as e:
        st.warning(f"市場整體法人 A/B 抓取失敗：{type(e).__name__}: {str(e)}。本次 inst_net 將以 0 顯示。")

    # 4) Merge institutional into df_top（若可用）
    if inst_status in ("READY", "PENDING") and (not inst_df.empty):
        df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)
    else:
        df_top2 = df_top.copy()

    # 5) Macro overview
    amount_str = _compute_market_amount_today(df, latest_date)
    a_net = float(market_inst_ab.get("A", 0.0) or 0.0)
    b_net = float(market_inst_ab.get("B", 0.0) or 0.0)
    inst_net_str = f"A:{a_net:,.0f} | B:{b_net:,.0f}"

    # 重點：不因法人缺失就進 Level-2（只讓 Arbiter 用 inst_status 判斷「法人缺失模式」）
    macro_data = {
        "overview": {
            "amount": amount_str,
            "inst_net": inst_net_str,
            "trade_date": trade_date,
            "inst_status": inst_status,   # READY / PENDING / UNAVAILABLE
            "inst_dates_3d": inst_dates_3d,
            "kill_switch": False,
            "v14_watch": False,
            "degraded_mode": False,
        },
        "indices": [],
    }

    # 6) Generate JSON（Arbiter input schema）
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # 7) Apply Arbiter → FinalDecision 寫回 payload
    try:
        payload = json.loads(json_text)
        payload = _apply_arbiter_to_payload(payload)
        json_text2 = json.dumps(payload, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"Arbiter 套用失敗：{type(e).__name__}: {str(e)}")
        json_text2 = json_text

    # UI
    st.subheader("Top List")
    st.dataframe(df_top2, use_container_width=True)

    st.subheader("Macro Overview")
    st.json(macro_data["overview"])

    st.subheader("AI JSON (Arbiter Output Included)")
    st.code(json_text2, language="json")

    # save
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text2)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
