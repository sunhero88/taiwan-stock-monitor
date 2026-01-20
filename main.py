# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import argparse
from datetime import datetime

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d


# ======================================================
# Helpers
# ======================================================

def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _resolve_market_csv_filename(market: str) -> str:
    """
    依 market 決定要讀哪個 CSV；找不到時 fallback。
    """
    fname = f"data_{market}.csv"
    if os.path.exists(fname):
        return fname
    if os.path.exists("data_tw-share.csv"):
        return "data_tw-share.csv"
    if os.path.exists("data_tw.csv"):
        return "data_tw.csv"
    raise FileNotFoundError(f"找不到資料檔：{fname} / data_tw-share.csv / data_tw.csv")


def _load_market_csv(market: str) -> pd.DataFrame:
    fname = _resolve_market_csv_filename(market)
    df = pd.read_csv(fname)
    return df


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
        if not inst_df.empty:
            dates_3d = sorted(inst_df["date"].astype(str).unique().tolist())[-3:]
    except Exception:
        dates_3d = []

    return ("READY" if ready_any else "PENDING"), dates_3d


def build_data_freshness_block(csv_filename: str, df: pd.DataFrame) -> dict:
    """
    ✅ 建議2：資料新鮮度鑑識（寫入 JSON）
    放在 macro.overview.data_freshness
    """
    # file mtime
    try:
        mtime_ts = os.path.getmtime(csv_filename)
        file_mtime = datetime.fromtimestamp(mtime_ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        file_mtime = "Unknown"

    # csv date max
    try:
        if "Date" in df.columns:
            d = pd.to_datetime(df["Date"], errors="coerce")
            csv_date_max_dt = d.max()
            csv_date_max = csv_date_max_dt.strftime("%Y-%m-%d") if pd.notna(csv_date_max_dt) else "Unknown"
        else:
            csv_date_max = "Unknown"
    except Exception:
        csv_date_max = "Unknown"

    # lag days (local)
    lag_days = None
    hint = ""
    try:
        if csv_date_max not in ("Unknown", None, ""):
            today = datetime.now().date()
            latest = pd.to_datetime(csv_date_max).date()
            lag_days = (today - latest).days
            if lag_days >= 2:
                hint = (
                    f"資料可能未更新：CSV 最新交易日為 {csv_date_max}，落後 {lag_days} 天。"
                    "請優先檢查 GitHub Actions 是否成功更新並 commit CSV，或部署環境是否已拉到最新 commit。"
                )
            else:
                hint = f"資料新鮮度正常：CSV 最新交易日為 {csv_date_max}，落後 {lag_days} 天（可接受範圍）。"
        else:
            hint = "無法判讀資料新鮮度：CSV Date 欄位缺失或解析失敗。"
    except Exception:
        hint = "無法判讀資料新鮮度：計算失敗。"

    return {
        "source_file": csv_filename,
        "file_mtime": file_mtime,
        "csv_date_max": csv_date_max,
        "lag_days": lag_days,
        "hint": hint,
    }


def generate_market_comment_retail(macro_overview: dict) -> str:
    """
    依據 Macro Overview 自動生成「今日市場狀態判斷」（一般投資人可讀版）
    每句話都可回溯到欄位，不做情緒化形容。
    """
    amount = macro_overview.get("amount")
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    # 系統級風險
    if kill_switch or v14_watch:
        return "今日市場風險標記已觸發（系統防護中），建議避免進場，以資金保全為優先。"

    # 流動性（成交額粗門檻）
    liquidity_ok = False
    try:
        if amount not in (None, "", "待更新"):
            liquidity_ok = float(str(amount).replace(",", "")) >= 300_000_000_000  # 3,000億
    except Exception:
        liquidity_ok = False

    liquidity_text = "市場成交量維持在正常水準，" if liquidity_ok else "市場成交量偏低，"

    # 法人可用性
    if inst_status in ("UNAVAILABLE", "PENDING"):
        inst_text = "目前法人動向尚無法提供有效支撐，操作建議以觀察或小額嘗試為主，不宜重倉。"
    elif inst_status == "READY":
        inst_text = "法人動向可用且具參考性，可搭配個股條件採取較積極的進出策略。"
    else:
        inst_text = "法人資訊狀態不完整，建議採取保守策略。"

    # 降級提示（與 Arbiter 一致）
    strategy_text = "整體策略以保守為主。" if degraded_mode else "可依個股條件彈性調整策略。"

    return liquidity_text + inst_text + strategy_text


# ======================================================
# Core pipeline (shared by UI / CLI)
# ======================================================

def run_pipeline(market: str, session: str) -> tuple[pd.DataFrame, dict, str]:
    """
    回傳：
      - df_top2（含 Institutional dict）
      - macro_data（含 overview / indices）
      - json_text（Arbiter Input）
    """
    # 1) Load market data
    csv_filename = _resolve_market_csv_filename(market)
    df = pd.read_csv(csv_filename)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    # 2) Run analyzer
    df_top, err = run_analysis(df, session=session)
    if err:
        raise RuntimeError(f"Analyzer error: {err}")

    symbols = df_top["Symbol"].astype(str).tolist()

    # 3) Fetch institutional (FinMind) — 可能 402（付費）
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    end_date = trade_date

    inst_fetch_error = None
    try:
        inst_df = fetch_finmind_institutional(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            token=os.getenv("FINMIND_TOKEN", None),
        )
    except Exception as e:
        inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])
        inst_fetch_error = f"{type(e).__name__}: {str(e)}"

    # 4) Determine macro inst_status + inst_dates_3d
    inst_status, inst_dates_3d = _decide_inst_status(inst_df, symbols, trade_date)

    # 若 API 付費/不可用（常見 402），標記 UNAVAILABLE
    if inst_fetch_error and ("402" in inst_fetch_error or "Payment Required" in inst_fetch_error):
        inst_status = "UNAVAILABLE"
        inst_dates_3d = []

    # 5) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 6) Macro overview
    amount_str = _compute_market_amount_today(df, latest_date)

    # degraded_mode：PENDING 代表法人「可能可用但缺資料」；UNAVAILABLE 代表「不可用」
    degraded_mode = (inst_status == "PENDING")

    macro_overview = {
        "amount": amount_str,
        "inst_net": "A:0 | B:0",  # 你若之後接市場 A/B 再更新
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
    }

    # ✅ 建議2：寫入資料新鮮度鑑識（JSON + UI）
    macro_overview["data_freshness"] = build_data_freshness_block(csv_filename, df)

    # ✅ 今日市場狀態判斷
    macro_overview["market_comment"] = generate_market_comment_retail(macro_overview)

    macro_data = {
        "overview": macro_overview,
        "indices": [],
    }

    # 7) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    return df_top2, macro_data, json_text


# ======================================================
# Streamlit UI
# ======================================================

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
        df_top2, macro_data, json_text = run_pipeline(market=market, session=session)
    except Exception as e:
        st.error(str(e))
        return

    macro_overview = macro_data["overview"]
    market_comment = macro_overview.get("market_comment", "")
    freshness = macro_overview.get("data_freshness", {})

    st.subheader("資料新鮮度鑑識")
    st.json(freshness)
    hint = freshness.get("hint", "")
    if hint:
        # lag_days>=2 時會是警示語
        if freshness.get("lag_days", 0) is not None and freshness.get("lag_days", 0) >= 2:
            st.warning(hint)
        else:
            st.info(hint)

    st.subheader("今日市場狀態判斷（一般投資人版）")
    st.info(market_comment)

    st.subheader("Top List")
    st.dataframe(df_top2)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    # Save JSON
    trade_date = macro_overview.get("trade_date", datetime.now().strftime("%Y-%m-%d"))
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


# ======================================================
# CLI mode (for GitHub Actions)
# ======================================================

def run_cli(market: str, session: str):
    df_top2, macro_data, json_text = run_pipeline(market=market, session=session)

    macro_overview = macro_data["overview"]
    trade_date = macro_overview.get("trade_date", datetime.now().strftime("%Y-%m-%d"))

    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)

    # Console output (讓 Actions log 可讀)
    print("=== Macro ===")
    print(json.dumps(macro_overview, ensure_ascii=False, indent=2))
    print("\n=== Market Comment ===")
    print(macro_overview.get("market_comment", ""))
    print("\n=== Output JSON ===")
    print(outname)

    # Top list quick view
    try:
        cols = [c for c in ["Date", "Symbol", "Close", "Score", "Predator_Tag"] if c in df_top2.columns]
        print("\n=== Top List (preview) ===")
        print(df_top2[cols].head(10).to_string(index=False))
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", action="store_true", help="Run in CLI mode (for GitHub Actions)")
    parser.add_argument("--market", default="tw-share", help="Market id, e.g. tw-share / tw")
    parser.add_argument("--session", default=SESSION_EOD, choices=[SESSION_INTRADAY, SESSION_EOD], help="INTRADAY or EOD")
    args = parser.parse_args()

    if args.cli:
        run_cli(market=args.market, session=args.session)
    else:
        app()


if __name__ == "__main__":
    main()
