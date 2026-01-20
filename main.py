# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import datetime, date

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d


def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _load_market_csv(market: str) -> pd.DataFrame:
    # repo 目前是 data_tw-share.csv / data_tw.csv
    fname = f"data_{market}.csv"
    if not os.path.exists(fname):
        # fallback
        if os.path.exists("data_tw-share.csv"):
            fname = "data_tw-share.csv"
        elif os.path.exists("data_tw.csv"):
            fname = "data_tw.csv"
        else:
            raise FileNotFoundError(f"找不到資料檔：{fname} / data_tw-share.csv / data_tw.csv")
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


def _safe_int(x, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _calc_data_freshness(trade_date_str: str) -> tuple[int, str, str]:
    """
    以「資料最新交易日 trade_date」對照「今天（本機日期）」估算落後天數（粗略，未排除假日）。
    回傳：
      - lag_days: int
      - csv_date: YYYY-MM-DD
      - today_date: YYYY-MM-DD
    """
    csv_d = None
    try:
        csv_d = pd.to_datetime(trade_date_str).date()
    except Exception:
        csv_d = date.today()

    today_d = date.today()
    lag = (today_d - csv_d).days
    return _safe_int(lag, 0), csv_d.strftime("%Y-%m-%d"), today_d.strftime("%Y-%m-%d")


def generate_market_comment_retail(macro_overview: dict) -> str:
    """
    依據 Macro Overview 自動生成「今日市場狀態判斷」（一般投資人可讀版）
    設計原則：
    - 每一句可回溯欄位
    - 與 Arbiter 行為一致
    """
    amount = macro_overview.get("amount")
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))
    data_mode = str(macro_overview.get("data_mode", "EOD") or "EOD").upper()
    lag_days = _safe_int(macro_overview.get("lag_days", 0), 0)

    # ---------- 系統風險層 ----------
    if kill_switch or v14_watch:
        return "今日市場存在較高不確定風險，系統已啟動防護機制，建議避免進場操作，以資金保全為優先。"

    # ---------- 資料新鮮度 ----------
    if data_mode == "STALE":
        return f"資料更新落後（落後 {lag_days} 天），今日以觀察與持倉風控為主，避免依舊資料貿然進場。"

    # ---------- 流動性（以成交額粗略判斷） ----------
    liquidity_ok = False
    try:
        if amount not in (None, "", "待更新"):
            liquidity_ok = float(str(amount).replace(",", "")) > 300_000_000_000  # 3000 億
    except Exception:
        liquidity_ok = False

    liquidity_text = "市場成交量維持在正常水準，" if liquidity_ok else "市場成交量偏低，"

    # ---------- 法人資訊狀態 ----------
    if inst_status in ("UNAVAILABLE", "PENDING"):
        inst_text = "目前法人動向尚不明確，操作上建議以觀察或小額嘗試為主，不宜貿然重倉。"
    elif inst_status == "READY":
        inst_text = "法人動向已有明確方向，可搭配個股條件進行較積極的操作。"
    else:
        inst_text = "法人資訊狀態不完整，建議審慎應對。"

    # ---------- data_mode 策略提示 ----------
    if data_mode == "INTRADAY":
        mode_text = "目前屬盤中模式，策略以試單或等待確認為主。"
    else:
        mode_text = "目前屬盤後模式，可依個股條件彈性調整策略。"

    # ---------- 降級補充 ----------
    if degraded_mode and inst_status != "READY":
        mode_text += "（因資料條件不完整，整體策略偏保守）"

    return liquidity_text + inst_text + mode_text


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

    # ✅ 1-1) 資料新鮮度鑑識（粗略）
    lag_days, csv_date_str, today_str = _calc_data_freshness(trade_date)

    # 2) Run analyzer
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 3) Fetch institutional (FinMind)
    # 建議抓 45 天，確保跨假日仍可拿到 3 交易日
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    end_date = trade_date

    symbols = df_top["Symbol"].astype(str).tolist()

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
        st.warning(f"個股法人資料抓取失敗：{inst_fetch_error}")

    # 4) Determine macro inst_status + inst_dates_3d
    inst_status, inst_dates_3d = _decide_inst_status(inst_df, symbols, trade_date)

    # 若 API 付費/不可用（常見 402），直接標記 UNAVAILABLE，避免誤判成 PENDING
    if inst_fetch_error and ("402" in inst_fetch_error or "Payment Required" in inst_fetch_error):
        inst_status = "UNAVAILABLE"
        inst_dates_3d = []

    # 5) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 6) Macro overview (amount / degraded / data_mode)
    amount_str = _compute_market_amount_today(df, latest_date)

    # degraded_mode：PENDING 表示「法人缺失」；UNAVAILABLE 表示「法人不可用」
    degraded_mode = (inst_status == "PENDING")

    # ✅ data_mode：以 session 為基礎；若資料落後則強制 STALE
    data_mode = str(session).upper()  # "INTRADAY" or "EOD"
    if lag_days >= 1:
        data_mode = "STALE"

    macro_overview = {
        "amount": amount_str,
        "inst_net": "A:0 | B:0",  # 市場法人 A/B 若未取到，先用 0
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        # ✅ 新增：data_mode + freshness
        "data_mode": data_mode,
        "lag_days": lag_days,
        "csv_date": csv_date_str,
        "today_date": today_str,
    }

    # 7) 在產生 JSON 前：自動生成「今日市場狀態判斷」
    market_comment = generate_market_comment_retail(macro_overview)
    macro_overview["market_comment"] = market_comment  # 也寫入 JSON，方便 notifier/報告使用

    macro_data = {
        "overview": macro_overview,
        "indices": [],
    }

    # 8) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # UI
    st.subheader("資料新鮮度（快速鑑識）")
    st.write(
        {
            "csv_latest_trade_date": csv_date_str,
            "today": today_str,
            "lag_days": lag_days,
            "data_mode": data_mode,
        }
    )

    st.subheader("今日市場狀態判斷（一般投資人版）")
    st.info(market_comment)

    st.subheader("Top List")
    st.dataframe(df_top2)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    # optional: save
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{str(session).lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()

