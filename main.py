# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d


# --------------------------
# Time helpers (Taiwan local)
# --------------------------
def _now_tw() -> datetime:
    # 你的環境多半是本機/雲端；這裡不做 pytz 依賴，直接用系統時間
    # 若你部署在 UTC 主機且要嚴謹台北時區，建議改用 zoneinfo（py>=3.9）
    return datetime.now()


def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return _now_tw().strftime("%Y-%m-%d")


def _file_mtime_str(fname: str) -> str:
    try:
        ts = os.path.getmtime(fname)
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "Unknown"


def _expected_latest_trade_day_taiwan(now_dt: datetime) -> str:
    """
    目的：只做「提示用」的推估，不保證正確（因為台股休市日曆未納入）。
    直覺規則：
    - 週六/週日：預期最新交易日 = 上週五
    - 週一：
        - 若現在已經過 16:00（你盤後更新流程通常會在 15:00~18:00），預期最新 = 今天(週一)
        - 否則預期最新 = 上週五
    - 週二~週五：
        - 若現在已過 16:00：預期最新 = 今天
        - 否則預期最新 = 昨天
    """
    wd = now_dt.weekday()  # Mon=0 ... Sun=6
    hhmm = now_dt.hour * 60 + now_dt.minute
    after_close = hhmm >= (16 * 60)  # 16:00 當成你「理應已更新昨日」的時間門檻

    def prev_weekday(d: datetime) -> datetime:
        # 往前找上一個週一~週五
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    today = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    # weekend
    if wd >= 5:
        return prev_weekday(today).strftime("%Y-%m-%d")

    # Mon
    if wd == 0:
        if after_close:
            return today.strftime("%Y-%m-%d")
        return prev_weekday(today - timedelta(days=1)).strftime("%Y-%m-%d")

    # Tue-Fri
    if after_close:
        return today.strftime("%Y-%m-%d")
    return (today - timedelta(days=1)).strftime("%Y-%m-%d")


def _load_market_csv(market: str) -> tuple[pd.DataFrame, str]:
    # 你的 repo 目前是 data_tw-share.csv / data_tw.csv
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
    return df, fname


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


def generate_market_comment_retail(macro_overview: dict) -> str:
    """
    依據 Macro Overview 自動生成「今日市場狀態判斷」（一般投資人可讀版）
    設計原則：
    - 每一句可回溯至實際欄位
    - 與 Arbiter 行為一致
    """
    amount = macro_overview.get("amount")
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    # 系統風險層
    if kill_switch or v14_watch:
        return "今日市場風險旗標已觸發（KillSwitch/V14 Watch），系統採防守模式：避免進場，以資金保全為優先。"

    # 流動性（用成交額粗略判斷，可自行調整門檻）
    liquidity_ok = False
    try:
        if amount not in (None, "", "待更新"):
            liquidity_ok = float(str(amount).replace(",", "")) > 300_000_000_000  # 3000億
    except Exception:
        liquidity_ok = False

    liquidity_text = "市場成交量維持在正常水準，" if liquidity_ok else "市場成交量偏低，"

    # 法人資訊狀態
    if inst_status in ("UNAVAILABLE", "PENDING"):
        inst_text = "目前法人動向尚不明確，建議以觀察或小額嘗試為主，不宜貿然重倉。"
    elif inst_status == "READY":
        inst_text = "法人動向已有可用資料支撐，可搭配個股條件採更積極的進出場策略。"
    else:
        inst_text = "法人資訊狀態不完整，建議審慎應對。"

    # 降級說明
    strategy_text = "整體策略以保守為主。" if (degraded_mode and inst_status != "READY") else "可依個股條件彈性調整策略。"

    return liquidity_text + inst_text + strategy_text


def _render_data_freshness_panel(df: pd.DataFrame, fname: str, latest_date: pd.Timestamp):
    """
    在 UI 顯示：你現在「實際讀到」的檔案與日期，以快速定位：
    - 是 GitHub Actions 沒更新？
    - 還是部署環境沒拉新 commit？
    """
    now_dt = _now_tw()
    expected = _expected_latest_trade_day_taiwan(now_dt)

    latest_str = _fmt_date(latest_date)
    mtime_str = _file_mtime_str(fname)

    st.subheader("資料新鮮度鑑識")
    st.write(
        {
            "讀取檔名": fname,
            "檔案最後修改時間": mtime_str,
            "CSV 最新交易日（Date max）": latest_str,
            "推估應有最新交易日（提示用）": expected,
        }
    )

    # 若落後 >= 2 天（以一般情境判斷），直接警告
    try:
        latest_dt = pd.to_datetime(latest_str)
        expected_dt = pd.to_datetime(expected)
        lag = (expected_dt - latest_dt).days
        if lag >= 2:
            st.warning(
                f"資料可能未更新：CSV 最新交易日為 {latest_str}，相對推估應有 {expected}，落後 {lag} 天。"
                "請優先檢查 GitHub Actions 是否成功更新並 commit CSV，或部署環境是否已拉到最新 commit。"
            )
    except Exception:
        st.info("資料新鮮度判斷失敗（日期解析異常），請以 CSV 最新交易日為準。")


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
    df, fname = _load_market_csv(market)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    # ✅ 先把「資料新鮮度」攤開給你看（避免 1/13 這種誤判）
    _render_data_freshness_panel(df, fname, latest_date)

    # 2) Run analyzer
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 3) Fetch institutional (FinMind)
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

    # 若 API 付費/不可用（常見 402），直接標記 UNAVAILABLE
    if inst_fetch_error and ("402" in inst_fetch_error or "Payment Required" in inst_fetch_error):
        inst_status = "UNAVAILABLE"
        inst_dates_3d = []

    # 5) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 6) Macro overview (amount / degraded)
    amount_str = _compute_market_amount_today(df, latest_date)

    # degraded_mode：PENDING 視為資料未齊全；UNAVAILABLE 表示法人不可用（給 Arbiter NA 規則處理）
    degraded_mode = (inst_status == "PENDING")

    macro_overview = {
        "amount": amount_str,
        "inst_net": "A:0 | B:0",  # 法人市場 A/B 未取到先用 0
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
    }

    # 7) 在產生 JSON 前：自動生成「今日市場狀態判斷」
    market_comment = generate_market_comment_retail(macro_overview)
    macro_overview["market_comment"] = market_comment

    macro_data = {
        "overview": macro_overview,
        "indices": [],
    }

    # 8) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # UI
    st.subheader("今日市場狀態判斷（一般投資人版）")
    st.info(market_comment)

    st.subheader("Top List")
    st.dataframe(df_top2)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    # optional: save
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
