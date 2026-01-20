# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime, date

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d

# ✅ 新增：載入 Arbiter
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


def _calc_data_mode(trade_date: str, session: str) -> tuple[str, int]:
    """
    用「資料日期 vs 今天」判定 data_mode：
      - 若 trade_date < 今天：STALE（落後天數 lag_days）
      - 否則：
          session == INTRADAY -> INTRADAY
          session == EOD      -> EOD
    """
    try:
        td = pd.to_datetime(trade_date).date()
        today = date.today()
        lag_days = int((today - td).days)
        if lag_days >= 1:
            return "STALE", lag_days
    except Exception:
        # 無法判定就當作 EOD（保守做法也可改 STALE）
        return "EOD", 0

    return ("INTRADAY" if session == SESSION_INTRADAY else "EOD"), 0


def generate_market_comment_retail(macro_overview: dict) -> str:
    """
    一般投資人可讀版：「今日市場狀態判斷」一句話（可回溯到欄位）
    """
    amount = macro_overview.get("amount")
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))
    data_mode = str(macro_overview.get("data_mode", "EOD") or "EOD")
    lag_days = int(macro_overview.get("lag_days", 0) or 0)

    if kill_switch or v14_watch:
        return "今日市場風險警示已觸發（系統防護中），建議以資金保全為主，避免新增部位。"

    # 流動性粗判（成交額）
    liquidity_ok = False
    try:
        if amount not in (None, "", "待更新"):
            liquidity_ok = float(str(amount).replace(",", "")) > 300_000_000_000  # 3,000 億
    except Exception:
        liquidity_ok = False

    liquidity_text = "成交量維持在正常水準；" if liquidity_ok else "成交量偏低；"

    # data_mode
    if data_mode == "STALE":
        mode_text = f"資料落後 {lag_days} 天（STALE），建議僅做持倉風控，避免用舊資料進場；"
    elif data_mode == "INTRADAY":
        mode_text = "目前為盤中資料（INTRADAY），建議以小額試單或觀察為主；"
    else:
        mode_text = "目前為盤後資料（EOD），可依條件進行策略執行；"

    # 法人
    if inst_status in ("UNAVAILABLE", "PENDING"):
        inst_text = "法人動向未能確認，策略以保守為主。"
    elif inst_status == "READY":
        inst_text = "法人動向可用，可搭配個股條件提高進場效率。"
    else:
        inst_text = "法人資訊狀態不完整，建議審慎。"

    # 降級補述
    if degraded_mode and inst_status != "READY":
        inst_text += "（degraded_mode=1）"

    return liquidity_text + mode_text + inst_text


def _apply_arbiter_final_decision(payload: dict) -> dict:
    """
    對 payload["stocks"] 逐一寫入 FinalDecision：
      FinalDecision = {
        "Conservative": arbitrate(stock, macro_overview, "Conservative"),
        "Aggressive":   arbitrate(stock, macro_overview, "Aggressive")
      }
    """
    if not isinstance(payload, dict):
        return payload
    if "stocks" not in payload or "macro" not in payload:
        return payload
    macro_overview = (payload.get("macro") or {}).get("overview") or {}

    for s in payload.get("stocks", []) or []:
        try:
            s["FinalDecision"] = {
                "Conservative": arbitrate(s, macro_overview, account="Conservative"),
                "Aggressive": arbitrate(s, macro_overview, account="Aggressive"),
            }
        except Exception as e:
            # Arbiter 出錯時：不要讓整包 crash，保留錯誤資訊以利定位
            s["FinalDecision"] = {
                "Conservative": {
                    "Decision": "WATCH",
                    "action_size_pct": 0,
                    "exit_reason_code": "ARBITER_ERROR",
                    "degraded_note": "資料降級：是（Arbiter 執行失敗）",
                    "reason_technical": f"{type(e).__name__}: {str(e)}",
                    "reason_structure": "ARBITER_ERROR",
                    "reason_inst": "ARBITER_ERROR",
                },
                "Aggressive": {
                    "Decision": "WATCH",
                    "action_size_pct": 0,
                    "exit_reason_code": "ARBITER_ERROR",
                    "degraded_note": "資料降級：是（Arbiter 執行失敗）",
                    "reason_technical": f"{type(e).__name__}: {str(e)}",
                    "reason_structure": "ARBITER_ERROR",
                    "reason_inst": "ARBITER_ERROR",
                },
            }

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

    # 402 Payment Required => UNAVAILABLE
    if inst_fetch_error and ("402" in inst_fetch_error or "Payment Required" in inst_fetch_error):
        inst_status = "UNAVAILABLE"
        inst_dates_3d = []

    # 5) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 6) Macro overview
    amount_str = _compute_market_amount_today(df, latest_date)

    # degraded_mode：PENDING 才視為降級；UNAVAILABLE 交由 Arbiter 的 data_mode/inst_missing 去處理
    degraded_mode = (inst_status == "PENDING")

    # ✅ data_mode / lag_days（關鍵）
    data_mode, lag_days = _calc_data_mode(trade_date, session=session)

    macro_overview = {
        "amount": amount_str,
        "inst_net": "A:0 | B:0",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        # ✅ 新增：供 Arbiter 使用
        "data_mode": data_mode,
        "lag_days": lag_days,
    }

    # ✅ 在產生 JSON 前：生成市場文字判斷
    market_comment = generate_market_comment_retail(macro_overview)
    macro_overview["market_comment"] = market_comment

    macro_data = {"overview": macro_overview, "indices": []}

    # 7) Generate base JSON (Analyzer output)
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # 8) ✅ 寫回 FinalDecision（Arbiter output）
    try:
        payload = json.loads(json_text)
        if isinstance(payload, dict) and "error" not in payload:
            payload = _apply_arbiter_final_decision(payload)
            json_text = json.dumps(payload, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"FinalDecision 寫回失敗：{type(e).__name__}: {str(e)}")

    # UI
    st.subheader("今日市場狀態判斷（一般投資人版）")
    st.info(market_comment)

    st.subheader("Top List（Analyzer）")
    st.dataframe(df_top2)

    st.subheader("AI JSON（含 FinalDecision）")
    st.code(json_text, language="json")

    # optional: save
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
