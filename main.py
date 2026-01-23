# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d


# =========================
# 0) 航運股估值（手動覆蓋層）
# =========================
# 你提供的資料（財報狗 + 玩股網）→ 先以「可回溯」方式寫入 override 區塊
# - opm_q: 最新單季 OPM（%）
# - eps_ttm: 近四季 EPS (TTM)
# - price_ref: 你引用的價格（例：1/22）
# - pe_calc: 以 price_ref / eps_ttm 計算的 PE
# - label: 你給的評語標籤（Deep Value / etc.）
# - source: 記錄資料來源與季度（便於索引）
SHIPPING_VALUATION = {
    "2603.TW": {
        "name_zh": "長榮",
        "sector": "航運",
        "opm_q": 22.73,
        "opm_q_period": "2025 Q3",
        "eps_ttm": 41.92,
        "price_ref": 192.54,
        "price_ref_date": "2026-01-22",
        "pe_calc": 4.59,
        "label": "🟢 極度低估 (Deep Value)",
        "source": "財報狗/玩股網（最新季報資料庫）",
    },
    "2609.TW": {
        "name_zh": "陽明",
        "sector": "航運",
        "opm_q": 10.49,
        "opm_q_period": "2025 Q3",
        "eps_ttm": 7.83,
        "price_ref": 55.57,
        "price_ref_date": "2026-01-22",
        "pe_calc": 7.09,
        "label": "🟡",
        "source": "財報狗/玩股網（最新季報資料庫）",
    },
}


def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _load_market_csv(market: str) -> pd.DataFrame:
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

    # 系統級風控
    if kill_switch or v14_watch:
        return "今日市場不確定性偏高，系統已啟動風控保護（禁止進場），建議以資金保全為優先。"

    # 流動性（成交金額門檻可自行調整）
    liquidity_ok = False
    amount_num = None
    try:
        if amount not in (None, "", "待更新"):
            amount_num = float(str(amount).replace(",", ""))
            liquidity_ok = amount_num > 300_000_000_000  # 3000 億
    except Exception:
        liquidity_ok = False

    liquidity_text = "成交金額維持在正常水準，" if liquidity_ok else "成交金額偏低，"

    # 法人狀態
    if inst_status in ("UNAVAILABLE", "PENDING"):
        inst_text = "法人資料尚不足以判讀方向，建議以觀察或小額試單為主，不宜貿然重倉。"
    elif inst_status == "READY":
        inst_text = "法人資料可用，可搭配個股條件做較積極的倉位調整。"
    else:
        inst_text = "法人資料狀態不完整，建議審慎應對。"

    # 降級說明
    strategy_text = "整體策略以保守為主。" if (degraded_mode and inst_status != "READY") else "倉位可依個股訊號彈性調整。"

    # 加上數字（更像可追溯的判斷）
    if amount_num is not None:
        amount_eok = amount_num / 100_000_000  # 換算億
        liquidity_text = f"{liquidity_text}（成交金額約 {amount_eok:,.0f} 億）"

    return liquidity_text + inst_text + strategy_text


# =========================
# 1) 航運估值注入（寫入 df_top2 的 Structure/Valuation 區塊）
# =========================
def _apply_shipping_valuation_overrides(df_top: pd.DataFrame) -> pd.DataFrame:
    """
    將 SHIPPING_VALUATION 以「Overlay」形式注入到 df_top 的 Structure 旁邊
    - 不覆蓋原本 yfinance 算出的 OPM/PE/Rev_Growth（避免污染）
    - 新增欄位 Valuation_Override（dict），並補 Name（中文名）
    """
    out = df_top.copy()

    def _inject(row: pd.Series) -> pd.Series:
        sym = str(row.get("Symbol", "")).strip()
        info = SHIPPING_VALUATION.get(sym)
        if not info:
            return row

        # 補名稱（以你要求：代碼 + 名稱）
        # Name 欄位若已存在（例如你後面已經做 Name = yfinance longName），此處不強制覆蓋
        if not row.get("Name"):
            row["Name"] = info.get("name_zh", sym)

        row["Valuation_Override"] = {
            "sector": info.get("sector"),
            "opm_q": float(info.get("opm_q", 0.0)),
            "opm_q_period": info.get("opm_q_period"),
            "eps_ttm": float(info.get("eps_ttm", 0.0)),
            "price_ref": float(info.get("price_ref", 0.0)),
            "price_ref_date": info.get("price_ref_date"),
            "pe_calc": float(info.get("pe_calc", 0.0)),
            "label": info.get("label"),
            "source": info.get("source"),
        }
        return row

    out = out.apply(_inject, axis=1)
    return out


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

    # 2.1) ✅ 航運估值 overlay（先進 df_top，讓後面 JSON 也吃到）
    df_top = _apply_shipping_valuation_overrides(df_top)

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

    # 6) Macro overview
    amount_str = _compute_market_amount_today(df, latest_date)

    degraded_mode = (inst_status == "PENDING")  # UNAVAILABLE 不強制 degraded（交給 Arbiter NA 規則）
    macro_overview = {
        "amount": amount_str,
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        # 你之前已要求接 data_mode，這裡先給穩定值（若你已有 freshness 模組可改成動態）
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
    }

    # 7) 產生市場一句話
    market_comment = generate_market_comment_retail(macro_overview)
    macro_overview["market_comment"] = market_comment

    macro_data = {
        "overview": macro_overview,
        "indices": [],
    }

    # 8) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # =========================
    # UI
    # =========================
    st.subheader("今日市場狀態判斷（一般投資人版）")
    st.info(market_comment)

    # ✅ 航運估值卡（只顯示命中者）
    hit = df_top2[df_top2["Symbol"].isin(list(SHIPPING_VALUATION.keys()))].copy()
    if not hit.empty:
        st.subheader("航運股估值快照（財報狗/玩股網）")
        cols = ["Symbol", "Name", "Valuation_Override"]
        show = hit[cols].copy()

        def _render(v: dict) -> str:
            if not isinstance(v, dict):
                return ""
            # 用關鍵數據呈現（OPM / EPS / 價格 / PE）
            return (
                f"OPM({v.get('opm_q_period')}): {v.get('opm_q')}% | "
                f"EPS(TTM): {v.get('eps_ttm')} | "
                f"Price({v.get('price_ref_date')}): {v.get('price_ref')} | "
                f"PE≈{v.get('pe_calc')} | {v.get('label')} | "
                f"來源: {v.get('source')}"
            )

        show["估值摘要"] = show["Valuation_Override"].apply(_render)
        st.dataframe(show[["Symbol", "Name", "估值摘要"]], use_container_width=True)

    st.subheader("Top List")
    st.dataframe(df_top2, use_container_width=True)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    # optional: save
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
