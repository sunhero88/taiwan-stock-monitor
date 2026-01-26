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

# ✅ 新增：全市場成交金額（上市+上櫃）與盤中量能正規化
from market_amount import fetch_amount_total, intraday_norm


# =========================
# 0) 航運股估值（手動覆蓋層）
# =========================
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


# =========================
# Utils
# =========================
def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _find_existing_path(candidates: list[str]) -> str | None:
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def _load_market_csv(market: str) -> pd.DataFrame:
    """
    你的 repo 目前可能出現：
      - data_tw-share.csv / data_tw.csv（放在根目錄）
      - data/data_tw-share.csv / data/data_tw.csv（放在 data/ 目錄）
    """
    fname = f"data_{market}.csv"
    candidates = [
        fname,
        os.path.join("data", fname),
        "data_tw-share.csv",
        os.path.join("data", "data_tw-share.csv"),
        "data_tw.csv",
        os.path.join("data", "data_tw.csv"),
    ]
    hit = _find_existing_path(candidates)
    if not hit:
        raise FileNotFoundError(f"找不到資料檔：{fname} / data_tw-share.csv / data_tw.csv（或 data/ 目錄下）")
    return pd.read_csv(hit)


def _apply_shipping_valuation_overrides(df_top: pd.DataFrame) -> pd.DataFrame:
    """
    航運估值 overlay：
    - 不污染原本 yfinance 的 Structure 欄位
    - 新增 Valuation_Override（dict）
    - 補 Name（中文名）
    """
    out = df_top.copy()

    def _inject(row: pd.Series) -> pd.Series:
        sym = str(row.get("Symbol", "")).strip()
        info = SHIPPING_VALUATION.get(sym)
        if not info:
            return row

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

    return out.apply(_inject, axis=1)


def _merge_institutional_into_df_top(df_top: pd.DataFrame, inst_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    """
    把 calc_inst_3d 的結果塞回 df_top 的 Institutional 欄位（dict）
    """
    df_out = df_top.copy()
    inst_map: dict[str, dict] = {}

    for _, r in df_out.iterrows():
        symbol = str(r.get("Symbol", ""))
        inst_calc = calc_inst_3d(inst_df, symbol=symbol, trade_date=trade_date)
        inst_map[symbol] = {
            "Inst_Visual": inst_calc.get("Inst_Status", "PENDING"),
            "Inst_Net_3d": float(inst_calc.get("Inst_Net_3d", 0.0)),
            "Inst_Streak3": int(inst_calc.get("Inst_Streak3", 0)),
            "Inst_Dir3": inst_calc.get("Inst_Dir3", "PENDING"),
            "Inst_Status": inst_calc.get("Inst_Status", "PENDING"),
        }

    df_out["Institutional"] = df_out["Symbol"].astype(str).map(inst_map)
    return df_out


def _decide_inst_status(inst_df: pd.DataFrame, symbols: list[str], trade_date: str) -> tuple[str, list[str]]:
    """
    macro.overview.inst_status + inst_dates_3d
    規則：只要有任何一檔能滿足「三日資料齊全」→ READY；否則 PENDING
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


def _load_avg20_amount_total(trade_date: str) -> int | None:
    """
    讀取你自己落地的市場成交金額歷史檔（建議你後續固定建立）：
      data/tw_market_turnover.csv
    欄位建議：
      date, amount_total
    這裡回傳近 20 日中位數（更抗極端值），若不足就回傳 None。
    """
    candidates = [
        os.path.join("data", "tw_market_turnover.csv"),
        "tw_market_turnover.csv",
    ]
    fp = _find_existing_path(candidates)
    if not fp:
        return None

    try:
        d = pd.read_csv(fp)
        if "date" not in d.columns or "amount_total" not in d.columns:
            return None

        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        d["amount_total"] = pd.to_numeric(d["amount_total"], errors="coerce")
        d = d.dropna(subset=["date", "amount_total"]).sort_values("date")

        td = pd.to_datetime(trade_date, errors="coerce")
        if pd.isna(td):
            return None

        d = d[d["date"] <= td].tail(20)
        if len(d) < 10:
            return None

        return int(d["amount_total"].median())
    except Exception:
        return None


def generate_market_comment_retail(macro_overview: dict) -> str:
    """
    依據 Macro Overview 自動生成「今日市場狀態判斷」（一般投資人可讀版）
    注意：你已規範 market_comment 僅供人類閱讀，Arbiter 必須忽略。
    """
    amount_total = macro_overview.get("amount_total")
    inst_status = macro_overview.get("inst_status", "PENDING")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))

    if kill_switch or v14_watch:
        return "今日市場不確定性偏高，系統已啟動風控保護（禁止進場），建議以資金保全為優先。"

    # 成交金額（以「上市+上櫃合計」為主）
    amount_num = None
    try:
        if isinstance(amount_total, str) and amount_total not in ("待更新", "", None):
            amount_num = float(amount_total.replace(",", ""))
        elif isinstance(amount_total, (int, float)):
            amount_num = float(amount_total)
    except Exception:
        amount_num = None

    # 量能文字（不再用固定 3000億門檻，而是偏向「描述 + 盤中正規化提示」）
    liquidity_text = "成交金額待更新，" if amount_num is None else "成交金額偏低，"
    if amount_num is not None:
        amount_eok = amount_num / 100_000_000  # 元→億
        liquidity_text = f"{liquidity_text}（成交金額約 {amount_eok:,.0f} 億）"

    # 法人狀態
    if inst_status in ("UNAVAILABLE", "PENDING"):
        inst_text = "法人資料尚不足以判讀方向，建議以觀察或小額試單為主，不宜貿然重倉。"
    elif inst_status == "READY":
        inst_text = "法人資料可用，但仍需以個股條件與風控規則為主。"
    else:
        inst_text = "法人資料狀態不完整，建議審慎應對。"

    # 降級說明
    strategy_text = "整體策略以保守為主。" if degraded_mode else "倉位可依個股訊號彈性調整。"

    # 若有盤中正規化（提供提示但不下結論）
    norm = macro_overview.get("amount_norm")
    norm_hint = ""
    if isinstance(norm, dict):
        cum_ratio = norm.get("amount_norm_cum_ratio")
        label = norm.get("amount_norm_label")
        if cum_ratio is not None and label in ("LOW", "NORMAL", "HIGH"):
            norm_hint = f"（盤中量能正規化：{label}，累積比率≈{cum_ratio}）"

    return liquidity_text + inst_text + strategy_text + norm_hint


# =========================
# App
# =========================
def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)
    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("按左側 Run 產生 Top 清單與 JSON。")
        return

    # 1) Load market data（個股池資料，用於選股矩陣/技術分數）
    df = _load_market_csv(market)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    # 2) Run analyzer（產生 Top 清單）
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 2.1) 航運估值 overlay
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

    # 6) ✅ 全市場成交金額（上市+上櫃合計）+ 盤中量能正規化
    amount_err = None
    amount_twse = amount_tpex = amount_total = None
    src_twse = src_tpex = None

    try:
        amt = fetch_amount_total()
        amount_twse = amt.amount_twse
        amount_tpex = amt.amount_tpex
        amount_total = amt.amount_total
        src_twse = amt.source_twse
        src_tpex = amt.source_tpex
    except Exception as e:
        amount_err = f"{type(e).__name__}: {e}"
        st.warning(f"全市場成交金額抓取失敗（amount_total 會顯示待更新）：{amount_err}")

    # 6.1) 讀取近20日基準（建議你後續固定落地 tw_market_turnover.csv）
    avg20_amount_total = _load_avg20_amount_total(trade_date)

    # 6.2) 記住上一筆 amount_total，供「保守型切片量」用（此處以『每次 Run』為一筆）
    prev_key = "amount_total_prev_int"
    amount_total_prev = st.session_state.get(prev_key)

    norm = None
    if isinstance(amount_total, int) and amount_total > 0 and isinstance(avg20_amount_total, int) and avg20_amount_total > 0:
        norm = intraday_norm(
            amount_total_now=amount_total,
            amount_total_prev=amount_total_prev,
            avg20_amount_total=avg20_amount_total,
        )

    if isinstance(amount_total, int):
        st.session_state[prev_key] = amount_total

    # 7) macro_overview（注意：amount_total 才是「市場成交金額口徑」）
    degraded_mode = (inst_status == "PENDING")  # UNAVAILABLE 不強制 degraded（交給 Arbiter 規則）

    macro_overview = {
        "amount_twse": f"{amount_twse:,}" if isinstance(amount_twse, int) else "待更新",
        "amount_tpex": f"{amount_tpex:,}" if isinstance(amount_tpex, int) else "待更新",
        "amount_total": f"{amount_total:,}" if isinstance(amount_total, int) else "待更新",
        "amount_sources": {
            "twse": src_twse,
            "tpex": src_tpex,
            "error": amount_err,
        },
        "avg20_amount_total_median": f"{avg20_amount_total:,}" if isinstance(avg20_amount_total, int) else None,
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
    }

    if norm:
        macro_overview["amount_norm"] = norm

    # 8) 產生市場一句話（僅供人類閱讀；Arbiter 必須忽略 market_comment）
    market_comment = generate_market_comment_retail(macro_overview)
    macro_overview["market_comment"] = market_comment

    macro_data = {
        "overview": macro_overview,
        "indices": [],
    }

    # 9) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # =========================
    # UI
    # =========================
    st.subheader("全市場成交金額（上市+上櫃合計）")
    col1, col2, col3 = st.columns(3)
    col1.metric("上市 TWSE 成交金額(元)", macro_overview["amount_twse"])
    col2.metric("上櫃 TPEx 成交金額(元)", macro_overview["amount_tpex"])
    col3.metric("合計 amount_total(元)", macro_overview["amount_total"])

    with st.expander("成交金額來源與除錯資訊"):
        st.write(macro_overview.get("amount_sources", {}))
        st.write({"avg20_amount_total_median": macro_overview.get("avg20_amount_total_median")})
        st.write({"amount_norm": macro_overview.get("amount_norm")})

    st.subheader("今日市場狀態判斷（一般投資人版；僅供閱讀）")
    st.info(market_comment)

    # 航運估值卡（只顯示命中者）
    hit = df_top2[df_top2["Symbol"].isin(list(SHIPPING_VALUATION.keys()))].copy()
    if not hit.empty:
        st.subheader("航運股估值快照（財報狗/玩股網）")
        cols = ["Symbol", "Name", "Valuation_Override"]
        show = hit[cols].copy()

        def _render(v: dict) -> str:
            if not isinstance(v, dict):
                return ""
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
