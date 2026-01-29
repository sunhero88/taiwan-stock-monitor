# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d

# =========================
# Safe import: market_amount (Streamlit Cloud 不要因匯入失敗整個掛掉)
# =========================
MARKET_AMOUNT_OK = False
MARKET_AMOUNT_IMPORT_ERROR = None

try:
    from market_amount import fetch_amount_total, intraday_norm  # noqa: F401
    MARKET_AMOUNT_OK = True
except Exception as e:
    MARKET_AMOUNT_OK = False
    MARKET_AMOUNT_IMPORT_ERROR = f"{type(e).__name__}: {e}"

    # fallback dummy functions (避免 NameError)
    def fetch_amount_total():  # type: ignore
        raise RuntimeError(f"market_amount import failed: {MARKET_AMOUNT_IMPORT_ERROR}")

    def intraday_norm(*args, **kwargs):  # type: ignore
        return {
            "progress": None,
            "amount_norm_cum_ratio": None,
            "amount_norm_slice_ratio": None,
            "amount_norm_label": "UNKNOWN",
        }


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


def _compute_market_amount_from_csv(df: pd.DataFrame, latest_date) -> str:
    """
    舊方法：用你 CSV 裡的 Close*Volume 加總。
    注意：這不是全市場 amount_total，只是你 CSV 涵蓋的 ticker 子集合。
    模擬期可保留，但 UI 必須清楚標註。
    """
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


def _decide_inst_status(inst_df: pd.DataFrame, symbols: list[str], trade_date: str, inst_fetch_error: str | None) -> tuple[str, list[str], str | None]:
    """
    inst_status 規則（模擬期，免費版）：
    - 若 API 明確付費/不可用（402）→ UNAVAILABLE
    - 若資料不足以形成 3D → PENDING
    - 若至少一檔 READY → READY
    """
    if inst_fetch_error and ("402" in inst_fetch_error or "Payment Required" in inst_fetch_error):
        return "UNAVAILABLE", [], None

    ready_any = False
    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True
            break

    dates_3d = []
    data_date_finmind = None
    try:
        if not inst_df.empty and "date" in inst_df.columns:
            ds = sorted(inst_df["date"].astype(str).unique().tolist())
            dates_3d = ds[-3:]
            data_date_finmind = ds[-1] if ds else None
    except Exception:
        dates_3d = []
        data_date_finmind = None

    return ("READY" if ready_any else "PENDING"), dates_3d, data_date_finmind


def generate_market_comment_retail(macro_overview: dict) -> str:
    """
    人類可讀的市場摘要：
    - 若 degraded_mode=True → 明確寫出禁止 BUY/TRIAL
    - amount_total 若未知 → 明確寫待更新
    """
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))
    inst_status = macro_overview.get("inst_status", "PENDING")

    amount_total = macro_overview.get("amount_total", "待更新")
    amount_norm_label = macro_overview.get("amount_norm_label", "UNKNOWN")

    if kill_switch or v14_watch:
        return "系統風控已啟動：禁止進場（BUY/TRIAL）。"

    # 降級：你的「絕對防線」
    if degraded_mode:
        return f"成交金額{amount_total}；法人資料{inst_status}；量能判定={amount_norm_label}。裁決層已進入資料降級：禁止 BUY/TRIAL。"

    # 非降級：給一般描述
    return f"成交金額={amount_total}；法人狀態={inst_status}；量能判定={amount_norm_label}。"


def _apply_shipping_valuation_overrides(df_top: pd.DataFrame) -> pd.DataFrame:
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

    out = out.apply(_inject, axis=1)
    return out


def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    # 顯示 market_amount 匯入狀態（避免你一直盲猜）
    if not MARKET_AMOUNT_OK:
        st.warning(f"market_amount 模組匯入失敗（不致命，系統仍可運行）：{MARKET_AMOUNT_IMPORT_ERROR}")

    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)
    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("按左側 Run 產生 Top 清單與 JSON。")
        return

    # 1) Load market data (你的 CSV)
    df = _load_market_csv(market)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    # 2) Run analyzer
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 2.1 航運估值 overlay
    df_top = _apply_shipping_valuation_overrides(df_top)

    # 3) Fetch institutional (FinMind) - 免費期常遇到 402
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

    inst_status, inst_dates_3d, data_date_finmind = _decide_inst_status(inst_df, symbols, trade_date, inst_fetch_error)

    # 4) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # =========================
    # 5) Market amount: 上市 + 上櫃 = amount_total（優先）
    # =========================
    amount_twse = "待更新"
    amount_tpex = "待更新"
    amount_total = "待更新"
    amount_sources = {"twse": None, "tpex": None, "error": None}

    # 20D Median(代理)：模擬期先不做（你之後要做再補）
    avg20_amount_total_median = None

    if MARKET_AMOUNT_OK:
        try:
            ma = fetch_amount_total()
            amount_twse = f"{ma.amount_twse:,}"
            amount_tpex = f"{ma.amount_tpex:,}"
            amount_total = f"{ma.amount_total:,}"
            amount_sources = {"twse": ma.source_twse, "tpex": ma.source_tpex, "error": None}
        except Exception as e:
            amount_sources = {"twse": None, "tpex": None, "error": f"{type(e).__name__}: {e}"}

    # INTRADAY 量能正規化（你的定義：保守看 slice，穩健看 cum，試投忽略）
    norm = intraday_norm(
        amount_total_now=int(str(amount_total).replace(",", "")) if str(amount_total).isdigit() else 0,
        amount_total_prev=None,
        avg20_amount_total=avg20_amount_total_median,
    )
    progress = norm.get("progress")
    amount_norm_cum_ratio = norm.get("amount_norm_cum_ratio")
    amount_norm_slice_ratio = norm.get("amount_norm_slice_ratio")
    amount_norm_label = norm.get("amount_norm_label", "UNKNOWN")

    # =========================
    # 6) Degraded mode（裁決層）
    # 規則：任一核心資料缺失 → degraded_mode=True → 禁止 BUY/TRIAL
    # 核心資料：amount_total（上市+上櫃）、inst_status 可用性
    # =========================
    amount_bad = (amount_total in (None, "", "待更新")) or (amount_norm_label == "UNKNOWN")
    inst_bad = (inst_status in ("UNAVAILABLE",))  # PENDING 不一定要降級，可依你 Arbiter 規則再細分

    degraded_mode = bool(amount_bad or inst_bad)

    macro_overview = {
        # amount 分拆
        "amount_twse": amount_twse,
        "amount_tpex": amount_tpex,
        "amount_total": amount_total,
        "amount_sources": amount_sources,
        "avg20_amount_total_median": avg20_amount_total_median,
        # norm
        "progress": progress,
        "amount_norm_cum_ratio": amount_norm_cum_ratio,
        "amount_norm_slice_ratio": amount_norm_slice_ratio,
        "amount_norm_label": amount_norm_label,
        # inst
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": data_date_finmind,
        # system flags
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
        # backward compatible
        "amount": amount_total,
    }

    market_comment = generate_market_comment_retail(macro_overview)
    macro_overview["market_comment"] = market_comment

    macro_data = {"overview": macro_overview, "indices": []}

    # 7) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # =========================
    # UI
    # =========================
    st.subheader("今日市場狀態判斷（V15.7 裁決）")
    st.info(market_comment)

    st.subheader("市場成交金額（上市 + 上櫃 = amount_total）")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", amount_twse)
    c2.metric("TPEx 上櫃", amount_tpex)
    c3.metric("Total 合計", amount_total)
    c4.metric("20D Median(代理)", str(avg20_amount_total_median))

    st.caption(f"來源/錯誤：{json.dumps(amount_sources, ensure_ascii=False)}")

    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")
    st.code(
        json.dumps(
            {
                "progress": progress,
                "cum_ratio(穩健型用)": amount_norm_cum_ratio,
                "slice_ratio(保守型用)": amount_norm_slice_ratio,
                "label": amount_norm_label,
            },
            ensure_ascii=False,
            indent=2,
        ),
        language="json",
    )

    # 航運估值卡
    hit = df_top2[df_top2["Symbol"].isin(list(SHIPPING_VALUATION.keys()))].copy()
    if not hit.empty:
        st.subheader("航運股估值快照（財報狗/玩股網）")

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

        show = hit[["Symbol", "Name", "Valuation_Override"]].copy()
        show["估值摘要"] = show["Valuation_Override"].apply(_render)
        st.dataframe(show[["Symbol", "Name", "估值摘要"]], use_container_width=True)

    st.subheader("Top List")
    st.dataframe(df_top2, use_container_width=True)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
