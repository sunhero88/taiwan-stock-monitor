# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from institutional_utils import calc_inst_3d
from finmind_institutional import fetch_finmind_institutional

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
# Paths / IO helpers
# =========================
DATA_DIR = "data"
CACHE_DIR = "configs"  # 你 repo 已有 configs/
AMOUNT_CACHE_PATH = os.path.join(CACHE_DIR, "amount_cache.json")
AMOUNT_HIST_PATH = os.path.join(DATA_DIR, "amount_total_history.csv")  # 自建（用於 20D median）


def _ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)


def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _load_market_csv(market: str) -> pd.DataFrame:
    """
    Streamlit Cloud 上常見問題：檔案實際放在 data/ 但程式只找根目錄 → FileNotFound。
    這裡做雙路徑 fallback：
    - data/data_{market}.csv
    - data/data_tw-share.csv / data/data_tw.csv
    - 根目錄 data_{market}.csv ...
    """
    fname = f"data_{market}.csv"
    candidates = [
        os.path.join(DATA_DIR, fname),
        os.path.join(DATA_DIR, "data_tw-share.csv"),
        os.path.join(DATA_DIR, "data_tw.csv"),
        fname,
        "data_tw-share.csv",
        "data_tw.csv",
    ]
    for p in candidates:
        if os.path.exists(p):
            df = pd.read_csv(p)
            return df
    raise FileNotFoundError(f"找不到資料檔：{candidates}")


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

    return out.apply(_inject, axis=1)


# =========================
# Amount cache / history
# =========================
def _read_amount_cache() -> dict:
    if not os.path.exists(AMOUNT_CACHE_PATH):
        return {}
    try:
        with open(AMOUNT_CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_amount_cache(payload: dict) -> None:
    with open(AMOUNT_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _load_amount_history() -> pd.DataFrame:
    if not os.path.exists(AMOUNT_HIST_PATH):
        return pd.DataFrame(columns=["trade_date", "amount_total"])
    try:
        d = pd.read_csv(AMOUNT_HIST_PATH)
        d["trade_date"] = d["trade_date"].astype(str)
        d["amount_total"] = pd.to_numeric(d["amount_total"], errors="coerce")
        d = d.dropna(subset=["amount_total"])
        return d
    except Exception:
        return pd.DataFrame(columns=["trade_date", "amount_total"])


def _append_amount_history(trade_date: str, amount_total: int) -> None:
    hist = _load_amount_history()
    # 去重（同日覆蓋）
    hist = hist[hist["trade_date"] != trade_date].copy()
    hist = pd.concat(
        [hist, pd.DataFrame([{"trade_date": trade_date, "amount_total": int(amount_total)}])],
        ignore_index=True,
    )
    # 保留最多 400 筆即可（約一年半交易日）
    hist = hist.tail(400)
    hist.to_csv(AMOUNT_HIST_PATH, index=False, encoding="utf-8")


def _median_20d_amount_total() -> int | None:
    hist = _load_amount_history()
    if hist.empty:
        return None
    tail = hist.tail(20)
    if tail["amount_total"].notna().sum() < 10:
        return None
    return int(tail["amount_total"].median())


# =========================
# Institutional merge/status
# =========================
def _merge_institutional_into_df_top(df_top: pd.DataFrame, inst_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    df_out = df_top.copy()
    inst_map = {}

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

    df_out["Institutional"] = df_out["Symbol"].map(inst_map)
    return df_out


def _decide_inst_status(inst_df: pd.DataFrame, symbols: list[str], trade_date: str, inst_fetch_error: str | None) -> tuple[str, list[str], str | None]:
    """
    V15.7 裁決邏輯：
    - FinMind 402 / Payment Required → UNAVAILABLE（不可用）
    - 否則：有足夠 3 日資料者比例 >= 60% → READY
    - 否則 → PENDING
    """
    if inst_fetch_error and ("402" in inst_fetch_error or "Payment Required" in inst_fetch_error):
        return "UNAVAILABLE", [], None

    if inst_df is None or inst_df.empty:
        return "PENDING", [], None

    # 取資料日期（最近 3 日）
    try:
        dates_uniq = sorted(inst_df["date"].astype(str).unique().tolist())
        dates_3d = dates_uniq[-3:] if len(dates_uniq) >= 3 else dates_uniq
        data_date_finmind = dates_uniq[-1] if dates_uniq else None
    except Exception:
        dates_3d = []
        data_date_finmind = None

    ready_cnt = 0
    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_cnt += 1

    coverage = ready_cnt / max(1, len(symbols))
    if coverage >= 0.60 and len(dates_3d) >= 3:
        return "READY", dates_3d, data_date_finmind
    return "PENDING", dates_3d, data_date_finmind


# =========================
# Market comment (human readable)
# =========================
def _to_eok(n: int | None) -> str:
    if not n or n <= 0:
        return "待更新"
    return f"{n/100_000_000:,.0f} 億"


def generate_market_comment_v15_7(macro_overview: dict) -> str:
    """
    V15.7：文字必須可回溯欄位
    """
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))
    degraded_mode = bool(macro_overview.get("degraded_mode", False))

    amt_total = macro_overview.get("amount_total_int", None)
    amt_text = _to_eok(amt_total)

    inst_status = macro_overview.get("inst_status", "PENDING")

    if kill_switch or v14_watch:
        return "市場不確定性偏高；裁決層啟動風控：禁止進場（kill_switch/v14_watch）。"

    # 量能文字
    norm_label = macro_overview.get("amount_norm_label", "UNKNOWN")
    if amt_total is None:
        amount_part = "成交金額待更新；"
    else:
        amount_part = f"成交金額約 {amt_text}（量能判定：{norm_label}）；"

    # 法人文字
    if inst_status == "READY":
        inst_part = "法人資料可用。"
    elif inst_status == "UNAVAILABLE":
        inst_part = "法人資料不可用。"
    else:
        inst_part = "法人資料不足（PENDING）。"

    # 裁決降級
    if degraded_mode:
        return f"{amount_part}{inst_part}裁決層已進入資料降級：禁止 BUY/TRIAL。"
    return f"{amount_part}{inst_part}可依個股訊號執行倉位調整。"


# =========================
# Streamlit app
# =========================
def app():
    _ensure_dirs()

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
    df["Date"] = pd.to_datetime(df.get("Date"), errors="coerce")
    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    # 2) Run analyzer
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 2.1) 航運估值 overlay
    df_top = _apply_shipping_valuation_overrides(df_top)

    symbols = df_top["Symbol"].astype(str).tolist()

    # 3) Market Amount (TWSE + TPEx)
    amount_twse = None
    amount_tpex = None
    amount_total = None
    amount_sources = {"twse": None, "tpex": None, "error": None}

    # 取前一次 amount_total 當 slice 參考（5分鐘一筆的概念：你可改成更細緻）
    cache = _read_amount_cache()
    amount_total_prev = cache.get("amount_total_int")

    try:
        ma = fetch_amount_total()  # MarketAmount dataclass
        amount_twse = int(ma.amount_twse)
        amount_tpex = int(ma.amount_tpex)
        amount_total = int(ma.amount_total)
        amount_sources["twse"] = ma.source_twse
        amount_sources["tpex"] = ma.source_tpex

        # 更新 cache（供 slice 使用）
        _write_amount_cache(
            {
                "ts": datetime.now().isoformat(),
                "trade_date": trade_date,
                "amount_twse_int": amount_twse,
                "amount_tpex_int": amount_tpex,
                "amount_total_int": amount_total,
            }
        )

        # 若是 EOD 或盤後（你可自行保守判斷），把 amount_total 寫入 history，提供 20D median
        if session == SESSION_EOD:
            _append_amount_history(trade_date, amount_total)

    except Exception as e:
        amount_sources["error"] = f"{type(e).__name__}: {str(e)}"

    avg20_median = _median_20d_amount_total()

    # intraday normalization（只在盤中或你想要時）
    norm = intraday_norm(
        amount_total_now=amount_total or 0,
        amount_total_prev=amount_total_prev if isinstance(amount_total_prev, int) else None,
        avg20_amount_total=avg20_median,
    ) if session == SESSION_INTRADAY else {
        "progress": None,
        "amount_norm_cum_ratio": None,
        "amount_norm_slice_ratio": None,
        "amount_norm_label": "UNKNOWN",
    }

    # 4) Fetch institutional (FinMind)
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
        st.warning(f"個股法人資料抓取失敗：{inst_fetch_error}")

    inst_status, inst_dates_3d, data_date_finmind = _decide_inst_status(
        inst_df=inst_df,
        symbols=symbols,
        trade_date=trade_date,
        inst_fetch_error=inst_fetch_error,
    )

    # 5) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 6) degraded_mode（V15.7 裁決版）
    # 你指定：免費模擬 → 只要「法人不可用」或「成交金額缺失」就降級，禁止 BUY/TRIAL（保守、安全、可回測）
    amount_ok = isinstance(amount_total, int) and amount_total > 0
    inst_ok = (inst_status == "READY")
    degraded_mode = (not amount_ok) or (inst_status in ("UNAVAILABLE", "PENDING"))

    macro_overview = {
        # ✅ 上市/上櫃/合計（字串與 int 都給，方便 UI/JSON/裁決）
        "amount_twse": "待更新" if amount_twse is None else f"{amount_twse:,}",
        "amount_tpex": "待更新" if amount_tpex is None else f"{amount_tpex:,}",
        "amount_total": "待更新" if amount_total is None else f"{amount_total:,}",
        "amount_twse_int": amount_twse,
        "amount_tpex_int": amount_tpex,
        "amount_total_int": amount_total,
        "amount_sources": amount_sources,

        # 20D 中位數（關鍵數據點）
        "avg20_amount_total_median": None if avg20_median is None else int(avg20_median),

        # INTRADAY 正規化（你要的四個欄位）
        "progress": norm.get("progress"),
        "amount_norm_cum_ratio": norm.get("amount_norm_cum_ratio"),
        "amount_norm_slice_ratio": norm.get("amount_norm_slice_ratio"),
        "amount_norm_label": norm.get("amount_norm_label", "UNKNOWN"),

        # 法人/資料日
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": data_date_finmind,

        # 系統級風控
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
    }

    macro_overview["market_comment"] = generate_market_comment_v15_7(macro_overview)

    macro_data = {"overview": macro_overview, "indices": []}

    # 7) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # =========================
    # UI
    # =========================
    st.subheader("今日市場狀態判斷（V15.7 裁決）")
    st.info(macro_overview["market_comment"])

    st.subheader("市場成交金額（上市 + 上櫃 = amount_total）")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("TWSE 上市", macro_overview["amount_twse"])
    col2.metric("TPEx 上櫃", macro_overview["amount_tpex"])
    col3.metric("Total 合計", macro_overview["amount_total"])
    col4.metric("20D Median(合計)", "None" if macro_overview["avg20_amount_total_median"] is None else f"{macro_overview['avg20_amount_total_median']:,}")

    st.caption(f"來源/錯誤：{json.dumps(amount_sources, ensure_ascii=False)}")

    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")
    st.write(
        {
            "progress": macro_overview["progress"],
            "cum_ratio(穩健型用)": macro_overview["amount_norm_cum_ratio"],
            "slice_ratio(保守型用)": macro_overview["amount_norm_slice_ratio"],
            "label": macro_overview["amount_norm_label"],
        }
    )

    # 航運估值卡
    hit = df_top2[df_top2["Symbol"].isin(list(SHIPPING_VALUATION.keys()))].copy()
    if not hit.empty:
        st.subheader("航運股估值快照（手動覆蓋層）")
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

    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
