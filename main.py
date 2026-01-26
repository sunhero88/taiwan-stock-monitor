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

# ✅ 你的新模組：上市+上櫃合計、盤中量能正規化
from market_amount import fetch_amount_total, intraday_norm, _now_taipei


# =========================
# 0) 估值覆蓋層（以 JSON 檔為準，避免 main.py 變成硬編碼）
# =========================
def _load_shipping_valuation() -> dict:
    """
    優先讀 shipping_valuation.json（repo 根目錄）
    格式建議（範例）：
    {
      "2603.TW": {"name_zh":"長榮","sector":"航運","opm_q":22.73,"opm_q_period":"2025 Q3",
                 "eps_ttm":41.92,"price_ref":192.54,"price_ref_date":"2026-01-22","pe_calc":4.59,
                 "label":"🟢 極度低估 (Deep Value)","source":"財報狗/玩股網（最新季報資料庫）"}
    }
    """
    path = "shipping_valuation.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


SHIPPING_VALUATION = _load_shipping_valuation()


def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


# =========================
# 1) 資料載入（支援根目錄 /data 兩種）
# =========================
def _load_market_csv(market: str) -> pd.DataFrame:
    """
    依市場讀取資料：
    - 先找 data/data_{market}.csv
    - 再找 data_{market}.csv
    - 再 fallback：data/data_tw-share.csv、data/data_tw.csv、根目錄同名
    """
    candidates = [
        os.path.join("data", f"data_{market}.csv"),
        f"data_{market}.csv",
        os.path.join("data", "data_tw-share.csv"),
        os.path.join("data", "data_tw.csv"),
        "data_tw-share.csv",
        "data_tw.csv",
    ]

    fname = None
    for p in candidates:
        if os.path.exists(p):
            fname = p
            break

    if not fname:
        raise FileNotFoundError("找不到市場資料檔：data/data_{market}.csv 或 data_tw-share.csv / data_tw.csv")

    df = pd.read_csv(fname)
    return df


# =========================
# 2) 法人資料注入（每檔三日）
# =========================
def _merge_institutional_into_df_top(df_top: pd.DataFrame, inst_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    out = df_top.copy()

    inst_map = {}
    for _, r in out.iterrows():
        symbol = str(r.get("Symbol", ""))
        inst_calc = calc_inst_3d(inst_df, symbol=symbol, trade_date=trade_date)

        inst_map[symbol] = {
            "Inst_Visual": inst_calc.get("Inst_Status", "PENDING"),
            "Inst_Net_3d": float(inst_calc.get("Inst_Net_3d", 0.0) or 0.0),
            "Inst_Streak3": int(inst_calc.get("Inst_Streak3", 0) or 0),
            "Inst_Dir3": inst_calc.get("Inst_Dir3", "PENDING"),
            "Inst_Status": inst_calc.get("Inst_Status", "PENDING"),
        }

    out["Institutional"] = out["Symbol"].astype(str).map(inst_map)
    return out


def _decide_inst_status(inst_df: pd.DataFrame, symbols: list[str], trade_date: str) -> tuple[str, list[str]]:
    """
    inst_status：
    - 任一檔可滿足三日資料齊全 → READY
    - 否則 → PENDING
    inst_dates_3d：抓 inst_df 最後三個 date（若有）
    """
    ready_any = False

    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True
            break

    dates_3d = []
    try:
        if not inst_df.empty and "date" in inst_df.columns:
            dates_3d = sorted(inst_df["date"].astype(str).unique().tolist())[-3:]
    except Exception:
        dates_3d = []

    return ("READY" if ready_any else "PENDING"), dates_3d


# =========================
# 3) 航運估值 overlay（不污染原 Structure）
# =========================
def _apply_shipping_valuation_overrides(df_top: pd.DataFrame) -> pd.DataFrame:
    if not SHIPPING_VALUATION:
        return df_top

    out = df_top.copy()

    def _inject(row: pd.Series) -> pd.Series:
        sym = str(row.get("Symbol", "")).strip()
        info = SHIPPING_VALUATION.get(sym)
        if not isinstance(info, dict):
            return row

        if not row.get("Name"):
            row["Name"] = info.get("name_zh", sym)

        row["Valuation_Override"] = {
            "sector": info.get("sector"),
            "opm_q": float(info.get("opm_q", 0.0) or 0.0),
            "opm_q_period": info.get("opm_q_period"),
            "eps_ttm": float(info.get("eps_ttm", 0.0) or 0.0),
            "price_ref": float(info.get("price_ref", 0.0) or 0.0),
            "price_ref_date": info.get("price_ref_date"),
            "pe_calc": float(info.get("pe_calc", 0.0) or 0.0),
            "label": info.get("label"),
            "source": info.get("source"),
        }
        return row

    return out.apply(_inject, axis=1)


# =========================
# 4) V15.7：INTRADAY 量能正規化（保守/穩健/試投）
# =========================
def _classify_ratio(r: float | None) -> str:
    if r is None:
        return "UNKNOWN"
    if r < 0.8:
        return "LOW"
    if r > 1.2:
        return "HIGH"
    return "NORMAL"


def _load_amount_history() -> pd.DataFrame:
    """
    量能歷史（可選）：data/amount_total_history.csv
    欄位：trade_date, amount_total
    """
    path = os.path.join("data", "amount_total_history.csv")
    if os.path.exists(path):
        try:
            d = pd.read_csv(path)
            if "trade_date" in d.columns and "amount_total" in d.columns:
                d["trade_date"] = d["trade_date"].astype(str)
                d["amount_total"] = pd.to_numeric(d["amount_total"], errors="coerce")
                d = d.dropna(subset=["amount_total"])
                d["amount_total"] = d["amount_total"].astype(int)
                return d
        except Exception:
            return pd.DataFrame(columns=["trade_date", "amount_total"])
    return pd.DataFrame(columns=["trade_date", "amount_total"])


def _calc_avg20_median_amount_total(history: pd.DataFrame) -> int | None:
    """
    用近 20 筆的中位數作基準（抗極端值）
    """
    if history is None or history.empty:
        return None
    vals = history["amount_total"].astype(int).tolist()
    if len(vals) < 10:
        return None
    tail = vals[-20:]
    tail_sorted = sorted(tail)
    mid = len(tail_sorted) // 2
    if len(tail_sorted) % 2 == 1:
        return int(tail_sorted[mid])
    return int((tail_sorted[mid - 1] + tail_sorted[mid]) / 2)


def _save_amount_history(trade_date: str, amount_total: int) -> None:
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", "amount_total_history.csv")
    hist = _load_amount_history()

    # 同日覆蓋
    if not hist.empty and (hist["trade_date"] == trade_date).any():
        hist.loc[hist["trade_date"] == trade_date, "amount_total"] = int(amount_total)
    else:
        hist = pd.concat([hist, pd.DataFrame([{"trade_date": trade_date, "amount_total": int(amount_total)}])], ignore_index=True)

    hist.to_csv(path, index=False, encoding="utf-8")


def _build_market_comment_v15_7(overview: dict, scenario: str) -> str:
    """
    scenario：
    - "保守型"：看切片
    - "穩健型"：看累積
    - "試投型"：忽略量能
    """
    amt_total = overview.get("amount_total")
    inst_status = overview.get("inst_status")
    degraded_mode = bool(overview.get("degraded_mode", False))

    # 成交金額敘述
    if amt_total == "待更新":
        amt_text = "成交金額待更新"
    else:
        try:
            amt_int = int(str(amt_total).replace(",", ""))
            amt_text = f"成交金額（上市+上櫃合計）約 {amt_int/1e8:,.0f} 億"
        except Exception:
            amt_text = "成交金額（上市+上櫃合計）"

    # 量能判讀（依情境）
    label = overview.get("amount_norm_label", "UNKNOWN")
    cum_ratio = overview.get("amount_norm_cum_ratio")
    slice_ratio = overview.get("amount_norm_slice_ratio")

    if scenario == "試投型":
        vol_text = "（試投型：量能不作為進場門檻）"
    elif scenario == "保守型":
        vol_text = f"（保守型切片量能：slice_ratio={slice_ratio}，判定={_classify_ratio(slice_ratio)}）"
    else:
        vol_text = f"（穩健型累積量能：cum_ratio={cum_ratio}，判定={label}）"

    # 法人判讀
    if inst_status == "READY":
        inst_text = "法人資料可用"
    elif inst_status == "UNAVAILABLE":
        inst_text = "法人資料不可用"
    else:
        inst_text = "法人資料不足"

    # 裁決降級
    if degraded_mode:
        gate_text = "；裁決層已進入資料降級：禁止 BUY/TRIAL"
    else:
        gate_text = ""

    return f"{amt_text}{vol_text}；{inst_text}{gate_text}。"


# =========================
# 5) 主程式
# =========================
def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    # ✅ 情境（你要讓 5 個 AI 各跑一套；此 app 先提供切換）
    scenario = st.sidebar.selectbox("情境模式", ["保守型", "穩健型", "試投型"], index=0)

    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)

    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("按左側 Run 產生 Top 清單與 JSON。")
        return

    # 1) Load market data（個股資料：Close/Volume）
    df = _load_market_csv(market)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    latest_date = df["Date"].max()
    trade_date = _fmt_date(latest_date)

    # 2) Run analyzer
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 2.1 航運估值 overlay（可回溯）
    df_top = _apply_shipping_valuation_overrides(df_top)

    # 3) 法人資料（FinMind）
    symbols = df_top["Symbol"].astype(str).tolist()
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

    inst_status_raw, inst_dates_3d = _decide_inst_status(inst_df, symbols, trade_date)

    # ✅ 法人狀態裁決：抓不到就 UNAVAILABLE
    inst_status = inst_status_raw
    if inst_fetch_error:
        inst_status = "UNAVAILABLE"
        inst_dates_3d = []

    # 4) 市場成交金額（上市+上櫃合計）+ INTRADAY 正規化
    amount_twse = None
    amount_tpex = None
    amount_total = None
    amount_sources = {"twse": None, "tpex": None, "error": None}

    try:
        ma = fetch_amount_total()
        amount_twse = ma.amount_twse
        amount_tpex = ma.amount_tpex
        amount_total = ma.amount_total
        amount_sources["twse"] = ma.source_twse
        amount_sources["tpex"] = ma.source_tpex
    except Exception as e:
        amount_sources["error"] = f"{type(e).__name__}: {str(e)}"

    # 可選：把 amount_total 寫入歷史，用於 avg20 median
    if amount_total is not None and session.upper() in ("EOD", "INTRADAY"):
        try:
            _save_amount_history(trade_date, amount_total)
        except Exception:
            pass

    history = _load_amount_history()
    avg20_median = _calc_avg20_median_amount_total(history)

    # 盤中正規化
    progress = None
    amount_norm_cum_ratio = None
    amount_norm_slice_ratio = None
    amount_norm_label = "UNKNOWN"

    # slice 需要上一筆，這裡用 history 的「同日上一筆」很難；先給 None（不會壞）
    amount_total_prev = None

    if session.upper() == "INTRADAY" and amount_total is not None and avg20_median:
        norm = intraday_norm(
            amount_total_now=amount_total,
            amount_total_prev=amount_total_prev,
            avg20_amount_total=avg20_median,
            now=_now_taipei(),
            alpha=0.65
        )
        progress = norm.get("progress")
        amount_norm_cum_ratio = norm.get("amount_norm_cum_ratio")
        amount_norm_slice_ratio = norm.get("amount_norm_slice_ratio")
        amount_norm_label = norm.get("amount_norm_label", "UNKNOWN")

    # 5) V15.7 資料健康門（degraded_mode）
    # 你要求：盤中不要動不動 LOW，但「資料抓不到」仍要降級
    # 保守型：只要 amount_total 缺失 或 inst 非 READY → 降級（禁止 BUY/TRIAL）
    # 穩健型：inst 非 READY → 降級；amount_total 缺失 → 仍降級（避免量能判讀失真）
    # 試投型：忽略量能，但 inst 非 READY 仍降級（因你裁決層仍要可回溯）
    kill_switch = False
    v14_watch = False

    degraded_mode = False
    if scenario in ("保守型", "穩健型"):
        if amount_total is None:
            degraded_mode = True
        if inst_status != "READY":
            degraded_mode = True
    else:  # 試投型
        if inst_status != "READY":
            degraded_mode = True

    if kill_switch or v14_watch:
        degraded_mode = True

    # 6) Merge institutional into df_top
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    # 7) Macro overview（核心欄位一致：amount_total 一定是「上市+上櫃合計」）
    macro_overview = {
        "amount_twse": "待更新" if amount_twse is None else f"{amount_twse:,}",
        "amount_tpex": "待更新" if amount_tpex is None else f"{amount_tpex:,}",
        "amount_total": "待更新" if amount_total is None else f"{amount_total:,}",
        "amount_sources": amount_sources,
        "avg20_amount_total_median": avg20_median,
        "progress": progress,
        "amount_norm_cum_ratio": amount_norm_cum_ratio,
        "amount_norm_slice_ratio": amount_norm_slice_ratio,
        "amount_norm_label": amount_norm_label,
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": None,
        "kill_switch": kill_switch,
        "v14_watch": v14_watch,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
        # ✅ 舊欄位 amount 仍保留（避免其他模組期待 amount）
        "amount": "待更新" if amount_total is None else f"{amount_total:,}",
        "scenario_mode": scenario,  # 讓 Arbiter/報表可回溯情境
    }

    market_comment = _build_market_comment_v15_7(macro_overview, scenario=scenario)
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
    st.subheader("今日市場狀態判定（V15.7 裁決口徑）")
    st.info(market_comment)

    # 航運估值卡（命中者才顯示）
    if "Valuation_Override" in df_top2.columns:
        hit = df_top2[df_top2["Valuation_Override"].notna()].copy()
        if not hit.empty:
            st.subheader("航運股估值快照（shipping_valuation.json）")

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

    # save
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
