# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD

# 你的盤中成交金額模組（可用就用；不可用就降級但不影響「未開盤顯示昨日」）
from market_amount import fetch_amount_total, intraday_norm, _now_taipei, TRADING_START

TZ_TAIPEI = timezone(timedelta(hours=8))


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
    # 你 repo 目前用 yfinance 生成：data_{market}.csv
    # 注意：你後來已把資料放進 data/ 資料夾，這裡同時支援根目錄與 data/。
    candidates = [
        f"data/data_{market}.csv",
        f"data_{market}.csv",
        "data/data_tw-share.csv",
        "data_tw-share.csv",
        "data/data_tw.csv",
        "data_tw.csv",
    ]
    fname = None
    for p in candidates:
        if os.path.exists(p):
            fname = p
            break
    if not fname:
        raise FileNotFoundError("找不到市場資料檔：請確認 data/data_tw-share.csv 或 data_tw-share.csv 存在")

    df = pd.read_csv(fname)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


def _pick_trade_date_for_session(df: pd.DataFrame, session: str) -> tuple[pd.Timestamp, str]:
    """
    - EOD：取資料中最後一個日期（視為「昨日收盤」）
    - INTRADAY：取最後一個日期（你的資料是日K，所以仍然用最後一日做基準）
    """
    latest = df["Date"].max()
    return latest, _fmt_date(latest)


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


def _load_global_summary() -> pd.DataFrame:
    """
    讀取你 repo 的全球摘要：data/global_market_summary.csv
    欄位：Market, Symbol, Change, Value
    """
    candidates = [
        "data/global_market_summary.csv",
        "global_market_summary.csv",
    ]
    for p in candidates:
        if os.path.exists(p):
            df = pd.read_csv(p)
            return df
    return pd.DataFrame(columns=["Market", "Symbol", "Change", "Value"])


def _is_premarket(now: datetime) -> bool:
    start_dt = now.replace(hour=TRADING_START.hour, minute=TRADING_START.minute, second=0, microsecond=0)
    return now < start_dt


def _market_comment_v15_7(overview: dict) -> str:
    """
    V15.7 口語化裁決訊息（可回溯欄位）
    """
    session = overview.get("data_mode")
    degraded = bool(overview.get("degraded_mode", False))
    inst_status = overview.get("inst_status", "UNAVAILABLE")

    if session == "EOD":
        # 未開盤：顯示昨日狀態，不做盤中成交金額裁決
        return "目前尚未開盤：畫面顯示『昨日收盤（EOD）』市場狀態與 Top List。"

    # INTRADAY：才需要盤中成交金額驗證
    label = overview.get("amount_norm_label", "UNKNOWN")
    if degraded:
        return f"盤中資料降級成立（量能標籤={label} / 法人狀態={inst_status}）：禁止 BUY/TRIAL。"

    return f"盤中資料可用（量能標籤={label} / 法人狀態={inst_status}）：可依模式規則執行。"


def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    now = datetime.now(tz=TZ_TAIPEI)
    premarket = _is_premarket(now)

    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)

    # ✅ 關鍵：未開盤自動鎖定 EOD（昨日）
    default_session = SESSION_EOD if premarket else SESSION_INTRADAY
    session = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0 if default_session == SESSION_INTRADAY else 1)

    run_btn = st.sidebar.button("Run")
    if not run_btn:
        st.info("按左側 Run 產生昨日（EOD）/盤中（INTRADAY）市場摘要、全球摘要與 Top 清單。")
        return

    df = _load_market_csv(market)
    latest_date, trade_date = _pick_trade_date_for_session(df, session=session)

    # 1) Analyzer（用你現有邏輯產出 Top List）
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    df_top = _apply_shipping_valuation_overrides(df_top)

    # 2) 全球摘要（美股/日經/匯率等）——免費來源
    global_df = _load_global_summary()

    # 3) 成交金額（上市+上櫃合計 amount_total）
    #    ✅ 注意：只有 INTRADAY 才需要做「盤中量能正規化」與裁決
    amount_twse = None
    amount_tpex = None
    amount_total = None
    amount_sources = {"twse": None, "tpex": None, "error": None}
    norm = {"progress": None, "amount_norm_cum_ratio": None, "amount_norm_slice_ratio": None, "amount_norm_label": "UNKNOWN"}

    if session == SESSION_INTRADAY:
        try:
            ma = fetch_amount_total()
            amount_twse = ma.amount_twse
            amount_tpex = ma.amount_tpex
            amount_total = ma.amount_total
            amount_sources["twse"] = ma.source_twse
            amount_sources["tpex"] = ma.source_tpex

            # 20D median（用你本地 yfinance 日K總額替代：免費、可重現）
            # 這裡用「你的成分股成交額總和」當作 proxy baseline（不是全市場，但可讓盤中不要動不動 LOW）
            # 你若之後願意再做「全市場 amount_total_history.csv」可替換
            d = df.copy()
            d = d.dropna(subset=["Date"])
            d = d.sort_values("Date")
            d["Close"] = pd.to_numeric(d["Close"], errors="coerce").fillna(0)
            d["Volume"] = pd.to_numeric(d["Volume"], errors="coerce").fillna(0)
            daily_amt = d.groupby("Date").apply(lambda x: float((x["Close"] * x["Volume"]).sum()))
            avg20_median = float(daily_amt.tail(20).median()) if len(daily_amt) >= 5 else None

            norm = intraday_norm(
                amount_total_now=int(amount_total),
                amount_total_prev=None,  # 你若之後存 5 分鐘前值可補
                avg20_amount_total=avg20_median,
                now=_now_taipei(),
                alpha=0.65,
            )
            norm["avg20_amount_total_median"] = avg20_median

        except Exception as e:
            amount_sources["error"] = f"{type(e).__name__}: {str(e)}"

    # 4) 法人狀態（FinMind 免費被 402 擋住 → 直接標 UNAVAILABLE）
    inst_status = "UNAVAILABLE"
    inst_dates_3d = []
    data_date_finmind = None

    # 5) V15.7 裁決：degraded_mode
    #    ✅ 原則：EOD（未開盤）不因為盤中成交金額抓不到而 degraded
    if session == SESSION_EOD:
        degraded_mode = False
    else:
        # INTRADAY：若成交金額抓不到，或 label UNKNOWN → degraded
        label = norm.get("amount_norm_label", "UNKNOWN")
        degraded_mode = (amount_total is None) or (label == "UNKNOWN")

    macro_overview = {
        "amount_twse": f"{amount_twse:,}" if isinstance(amount_twse, int) else "待更新",
        "amount_tpex": f"{amount_tpex:,}" if isinstance(amount_tpex, int) else "待更新",
        "amount_total": f"{amount_total:,}" if isinstance(amount_total, int) else "待更新",
        "amount_sources": amount_sources,
        "avg20_amount_total_median": norm.get("avg20_amount_total_median"),
        "progress": norm.get("progress"),
        "amount_norm_cum_ratio": norm.get("amount_norm_cum_ratio"),
        "amount_norm_slice_ratio": norm.get("amount_norm_slice_ratio"),
        "amount_norm_label": norm.get("amount_norm_label", "UNKNOWN"),
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": data_date_finmind,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
    }
    macro_overview["market_comment"] = _market_comment_v15_7(macro_overview)

    macro_data = {"overview": macro_overview, "indices": []}

    json_text = generate_ai_json(df_top, market=market, session=session, macro_data=macro_data)

    # =========================
    # UI
    # =========================
    st.subheader("今日市場狀態判斷（V15.7 裁決）")
    st.info(macro_overview["market_comment"])

    st.subheader("全球市場摘要（美股/日經/匯率）")
    if global_df.empty:
        st.warning("找不到 data/global_market_summary.csv，請確認 GitHub Actions 有產出並 commit 到 repo。")
    else:
        # 讓人類看得懂：只保留關鍵欄位
        view = global_df.copy()
        # Change 可能是小數（例如 0.47 表示 +0.47%），你可以依你的產製規格調整
        st.dataframe(view, use_container_width=True)

    st.subheader("市場成交金額（上市 + 上櫃 = amount_total）")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", macro_overview["amount_twse"])
    c2.metric("TPEx 上櫃", macro_overview["amount_tpex"])
    c3.metric("Total 合計", macro_overview["amount_total"])
    c4.metric("20D Median(代理)", str(macro_overview["avg20_amount_total_median"]))

    st.caption(f"來源/錯誤：{macro_overview['amount_sources']}")

    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")
    st.json(
        {
            "progress": macro_overview["progress"],
            "cum_ratio(穩健型用)": macro_overview["amount_norm_cum_ratio"],
            "slice_ratio(保守型用)": macro_overview["amount_norm_slice_ratio"],
            "label": macro_overview["amount_norm_label"],
        }
    )

    st.subheader("Top List")
    st.dataframe(df_top, use_container_width=True)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{'intraday' if session==SESSION_INTRADAY else 'eod'}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
