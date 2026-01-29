# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone, time as dtime

import pandas as pd
import streamlit as st
import yfinance as yf

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD

# 你原本的 FinMind（免費會 402）先保留 import，但我們在主流程會自動降級
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d

from market_amount import fetch_amount_total_safe, intraday_norm


# =========================
# 0) 基本設定
# =========================
TZ_TAIPEI = timezone(timedelta(hours=8))
TRADING_START = dtime(9, 0)

WATCHLIST_TW = ["2330.TW", "2317.TW", "2308.TW", "2454.TW", "2382.TW", "3231.TW", "2603.TW", "2609.TW"]

# 股票中文名（免費/穩定：先用你關注清單硬對照，避免 yfinance.info 太慢）
NAME_ZH_MAP = {
    "2330.TW": "台積電",
    "2317.TW": "鴻海",
    "2308.TW": "台達電",
    "2454.TW": "聯發科",
    "2382.TW": "廣達",
    "3231.TW": "緯創",
    "2603.TW": "長榮",
    "2609.TW": "陽明",
}

# 美股/全球參考（你畫面中那張 global_market_summary.csv 類似）
GLOBAL_SYMBOLS = [
    ("US", "^SOX", "SOX_Semi"),
    ("US", "TSM", "TSM_ADR"),
    ("US", "NVDA", "NVIDIA"),
    ("US", "AAPL", "Apple"),
    ("ASIA", "^N225", "Nikkei_225"),
    ("FX", "JPY=X", "USD_JPY"),   # 注意：JPY=X 是 USDJPY
    ("FX", "TWD=X", "USD_TWD"),   # 注意：TWD=X 是 USDTWD
]


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _is_before_open(now: datetime) -> bool:
    start_dt = now.replace(hour=TRADING_START.hour, minute=TRADING_START.minute, second=0, microsecond=0)
    return now < start_dt


def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _fmt_yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _download_tw_daily(tickers: list[str], lookback_days: int = 30) -> pd.DataFrame:
    """
    免費方案：直接用 yfinance 抓「最近 N 天」日線，確保盤前顯示的是『最新交易日（昨日/最近一日）』
    """
    period = f"{lookback_days}d"
    data = yf.download(tickers, period=period, interval="1d", group_by="column", auto_adjust=False, progress=False)

    if data is None or data.empty:
        return pd.DataFrame(columns=["Date", "Symbol", "Close", "Volume", "Open", "High", "Low"])

    # yfinance 回傳 MultiIndex 欄位：('Close', '2330.TW')...
    # 轉為長表
    frames = []
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in data.columns.get_level_values(0):
            continue
        tmp = data[col].copy()
        tmp = tmp.stack(dropna=False).reset_index()
        tmp.columns = ["Date", "Symbol", col]
        frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=["Date", "Symbol", "Close", "Volume", "Open", "High", "Low"])

    df = frames[0]
    for f in frames[1:]:
        df = df.merge(f, on=["Date", "Symbol"], how="outer")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


def _latest_trading_date(df: pd.DataFrame) -> datetime | None:
    if df.empty:
        return None
    d = pd.to_datetime(df["Date"], errors="coerce").dropna()
    if d.empty:
        return None
    return d.max().to_pydatetime().replace(tzinfo=None)


def _build_global_summary_from_yf() -> pd.DataFrame:
    """
    免費：直接用 yfinance 抓最近兩日收盤，算變動（%）
    """
    rows = []
    for market, yf_symbol, alias in GLOBAL_SYMBOLS:
        try:
            hist = yf.download(yf_symbol, period="10d", interval="1d", progress=False)
            if hist is None or hist.empty or "Close" not in hist:
                continue
            hist = hist.dropna()
            if len(hist) < 2:
                continue
            c0 = float(hist["Close"].iloc[-2])
            c1 = float(hist["Close"].iloc[-1])
            chg = (c1 / c0 - 1.0) * 100.0
            rows.append({"Market": market, "Symbol": alias, "Change": round(chg, 4), "Value": round(c1, 2)})
        except Exception:
            continue

    return pd.DataFrame(rows, columns=["Market", "Symbol", "Change", "Value"])


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


def _decide_inst_status(inst_fetch_error: str | None, inst_df: pd.DataFrame, symbols: list[str], trade_date: str) -> tuple[str, list[str], str | None]:
    """
    你要的裁決邏輯：
    - 免費 FinMind 常見 402 → UNAVAILABLE
    - 若資料齊全才 READY
    """
    if inst_fetch_error:
        if "402" in inst_fetch_error or "Payment Required" in inst_fetch_error:
            return "UNAVAILABLE", [], None
        return "PENDING", [], None

    if inst_df is None or inst_df.empty:
        return "PENDING", [], None

    ready_any = False
    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True
            break

    dates_3d = []
    try:
        dates_3d = sorted(inst_df["date"].astype(str).unique().tolist())[-3:]
    except Exception:
        dates_3d = []

    return ("READY" if ready_any else "PENDING"), dates_3d, None


def _market_comment(macro_overview: dict) -> str:
    """
    人類可讀版（但每句都能回溯到 macro 欄位）
    """
    # 關鍵欄位
    amount_total = macro_overview.get("amount_total")
    amount_norm_label = macro_overview.get("amount_norm_label", "UNKNOWN")
    inst_status = macro_overview.get("inst_status", "UNAVAILABLE")
    degraded_mode = bool(macro_overview.get("degraded_mode", False))
    mode = macro_overview.get("mode", "Balanced")

    parts = []

    # 量能敘述
    if amount_total in (None, "", "待更新"):
        parts.append("成交金額待更新")
    else:
        try:
            amt = float(str(amount_total).replace(",", ""))
            parts.append(f"成交金額約 {amt/1e8:,.0f} 億（上市+上櫃合計）")
        except Exception:
            parts.append("成交金額已取得（格式待校正）")

    parts.append(f"量能判定：{amount_norm_label}")

    # 法人敘述
    if inst_status == "READY":
        parts.append("法人資料可用")
    elif inst_status == "PENDING":
        parts.append("法人資料不足")
    else:
        parts.append("法人資料不可用（免費方案常見 402/限制）")

    # 裁決敘述
    if degraded_mode:
        if mode == "Trial":
            parts.append("裁決：資料降級成立，但試投型允許 TRIAL（禁 BUY）")
        else:
            parts.append("裁決：資料降級成立（禁 BUY/TRIAL）")
    else:
        parts.append("裁決：資料完整，可依訊號執行")

    return "；".join(parts) + "。"


def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台")

    # -------------------------
    # Sidebar（模式裁決）
    # -------------------------
    mode = st.sidebar.selectbox("Mode（裁決）", ["Conservative", "Balanced", "Trial"], index=1)
    session_user = st.sidebar.selectbox("Session", [SESSION_INTRADAY, SESSION_EOD], index=0)
    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("按左側 Run。盤前會自動顯示『昨日台股EOD + 最新美股』。")
        return

    now = _now_taipei()
    before_open = _is_before_open(now)

    # -------------------------
    # 1) 台股日線（用 yfinance，確保盤前不是三天前）
    # -------------------------
    df = _download_tw_daily(WATCHLIST_TW, lookback_days=60)
    latest_dt = _latest_trading_date(df)
    if latest_dt is None:
        st.error("yfinance 台股日線取得失敗（無法判定最新交易日）")
        return

    trade_date = _fmt_yyyymmdd(latest_dt)

    # 盤前：強制用 EOD 產生昨日狀態；盤中：用使用者選擇
    session = SESSION_EOD if before_open else session_user

    # 只保留最新交易日那一列（昨日/最近交易日）
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df_last = df[df["Date"] == pd.to_datetime(latest_dt)].copy()

    # -------------------------
    # 2) Analyzer：Top List（你的策略核心）
    # -------------------------
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # 中文名補齊（你要求：代碼 + 名稱）
    if "Name" not in df_top.columns:
        df_top["Name"] = None
    df_top["Name"] = df_top["Symbol"].astype(str).map(NAME_ZH_MAP).fillna(df_top["Name"])

    # -------------------------
    # 3) 全球市場摘要（美股/日經/匯率）：免費 yfinance
    # -------------------------
    st.subheader("全球市場摘要（美股/日經/匯率｜免費 yfinance）")
    gdf = _build_global_summary_from_yf()
    if gdf.empty:
        st.warning("全球市場摘要取得失敗（yfinance 可能短暫受限）")
    else:
        st.dataframe(gdf, use_container_width=True)

    # -------------------------
    # 4) 成交金額（上市+上櫃合計）+ 盤中正規化
    # -------------------------
    # 盤前你要的是『昨日』，所以用 trade_date（最新交易日）
    amt = fetch_amount_total_safe(trade_date=trade_date)

    amount_twse = amt.get("amount_twse_fmt", "待更新")
    amount_tpex = amt.get("amount_tpex_fmt", "待更新")
    amount_total = amt.get("amount_total_fmt", "待更新")
    amount_sources = amt.get("sources", {})

    # 盤中正規化：若拿不到 amount_total 或 avg20，就會 UNKNOWN（但 Trial 不會因此禁 TRIAL）
    avg20_median = None  # 免費方案：若你日後有 history 檔，可在這裡灌入 20D median
    norm = {"progress": None, "amount_norm_cum_ratio": None, "amount_norm_slice_ratio": None, "amount_norm_label": "UNKNOWN"}
    if amt.get("amount_total_int") is not None and avg20_median:
        norm = intraday_norm(
            amount_total_now=int(amt["amount_total_int"]),
            amount_total_prev=None,
            avg20_amount_total=int(avg20_median),
            now=now,
            alpha=0.65,
        )

    st.subheader("市場成交金額（上市 + 上櫃 = amount_total）")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TWSE 上市", amount_twse)
    c2.metric("TPEx 上櫃", amount_tpex)
    c3.metric("Total 合計", amount_total)
    c4.metric("20D Median(代理)", str(avg20_median))

    st.caption(f"來源/錯誤：{json.dumps(amount_sources, ensure_ascii=False)}")

    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")
    st.code(
        json.dumps(
            {
                "progress": norm.get("progress"),
                "cum_ratio(穩健型用)": norm.get("amount_norm_cum_ratio"),
                "slice_ratio(保守型用)": norm.get("amount_norm_slice_ratio"),
                "label": norm.get("amount_norm_label"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        language="json",
    )

    # -------------------------
    # 5) 法人資料（免費：可能 402 → UNAVAILABLE）
    # -------------------------
    symbols = df_top["Symbol"].astype(str).tolist()
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    end_date = trade_date

    inst_fetch_error = None
    inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])
    try:
        inst_df = fetch_finmind_institutional(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            token=os.getenv("FINMIND_TOKEN", None),
        )
    except Exception as e:
        inst_fetch_error = f"{type(e).__name__}: {str(e)}"

    inst_status, inst_dates_3d, _ = _decide_inst_status(inst_fetch_error, inst_df, symbols, trade_date)

    # -------------------------
    # 6) V15.7 裁決：degraded_mode
    # -------------------------
    # 規則（你要的整合版）：
    # - Conservative: 量能 UNKNOWN/LOW 或法人非 READY → 禁 BUY/TRIAL
    # - Balanced:     量能 UNKNOWN/LOW 或法人非 READY → 禁 BUY（TRIAL 可選；此處採禁 TRIAL 以符合你「絕對防線」）
    # - Trial:        忽略量能；法人 UNAVAILABLE 仍可 TRIAL（但禁 BUY）
    amount_norm_label = norm.get("amount_norm_label", "UNKNOWN")
    amount_bad = (amount_total in (None, "", "待更新")) or (amount_norm_label == "UNKNOWN")

    if mode == "Trial":
        degraded_mode = (inst_status == "UNAVAILABLE") and True  # 仍標記降級成立（但裁決訊息會寫清楚）
        allow_trial = True
        allow_buy = False if inst_status != "READY" else True  # 你若想 Trial 永遠禁 BUY，可改成固定 False
    else:
        degraded_mode = amount_bad or (inst_status != "READY")
        allow_trial = False
        allow_buy = False if degraded_mode else True

    # -------------------------
    # 7) Macro Overview + Market Comment
    # -------------------------
    macro_overview = {
        "mode": mode,
        "amount_twse": amount_twse,
        "amount_tpex": amount_tpex,
        "amount_total": amount_total,
        "amount_sources": amount_sources,
        "avg20_amount_total_median": avg20_median,
        "progress": norm.get("progress"),
        "amount_norm_cum_ratio": norm.get("amount_norm_cum_ratio"),
        "amount_norm_slice_ratio": norm.get("amount_norm_slice_ratio"),
        "amount_norm_label": amount_norm_label,
        "inst_net": "A:0.00億 | B:0.00億",  # 免費版先保留欄位
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": None,
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
        "allow_buy": allow_buy,
        "allow_trial": allow_trial,
        "amount": amount_total,  # 相容舊欄位
    }
    macro_overview["market_comment"] = _market_comment(macro_overview)

    st.subheader("今日市場狀態判斷（V15.7 裁決）")
    # 盤前要明確寫：顯示昨日EOD
    if before_open:
        st.info(f"目前尚未開盤：畫面顯示『昨日收盤（EOD）』市場狀態與 Top List。交易日：{trade_date}")
    st.success(macro_overview["market_comment"])

    # -------------------------
    # 8) 合併法人到 df_top（若 unavailable，會是 PENDING 占位）
    # -------------------------
    if inst_status == "READY":
        df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)
    else:
        df_top2 = df_top.copy()
        df_top2["Institutional"] = df_top2["Symbol"].map(
            lambda _: {
                "Inst_Visual": "PENDING",
                "Inst_Net_3d": 0.0,
                "Inst_Streak3": 0,
                "Inst_Dir3": "PENDING",
                "Inst_Status": "PENDING",
            }
        )

    # -------------------------
    # 9) 產生 AI JSON
    # -------------------------
    macro_data = {"overview": macro_overview, "indices": []}
    json_text = generate_ai_json(df_top2, market="tw-share", session=session, macro_data=macro_data)

    # -------------------------
    # UI：Top List（含中文名）
    # -------------------------
    st.subheader("Top List（代碼 + 中文名）")
    show_cols = [c for c in ["Symbol", "Name", "Close", "Vol_Ratio", "MA_Bias", "Score", "Predator_Tag", "Structure"] if c in df_top2.columns]
    st.dataframe(df_top2[show_cols] if show_cols else df_top2, use_container_width=True)

    st.subheader("AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    outname = f"ai_payload_tw-share_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
