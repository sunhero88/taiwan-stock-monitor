# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import glob
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

# 只 import 你 market_amount.py 內確實存在的名稱
from market_amount import TZ_TAIPEI, fetch_amount_total, intraday_norm


APP_TITLE = "Sunhero｜股市智能超盤中控台"
SYSTEM_NAME = "Predator V15.7（Free/Sim）"


# ====== 基礎工具 ======
def now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (float, int, np.floating, np.integer)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() in ("none", "nan"):
            return default
        return float(s)
    except Exception:
        return default


def to_bn_twd(amount_int: Optional[int]) -> Optional[float]:
    """元 -> 億（1e8）"""
    if amount_int is None:
        return None
    try:
        return round(float(amount_int) / 1e8, 2)
    except Exception:
        return None


def fmt_bn(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "待更新"
    return f"{x:,.2f} 億"


def _normalize_symbol(s: str) -> str:
    s = (s or "").strip().upper()
    # 允許使用者輸入 2330 也能自動補 .TW
    if s.isdigit():
        return f"{s}.TW"
    return s


# ====== 股票中文名稱（最小可用版）=====
# 你可自行擴充；或未來改為讀取一份 data/tw_names.csv
TW_NAME_MAP = {
    "2330.TW": "台積電",
    "2317.TW": "鴻海",
    "2382.TW": "廣達",
    "2454.TW": "聯發科",
    "2308.TW": "台達電",
    "2603.TW": "長榮",
    "2609.TW": "陽明",
    "3231.TW": "緯創",
    "0050.TW": "元大台灣50",
}


def symbol_to_name(sym: str) -> str:
    return TW_NAME_MAP.get(sym, "")


# ====== 讀取台股資料（Top20 Universe）=====
def _find_candidate_csv_files(market: str) -> List[str]:
    """
    你的 repo 可能存在多種命名：
    - data/data_tw-share.csv
    - data_tw-share.csv
    - data_tw.csv
    - data/data_tw.csv
    這裡都會找，並挑最新檔。
    """
    patterns = [
        f"data/data_{market}.csv",
        f"data/data-{market}.csv",
        f"data/data_{market.replace('-', '_')}.csv",
        f"data/data_{market.replace('_', '-')}.csv",
        f"data_{market}.csv",
        f"data-{market}.csv",
        "data/data_tw.csv",
        "data_tw.csv",
        "data/data-tw.csv",
        "data-tw.csv",
        "data/tw-share.csv",
        "tw-share.csv",
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    # 去重
    files = sorted(list(set(files)))
    return files


def _pick_latest_file(files: List[str]) -> Optional[str]:
    if not files:
        return None
    files_sorted = sorted(files, key=lambda f: os.path.getmtime(f), reverse=True)
    return files_sorted[0]


def load_market_df(market: str) -> pd.DataFrame:
    files = _find_candidate_csv_files(market)
    latest = _pick_latest_file(files)
    if not latest:
        raise FileNotFoundError(
            f"找不到 {market} 的資料檔。已嘗試：data/data_{market}.csv、data_{market}.csv、data_tw.csv 等常見命名。"
        )

    df = pd.read_csv(latest)

    # 欄位標準化（兼容你不同版本輸出）
    # 必要欄位：Date / Symbol / Close / Volume
    col_map = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc == "date":
            col_map[c] = "Date"
        elif lc in ("symbol", "ticker", "code"):
            col_map[c] = "Symbol"
        elif lc in ("close", "adj close", "adj_close"):
            col_map[c] = "Close"
        elif lc in ("volume",):
            col_map[c] = "Volume"
        elif lc in ("open",):
            col_map[c] = "Open"
        elif lc in ("high",):
            col_map[c] = "High"
        elif lc in ("low",):
            col_map[c] = "Low"
        elif lc in ("vol_ratio", "volume_ratio"):
            col_map[c] = "Vol_Ratio"
        elif lc in ("ma_bias", "ma_bias_pct", "bias_pct"):
            col_map[c] = "MA_Bias"
        elif lc in ("score",):
            col_map[c] = "Score"
        elif lc in ("ret20_pct", "ret_20", "ret20"):
            col_map[c] = "ret20_pct"

    df = df.rename(columns=col_map)

    # Date 轉 datetime
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Symbol 統一格式：2330.TW
    if "Symbol" in df.columns:
        df["Symbol"] = df["Symbol"].astype(str).map(_normalize_symbol)

    # 補 name
    if "Name" not in df.columns:
        df["Name"] = df["Symbol"].map(symbol_to_name)

    # 若 Score 不存在，做一個最小可用的替代分數（僅供排序，非投資建議）
    if "Score" not in df.columns:
        # 可用欄位：ret20_pct / Vol_Ratio / MA_Bias
        r20 = df["ret20_pct"] if "ret20_pct" in df.columns else 0.0
        vr = df["Vol_Ratio"] if "Vol_Ratio" in df.columns else 1.0
        mb = df["MA_Bias"] if "MA_Bias" in df.columns else 0.0

        df["Score"] = (
            pd.to_numeric(r20, errors="coerce").fillna(0.0) * 0.7
            + pd.to_numeric(vr, errors="coerce").fillna(1.0) * 20.0
            + pd.to_numeric(mb, errors="coerce").fillna(0.0) * 0.8
        )

    return df


def latest_trade_date(df: pd.DataFrame) -> Optional[pd.Timestamp]:
    if "Date" not in df.columns:
        return None
    d = df["Date"].dropna()
    if d.empty:
        return None
    return d.max().normalize()


def build_top20(df: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
    sub = df[df["Date"].dt.normalize() == trade_date].copy()
    if sub.empty:
        return sub

    # 欄位補齊
    for c in ["Close", "Volume", "Open", "High", "Low", "Vol_Ratio", "MA_Bias", "Score"]:
        if c not in sub.columns:
            sub[c] = np.nan

    # 排序：Score 高者在前
    sub = sub.sort_values("Score", ascending=False).reset_index(drop=True)
    sub["Rank"] = np.arange(1, len(sub) + 1)
    top = sub.head(20).copy()
    return top


# ====== 國際市場（美股/匯率）=====
@dataclass
class GlobalRow:
    name: str
    symbol: str
    date: str
    close: float
    chg_pct: float


def fetch_yf_last_close(symbol: str, period_days: int = 10) -> Optional[Tuple[pd.Timestamp, float, float]]:
    """
    抓取「最新可用交易日」收盤價與日變動百分比
    回傳：(date, close, chg_pct)
    """
    try:
        df = yf.download(symbol, period=f"{period_days}d", interval="1d", progress=False, auto_adjust=False)
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["Close"])
        if df.empty:
            return None
        last = df.iloc[-1]
        last_date = pd.to_datetime(df.index[-1]).normalize()
        last_close = float(last["Close"])
        if len(df) >= 2:
            prev_close = float(df.iloc[-2]["Close"])
            chg_pct = 0.0 if prev_close == 0 else (last_close / prev_close - 1.0) * 100.0
        else:
            chg_pct = 0.0
        return last_date, last_close, chg_pct
    except Exception:
        return None


def build_global_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    us_items = [
        ("S&P500", "^GSPC"),
        ("NASDAQ", "^IXIC"),
        ("DOW", "^DJI"),
        ("SOX", "^SOX"),
        ("VIX", "^VIX"),
    ]
    fx_items = [
        ("USD/JPY", "JPY=X"),
        ("USD/TWD", "TWD=X"),
    ]

    us_rows: List[Dict] = []
    for name, sym in us_items:
        got = fetch_yf_last_close(sym, period_days=15)
        if not got:
            us_rows.append({"Name": name, "Symbol": sym, "Date": "N/A", "Close": np.nan, "Chg%": np.nan})
        else:
            d, close, chg = got
            us_rows.append({"Name": name, "Symbol": sym, "Date": str(d.date()), "Close": close, "Chg%": round(chg, 2)})

    fx_rows: List[Dict] = []
    for name, sym in fx_items:
        got = fetch_yf_last_close(sym, period_days=15)
        if not got:
            fx_rows.append({"Name": name, "Symbol": sym, "Date": "N/A", "Close": np.nan, "Chg%": np.nan})
        else:
            d, close, chg = got
            fx_rows.append({"Name": name, "Symbol": sym, "Date": str(d.date()), "Close": close, "Chg%": round(chg, 2)})

    us_df = pd.DataFrame(us_rows)
    fx_df = pd.DataFrame(fx_rows)
    return us_df, fx_df


# ====== 成交金額（官方口徑）=====
def fetch_official_amount(verify_ssl: bool = True) -> Dict:
    """
    使用 market_amount.py 的 fetch_amount_total()
    - 若 TPEx 抓不到：回傳缺口 warning，但 UI 不崩潰
    """
    out = {
        "trade_date": None,
        "amount_twse": None,
        "amount_tpex": None,
        "amount_total": None,
        "sources": {"twse": None, "tpex": None},
        "warning": None,
        "error": None,
    }

    try:
        # market_amount.py 本身使用 requests.get(..., timeout=15) 並未暴露 verify 參數
        # Streamlit Cloud 若遇到憑證問題，建議在 market_amount.py 內加 verify=verify_ssl
        # 這裡先以 UI 提醒；不強行繞過（避免把安全降級默認化）。
        ma = fetch_amount_total()
        out["amount_twse"] = ma.amount_twse
        out["amount_tpex"] = ma.amount_tpex
        out["amount_total"] = ma.amount_total
        out["sources"]["twse"] = ma.source_twse
        out["sources"]["tpex"] = ma.source_tpex
        out["trade_date"] = str(now_taipei().date())
    except Exception as e:
        out["error"] = str(e)

    return out


# ====== Arbiter Input（Free/Sim）=====
def decide_inst_status_free() -> Tuple[str, List[str]]:
    """
    免費/模擬期：不使用 FinMind（避免 402）
    所以 inst_status 永遠不是 READY，除非你未來改用其他免費來源。
    """
    return "UNAVAILABLE", []


def decide_degraded_mode(amount_total: Optional[int], inst_status: str) -> bool:
    # 絕對防線：任一關鍵資料缺失 => degraded_mode = True
    if amount_total is None or amount_total <= 0:
        return True
    if inst_status != "READY":
        return True
    return False


def build_market_comment(
    amount_total: Optional[int],
    inst_status: str,
    degraded_mode: bool,
    data_mode: str,
    amount_sources: Dict,
) -> str:
    msgs = []
    if amount_total is None or amount_total <= 0:
        msgs.append("成交金額缺失")
    if inst_status != "READY":
        msgs.append("法人資料不可用（免費模擬期）")
    if degraded_mode:
        msgs.append("裁決層已進入資料降級：禁止 BUY/TRIAL")
    else:
        msgs.append("資料狀態可用（允許進入個股裁決）")

    # 顯示來源/錯誤摘要（避免失真）
    if amount_sources.get("error"):
        msgs.append(f"官方抓取錯誤：{amount_sources.get('error')}")

    return "；".join(msgs) + "。"


def build_arbiter_input(
    market: str,
    session: str,
    display_trade_date: Optional[pd.Timestamp],
    amount_pack: Dict,
    amount_norm_pack: Dict,
    us_df: pd.DataFrame,
    fx_df: pd.DataFrame,
    top_df: pd.DataFrame,
    holdings: List[str],
) -> Dict:
    inst_status, inst_dates_3d = decide_inst_status_free()
    amount_total = amount_pack.get("amount_total", None)

    degraded_mode = decide_degraded_mode(amount_total=amount_total, inst_status=inst_status)

    macro_overview = {
        "trade_date": str(display_trade_date.date()) if display_trade_date is not None else None,
        "amount_twse": fmt_bn(to_bn_twd(amount_pack.get("amount_twse"))) if amount_pack.get("amount_twse") else "待更新",
        "amount_tpex": fmt_bn(to_bn_twd(amount_pack.get("amount_tpex"))) if amount_pack.get("amount_tpex") else "待更新",
        "amount_total": fmt_bn(to_bn_twd(amount_pack.get("amount_total"))) if amount_pack.get("amount_total") else "待更新",
        "amount_sources": {
            "twse": amount_pack.get("sources", {}).get("twse"),
            "tpex": amount_pack.get("sources", {}).get("tpex"),
            "error": amount_pack.get("error"),
            "warning": amount_pack.get("warning"),
        },
        "progress": amount_norm_pack.get("progress"),
        "amount_norm_cum_ratio": amount_norm_pack.get("amount_norm_cum_ratio"),
        "amount_norm_slice_ratio": amount_norm_pack.get("amount_norm_slice_ratio"),
        "amount_norm_label": amount_norm_pack.get("amount_norm_label", "UNKNOWN"),
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "degraded_mode": degraded_mode,
        "data_mode": session,
        "market_comment": build_market_comment(
            amount_total=amount_total,
            inst_status=inst_status,
            degraded_mode=degraded_mode,
            data_mode=session,
            amount_sources=macro_overview["amount_sources"],
        ),
    }

    # Global 摘要（僅取關鍵欄位）
    def df_to_records(df: pd.DataFrame) -> List[Dict]:
        cols = [c for c in ["Name", "Symbol", "Date", "Close", "Chg%"] if c in df.columns]
        return df[cols].to_dict(orient="records")

    # Top list（含持倉 + 候選）
    top_records = []
    if not top_df.empty:
        for _, r in top_df.iterrows():
            sym = str(r.get("Symbol", ""))
            top_records.append({
                "Symbol": sym,
                "Name": str(r.get("Name", "")),
                "Price": safe_float(r.get("Close", np.nan), default=np.nan),
                "ranking": {
                    "rank": int(r.get("Rank", 0)) if not pd.isna(r.get("Rank", np.nan)) else None,
                    "tier": "A" if int(r.get("Rank", 999)) <= 20 else "B",
                    "top20_flag": True if int(r.get("Rank", 999)) <= 20 else False,
                },
                "Technical": {
                    "MA_Bias": round(safe_float(r.get("MA_Bias", 0.0)), 4),
                    "Vol_Ratio": round(safe_float(r.get("Vol_Ratio", 0.0)), 4),
                    "Score": round(safe_float(r.get("Score", 0.0)), 2),
                },
                "Institutional": {
                    "Inst_Status": "PENDING" if inst_status != "READY" else "READY"
                },
            })

    arb = {
        "meta": {
            "system": SYSTEM_NAME,
            "market": market,
            "timestamp": now_taipei().strftime("%Y-%m-%d %H:%M"),
            "session": session,
        },
        "macro": {
            "overview": macro_overview,
        },
        "global": {
            "us": df_to_records(us_df),
            "fx": df_to_records(fx_df),
        },
        "holdings": holdings,
        "stocks": top_records,
    }
    return arb


# ====== UI ======
def ui_header():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)


def ui_sidebar() -> Dict:
    st.sidebar.header("設定")

    market = st.sidebar.selectbox("Market", ["tw-share"], index=0)

    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=1)

    # 持倉：輸入代碼（逗號分隔）；支援 2330 自動補 .TW
    st.sidebar.subheader("持倉（會納入追蹤）")
    raw_hold = st.sidebar.text_area("輸入代碼（逗號分隔）", value="2330.TW", height=90)
    holdings = []
    for x in raw_hold.split(","):
        s = _normalize_symbol(x)
        if s:
            holdings.append(s)
    holdings = sorted(list(dict.fromkeys(holdings)))  # 去重且保序

    # SSL 仍然保留（但本版不強制繞過）
    verify_ssl = st.sidebar.checkbox("SSL 驗證（官方資料）", value=True)

    # 官方資料回溯天數：預留未來用（例如抓「最近可用交易日」）
    lookback = st.sidebar.slider("官方資料回溯天數", min_value=3, max_value=30, value=10, step=1)

    run = st.sidebar.button("Run")

    st.sidebar.caption("免費模擬期：法人資料（FinMind）不使用，避免 402。")
    return {
        "market": market,
        "session": session,
        "holdings": holdings,
        "verify_ssl": verify_ssl,
        "lookback": lookback,
        "run": run,
    }


def main():
    ui_header()
    opts = ui_sidebar()

    market = opts["market"]
    session = opts["session"]
    holdings = opts["holdings"]
    verify_ssl = opts["verify_ssl"]

    # === 主流程（每次刷新都跑；也可依 run 觸發，這裡不阻擋以保資料最新） ===
    now = now_taipei()
    st.info(f"目前台北時間：{now.strftime('%Y-%m-%d %H:%M')}｜模式：{session}（顯示『最近可用交易日』資料）")

    # 1) 讀取台股 Universe CSV（Top20）
    try:
        df = load_market_df(market)
        td = latest_trade_date(df)
        if td is None:
            st.error("台股資料檔存在，但 Date 欄位無有效日期。請確認 CSV 格式。")
            return
    except Exception as e:
        st.error(f"讀取台股資料失敗：{e}")
        return

    st.caption(f"顯示交易日：{td.date()}（從資料檔最新日期推定）")

    top20_df = build_top20(df, td)

    # 2) 國際市場（美股/匯率）— 最新可用交易日
    us_df, fx_df = build_global_tables()

    st.subheader("全球市場摘要（美股）— 最新可用交易日收盤")
    st.dataframe(us_df, use_container_width=True, hide_index=True)

    st.subheader("匯率（參考）— 最新可用交易日")
    st.dataframe(fx_df, use_container_width=True, hide_index=True)

    # 3) 成交金額（官方口徑：TWSE + TPEx）
    st.subheader("市場成交金額（官方口徑：TWSE + TPEx = amount_total）")

    amount_pack = fetch_official_amount(verify_ssl=verify_ssl)

    col1, col2, col3, col4 = st.columns(4)
    twse_bn = to_bn_twd(amount_pack.get("amount_twse"))
    tpex_bn = to_bn_twd(amount_pack.get("amount_tpex"))
    total_bn = to_bn_twd(amount_pack.get("amount_total"))

    col1.metric("TWSE 上市", fmt_bn(twse_bn))
    col2.metric("TPEx 上櫃", fmt_bn(tpex_bn))
    col3.metric("Total 合計", fmt_bn(total_bn))
    col4.metric("官方交易日（推定）", amount_pack.get("trade_date") or "未知")

    st.caption(f"來源：TWSE={amount_pack.get('sources', {}).get('twse')}｜TPEx={amount_pack.get('sources', {}).get('tpex')}")
    if amount_pack.get("error"):
        st.warning(f"官方抓取失敗（仍可顯示其他區塊）：{amount_pack.get('error')}")

    # 4) 量能正規化（盤中/盤後都可算；avg20 目前若你沒提供就會是 None）
    #   - 模擬期：以「沒有 avg20」視為 UNKNOWN，避免假精準
    avg20_amount_total_median = None  # 你若有 20D 中位數來源，放這裡
    amount_total_now = amount_pack.get("amount_total") or 0
    amount_norm = intraday_norm(
        amount_total_now=int(amount_total_now),
        amount_total_prev=None,
        avg20_amount_total=avg20_amount_total_median,
        now=now,
        alpha=0.65,
    )

    st.subheader("INTRADAY 量能正規化（避免早盤誤判 LOW）")
    st.json({
        "progress": amount_norm.get("progress"),
        "cum_ratio(穩健型用)": amount_norm.get("amount_norm_cum_ratio"),
        "slice_ratio(保守型用)": amount_norm.get("amount_norm_slice_ratio"),
        "label": amount_norm.get("amount_norm_label"),
    })

    # 5) Top list（Route A：Universe + 持倉合併 = 20 + N）
    st.subheader("Top List（Route A：Universe 排名 + 持倉合併）")

    # 合併持倉：若持倉不在 top20 內，補一列（以該日資料為主；找不到就留空）
    top_symbols = set(top20_df["Symbol"].tolist()) if not top20_df.empty else set()
    add_rows = []
    for sym in holdings:
        if sym in top_symbols:
            continue
        hit = df[(df["Date"].dt.normalize() == td) & (df["Symbol"] == sym)]
        if hit.empty:
            add_rows.append({
                "Symbol": sym,
                "Name": symbol_to_name(sym),
                "Date": td,
                "Close": np.nan,
                "Volume": np.nan,
                "Vol_Ratio": np.nan,
                "MA_Bias": np.nan,
                "Score": np.nan,
                "Rank": None,
            })
        else:
            r = hit.iloc[0].to_dict()
            r["Rank"] = None
            add_rows.append(r)

    if add_rows:
        add_df = pd.DataFrame(add_rows)
        # 欄位對齊
        for c in top20_df.columns:
            if c not in add_df.columns:
                add_df[c] = np.nan
        top_view = pd.concat([top20_df, add_df[top20_df.columns]], ignore_index=True)
    else:
        top_view = top20_df.copy()

    # 欄位整理（中文名稱欄）
    if "Name" not in top_view.columns:
        top_view["Name"] = top_view["Symbol"].map(symbol_to_name)

    # 統一 Date 顯示
    if "Date" in top_view.columns:
        top_view["Date"] = pd.to_datetime(top_view["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    show_cols = [c for c in [
        "Symbol", "Name", "Date", "Close", "Volume", "Vol_Ratio", "MA_Bias", "Score", "Rank"
    ] if c in top_view.columns]
    st.dataframe(top_view[show_cols], use_container_width=True, hide_index=True)

    # 6) Arbiter Input（可回溯 + 可複製）
    st.subheader("AI JSON（Arbiter Input）— 可回溯（模擬期免費）")

    arbiter_input = build_arbiter_input(
        market=market,
        session=session,
        display_trade_date=td,
        amount_pack=amount_pack,
        amount_norm_pack=amount_norm,
        us_df=us_df,
        fx_df=fx_df,
        top_df=top20_df,
        holdings=holdings,
    )

    # 人眼閱讀
    st.json(arbiter_input)

    # 工程用：可複製
    st.markdown("#### 📋 複製用 JSON（工程 / Agent / 回測）")
    arbiter_json_str = json.dumps(arbiter_input, ensure_ascii=False, indent=2)
    st.code(arbiter_json_str, language="json")
    st.caption("⬆️ 右上角 Copy 可直接複製，作為 Arbiter / Agent / 回測系統輸入。")


if __name__ == "__main__":
    main()
