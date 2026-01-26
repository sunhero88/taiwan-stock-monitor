# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional, Tuple

import pandas as pd
import streamlit as st

from analyzer import run_analysis, generate_ai_json, SESSION_INTRADAY, SESSION_EOD
from finmind_institutional import fetch_finmind_institutional
from institutional_utils import calc_inst_3d

# 盤中/盤後成交金額（TWSE+TPEx 合計）
# 你已經有 market_amount.py（含 intraday_norm / fetch_amount_total 等）
from market_amount import (
    fetch_amount_total,
    intraday_norm,
)

# =========================
# 基本設定
# =========================
TZ_TAIPEI = timezone(timedelta(hours=8))
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

AMOUNT_HISTORY_CSV = DATA_DIR / "amount_history_tw.csv"           # 日資料（EOD 或盤中快照都可記）
AMOUNT_INTRADAY_CACHE = DATA_DIR / "amount_intraday_cache.json"   # 盤中切片用（前一次快照）

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


def _now_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _fmt_date(dt) -> str:
    try:
        return pd.to_datetime(dt).strftime("%Y-%m-%d")
    except Exception:
        return _now_taipei().strftime("%Y-%m-%d")


def _repo_paths_for_market_csv(market: str) -> list[Path]:
    """
    你 repo 已經把檔案逐步移到 data/ 資料夾，因此要同時支援：
    - data/data_tw-share.csv（優先）
    - data/data_tw.csv
    - 根目錄 data_tw-share.csv（相容舊版）
    - 根目錄 data_tw.csv
    """
    fname = f"data_{market}.csv"
    return [
        DATA_DIR / fname,
        DATA_DIR / "data_tw-share.csv",
        DATA_DIR / "data_tw.csv",
        ROOT / fname,
        ROOT / "data_tw-share.csv",
        ROOT / "data_tw.csv",
    ]


def _load_market_csv(market: str) -> pd.DataFrame:
    candidates = _repo_paths_for_market_csv(market)
    for p in candidates:
        if p.exists():
            df = pd.read_csv(p)
            return df
    raise FileNotFoundError(
        "找不到市場資料檔。已嘗試：\n" + "\n".join([str(p) for p in candidates])
    )


def _apply_shipping_valuation_overrides(df_top: pd.DataFrame) -> pd.DataFrame:
    """
    將 SHIPPING_VALUATION 以 Overlay 形式注入，不覆蓋原本 Structure 欄位。
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

    out = out.apply(_inject, axis=1)
    return out


# =========================
# 成交金額：歷史中位數（20日）
# =========================
def _read_amount_history() -> pd.DataFrame:
    if not AMOUNT_HISTORY_CSV.exists():
        return pd.DataFrame(columns=["date", "amount_twse", "amount_tpex", "amount_total"])
    try:
        df = pd.read_csv(AMOUNT_HISTORY_CSV)
        df["date"] = df["date"].astype(str)
        for c in ["amount_twse", "amount_tpex", "amount_total"]:
            df[c] = pd.to_numeric(df.get(c), errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame(columns=["date", "amount_twse", "amount_tpex", "amount_total"])


def _append_amount_history(date_str: str, twse: int, tpex: int, total: int) -> None:
    df = _read_amount_history()
    # 同一天只保留最新一筆
    df = df[df["date"] != date_str].copy()
    df = pd.concat(
        [
            df,
            pd.DataFrame(
                [{"date": date_str, "amount_twse": twse, "amount_tpex": tpex, "amount_total": total}]
            ),
        ],
        ignore_index=True,
    )
    df.to_csv(AMOUNT_HISTORY_CSV, index=False, encoding="utf-8")


def _avg20_median_amount_total() -> Optional[int]:
    df = _read_amount_history()
    if df.empty:
        return None
    last20 = df.sort_values("date").tail(20)
    if last20.empty:
        return None
    med = int(last20["amount_total"].median())
    return med if med > 0 else None


# =========================
# 盤中切片快照（保守型用）
# =========================
@dataclass
class IntradayCache:
    ts_iso: str
    amount_total: int


def _load_intraday_cache() -> Optional[IntradayCache]:
    if not AMOUNT_INTRADAY_CACHE.exists():
        return None
    try:
        d = json.loads(AMOUNT_INTRADAY_CACHE.read_text(encoding="utf-8"))
        return IntradayCache(ts_iso=str(d.get("ts_iso")), amount_total=int(d.get("amount_total", 0)))
    except Exception:
        return None


def _save_intraday_cache(amount_total: int) -> None:
    payload = {"ts_iso": _now_taipei().isoformat(), "amount_total": int(amount_total)}
    AMOUNT_INTRADAY_CACHE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _prev_amount_for_slice(now: datetime) -> Optional[int]:
    """
    保守型要看「切片量」：需要前一筆 amount_total。
    若快照太久（>30分鐘），就不拿來算切片，避免切片失真。
    """
    c = _load_intraday_cache()
    if not c:
        return None
    try:
        prev_ts = datetime.fromisoformat(c.ts_iso)
        age_min = (now - prev_ts).total_seconds() / 60.0
        if age_min <= 30:
            return int(c.amount_total)
        return None
    except Exception:
        return None


# =========================
# 法人資料整合（FinMind）
# =========================
def _merge_institutional_into_df_top(df_top: pd.DataFrame, inst_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    df_out = df_top.copy()
    inst_map: dict[str, dict[str, Any]] = {}

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


def _decide_inst_status_and_dates(inst_df: pd.DataFrame, symbols: list[str], trade_date: str) -> tuple[str, list[str]]:
    """
    V15.7 定義：
    - READY：至少有一檔能算出 Inst_Status=READY（代表三日資料齊）
    - PENDING：FinMind 有回資料，但不足以達 READY（常見：盤中/資料尚未齊）
    - UNAVAILABLE：抓取失敗或空資料（API 失效/被擋/錯誤）
    """
    if inst_df is None or inst_df.empty:
        return "UNAVAILABLE", []

    ready_any = False
    for sym in symbols:
        r = calc_inst_3d(inst_df, symbol=sym, trade_date=trade_date)
        if r.get("Inst_Status") == "READY":
            ready_any = True
            break

    # 取最近三個交易日（就算不是連續，也用於資訊呈現）
    dates_3d: list[str] = []
    try:
        if "date" in inst_df.columns:
            dates_3d = sorted(inst_df["date"].astype(str).unique().tolist())[-3:]
    except Exception:
        dates_3d = []

    return ("READY" if ready_any else "PENDING"), dates_3d


def _finmind_data_date(inst_df: pd.DataFrame) -> Optional[str]:
    try:
        if inst_df is None or inst_df.empty or "date" not in inst_df.columns:
            return None
        return str(pd.to_datetime(inst_df["date"], errors="coerce").max().date())
    except Exception:
        return None


# =========================
# V15.7：市場敘述（人類用，Arbiter 必須忽略）
# =========================
def generate_market_comment_retail(macro_overview: dict) -> str:
    """
    注意：這段只給人類看；Arbiter 必須完全忽略 market_comment（你已在規則中明確要求）。
    """
    kill_switch = bool(macro_overview.get("kill_switch", False))
    v14_watch = bool(macro_overview.get("v14_watch", False))
    degraded_mode = bool(macro_overview.get("degraded_mode", False))

    amount_total = macro_overview.get("amount_total", "待更新")
    amount_norm_label = macro_overview.get("amount_norm_label", "UNKNOWN")

    inst_status = macro_overview.get("inst_status", "UNAVAILABLE")

    if kill_switch or v14_watch:
        return "系統風控已啟動（禁止進場）。"

    # 成交金額呈現（億）
    amt_text = "成交金額待更新"
    try:
        if isinstance(amount_total, str) and amount_total != "待更新":
            amt = float(str(amount_total).replace(",", ""))
            amt_text = f"成交金額約 {amt/1e8:,.0f} 億"
        elif isinstance(amount_total, (int, float)) and amount_total > 0:
            amt_text = f"成交金額約 {float(amount_total)/1e8:,.0f} 億"
    except Exception:
        amt_text = "成交金額待更新"

    # 量能標籤（盤中正規化後）
    vol_text = ""
    if amount_norm_label in ("LOW", "NORMAL", "HIGH"):
        vol_text = f"（量能正規化：{amount_norm_label}）"

    # 法人狀態
    if inst_status == "READY":
        inst_text = "法人資料可用。"
    elif inst_status == "PENDING":
        inst_text = "法人資料未齊。"
    else:
        inst_text = "法人資料不可用。"

    # 降級模式
    if degraded_mode:
        strat = "裁決層已進入資料降級：禁止 BUY/TRIAL。"
    else:
        strat = "裁決層允許正常評估（仍以個股條件為主）。"

    return f"{amt_text}{vol_text}；{inst_text}{strat}"


# =========================
# V15.7：狀態判定區塊（可直接覆蓋）
# =========================
def build_v15_7_status_block(
    session: str,
    trade_date: str,
    df_top_symbols: list[str],
) -> tuple[dict, pd.DataFrame]:
    """
    產出：
    - macro_overview（含 inst_status / degraded_mode / amount_total 等）
    - inst_df（個股法人原始資料，供後續 merge）
    """
    now = _now_taipei()

    # ---------- A) 成交金額（TWSE+TPEx 合計） ----------
    amount_twse = "待更新"
    amount_tpex = "待更新"
    amount_total = "待更新"
    amount_sources: dict[str, Any] = {"twse": None, "tpex": None, "error": None}

    try:
        amt = fetch_amount_total()  # MarketAmount
        amount_twse = f"{amt.amount_twse:,d}"
        amount_tpex = f"{amt.amount_tpex:,d}"
        amount_total = f"{amt.amount_total:,d}"
        amount_sources["twse"] = amt.source_twse
        amount_sources["tpex"] = amt.source_tpex

        # 記錄歷史（不分盤中/盤後；用來估 20 日中位數）
        _append_amount_history(
            date_str=trade_date,
            twse=amt.amount_twse,
            tpex=amt.amount_tpex,
            total=amt.amount_total,
        )
    except Exception as e:
        amount_sources["error"] = f"{type(e).__name__}: {str(e)}"

    # 20日中位數（用 amount_total）
    avg20_median = _avg20_median_amount_total()
    avg20_amount_total_median = None if avg20_median is None else int(avg20_median)

    # 盤中正規化（避免動不動就 LOW）
    amount_norm = {
        "progress": None,
        "amount_norm_cum_ratio": None,
        "amount_norm_slice_ratio": None,
        "amount_norm_label": "UNKNOWN",
        "method_note": None,
    }

    # amount_total_now（int）才算得出 norm
    amount_total_now_int: Optional[int] = None
    try:
        if isinstance(amount_total, str) and amount_total != "待更新":
            amount_total_now_int = int(str(amount_total).replace(",", ""))
        elif isinstance(amount_total, (int, float)) and float(amount_total) > 0:
            amount_total_now_int = int(float(amount_total))
    except Exception:
        amount_total_now_int = None

    # 盤中：保守型看切片、穩健型看累積、試投型忽略
    # 這裡先把兩個 ratio 都算出來，讓 Arbiter（或人類）依情境取用
    if session == SESSION_INTRADAY and amount_total_now_int is not None and avg20_median:
        prev = _prev_amount_for_slice(now)
        amount_norm = intraday_norm(
            amount_total_now=amount_total_now_int,
            amount_total_prev=prev,
            avg20_amount_total=avg20_median,
            now=now,
            alpha=0.65,
        )
        # 更新快照，供下一次切片
        _save_intraday_cache(amount_total_now_int)

    # ---------- B) 法人資料 ----------
    inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])
    inst_fetch_error: Optional[str] = None

    # 盤中也抓，但抓不到就 UNAVAILABLE
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    end_date = trade_date

    try:
        inst_df = fetch_finmind_institutional(
            symbols=df_top_symbols,
            start_date=start_date,
            end_date=end_date,
            token=os.getenv("FINMIND_TOKEN", None),
        )
    except Exception as e:
        inst_fetch_error = f"{type(e).__name__}: {str(e)}"
        inst_df = pd.DataFrame(columns=["date", "symbol", "net_amount"])

    inst_status, inst_dates_3d = _decide_inst_status_and_dates(inst_df, df_top_symbols, trade_date)
    data_date_finmind = _finmind_data_date(inst_df)

    # 若明確是「付費/授權/被擋」類錯誤，強制 UNAVAILABLE
    if inst_fetch_error and any(k in inst_fetch_error for k in ["402", "Payment Required", "403", "Unauthorized"]):
        inst_status = "UNAVAILABLE"
        inst_dates_3d = []

    # ---------- C) V15.7 Data Health Gate（狀態 → 降級） ----------
    # 你規則寫得很硬：任一條件成立 → 降級
    # 這裡先落地兩個「最關鍵且可回溯」的判定：
    # 1) inst_status != READY → 降級（包含 UNAVAILABLE/PENDING）
    # 2) data_date_finmind != trade_date → 降級（如果有 data_date_finmind）
    kill_switch = False
    v14_watch = False

    cond_inst_not_ready = (inst_status != "READY")
    cond_date_mismatch = (data_date_finmind is not None and str(data_date_finmind) != str(trade_date))

    degraded_mode = bool(cond_inst_not_ready or cond_date_mismatch or kill_switch or v14_watch)

    macro_overview = {
        # 成交金額：上市+上櫃合計（你要求 amount_total 作為主值）
        "amount_twse": amount_twse,
        "amount_tpex": amount_tpex,
        "amount_total": amount_total,
        "amount_sources": {
            "twse": amount_sources.get("twse"),
            "tpex": amount_sources.get("tpex"),
            "error": amount_sources.get("error"),
        },
        "avg20_amount_total_median": avg20_amount_total_median,

        # 盤中量能正規化（兩種 ratio 都提供）
        "progress": amount_norm.get("progress"),
        "amount_norm_cum_ratio": amount_norm.get("amount_norm_cum_ratio"),
        "amount_norm_slice_ratio": amount_norm.get("amount_norm_slice_ratio"),
        "amount_norm_label": amount_norm.get("amount_norm_label"),

        # 法人
        "inst_net": "A:0.00億 | B:0.00億",
        "trade_date": trade_date,
        "inst_status": inst_status,
        "inst_dates_3d": inst_dates_3d,
        "data_date_finmind": data_date_finmind,

        # 系統旗標
        "kill_switch": kill_switch,
        "v14_watch": v14_watch,
        "degraded_mode": degraded_mode,
        "data_mode": "INTRADAY" if session == SESSION_INTRADAY else "EOD",
    }

    # market_comment：只給人類看（Arbiter 必須忽略）
    macro_overview["market_comment"] = generate_market_comment_retail(macro_overview)

    # 為了相容舊前端：保留 amount 欄位（指向 amount_total）
    macro_overview["amount"] = amount_total

    return macro_overview, inst_df


# =========================
# Streamlit App
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

    # 1) Load market data（你的 repo 可能會沒有 indices，所以這裡只用於 analyzer）
    try:
        df = _load_market_csv(market)
    except Exception as e:
        st.error(f"市場資料讀取失敗：{type(e).__name__}: {str(e)}")
        st.stop()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    latest_date = df["Date"].max() if "Date" in df.columns else _now_taipei()
    trade_date = _fmt_date(latest_date)

    # 2) Run analyzer（Top 清單）
    df_top, err = run_analysis(df, session=session)
    if err:
        st.error(f"Analyzer error: {err}")
        st.stop()

    # 2.1) 航運估值 overlay
    df_top = _apply_shipping_valuation_overrides(df_top)

    # 3) V15.7 狀態判定（可直接覆蓋的區塊）
    symbols = df_top["Symbol"].astype(str).tolist()
    macro_overview, inst_df = build_v15_7_status_block(
        session=session,
        trade_date=trade_date,
        df_top_symbols=symbols,
    )

    # 4) Merge institutional into df_top（若 UNAVAILABLE/PENDING，calc_inst_3d 會回 PENDING）
    df_top2 = _merge_institutional_into_df_top(df_top, inst_df, trade_date=trade_date)

    macro_data = {"overview": macro_overview, "indices": []}

    # 5) Generate JSON for Arbiter
    json_text = generate_ai_json(df_top2, market=market, session=session, macro_data=macro_data)

    # =========================
    # UI
    # =========================
    st.subheader("今日市場狀態（人類閱讀版；Arbiter 必須忽略 market_comment）")
    st.info(macro_overview.get("market_comment", ""))

    # 關鍵數據透明化（可回溯）
    st.subheader("關鍵狀態與數據（可回溯）")
    st.write(
        {
            "trade_date": macro_overview.get("trade_date"),
            "data_mode": macro_overview.get("data_mode"),
            "amount_twse": macro_overview.get("amount_twse"),
            "amount_tpex": macro_overview.get("amount_tpex"),
            "amount_total": macro_overview.get("amount_total"),
            "avg20_amount_total_median": macro_overview.get("avg20_amount_total_median"),
            "progress": macro_overview.get("progress"),
            "amount_norm_label": macro_overview.get("amount_norm_label"),
            "amount_norm_cum_ratio": macro_overview.get("amount_norm_cum_ratio"),
            "amount_norm_slice_ratio": macro_overview.get("amount_norm_slice_ratio"),
            "inst_status": macro_overview.get("inst_status"),
            "inst_dates_3d": macro_overview.get("inst_dates_3d"),
            "data_date_finmind": macro_overview.get("data_date_finmind"),
            "degraded_mode": macro_overview.get("degraded_mode"),
            "amount_sources": macro_overview.get("amount_sources"),
        }
    )

    # 航運估值卡（只顯示命中者）
    hit = df_top2[df_top2["Symbol"].isin(list(SHIPPING_VALUATION.keys()))].copy()
    if not hit.empty:
        st.subheader("航運股估值快照（財報狗/玩股網；Overlay）")
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

    # Save JSON
    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    outpath = DATA_DIR / outname
    outpath.write_text(json_text, encoding="utf-8")
    st.success(f"JSON 已輸出：{outpath}")


if __name__ == "__main__":
    app()
