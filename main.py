# main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

from analyzer import (
    run_analysis,
    generate_ai_json,
    SESSION_PREOPEN,
    SESSION_INTRADAY,
    SESSION_EOD,
)

TZ_TAIPEI = timezone(timedelta(hours=8))


# =========
# 工具：資料載入 / 名稱映射
# =========
def _load_market_csv(market: str) -> pd.DataFrame:
    """
    預設你 repo 新結構：data/xxx.csv
    相容舊結構：根目錄 data_tw-share.csv / data_tw.csv
    """
    candidates = [
        os.path.join("data", f"data_{market}.csv"),
        f"data_{market}.csv",
        os.path.join("data", "data_tw-share.csv"),
        "data_tw-share.csv",
        os.path.join("data", "data_tw.csv"),
        "data_tw.csv",
    ]
    for p in candidates:
        if os.path.exists(p):
            return pd.read_csv(p)
    raise FileNotFoundError("找不到市場資料檔。請確認 data/data_tw-share.csv 或 data_tw-share.csv 是否存在。")


def _load_name_map() -> Dict[str, str]:
    """
    優先讀取 configs/name_map.json（你可自行維護，避免 yfinance 限流）
    格式：{"2330.TW":"台積電", ...}
    """
    path = os.path.join("configs", "name_map.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                m = json.load(f)
            return {str(k): str(v) for k, v in m.items()}
        except Exception:
            return {}
    # 最小內建（你可自行擴充）
    return {
        "2330.TW": "台積電",
        "2317.TW": "鴻海",
        "2454.TW": "聯發科",
        "2382.TW": "廣達",
        "3231.TW": "緯創",
        "2308.TW": "台達電",
        "2603.TW": "長榮",
        "2609.TW": "陽明",
    }


def _today_taipei() -> datetime:
    return datetime.now(tz=TZ_TAIPEI)


def _fmt_date(dt) -> str:
    return pd.to_datetime(dt).strftime("%Y-%m-%d")


def _staleness_guard(trade_date_str: str, session: str) -> Tuple[bool, str]:
    """
    你要「最新資料」：若資料落後太多 → 直接 degraded_mode
    - 盤前：允許 trade_date = 最近一個交易日（通常是昨日）
    - 盤中/盤後：trade_date 不能落後太久（>1個自然日先保守降級）
    """
    now = _today_taipei()
    td = pd.to_datetime(trade_date_str).to_pydatetime().replace(tzinfo=TZ_TAIPEI)

    delta_days = (now.date() - td.date()).days

    if session == SESSION_PREOPEN:
        # 盤前：至少要是「最近一個交易日」，自然日差距通常 1（遇假日可能>1）
        if delta_days >= 4:
            return True, f"DATA_STALE_PREOPEN_{delta_days}D"
        return False, "OK"

    # 盤中/盤後：若落後 >=2 天 → 視為失真
    if delta_days >= 2:
        return True, f"DATA_STALE_{delta_days}D"
    return False, "OK"


def _merge_top20_plus_positions(df_top: pd.DataFrame, positions: List[dict]) -> pd.DataFrame:
    """
    最終分析清單 = Top20 + positions（去重）
    若持倉不在 Top20，也要拉進來，避免「買了台積電隔天沒入榜就不管」。
    """
    if not positions:
        return df_top

    pos_syms = []
    for p in positions:
        sym = str(p.get("symbol", "")).strip()
        if sym:
            pos_syms.append(sym)

    pos_syms = sorted(set(pos_syms))
    if not pos_syms:
        return df_top

    # 把持倉補進 df_top（不存在則新增一行，Price/Score 等先空白，後面用 market_df 最新價補）
    out = df_top.copy()
    existing = set(out["Symbol"].astype(str).tolist())
    missing = [s for s in pos_syms if s not in existing]

    if missing:
        add = pd.DataFrame({
            "Date": [out["Date"].iloc[0] if len(out) else "" for _ in missing],
            "Symbol": missing,
            "Price": [0.0 for _ in missing],
            "Volume": [0.0 for _ in missing],
            "MA_Bias": [0.0 for _ in missing],
            "Vol_Ratio": [0.0 for _ in missing],
            "Body_Power": [0.0 for _ in missing],
            "Score": [-9999.0 for _ in missing],
            "Tag": ["○觀察(持倉監控)" for _ in missing],
        })
        out = pd.concat([out, add], ignore_index=True)

    return out


def _fill_prices_from_market(df_list: pd.DataFrame, df_market: pd.DataFrame) -> pd.DataFrame:
    """
    對於「持倉補入」但沒有 Price 的列，用 market_df 最新 Close 補齊。
    """
    m = df_market.copy()
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce")
    m["Symbol"] = m["Symbol"].astype(str)
    m["Close"] = pd.to_numeric(m["Close"], errors="coerce")
    m = m.dropna(subset=["Date", "Symbol", "Close"]).sort_values(["Symbol", "Date"])
    last = m.groupby("Symbol", as_index=False).tail(1)[["Symbol", "Close"]].copy()
    price_map = dict(zip(last["Symbol"].tolist(), last["Close"].tolist()))

    out = df_list.copy()
    out["Price"] = out.apply(
        lambda r: float(price_map.get(str(r["Symbol"]), r.get("Price", 0.0))) if float(r.get("Price", 0.0)) <= 0 else float(r["Price"]),
        axis=1
    )
    return out


def app():
    st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
    st.title("Sunhero｜股市智能超盤中控台（Top20 + 持倉監控 / V15.7 SIM-FREE）")

    # =========
    # Sidebar：模式/市場/TopN
    # =========
    market = st.sidebar.selectbox("Market", ["tw-share", "tw"], index=0)
    session = st.sidebar.selectbox("Session", [SESSION_PREOPEN, SESSION_INTRADAY, SESSION_EOD], index=0)
    topn = st.sidebar.selectbox("TopN（固定追蹤數量）", [20, 30, 50], index=0)

    st.sidebar.markdown("---")
    st.sidebar.subheader("大盤指數輸入（你指定要寫入 macro）")

    # 盤前：輸入昨日/最後收盤日大盤
    idx_level = st.sidebar.number_input("Index Level（大盤指數）", value=0.0, step=1.0, format="%.2f")
    idx_change = st.sidebar.number_input("Index Change（漲跌點數）", value=0.0, step=1.0, format="%.2f")

    st.sidebar.markdown("---")
    st.sidebar.subheader("帳戶/持倉（模擬用）")

    cash_balance = st.sidebar.number_input("cash_balance（NTD）", value=2_000_000, step=10_000)
    total_equity = st.sidebar.number_input("total_equity（NTD）", value=2_000_000, step=10_000)

    default_positions = [
        # {"symbol":"2330.TW","shares":1000,"avg_cost":1500,"entry_date":"2026-01-15","status":"HOLD","sector":"Semiconductor"}
    ]
    positions_text = st.sidebar.text_area(
        "positions（JSON array）",
        value=json.dumps(default_positions, ensure_ascii=False, indent=2),
        height=180
    )

    run_btn = st.sidebar.button("Run")

    if not run_btn:
        st.info("按左側 Run：會產生『Top20 + 持倉』清單與 Arbiter JSON。")
        return

    # =========
    # 1) Load market data
    # =========
    df_market = _load_market_csv(market)

    df_market["Date"] = pd.to_datetime(df_market["Date"], errors="coerce")
    latest_date = df_market["Date"].max()
    trade_date = _fmt_date(latest_date)

    # =========
    # 2) Top20 analysis（全市場掃描）
    # =========
    from analyzer import AnalyzerConfig
    cfg = AnalyzerConfig(topn=int(topn))
    df_top, err = run_analysis(df_market, session=session, cfg=cfg)
    if err:
        st.error(f"Analyzer error: {err}")
        return

    # =========
    # 3) Parse positions
    # =========
    try:
        positions = json.loads(positions_text) if positions_text.strip() else []
        if not isinstance(positions, list):
            raise ValueError("positions 必須是 JSON array")
    except Exception as e:
        st.error(f"positions JSON 解析失敗：{type(e).__name__}: {str(e)}")
        return

    account = {
        "cash_balance": int(cash_balance),
        "total_equity": int(total_equity),
        "positions": positions
    }

    # =========
    # 4) Top20 + positions（去重後一起分析）
    # =========
    df_list = _merge_top20_plus_positions(df_top, positions)
    df_list = _fill_prices_from_market(df_list, df_market)

    # =========
    # 5) 名稱（中文）
    # =========
    name_map = _load_name_map()
    df_list["Name"] = df_list["Symbol"].astype(str).map(lambda s: name_map.get(s, s))

    # =========
    # 6) Macro：大盤指數與漲跌幅（你指定的 2/3/4）
    # =========
    # 你要求：
    # - 盤前：輸入昨日(最後收盤日)大盤與漲跌幅
    # - 盤中：輸入最新大盤與漲跌幅
    # - 盤後：輸入當日大盤與漲跌幅
    # 這裡統一由 UI 輸入寫入 macro（因為你要「最新可靠」，不要自動亂抓）
    degraded_by_stale, stale_reason = _staleness_guard(trade_date, session)

    macro_overview = {
        "trade_date": trade_date,
        "data_mode": session,
        "index_level": float(idx_level),
        "index_change": float(idx_change),

        # 量能/法人（免費模擬期：允許 UNAVAILABLE，不讓系統爆掉）
        "amount_total": "UNAVAILABLE(FREE_SIM)",
        "inst_status": "UNAVAILABLE(FREE_SIM)",
        "inst_dates_3d": [],
        "data_date_finmind": None,

        # 系統旗標（保留你的風控入口）
        "kill_switch": False,
        "v14_watch": False,
        "degraded_mode": bool(degraded_by_stale),  # 最新性不夠就降級
        "degraded_reason": stale_reason,
    }

    # market_comment 僅供人類讀，不應成為 arbiter decision 依據
    if degraded_by_stale:
        macro_overview["market_comment"] = (
            f"資料日期 {trade_date} 與目前時間落差過大（{stale_reason}），"
            "為避免用舊數據導致裁決失真，已啟動資料降級：禁止 BUY/TRIAL。"
        )
    else:
        macro_overview["market_comment"] = (
            f"{session} 模式：大盤 {idx_level:,.2f} 點、漲跌 {idx_change:+,.2f}。"
            "Top20 以全市場相對排名每日更新；持倉會額外加入監控。"
        )

    macro_data = {
        "overview": macro_overview,
        "indices": []
    }

    # =========
    # 7) Generate Arbiter JSON
    # =========
    json_text = generate_ai_json(
        df_top=df_list[[
            "Date","Symbol","Price","Volume","MA_Bias","Vol_Ratio","Body_Power","Score","Tag"
        ]].copy(),
        market=market,
        session=session,
        macro_data=macro_data,
        name_map={k: v for k, v in name_map.items()},
        account=account,
    )

    # =========
    # UI output
    # =========
    st.subheader("1) 今日分析清單（Top20 + 持倉監控）")
    st.caption(f"資料日期：{trade_date}｜Session：{session}｜TopN：{topn}｜清單筆數：{len(df_list)}（Top20 + positions 去重後）")
    st.dataframe(df_list[["Symbol","Name","Price","MA_Bias","Vol_Ratio","Score","Tag"]], use_container_width=True)

    st.subheader("2) Macro（你指定要補的大盤指數與漲跌幅）")
    st.json(macro_overview)

    st.subheader("3) AI JSON (Arbiter Input)")
    st.code(json_text, language="json")

    outname = f"ai_payload_{market}_{trade_date.replace('-', '')}_{session.lower()}.json"
    with open(outname, "w", encoding="utf-8") as f:
        f.write(json_text)
    st.success(f"JSON 已輸出：{outname}")


if __name__ == "__main__":
    app()
