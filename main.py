# -*- coding: utf-8 -*-
"""
Filename: main.py
Version: Predator V15.5.5 (Inst Fix + FinMind TradeDate + CacheKey)
Key Fix:
1) FinMind trade_date auto-detect (avoid wrong date / empty EOD)
2) Cache key binds to trade_date (avoid stale empty cache)
3) FinMind response diagnostics (msg / count / status)
4) Inst merge hardening (Symbol normalization)
"""

import os
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
from datetime import datetime, timedelta
import pytz

TW_TZ = pytz.timezone("Asia/Taipei")

# ---------------------------
# Optional: FinMind API token
# ---------------------------
# Streamlit Cloud: set in Secrets as:
# FINMIND_TOKEN = "YOUR_TOKEN"
FINMIND_TOKEN = None
try:
    FINMIND_TOKEN = st.secrets.get("FINMIND_TOKEN", None)
except Exception:
    FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN")


# ======================================================
# FinMind low-level helper (with diagnostics)
# ======================================================
def finmind_get(dataset: str, params: dict, timeout: int = 6):
    """
    Returns: (ok: bool, payload: dict, diag: dict)
    diag includes http_status, msg, data_len
    """
    url = "https://api.finmindtrade.com/api/v4/data"
    q = dict(params)
    q["dataset"] = dataset

    # attach token if provided
    if FINMIND_TOKEN:
        q["token"] = FINMIND_TOKEN

    diag = {"http_status": None, "msg": None, "data_len": 0, "dataset": dataset}

    try:
        r = requests.get(url, params=q, timeout=timeout)
        diag["http_status"] = r.status_code
        payload = r.json()

        diag["msg"] = payload.get("msg")
        data = payload.get("data") or []
        diag["data_len"] = len(data)

        ok = (payload.get("msg") == "success") and (len(data) > 0)
        return ok, payload, diag
    except Exception as e:
        diag["msg"] = f"exception:{type(e).__name__}"
        return False, {}, diag


# ======================================================
# 1) Trade date detection (critical)
# ======================================================
@st.cache_data(ttl=120, show_spinner=False)
def detect_finmind_trade_date(lookback_days: int = 10) -> str:
    """
    Use TAIEX index price dataset to get latest available trading date from FinMind.
    Returns 'YYYY-MM-DD' or '' if failed.
    """
    end = datetime.now(TW_TZ).date()
    start = end - timedelta(days=lookback_days)

    ok, payload, _ = finmind_get(
        "TaiwanStockPrice",
        {
            "data_id": "TAIEX",
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
        },
        timeout=6,
    )
    if not ok:
        return ""

    df = pd.DataFrame(payload.get("data", []))
    if df.empty or "date" not in df.columns:
        return ""

    # FinMind returns date as string
    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return ""

    return str(df.iloc[-1]["date"])


# ======================================================
# 2) Indices (yfinance) - keep as you had (stable)
# ======================================================
@st.cache_data(ttl=60, show_spinner=False)
def fetch_detailed_indices() -> pd.DataFrame:
    tickers = {
        "^TWII": "TW 加權指數",
        "^TWOII": "TW 櫃買指數",
        "^SOX": "US 費城半導體",
        "^DJI": "US 道瓊工業",
    }

    rows = []
    for ticker, name in tickers.items():
        try:
            hist = yf.Ticker(ticker).history(period="6d").dropna()
            if hist.empty or len(hist) < 2:
                raise ValueError("history too short")

            latest = hist.iloc[-1]
            prev = hist.iloc[-2]

            price = float(latest["Close"])
            prev_close = float(prev["Close"])
            change = price - prev_close
            pct = (change / prev_close) * 100 if prev_close != 0 else 0.0

            rows.append({
                "指數名稱": name,
                "現價": f"{price:,.0f}",
                "漲跌": f"{change:+.2f}",
                "幅度": f"{pct:+.2f}%",
                "開盤": f"{float(latest.get('Open', 0.0)):,.0f}",
                "最高": f"{float(latest.get('High', 0.0)):,.0f}",
                "最低": f"{float(latest.get('Low', 0.0)):,.0f}",
                "昨收": f"{prev_close:,.0f}",
            })
        except Exception:
            rows.append({
                "指數名稱": name,
                "現價": "-",
                "漲跌": "-",
                "幅度": "-",
                "開盤": "-",
                "最高": "-",
                "最低": "-",
                "昨收": "-",
            })

    return pd.DataFrame(rows)


# ======================================================
# 3) Macro: amount & total inst (FinMind) - cache by trade_date
# ======================================================
@st.cache_data(ttl=120, show_spinner=False)
def fetch_market_amount(trade_date: str) -> tuple[str, dict]:
    """
    Returns (value_str, diag)
    """
    if not trade_date:
        return "待更新", {"msg": "trade_date_empty", "data_len": 0, "http_status": None}

    ok, payload, diag = finmind_get(
        "TaiwanStockPrice",
        {"data_id": "TAIEX", "date": trade_date},
        timeout=6,
    )
    if not ok:
        return "待更新", diag

    row = (payload.get("data") or [])[0]
    money = row.get("Trading_Money", None)
    if money is None:
        return "待更新", diag

    return f"{float(money) / 100000000:.0f} 億", diag


@st.cache_data(ttl=120, show_spinner=False)
def fetch_market_total_inst(trade_date: str) -> tuple[str, dict]:
    """
    Returns (value_str, diag)
    """
    if not trade_date:
        return "待更新", {"msg": "trade_date_empty", "data_len": 0, "http_status": None}

    ok, payload, diag = finmind_get(
        "TaiwanStockTotalInstitutionalInvestors",
        {"date": trade_date},
        timeout=6,
    )
    if not ok:
        return "待更新", diag

    df = pd.DataFrame(payload.get("data", []))
    if df.empty or ("buy" not in df.columns) or ("sell" not in df.columns):
        return "待更新", diag

    net = (df["buy"].sum() - df["sell"].sum()) / 100000000
    return (f"+{net:.1f} 億" if net > 0 else f"{net:.1f} 億"), diag


# ======================================================
# 4) Per-stock institutional (FinMind) - cache by trade_date
# ======================================================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_inst_data_finmind_stock(trade_date: str) -> tuple[pd.DataFrame, dict]:
    """
    Returns (df_inst, diag)
    df_inst columns: Symbol, Inst_Net
    """
    if not trade_date:
        return pd.DataFrame(), {"msg": "trade_date_empty", "data_len": 0, "http_status": None}

    ok, payload, diag = finmind_get(
        "TaiwanStockInstitutionalInvestorsBuySell",
        {"date": trade_date},
        timeout=8,
    )
    if not ok:
        return pd.DataFrame(), diag

    df = pd.DataFrame(payload.get("data", []))
    if df.empty or ("stock_id" not in df.columns):
        return pd.DataFrame(), diag

    # Net in shares
    df["Net"] = pd.to_numeric(df.get("buy", 0), errors="coerce").fillna(0) - pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
    g = df.groupby("stock_id")["Net"].sum().reset_index()
    g.columns = ["stock_id", "Inst_Net"]

    # normalize symbol key to match yfinance symbols: 2330.TW
    g["Symbol"] = g["stock_id"].astype(str).str.upper().str.strip() + ".TW"
    g["Inst_Net"] = pd.to_numeric(g["Inst_Net"], errors="coerce").fillna(0.0)

    out = g[["Symbol", "Inst_Net"]].copy()
    return out, diag


# ======================================================
# 5) OHLCV (yfinance) + Inst merge (hardening)
# ======================================================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_data(market_id: str, trade_date: str) -> pd.DataFrame:
    targets = {
        "tw-share": [
            "2330.TW", "2317.TW", "2454.TW", "2308.TW",
            "2382.TW", "3231.TW", "2376.TW", "6669.TW",
            "2603.TW", "2609.TW", "2408.TW", "2303.TW",
            "2881.TW", "2882.TW", "2357.TW", "3035.TW",
        ],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"],
    }
    symbols = targets.get(market_id, targets["tw-share"])

    raw = yf.download(
        symbols,
        period="2mo",
        interval="1d",
        group_by="ticker",
        progress=False,
        threads=True,
    )

    inst_df, _ = fetch_inst_data_finmind_stock(trade_date) if market_id == "tw-share" else (pd.DataFrame(), {})

    all_res = []

    # strict structure detect
    multi = isinstance(raw.columns, pd.MultiIndex)
    if len(symbols) > 1 and not multi:
        return pd.DataFrame()

    for s in symbols:
        try:
            if multi:
                if s not in list(raw.columns.levels[0]):
                    continue
                s_df = raw[s].copy().dropna()
            else:
                s_df = raw.copy().dropna()

            if s_df.empty:
                continue

            s_df["Symbol"] = str(s).upper().strip()

            if market_id == "tw-share":
                # merge inst
                if not inst_df.empty:
                    match = inst_df.loc[inst_df["Symbol"] == s_df["Symbol"].iloc[0], "Inst_Net"]
                    if not match.empty:
                        net_val = float(match.values[0])
                    else:
                        net_val = 0.0
                else:
                    net_val = 0.0

                s_df["Inst_Net"] = net_val
                val_k = round(net_val / 1000.0, 1)
                # keep explicit sign for quick visual
                if net_val > 0:
                    s_df["Inst_Status"] = f"+{val_k}k"
                elif net_val < 0:
                    s_df["Inst_Status"] = f"{val_k}k"
                else:
                    s_df["Inst_Status"] = "0.0k"

            all_res.append(s_df)
        except Exception:
            continue

    if not all_res:
        return pd.DataFrame()

    out = pd.concat(all_res).reset_index()
    if "Datetime" in out.columns and "Date" not in out.columns:
        out = out.rename(columns={"Datetime": "Date"})

    # normalize required fields for analyzer
    if "Date" not in out.columns:
        return pd.DataFrame()

    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out["Symbol"] = out["Symbol"].astype(str).str.upper().str.strip()
    return out


# ======================================================
# UI
# ======================================================
st.set_page_config(page_title="Predator V15.5.5", layout="wide")
st.title("Predator 指揮中心 V15.5.5（Inst Fix + FinMind TradeDate + Rev_Growth）")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("啟動全域掃描與結構分析"):
    try:
        with st.spinner("執行中：技術面篩選 → 籌碼合併 → 結構面掃描"):
            # 0) detect trade_date from FinMind
            trade_date = detect_finmind_trade_date(lookback_days=14)

            # 1) macro
            indices_df = fetch_detailed_indices()

            total_amount, diag_amt = fetch_market_amount(trade_date)
            total_inst, diag_inst = fetch_market_total_inst(trade_date)

            # 2) per-stock inst diag
            inst_df, diag_inst_stock = fetch_inst_data_finmind_stock(trade_date) if market == "tw-share" else (pd.DataFrame(), {})

            # 3) ohlcv + merge
            full_df = fetch_market_data(market, trade_date)

            # --- sidebar diagnostics (critical for Cloud) ---
            st.sidebar.markdown("### FinMind 診斷")
            st.sidebar.write(f"trade_date: {trade_date if trade_date else 'N/A'}")
            st.sidebar.write({"amount": diag_amt, "total_inst": diag_inst, "stock_inst": diag_inst_stock})
            if market == "tw-share":
                st.sidebar.write(f"stock_inst rows: {len(inst_df):,}")

            # --- macro panel ---
            st.subheader("宏觀戰情室")
            c1, c2, c3 = st.columns(3)
            c1.metric("FinMind 查詢交易日", trade_date if trade_date else "待更新")
            c2.metric("大盤成交金額", total_amount)
            c3.metric("全市場法人", total_inst)

            if not indices_df.empty:
                def color_change(val):
                    if isinstance(val, str) and val.startswith("+"):
                        return "color: #ff4b4b"
                    if isinstance(val, str) and val.startswith("-"):
                        return "color: #00c853"
                    return ""
                st.dataframe(
                    indices_df.style.applymap(color_change, subset=["漲跌", "幅度"]),
                    use_container_width=True,
                    hide_index=True,
                )

            st.divider()

            # --- session logic (do not lie) ---
            now = datetime.now(TW_TZ)
            # If before 15:00, treat as INTRADAY even if you show FinMind previous trade_date
            current_session = analyzer.SESSION_EOD if now.hour >= 15 else analyzer.SESSION_INTRADAY

            st.subheader("戰略核心分析")

            top_10, err_msg = analyzer.run_analysis(full_df, session=current_session)

            # data quality
            if full_df is not None and not full_df.empty:
                total_rows = len(full_df)
                total_symbols = full_df["Symbol"].nunique() if "Symbol" in full_df.columns else 0
                missing_close = full_df["Close"].isna().mean() * 100 if "Close" in full_df.columns else 100.0
                missing_vol = full_df["Volume"].isna().mean() * 100 if "Volume" in full_df.columns else 100.0

                st.sidebar.markdown("### 資料源品質")
                st.sidebar.write({
                    "rows": total_rows,
                    "symbols": total_symbols,
                    "close_missing_%": round(missing_close, 1),
                    "volume_missing_%": round(missing_vol, 1),
                })
            else:
                st.sidebar.warning("OHLCV 資料源為空（yfinance 連線失敗 / 市場休市 / 回傳結構異常）")

            if err_msg:
                st.sidebar.error(f"Analyzer: {err_msg}")

            if top_10 is None or top_10.empty:
                st.warning("本次掃描無符合策略標準之標的")
                st.caption(err_msg if err_msg else "可能原因：量縮、乖離過大、結構面惡化、或資料品質不足")
                st.stop()

            st.success(f"結構化數據構建完成（{len(top_10)} 檔入選）")

            macro_dict = {
                "overview": {
                    "amount": total_amount,
                    "inst_net": total_inst,
                    "trade_date": trade_date if trade_date else "",
                },
                "indices": indices_df.to_dict(orient="records") if not indices_df.empty else [],
            }

            json_payload = analyzer.generate_ai_json(
                top_10,
                market=market,
                session=current_session,
                macro_data=macro_dict,
            )

            st.subheader("AI 戰略數據包（JSON）")
            st.caption("包含：技術評分、籌碼（Inst_Status + Inst_Net_Raw）、結構面（OPM/Rev_Growth/PE）、風控 Kill Switch。")
            st.code(json_payload, language="json")

            st.subheader("關鍵標的指標")
            cols = ["Symbol", "Close", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
            if "Inst_Status" in top_10.columns:
                cols.insert(3, "Inst_Status")
            if "Inst_Net" in top_10.columns:
                cols.insert(4, "Inst_Net")
            st.dataframe(top_10[cols], use_container_width=True)

    except Exception as e:
        st.error("系統發生預期外錯誤")
        st.exception(e)
        st.stop()
