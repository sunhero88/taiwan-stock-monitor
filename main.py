# -*- coding: utf-8 -*-
"""
Filename: main.py
Version: Predator V15.6 (Inst 3D Streak + Dual Engine + FinMind Date Fallback)
Notes:
- NBSP-safe: avoid non-printable characters
- FinMind EOD date fallback: find latest available trade date automatically
- Institutional: 3-day same-direction streak
- Dual decision engine: Conservative vs Aggressive
"""

import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
from datetime import datetime, timedelta
import pytz

TW_TZ = pytz.timezone("Asia/Taipei")

FINMIND_ENDPOINT = "https://api.finmindtrade.com/api/v4/data"

# ======================================================
# 0) FinMind helpers (date fallback)
# ======================================================

@st.cache_data(ttl=300, show_spinner=False)
def finmind_get_latest_trade_date(max_lookback_days: int = 10) -> str:
    """
    Find latest FinMind-available trade date within lookback window.
    Strategy: query TaiwanStockPrice for TAIEX; if empty, go back day by day.
    Return YYYY-MM-DD string.
    """
    now = datetime.now(TW_TZ)
    for i in range(max_lookback_days):
        d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            r = requests.get(
                FINMIND_ENDPOINT,
                params={"dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": d},
                timeout=4,
            )
            j = r.json()
            if j.get("msg") == "success" and j.get("data"):
                return d
        except Exception:
            pass
    # fallback: today
    return now.strftime("%Y-%m-%d")


# ======================================================
# 1) Indices (yfinance) - caching
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
# 2) FinMind macro (amount + total inst) - caching
# ======================================================

@st.cache_data(ttl=120, show_spinner=False)
def fetch_market_amount(trade_date: str) -> str:
    """
    Market trading amount (FinMind). Uses latest available trade_date.
    """
    try:
        r = requests.get(
            FINMIND_ENDPOINT,
            params={"dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": trade_date},
            timeout=4,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            money = data["data"][0].get("Trading_Money", None)
            if money is not None:
                return f"{float(money) / 1e8:.0f} 億"
    except Exception:
        pass
    return "待更新"


@st.cache_data(ttl=120, show_spinner=False)
def fetch_market_total_inst(trade_date: str) -> str:
    """
    Total institutional net (FinMind): dataset TaiwanStockTotalInstitutionalInvestors.
    """
    try:
        r = requests.get(
            FINMIND_ENDPOINT,
            params={"dataset": "TaiwanStockTotalInstitutionalInvestors", "date": trade_date},
            timeout=4,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            net = (df["buy"].sum() - df["sell"].sum()) / 1e8
            return f"+{net:.1f} 億" if net > 0 else f"{net:.1f} 億"
    except Exception:
        pass
    return "待更新"


# ======================================================
# 3) FinMind inst per stock (3-day) - caching
# ======================================================

@st.cache_data(ttl=300, show_spinner=False)
def fetch_inst_data_finmind_stock_3d(trade_date: str) -> pd.DataFrame:
    """
    Fetch per-stock institutional buy/sell for recent 3 available trade days (<= trade_date).
    Dataset: TaiwanStockInstitutionalInvestorsBuySell
    Output columns:
      - Symbol (e.g., 2330.TW)
      - Inst_Net_D0, Inst_Net_D1, Inst_Net_D2  (raw shares)
      - Inst_Net_3d (sum)
      - Inst_Dir3 (BUY/SELL/FLAT)
      - Inst_Streak3 (0-3)
      - Inst_Visual (e.g., +12.3k, -8.1k, 0.0k)
      - Inst_Ready (bool)
      - Trade_Dates (list string for debug)
    """
    # ask analyzer helper to find last 3 trade dates from FinMind
    dates = analyzer.get_recent_finmind_trade_dates(trade_date, lookback_days=12, need_days=3)
    if len(dates) < 3:
        return pd.DataFrame()

    frames = []
    for d in dates:
        try:
            r = requests.get(
                FINMIND_ENDPOINT,
                params={"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": d},
                timeout=6,
            )
            j = r.json()
            if j.get("msg") == "success" and j.get("data"):
                df = pd.DataFrame(j["data"])
                # Net = buy - sell, then sum by stock_id (all inst types aggregated)
                df["Net"] = df["buy"] - df["sell"]
                g = df.groupby("stock_id")["Net"].sum().reset_index()
                g.columns = ["stock_id", f"Inst_Net_{d}"]
                frames.append(g)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on="stock_id", how="outer")

    out = out.fillna(0.0)
    out["Symbol"] = out["stock_id"].astype(str) + ".TW"

    # map dates to D0/D1/D2 (D0 = most recent)
    dates_sorted = sorted(dates)  # ascending
    d2, d1, d0 = dates_sorted[-3], dates_sorted[-2], dates_sorted[-1]

    out["Inst_Net_D0"] = out.get(f"Inst_Net_{d0}", 0.0)
    out["Inst_Net_D1"] = out.get(f"Inst_Net_{d1}", 0.0)
    out["Inst_Net_D2"] = out.get(f"Inst_Net_{d2}", 0.0)
    out["Inst_Net_3d"] = out["Inst_Net_D0"] + out["Inst_Net_D1"] + out["Inst_Net_D2"]

    # direction + streak (3-day)
    out["Inst_Dir3"] = out.apply(lambda r: analyzer.inst_direction_3d(r["Inst_Net_D0"], r["Inst_Net_D1"], r["Inst_Net_D2"]), axis=1)
    out["Inst_Streak3"] = out.apply(lambda r: analyzer.inst_streak_3d(r["Inst_Net_D0"], r["Inst_Net_D1"], r["Inst_Net_D2"]), axis=1)

    # ready: total abs sum > 0 means data not all zeros (rough but effective in practice)
    abs_sum = float(out["Inst_Net_D0"].abs().sum() + out["Inst_Net_D1"].abs().sum() + out["Inst_Net_D2"].abs().sum())
    out["Inst_Ready"] = abs_sum > 0

    # visual: show D0 (most recent) in k
    def to_k(x: float) -> str:
        k = round(float(x) / 1000.0, 1)
        if k > 0:
            return f"+{k}k"
        return f"{k}k"

    out["Inst_Visual"] = out["Inst_Net_D0"].apply(to_k)
    out["Trade_Dates"] = str([d2, d1, d0])

    return out[["Symbol", "Inst_Net_D0", "Inst_Net_D1", "Inst_Net_D2", "Inst_Net_3d", "Inst_Dir3", "Inst_Streak3", "Inst_Visual", "Inst_Ready", "Trade_Dates"]]


# ======================================================
# 4) Market OHLCV (yfinance) - caching
# ======================================================

@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_data(market_id: str) -> pd.DataFrame:
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

    all_res = []

    if isinstance(raw.columns, pd.MultiIndex):
        available = list(raw.columns.levels[0])
        multi = True
    else:
        available = symbols if len(symbols) == 1 and not raw.empty else []
        multi = False

    if len(symbols) > 1 and not multi:
        return pd.DataFrame()

    for s in symbols:
        try:
            if multi:
                if s not in available:
                    continue
                s_df = raw[s].copy().dropna()
            else:
                s_df = raw.copy().dropna()

            if s_df.empty:
                continue

            s_df["Symbol"] = s
            all_res.append(s_df)
        except Exception:
            continue

    if not all_res:
        return pd.DataFrame()

    out = pd.concat(all_res).reset_index()
    if "Datetime" in out.columns and "Date" not in out.columns:
        out = out.rename(columns={"Datetime": "Date"})
    return out


# ======================================================
# 5) UI
# ======================================================

st.set_page_config(page_title="Predator V15.6", layout="wide")
st.title("Predator 指揮中心 V15.6（Inst 3D + 雙規則引擎）")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("啟動全域掃描與結構分析"):
    try:
        with st.spinner("執行中：技術面篩選 → 法人連續性（3日） → 結構面 → 雙規則決策"):
            now = datetime.now(TW_TZ)
            current_hour = now.hour
            current_session = analyzer.SESSION_EOD if current_hour >= 15 else analyzer.SESSION_INTRADAY

            # Macro: trade date (FinMind)
            trade_date = finmind_get_latest_trade_date(max_lookback_days=10)

            indices_df = fetch_detailed_indices()

            # FinMind macro is meaningful mainly in/after EOD; but we still fetch latest available date
            total_amount = fetch_market_amount(trade_date)
            total_inst = fetch_market_total_inst(trade_date)

            full_df = fetch_market_data(market)

            # Inst 3D only for TW
            inst3_df = pd.DataFrame()
            inst_ready = False
            inst_dates = "[]"
            if market == "tw-share":
                inst3_df = fetch_inst_data_finmind_stock_3d(trade_date)
                if inst3_df is not None and not inst3_df.empty:
                    inst_ready = bool(inst3_df["Inst_Ready"].iloc[0])  # global ready flag repeated per row
                    inst_dates = inst3_df["Trade_Dates"].iloc[0]
                else:
                    inst_ready = False

            # --- Macro panel ---
            st.subheader("宏觀戰情室")
            c1, c2, c3 = st.columns(3)
            c1.metric("FinMind 查詢交易日", trade_date)
            c2.metric("大盤成交金額", total_amount)
            c3.metric("全市場法人", total_inst)

            if market == "tw-share":
                st.caption(f"法人資料狀態：{'READY' if inst_ready else 'PENDING'}；法人採樣日（3日）：{inst_dates}")

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
            else:
                st.warning("國際指數數據暫時無法獲取")

            st.divider()

            # --- Core analysis ---
            st.subheader("戰略核心分析")

            # Run analyzer (inst_ready influences confirmation gating)
            top_10, err_msg = analyzer.run_analysis(
                full_df,
                session=current_session,
                inst_ready=inst_ready,
            )

            # Merge inst3 into top_10 (so JSON carries it)
            if market == "tw-share" and top_10 is not None and not top_10.empty:
                if inst3_df is not None and not inst3_df.empty:
                    top_10 = top_10.merge(inst3_df.drop(columns=["Inst_Ready", "Trade_Dates"], errors="ignore"), on="Symbol", how="left")
                    # if missing merge => fill pending
                    for c in ["Inst_Visual", "Inst_Net_D0", "Inst_Net_D1", "Inst_Net_D2", "Inst_Net_3d", "Inst_Dir3", "Inst_Streak3"]:
                        if c not in top_10.columns:
                            top_10[c] = None
                    top_10["Inst_Visual"] = top_10["Inst_Visual"].fillna("PENDING")
                    top_10["Inst_Net_3d"] = pd.to_numeric(top_10["Inst_Net_3d"], errors="coerce").fillna(0.0)
                    top_10["Inst_Streak3"] = pd.to_numeric(top_10["Inst_Streak3"], errors="coerce").fillna(0).astype(int)
                    top_10["Inst_Dir3"] = top_10["Inst_Dir3"].fillna("PENDING")
                else:
                    top_10["Inst_Visual"] = "PENDING"
                    top_10["Inst_Streak3"] = 0
                    top_10["Inst_Dir3"] = "PENDING"
                    top_10["Inst_Net_3d"] = 0.0

            # Diagnostics
            if err_msg:
                st.sidebar.error(f"系統警示: {err_msg}")

            if full_df is not None and not full_df.empty:
                total_rows = len(full_df)
                total_symbols = full_df["Symbol"].nunique() if "Symbol" in full_df.columns else 0
                missing_close = full_df["Close"].isna().mean() * 100 if "Close" in full_df.columns else 100.0
                missing_vol = full_df["Volume"].isna().mean() * 100 if "Volume" in full_df.columns else 100.0
                st.sidebar.info(
                    "資料源診斷\n"
                    f"- 總筆數: {total_rows:,}\n"
                    f"- 監控標的: {total_symbols}\n"
                    "資料品質\n"
                    f"- Close 缺值: {missing_close:.1f}%\n"
                    f"- Volume 缺值: {missing_vol:.1f}%"
                )
            else:
                st.sidebar.warning("資料源為空（可能連線失敗或市場休市）")

            if top_10 is None or top_10.empty:
                st.warning("本次掃描無符合策略標準之標的")
                st.caption(err_msg if err_msg else "可能原因：量縮、乖離過大、結構面惡化、或資料品質不足")
                st.stop()

            st.success(f"結構化數據構建完成（{len(top_10)} 檔入選）")

            macro_dict = {
                "overview": {
                    "amount": total_amount,
                    "inst_net": total_inst,
                    "trade_date": trade_date,
                    "inst_status": "READY" if inst_ready else "PENDING",
                    "inst_dates_3d": inst_dates,
                },
                "indices": indices_df.to_dict(orient="records") if not indices_df.empty else [],
            }

            json_payload = analyzer.generate_ai_json(
                top_10,
                market=market,
                session=current_session,
                macro_data=macro_dict,
                inst_ready=inst_ready,
            )

            st.subheader("AI 戰略數據包（JSON）")
            st.caption(f"包含：技術評分、結構面（OPM/Rev_Growth/PE）、法人連續性（3日）、雙規則決策。Session={current_session}")
            st.code(json_payload, language="json")

            st.subheader("關鍵標的指標")
            cols = ["Symbol", "Close", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
            if market == "tw-share":
                cols = ["Symbol", "Close", "Inst_Visual", "Inst_Streak3", "Inst_Dir3", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
            st.dataframe(top_10[cols], use_container_width=True)

    except Exception as e:
        st.error("系統發生預期外錯誤")
        st.exception(e)
        st.stop()
