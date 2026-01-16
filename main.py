# main.py
# -*- coding: utf-8 -*-
"""
Filename: main.py
Version: Predator V15.5.1 (HA + Caching + NBSP-safe)
Notes:
- Avoids non-printable characters (e.g., U+00A0)
- Uses caching for stability on Streamlit Cloud
"""
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
from datetime import datetime
import pytz

TW_TZ = pytz.timezone("Asia/Taipei")

# ======================================================
# 1) Data Fetchers (with caching)
# ======================================================

@st.cache_data(ttl=60, show_spinner=False)
def fetch_detailed_indices() -> pd.DataFrame:
    """
    Fetch indices one-by-one to avoid yfinance MultiIndex quirks.
    Cached for 60 seconds.
    """
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


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_amount() -> str:
    """
    Market trading amount (FinMind). Cached for 60 seconds.
    FinMind often updates final value after 15:00.
    """
    now = datetime.now(TW_TZ)
    if 9 <= now.hour < 15:
        return "盤中統計中"

    date_str = now.strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": date_str},
            timeout=3,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            money = data["data"][0]["Trading_Money"]
            return f"{money / 100000000:.0f} 億"
    except Exception:
        pass

    return "待更新"


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_total_inst() -> str:
    """
    Total institutional net (FinMind). Cached for 60 seconds.
    """
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return "盤中動能觀測"

    date_str = now.strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockTotalInstitutionalInvestors", "date": date_str},
            timeout=3,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            net = (df["buy"].sum() - df["sell"].sum()) / 100000000
            return f"+{net:.1f} 億" if net > 0 else f"{net:.1f} 億"
    except Exception:
        pass

    return "待更新"


@st.cache_data(ttl=300, show_spinner=False)
def fetch_inst_data_finmind_stock() -> pd.DataFrame:
    """
    Per-stock institutional net (FinMind). Cached for 5 minutes.
    Returns columns: Symbol, Inst_Net
    """
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return pd.DataFrame()

    date_str = now.strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": date_str},
            timeout=5,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            df["Net"] = df["buy"] - df["sell"]
            g = df.groupby("stock_id")["Net"].sum().reset_index()
            g.columns = ["Symbol", "Inst_Net"]
            g["Symbol"] = g["Symbol"].astype(str) + ".TW"
            return g
    except Exception:
        pass

    return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_data(market_id: str) -> pd.DataFrame:
    """
    Fetch OHLCV for a predefined list of symbols.
    Returns a long-form dataframe with columns: Date index + OHLCV + Symbol + (optional) Inst fields.
    Cached for 5 minutes.
    """
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

    inst_df = fetch_inst_data_finmind_stock() if market_id == "tw-share" else pd.DataFrame()
    all_res = []

    # Determine structure
    if isinstance(raw.columns, pd.MultiIndex):
        available = list(raw.columns.levels[0])
        multi = True
    else:
        available = symbols if len(symbols) == 1 and not raw.empty else []
        multi = False

    # If multi-symbol but not MultiIndex, treat as failure
    if len(symbols) > 1 and not multi:
        return pd.DataFrame()

    for s in symbols:
        try:
            if multi:
                if s not in available:
                    continue
                s_df = raw[s].copy().dropna()
            else:
                # single symbol
                s_df = raw.copy().dropna()

            if s_df.empty:
                continue

            s_df["Symbol"] = s

            if market_id == "tw-share":
                if not inst_df.empty and s in inst_df["Symbol"].values:
                    net_val = float(inst_df.loc[inst_df["Symbol"] == s, "Inst_Net"].values[0])
                    s_df["Inst_Net"] = net_val
                    val_k = round(net_val / 1000.0, 1)
                    s_df["Inst_Status"] = f"+{val_k}k" if net_val > 0 else f"{val_k}k"
                else:
                    s_df["Inst_Net"] = 0.0
                    s_df["Inst_Status"] = "N/A"

            all_res.append(s_df)
        except Exception:
            continue

    if not all_res:
        return pd.DataFrame()

    out = pd.concat(all_res)
    out = out.reset_index()  # bring Date out as a column named 'Date'
    # yfinance uses 'Date' or 'Datetime' depending on interval; normalize:
    if "Datetime" in out.columns and "Date" not in out.columns:
        out = out.rename(columns={"Datetime": "Date"})
    return out


# ======================================================
# 2) UI
# ======================================================

st.set_page_config(page_title="Predator V15.5.1", layout="wide")
st.title("Predator 指揮中心 V15.5.1")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("啟動全域掃描與結構分析"):
    try:
        with st.spinner("執行中：技術面篩選 → 動能估算 → 基本面結構掃描"):
            indices_df = fetch_detailed_indices()
            total_amount = fetch_market_amount()
            total_inst = fetch_market_total_inst()

            full_df = fetch_market_data(market)

            # Macro panel
            st.subheader("宏觀戰情室")
            c1, c2 = st.columns(2)
            c1.metric("大盤成交金額", total_amount)
            c2.metric("全市場法人", total_inst)

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

            # Core analysis
            st.subheader("戰略核心分析")

            current_hour = datetime.now(TW_TZ).hour
            current_session = analyzer.SESSION_EOD if current_hour >= 15 else analyzer.SESSION_INTRADAY

            top_10, err_msg = analyzer.run_analysis(full_df, session=current_session)

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
                "overview": {"amount": total_amount, "inst_net": total_inst},
                "indices": indices_df.to_dict(orient="records") if not indices_df.empty else [],
            }

            json_payload = analyzer.generate_ai_json(
                top_10,
                market=market,
                session=current_session,
                macro_data=macro_dict,
            )

            st.subheader("AI 戰略數據包（JSON）")
            st.caption(f"包含：技術評分、結構面（OPM/QoQ/PE）、Kill Switch。Session={current_session}")
            st.code(json_payload, language="json")

            st.subheader("關鍵標的指標")
            cols = ["Symbol", "Close", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
            if "Inst_Status" in top_10.columns:
                cols.insert(3, "Inst_Status")
            st.dataframe(top_10[cols], use_container_width=True)

    except Exception as e:
        st.error("系統發生預期外錯誤")
        st.exception(e)
        st.stop()
