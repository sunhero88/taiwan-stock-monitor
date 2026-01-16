# =========================
# main.py
# Predator V15.5.3 Patch (FinMind date aligned + Inst fixed + Rev_Growth + NBSP-safe)
# =========================
# -*- coding: utf-8 -*-
"""
Filename: main.py
Version: Predator V15.5.3 (FinMind date aligned + Inst fixed + Rev_Growth)
Key Fix:
- FinMind 的查詢日期改為「最新交易日」(來自 yfinance full_df['Date'].max())
- 早上也可看「上一交易日 EOD」籌碼，而不是拿 today 去問空資料
- FinMind token：若 st.secrets / env 有就自動帶入
- Sidebar 顯示 FinMind 回傳筆數（可觀測性）
"""
import os
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer
from datetime import datetime
import pytz

TW_TZ = pytz.timezone("Asia/Taipei")


# ======================================================
# 0) Utilities
# ======================================================

def _safe_float(x, default=0.0) -> float:
    try:
        v = float(x)
        if pd.isna(v):
            return default
        return v
    except Exception:
        return default


def _get_finmind_token() -> str:
    """
    Optional token support:
    - Streamlit Cloud: st.secrets["FINMIND_TOKEN"]
    - Env var: FINMIND_TOKEN
    """
    try:
        if "FINMIND_TOKEN" in st.secrets:
            return str(st.secrets["FINMIND_TOKEN"]).strip()
    except Exception:
        pass
    return str(os.getenv("FINMIND_TOKEN", "")).strip()


def _to_yyyymmdd(d) -> str:
    """
    Convert pandas Timestamp / datetime / date to YYYY-MM-DD string.
    """
    try:
        ts = pd.to_datetime(d)
        if pd.isna(ts):
            return ""
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _latest_trading_date_from_df(df: pd.DataFrame) -> str:
    """
    Use yfinance market data to derive latest trading date for FinMind query.
    """
    if df is None or df.empty:
        return ""
    if "Date" not in df.columns:
        return ""
    latest = pd.to_datetime(df["Date"], errors="coerce").max()
    return _to_yyyymmdd(latest)


# ======================================================
# 1) Data fetchers (with caching)
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

            price = _safe_float(latest.get("Close", 0.0))
            prev_close = _safe_float(prev.get("Close", 0.0))
            change = price - prev_close
            pct = (change / prev_close) * 100.0 if prev_close != 0 else 0.0

            rows.append({
                "指數名稱": name,
                "現價": f"{price:,.0f}",
                "漲跌": f"{change:+.2f}",
                "幅度": f"{pct:+.2f}%",
                "開盤": f"{_safe_float(latest.get('Open', 0.0)):,.0f}",
                "最高": f"{_safe_float(latest.get('High', 0.0)):,.0f}",
                "最低": f"{_safe_float(latest.get('Low', 0.0)):,.0f}",
                "昨收": f"{prev_close:,.0f}",
            })
        except Exception:
            rows.append({
                "指數名稱": name, "現價": "-", "漲跌": "-", "幅度": "-",
                "開盤": "-", "最高": "-", "最低": "-", "昨收": "-"
            })

    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_data(market_id: str) -> pd.DataFrame:
    """
    下載 OHLCV；回傳包含 Date + Symbol 的 long-form。
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

    all_rows = []
    multi = isinstance(raw.columns, pd.MultiIndex)
    if len(symbols) > 1 and not multi:
        return pd.DataFrame()

    for s in symbols:
        try:
            if multi:
                if s not in raw.columns.levels[0]:
                    continue
                s_df = raw[s].copy().dropna()
            else:
                s_df = raw.copy().dropna()

            if s_df.empty:
                continue

            s_df = s_df.reset_index()
            if "Datetime" in s_df.columns and "Date" not in s_df.columns:
                s_df = s_df.rename(columns={"Datetime": "Date"})

            s_df["Symbol"] = s
            all_rows.append(s_df)
        except Exception:
            continue

    if not all_rows:
        return pd.DataFrame()

    out = pd.concat(all_rows, ignore_index=True)
    for c in ["Date", "Open", "High", "Low", "Close", "Volume", "Symbol"]:
        if c not in out.columns:
            out[c] = pd.NA
    return out


@st.cache_data(ttl=120, show_spinner=False)
def finmind_market_amount(trade_date: str) -> str:
    """
    大盤成交額（指定 trade_date）
    """
    if not trade_date:
        return "待更新"

    token = _get_finmind_token()
    params = {"dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": trade_date}
    if token:
        params["token"] = token

    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=10)
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            money = data["data"][0].get("Trading_Money", 0)
            return f"{_safe_float(money) / 100000000:.0f} 億"
    except Exception:
        pass
    return "待更新"


@st.cache_data(ttl=120, show_spinner=False)
def finmind_market_total_inst(trade_date: str) -> str:
    """
    全市場法人（指定 trade_date）
    """
    if not trade_date:
        return "待更新"

    token = _get_finmind_token()
    params = {"dataset": "TaiwanStockTotalInstitutionalInvestors", "date": trade_date}
    if token:
        params["token"] = token

    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=10)
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            net = (_safe_float(df["buy"].sum()) - _safe_float(df["sell"].sum())) / 100000000
            return f"🔴+{net:.1f}億" if net > 0 else f"🔵{net:.1f}億"
    except Exception:
        pass
    return "待更新"


@st.cache_data(ttl=300, show_spinner=False)
def finmind_inst_by_stock(trade_date: str) -> pd.DataFrame:
    """
    個股法人（指定 trade_date）
    output: Symbol, Inst_Net
    """
    if not trade_date:
        return pd.DataFrame()

    token = _get_finmind_token()
    params = {"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": trade_date}
    if token:
        params["token"] = token

    try:
        r = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=15)
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
            df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
            df["Net"] = df["buy"] - df["sell"]

            g = df.groupby("stock_id")["Net"].sum().reset_index()
            g.columns = ["stock_id", "Inst_Net"]
            g["Symbol"] = g["stock_id"].astype(str).str.strip() + ".TW"
            return g[["Symbol", "Inst_Net"]]
    except Exception:
        pass

    return pd.DataFrame()


def attach_inst_to_ohlcv(full_df: pd.DataFrame, trade_date: str, market_id: str):
    """
    將 FinMind 的 Inst_Net/Inst_Status 合併到 full_df（以 Symbol 為 key）
    """
    if full_df is None or full_df.empty:
        return full_df, 0

    if market_id != "tw-share":
        full_df["Inst_Net"] = 0.0
        full_df["Inst_Status"] = "N/A"
        return full_df, 0

    inst_df = finmind_inst_by_stock(trade_date)
    if inst_df is None or inst_df.empty:
        full_df["Inst_Net"] = 0.0
        full_df["Inst_Status"] = "N/A"
        return full_df, 0

    full_df = full_df.copy()
    full_df["Symbol"] = full_df["Symbol"].astype(str)

    inst_df = inst_df.copy()
    inst_df["Symbol"] = inst_df["Symbol"].astype(str)
    inst_df["Inst_Net"] = pd.to_numeric(inst_df["Inst_Net"], errors="coerce").fillna(0)

    merged = full_df.merge(inst_df, on="Symbol", how="left")
    merged["Inst_Net"] = pd.to_numeric(merged["Inst_Net"], errors="coerce").fillna(0)

    def _fmt_status(net):
        try:
            net = float(net)
        except Exception:
            net = 0.0
        val_k = round(net / 1000.0, 1)
        return f"🔴+{val_k}k" if net > 0 else f"🔵{val_k}k"

    merged["Inst_Status"] = merged["Inst_Net"].apply(lambda x: _fmt_status(x) if x != 0 else "0.0k")
    return merged, len(inst_df)


# ======================================================
# 2) UI
# ======================================================

st.set_page_config(page_title="Predator V15.5.3", layout="wide")
st.title("Predator 指揮中心 V15.5.3 (FinMind Date Aligned + Inst + Rev_Growth)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

# 提供手動清 cache（避免你調整後還被 cache 卡住）
if st.sidebar.button("清除快取並重跑"):
    st.cache_data.clear()

if st.button("啟動全域掃描與結構分析"):
    try:
        with st.spinner("執行中：下載行情 → 推導交易日 → FinMind 籌碼/大盤 → 策略引擎"):
            indices_df = fetch_detailed_indices()
            full_df = fetch_market_data(market)

            # 用 yfinance 的最新交易日當作 FinMind 查詢日期（關鍵修補）
            trade_date = _latest_trading_date_from_df(full_df)

            total_amount = finmind_market_amount(trade_date) if market == "tw-share" else "N/A"
            total_inst = finmind_market_total_inst(trade_date) if market == "tw-share" else "N/A"

            full_df, finmind_rows = attach_inst_to_ohlcv(full_df, trade_date, market)

            # Macro panel
            st.subheader("宏觀戰情室")
            c1, c2, c3 = st.columns(3)
            c1.metric("FinMind 查詢交易日", trade_date if trade_date else "N/A")
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
            else:
                st.warning("國際指數數據暫時無法獲取")

            st.divider()
            st.subheader("戰略核心分析")

            current_hour = datetime.now(TW_TZ).hour
            current_session = analyzer.SESSION_EOD if current_hour >= 15 else analyzer.SESSION_EOD
            # 你早上跑也要看「昨日日終」，所以這裡直接固定 EOD 也合理（你要改回 intraday 再調）

            top_10, err_msg = analyzer.run_analysis(full_df, session=current_session)

            # Sidebar diagnostics
            if err_msg:
                st.sidebar.error(f"系統警示: {err_msg}")

            if full_df is not None and not full_df.empty:
                total_rows = len(full_df)
                total_symbols = full_df["Symbol"].nunique() if "Symbol" in full_df.columns else 0
                missing_close = full_df["Close"].isna().mean() * 100 if "Close" in full_df.columns else 100.0
                missing_vol = full_df["Volume"].isna().mean() * 100 if "Volume" in full_df.columns else 100.0
                inst_coverage = (full_df["Inst_Status"] != "N/A").mean() * 100 if "Inst_Status" in full_df.columns else 0.0

                st.sidebar.info(
                    "資料源診斷\n"
                    f"- 總筆數: {total_rows:,}\n"
                    f"- 監控標的: {total_symbols}\n"
                    "資料品質\n"
                    f"- Close 缺值: {missing_close:.1f}%\n"
                    f"- Volume 缺值: {missing_vol:.1f}%\n"
                    f"- 籌碼覆蓋率(非N/A): {inst_coverage:.1f}%\n"
                    f"- FinMind 個股筆數: {finmind_rows}"
                )
            else:
                st.sidebar.warning("資料源為空（可能連線失敗或市場休市）")

            if top_10 is None or top_10.empty:
                st.warning("本次掃描無符合策略標準之標的")
                st.caption(err_msg if err_msg else "可能原因：量縮、乖離過大、結構面惡化、或資料品質不足")
                st.stop()

            st.success(f"結構化數據構建完成（{len(top_10)} 檔入選）")

            macro_dict = {
                "overview": {"amount": total_amount, "inst_net": total_inst, "trade_date": trade_date},
                "indices": indices_df.to_dict(orient="records") if not indices_df.empty else [],
            }

            json_payload = analyzer.generate_ai_json(
                top_10,
                market=market,
                session=current_session,
                macro_data=macro_dict,
            )

            st.subheader("AI 戰略數據包（JSON）")
            st.caption("Structure.Rev_Growth 來源：yfinance.info['revenueGrowth']（避免誤讀為嚴格 QoQ）。")
            st.code(json_payload, language="json")

            st.subheader("關鍵標的指標")
            cols = ["Symbol", "Close", "Inst_Status", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
            cols = [c for c in cols if c in top_10.columns]
            st.dataframe(top_10[cols], use_container_width=True)

    except Exception as e:
        st.error("系統發生預期外錯誤")
        st.exception(e)
        st.stop()
