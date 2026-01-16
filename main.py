# =========================
# main.py
# Predator V15.5.2 Patch (Inst fix + Rev_Growth rename + NBSP-safe)
# =========================
# -*- coding: utf-8 -*-
"""
Filename: main.py
Version: Predator V15.5.2 (Inst-Aware + Rev_Growth + NBSP-safe)
Patch goals:
A) 修正 Inst_Visual 全部 N/A：確保 Inst_Status / Inst_Net 會進入 analyzer 的輸出
B) 修正 QoQ 命名誤判：Structure 欄位改為 Rev_Growth（來源 yfinance: revenueGrowth）
C) Streamlit Cloud 穩定：cache + 嚴格 MultiIndex 判斷 + 全域 try/except
D) 避免 U+00A0：全檔案僅使用一般空白
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


# ======================================================
# 1) Data fetchers (with caching)
# ======================================================

@st.cache_data(ttl=60, show_spinner=False)
def fetch_detailed_indices() -> pd.DataFrame:
    """
    逐一抓取指數，避免 yfinance MultiIndex 型別不一致。
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
    大盤成交額 (FinMind)。
    """
    now = datetime.now(TW_TZ)
    if 9 <= now.hour < 15:
        return "盤中統計中"

    date_str = now.strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": date_str},
            timeout=5,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            money = data["data"][0].get("Trading_Money", 0)
            return f"{_safe_float(money) / 100000000:.0f} 億"
    except Exception:
        pass

    return "待更新"


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_total_inst() -> str:
    """
    全市場法人買賣超 (FinMind)。
    """
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return "盤中動能觀測"

    date_str = now.strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockTotalInstitutionalInvestors", "date": date_str},
            timeout=5,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            net = (_safe_float(df["buy"].sum()) - _safe_float(df["sell"].sum())) / 100000000
            return f"+{net:.1f} 億" if net > 0 else f"{net:.1f} 億"
    except Exception:
        pass

    return "待更新"


@st.cache_data(ttl=300, show_spinner=False)
def fetch_inst_data_finmind_stock() -> pd.DataFrame:
    """
    個股法人買賣超 (FinMind)。
    output: Symbol, Inst_Net
    """
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return pd.DataFrame()

    date_str = now.strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": date_str},
            timeout=10,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            df["Net"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0) - pd.to_numeric(df["sell"], errors="coerce").fillna(0)
            g = df.groupby("stock_id")["Net"].sum().reset_index()
            g.columns = ["stock_id", "Inst_Net"]
            # 正規化成 yfinance 的 Symbol 格式：xxxx.TW
            g["Symbol"] = g["stock_id"].astype(str).str.strip() + ".TW"
            g = g[["Symbol", "Inst_Net"]]
            return g
    except Exception:
        pass

    return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_data(market_id: str) -> pd.DataFrame:
    """
    下載 OHLCV 並合併 Inst_Net / Inst_Status。
    output columns: Date, Open, High, Low, Close, Volume, Symbol, Inst_Net, Inst_Status
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
    all_rows = []

    # 嚴格判定 MultiIndex
    multi = isinstance(raw.columns, pd.MultiIndex)

    # 多檔但非 MultiIndex => 視為下載異常
    if len(symbols) > 1 and not multi:
        return pd.DataFrame()

    for s in symbols:
        try:
            if multi:
                if s not in raw.columns.levels[0]:
                    continue
                s_df = raw[s].copy().dropna()
            else:
                # 單檔回傳單層 columns
                s_df = raw.copy().dropna()

            if s_df.empty:
                continue

            s_df = s_df.reset_index()
            if "Datetime" in s_df.columns and "Date" not in s_df.columns:
                s_df = s_df.rename(columns={"Datetime": "Date"})

            s_df["Symbol"] = s

            # 合併籌碼
            if market_id == "tw-share":
                if not inst_df.empty and s in inst_df["Symbol"].values:
                    net_val = _safe_float(inst_df.loc[inst_df["Symbol"] == s, "Inst_Net"].values[0], 0.0)
                    s_df["Inst_Net"] = net_val
                    val_k = round(net_val / 1000.0, 1)
                    s_df["Inst_Status"] = f"🔴+{val_k}k" if net_val > 0 else f"🔵{val_k}k"
                else:
                    s_df["Inst_Net"] = 0.0
                    s_df["Inst_Status"] = "N/A"
            else:
                s_df["Inst_Net"] = 0.0
                s_df["Inst_Status"] = "N/A"

            all_rows.append(s_df)
        except Exception:
            continue

    if not all_rows:
        return pd.DataFrame()

    out = pd.concat(all_rows, ignore_index=True)

    # 統一欄位存在性（避免 analyzer 防呆後全 NaN）
    for c in ["Date", "Open", "High", "Low", "Close", "Volume", "Symbol", "Inst_Net", "Inst_Status"]:
        if c not in out.columns:
            out[c] = pd.NA

    return out


# ======================================================
# 2) UI
# ======================================================

st.set_page_config(page_title="Predator V15.5.2", layout="wide")
st.title("Predator 指揮中心 V15.5.2 (Inst + Rev_Growth)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("啟動全域掃描與結構分析"):
    try:
        with st.spinner("執行中：技術面篩選 → 籌碼合併 → 結構面掃描"):
            indices_df = fetch_detailed_indices()
            total_amount = fetch_market_amount()
            total_inst = fetch_market_total_inst()
            full_df = fetch_market_data(market)

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
            st.subheader("戰略核心分析")

            # Session：15:00 後才視為 EOD（對齊 FinMind）
            current_hour = datetime.now(TW_TZ).hour
            current_session = analyzer.SESSION_EOD if current_hour >= 15 else analyzer.SESSION_INTRADAY

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
                    f"- 籌碼覆蓋率(非N/A): {inst_coverage:.1f}%"
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
            st.caption("注意：Structure.Rev_Growth 來源為 yfinance.info['revenueGrowth']（非嚴格 QoQ）。")
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
