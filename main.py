# main.py
# -*- coding: utf-8 -*-
"""
Sunhero｜股市智能超盤中控台
Predator V15.6.3 (Production)
"""

import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
import pytz

import analyzer
from arbiter import arbitrate  # 你已新增的 arbiter.py

TW_TZ = pytz.timezone("Asia/Taipei")

# ======================================================
# 0) Trade date helper (避免凌晨日期錯亂)
# ======================================================

def get_trade_date_tw(now: datetime) -> str:
    """
    台股交易日簡化處理：
    - 週六/週日：回推到週五
    - 若時間 < 15:00：視為盤中，trade_date = 今日
    - 若時間 >= 15:00：trade_date = 今日（收盤後）
    備註：遇國定假日/休市，FinMind 會回空，inst_status 會保持 PENDING，系統自動降級保守。
    """
    d = now.date()
    # weekend rollback
    if d.weekday() == 5:  # Sat
        d = d - timedelta(days=1)
    elif d.weekday() == 6:  # Sun
        d = d - timedelta(days=2)
    return d.strftime("%Y-%m-%d")


# ======================================================
# 1) Data Fetchers (with caching)
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


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_amount(trade_date: str) -> str:
    """
    Market trading amount (FinMind)
    注意：FinMind 有時收盤後仍會延遲更新，取不到就回傳「待更新」。
    """
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockPrice", "data_id": "TAIEX", "date": trade_date},
            timeout=5,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            money = data["data"][0].get("Trading_Money", None)
            if money is not None:
                return f"{money / 100000000:.0f} 億"
    except Exception:
        pass
    return "待更新"


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_total_inst(trade_date: str) -> str:
    """
    Total institutional net (FinMind).
    """
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockTotalInstitutionalInvestors", "date": trade_date},
            timeout=8,
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
def fetch_inst_data_3d_finmind(trade_date: str) -> (pd.DataFrame, str, list):
    """
    取近三日法人資料（個股）：
    - 回傳 inst_df: columns: date, symbol, net_amount
    - 回傳 inst_status: READY / PENDING
    - 回傳 inst_dates_3d: list[str]
    """
    # 近 7 天內嘗試找最近 3 個有資料的交易日（簡化）
    dates = []
    base = datetime.strptime(trade_date, "%Y-%m-%d").date()

    # 往回最多抓 10 天，挑出 3 個有資料的日子
    inst_rows = []
    for back in range(0, 12):
        d = base - timedelta(days=back)
        if d.weekday() >= 5:
            continue
        d_str = d.strftime("%Y-%m-%d")

        try:
            r = requests.get(
                "https://api.finmindtrade.com/api/v4/data",
                params={"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "date": d_str},
                timeout=10,
            )
            data = r.json()
            if data.get("msg") == "success" and data.get("data"):
                df = pd.DataFrame(data["data"])
                # 欄位：stock_id / buy / sell / ...
                df["net_amount"] = df["buy"] - df["sell"]
                # 彙總三大法人（同日同股多筆）-> net_amount sum
                g = df.groupby("stock_id")["net_amount"].sum().reset_index()
                g["date"] = d_str
                g["symbol"] = g["stock_id"].astype(str) + ".TW"
                inst_rows.append(g[["date", "symbol", "net_amount"]])
                dates.append(d_str)
        except Exception:
            pass

        if len(dates) >= 3:
            break

    if len(dates) < 3 or not inst_rows:
        return pd.DataFrame(), "PENDING", []

    inst_df = pd.concat(inst_rows, ignore_index=True)
    inst_dates_3d = sorted(list(set(dates)))[:3]
    return inst_df, "READY", inst_dates_3d


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_data(market_id: str) -> pd.DataFrame:
    """
    取 OHLCV
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
        period="3mo",
        interval="1d",
        group_by="ticker",
        progress=False,
        threads=True,
    )

    all_res = []
    if len(symbols) > 1:
        if not isinstance(raw.columns, pd.MultiIndex):
            return pd.DataFrame()
        available = list(raw.columns.levels[0])

        for s in symbols:
            if s not in available:
                continue
            s_df = raw[s].copy().dropna()
            if s_df.empty:
                continue
            s_df["Symbol"] = s
            all_res.append(s_df)
    else:
        s_df = raw.copy().dropna()
        if not s_df.empty:
            s_df["Symbol"] = symbols[0]
            all_res.append(s_df)

    if not all_res:
        return pd.DataFrame()

    out = pd.concat(all_res)
    out = out.reset_index()
    if "Datetime" in out.columns and "Date" not in out.columns:
        out = out.rename(columns={"Datetime": "Date"})
    return out


# ======================================================
# 2) UI
# ======================================================

st.set_page_config(page_title="Sunhero｜股市智能超盤中控台", layout="wide")
st.title("Sunhero｜股市智能超盤中控台")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("啟動全域掃描與結構分析"):
    try:
        with st.spinner("執行中：技術面篩選 → 結構面補強 → 法人 3 日 → 裁決引擎"):
            now = datetime.now(TW_TZ)
            trade_date = get_trade_date_tw(now)
            current_hour = now.hour
            current_session = analyzer.SESSION_EOD if current_hour >= 15 else analyzer.SESSION_INTRADAY

            # --- Macro ---
            indices_df = fetch_detailed_indices()

            total_amount = fetch_market_amount(trade_date) if market == "tw-share" else "N/A"
            total_inst = fetch_market_total_inst(trade_date) if market == "tw-share" else "N/A"

            inst_df_3d, inst_status, inst_dates_3d = (pd.DataFrame(), "PENDING", [])
            if market == "tw-share":
                inst_df_3d, inst_status, inst_dates_3d = fetch_inst_data_3d_finmind(trade_date)

            # --- Market OHLCV ---
            full_df = fetch_market_data(market)

            # --- Analyzer ---
            top_20, err = analyzer.run_analysis(
                full_df,
                session=current_session,
                market=market,
                trade_date=trade_date,
                inst_df_3d=inst_df_3d,
                inst_status=inst_status,
                inst_dates_3d=inst_dates_3d,
            )

            # --- Macro panel ---
            st.subheader("宏觀戰情室")
            c1, c2, c3 = st.columns(3)
            c1.metric("交易日", trade_date)
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

            # --- Diagnostics ---
            if err:
                st.sidebar.error(f"系統警示: {err}")

            if full_df is None or full_df.empty:
                st.sidebar.warning("資料源為空（可能連線失敗或市場休市）")
                st.stop()

            # --- JSON build ---
            macro_overview = {
                "amount": total_amount,
                "inst_net": total_inst,
                "trade_date": trade_date,
                "inst_status": inst_status,
                "inst_dates_3d": str(inst_dates_3d),
                "kill_switch": False,      # 預留：你未來接 V14-Watch 或其他風險源
                "degraded_mode": (inst_status != "READY"),
                "v14_watch": False,        # 預留
            }

            macro_dict = {
                "overview": macro_overview,
                "indices": indices_df.to_dict(orient="records") if not indices_df.empty else [],
            }

            # --- Arbiter (V15.6.3) ---
            stocks = top_20.to_dict("records") if top_20 is not None and not top_20.empty else []
            for s in stocks:
                # 這裡 macro 只傳 overview，符合你 Prompt 的 macro.overview
                s["FinalDecision"] = {
                    "Conservative": arbitrate(s, macro_overview, "Conservative"),
                    "Aggressive": arbitrate(s, macro_overview, "Aggressive"),
                }

            payload = analyzer.generate_ai_json_v1563(
                stocks=stocks,
                market=market,
                session=current_session,
                macro=macro_dict,
            )

            # --- Output ---
            st.subheader("AI 戰略數據包（JSON｜V15.6.3）")
            st.code(payload, language="json")

            st.subheader("Top20 核心清單")
            if top_20 is not None and not top_20.empty:
                cols = ["Symbol", "Close", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
                keep = [c for c in cols if c in top_20.columns]
                st.dataframe(top_20[keep], use_container_width=True)

    except Exception as e:
        st.error("系統發生預期外錯誤")
        st.exception(e)
        st.stop()
