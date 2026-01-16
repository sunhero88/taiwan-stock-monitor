# -*- coding: utf-8 -*-
"""
Filename: main.py
Version: Predator V15.5.1 Patch (High-Availability & Caching + Data Normalization)
Notes:
- 修補單檔下載結構漂移（單檔直接 return）
- yfinance index 正規化為 Date 欄位（避免 analyzer 端補救機制頻繁啟動）
- 快取 key 帶入 date_str（跨日一致性更穩）
- 追加資料品質指標：Volume=0 比例（最常見 no_results 根因）
"""

import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import analyzer  # 確保 analyzer.py V15.4 在同目錄
from datetime import datetime
import pytz

TW_TZ = pytz.timezone("Asia/Taipei")

# ======================================================
# 0. 小工具
# ======================================================

def _today_str_tw() -> str:
    return datetime.now(TW_TZ).strftime("%Y-%m-%d")


# ======================================================
# 1. 數據抓取模組 (全面導入快取機制)
# ======================================================

@st.cache_data(ttl=60, show_spinner=False)
def fetch_detailed_indices():
    """
    抓取國際指數 (快取 60秒)
    使用逐一抓取策略，確保數值型別安全
    """
    tickers = {
        "^TWII": "🇹🇼 加權指數",
        "^TWOII": "🇹🇼 櫃買指數",
        "^SOX": "🇺🇸 費城半導體",
        "^DJI": "🇺🇸 道瓊工業",
    }

    data_list = []

    for ticker, name in tickers.items():
        try:
            # 取 6 天防缺漏
            hist = yf.Ticker(ticker).history(period="6d")
            hist = hist.dropna()

            if hist.empty or len(hist) < 2:
                raise ValueError("History empty")

            latest = hist.iloc[-1]
            prev = hist.iloc[-2]

            price = float(latest["Close"])
            prev_close = float(prev["Close"])
            change = price - prev_close
            pct = (change / prev_close) * 100 if prev_close != 0 else 0

            data_list.append(
                {
                    "指數名稱": name,
                    "現價": f"{price:,.0f}",
                    "漲跌": f"{change:+.2f}",
                    "幅度": f"{pct:+.2f}%",
                    "開盤": f"{float(latest.get('Open', 0)):,.0f}",
                    "最高": f"{float(latest.get('High', 0)):,.0f}",
                    "最低": f"{float(latest.get('Low', 0)):,.0f}",
                    "昨收": f"{prev_close:,.0f}",
                }
            )

        except Exception:
            data_list.append(
                {
                    "指數名稱": name,
                    "現價": "-",
                    "漲跌": "-",
                    "幅度": "-",
                    "開盤": "-",
                    "最高": "-",
                    "最低": "-",
                    "昨收": "-",
                }
            )

    return pd.DataFrame(data_list)


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_amount(date_str: str):
    """
    抓取大盤成交額 (快取 60秒)
    以 date_str 作為 cache key，避免跨日短暫混入
    """
    now = datetime.now(TW_TZ)
    if 9 <= now.hour < 15:
        return "⚡ 盤中統計中"

    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={
                "dataset": "TaiwanStockPrice",
                "data_id": "TAIEX",
                "date": date_str,
            },
            timeout=3,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            return f"{data['data'][0]['Trading_Money'] / 100000000:.0f} 億"
    except Exception:
        pass

    return "待更新"


@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_total_inst(date_str: str):
    """
    抓取全市場法人動向 (快取 60秒)
    以 date_str 作為 cache key，避免跨日短暫混入
    """
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return "⚡ 盤中動能觀測"

    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={
                "dataset": "TaiwanStockTotalInstitutionalInvestors",
                "date": date_str,
            },
            timeout=3,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            net = (df["buy"].sum() - df["sell"].sum()) / 100000000
            return f"🔴+{net:.1f}億" if net > 0 else f"🔵{net:.1f}億"
    except Exception:
        pass

    return "待更新"


@st.cache_data(ttl=300, show_spinner=False)
def fetch_inst_data_finmind_stock(date_str: str):
    """
    抓取個股法人籌碼 (快取 5分鐘)
    以 date_str 作為 cache key，避免跨日短暫混入
    """
    now = datetime.now(TW_TZ)
    if now.hour < 15:
        return pd.DataFrame()

    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={
                "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                "date": date_str,
            },
            timeout=5,
        )
        data = r.json()
        if data.get("msg") == "success" and data.get("data"):
            df = pd.DataFrame(data["data"])
            df["Net"] = df["buy"] - df["sell"]
            df_group = df.groupby("stock_id")["Net"].sum().reset_index()
            df_group.columns = ["Symbol", "Inst_Net"]
            df_group["Symbol"] = df_group["Symbol"].astype(str) + ".TW"
            return df_group
    except Exception:
        pass

    return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_market_data(m_id: str, date_str: str):
    """
    核心數據下載 (快取 5分鐘)
    - 嚴格判定 MultiIndex
    - index 正規化為 Date 欄位
    - 單檔下載：直接提醒式 return，避免結構漂移與重複 append
    """
    targets = {
        "tw-share": [
            "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW", "3231.TW", "2376.TW", "6669.TW",
            "2603.TW", "2609.TW", "2408.TW", "2303.TW", "2881.TW", "2882.TW", "2357.TW", "3035.TW"
        ],
        "us": ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMD", "META", "AMZN"],
    }
    symbols = targets.get(m_id, targets["tw-share"])

    # 下載數據
    raw_data = yf.download(
        symbols, period="2mo", interval="1d", group_by="ticker", progress=False
    )

    inst_df = fetch_inst_data_finmind_stock(date_str) if m_id == "tw-share" else pd.DataFrame()

    # ---------- 單檔：yfinance 可能回傳單層欄位（非 MultiIndex） ----------
    if len(symbols) == 1 and not raw_data.empty and not isinstance(raw_data.columns, pd.MultiIndex):
        s = symbols[0]
        s_df = raw_data.copy().dropna()
        if s_df.empty:
            return pd.DataFrame()

        # index 正規化成 Date 欄位
        s_df.index.name = "Date"
        s_df = s_df.reset_index()

        s_df["Symbol"] = s

        # 合併籌碼
        if not inst_df.empty and s in inst_df["Symbol"].values:
            net_val = inst_df.loc[inst_df["Symbol"] == s, "Inst_Net"].values[0]
            s_df["Inst_Net"] = net_val
            val_k = round(net_val / 1000, 1)
            s_df["Inst_Status"] = f"🔴+{val_k}k" if net_val > 0 else f"🔵{val_k}k"
        else:
            s_df["Inst_Net"] = 0
            s_df["Inst_Status"] = "N/A"

        return s_df

    # ---------- 多檔：必須是 MultiIndex，否則視為下載失敗/結構異常 ----------
    if not isinstance(raw_data.columns, pd.MultiIndex):
        return pd.DataFrame()

    available_tickers = list(raw_data.columns.levels[0])
    all_res = []

    for s in symbols:
        try:
            if s not in available_tickers:
                continue

            s_df = raw_data[s].copy().dropna()
            if s_df.empty:
                continue

            # index 正規化成 Date 欄位（強烈建議：讓 analyzer 不必猜 index 名稱）
            s_df.index.name = "Date"
            s_df = s_df.reset_index()

            s_df["Symbol"] = s

            # 合併籌碼
            if not inst_df.empty and s in inst_df["Symbol"].values:
                net_val = inst_df.loc[inst_df["Symbol"] == s, "Inst_Net"].values[0]
                s_df["Inst_Net"] = net_val
                val_k = round(net_val / 1000, 1)
                s_df["Inst_Status"] = f"🔴+{val_k}k" if net_val > 0 else f"🔵{val_k}k"
            else:
                s_df["Inst_Net"] = 0
                s_df["Inst_Status"] = "N/A"

            all_res.append(s_df)

        except Exception:
            continue

    return pd.concat(all_res, ignore_index=True) if all_res else pd.DataFrame()


# ======================================================
# 2. UI 主程式
# ======================================================

st.set_page_config(page_title="Predator V15.5.1", layout="wide")
st.title("🦅 Predator 指揮中心 V15.5.1 (高可用快取修補版)")

market = st.sidebar.selectbox("市場介入", ["tw-share", "us"])

if st.button("🔥 啟動全域掃描與結構分析"):
    try:
        with st.spinner("🚀 正在執行：技術面篩選 ➔ 動能估算 ➔ 基本面結構掃描..."):

            date_str = _today_str_tw()

            # 1. 宏觀數據 (快取)
            indices_df = fetch_detailed_indices()
            total_amount = fetch_market_amount(date_str)
            total_inst = fetch_market_total_inst(date_str)

            # 2. 下載個股數據 (快取)
            full_df = fetch_market_data(market, date_str=date_str)

            # --- 宏觀儀表板 ---
            st.subheader("🌍 宏觀戰情室")
            c1, c2 = st.columns(2)
            c1.metric("💰 大盤成交金額", total_amount)
            c2.metric("🏦 全市場法人", total_inst)

            if not indices_df.empty:
                def color_change(val):
                    if isinstance(val, str) and "+" in val:
                        return "color: #ff4b4b"
                    elif isinstance(val, str) and "-" in val:
                        return "color: #00c853"
                    return ""

                st.dataframe(
                    indices_df.style.applymap(color_change, subset=["漲跌", "幅度"]),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.warning("⚠️ 國際指數數據暫時無法獲取")

            st.divider()

            # --- 核心分析 ---
            st.subheader("🦅 Predator 戰略核心分析")

            current_hour = datetime.now(TW_TZ).hour
            # Session 對齊 FinMind：15:00 後才視為 EOD
            current_session = analyzer.SESSION_EOD if current_hour >= 15 else analyzer.SESSION_INTRADAY

            # 執行分析
            top_10, err_msg = analyzer.run_analysis(full_df, session=current_session)

            # --- 可觀測性 & 資料品質指標 ---
            if err_msg:
                st.sidebar.error(f"⚠️ [系統警示] {err_msg}")

            if full_df is not None and not full_df.empty:
                total_rows = len(full_df)
                total_symbols = full_df["Symbol"].nunique() if "Symbol" in full_df.columns else 0

                missing_close = (
                    full_df["Close"].isna().mean() * 100 if "Close" in full_df.columns else 100
                )
                missing_vol = (
                    full_df["Volume"].isna().mean() * 100 if "Volume" in full_df.columns else 100
                )
                zero_vol = (
                    (full_df["Volume"] == 0).mean() * 100 if "Volume" in full_df.columns else 100
                )

                st.sidebar.info(
                    "📊 資料源診斷:\n"
                    f"- 日期鍵: {date_str}\n"
                    f"- 總筆數: {total_rows:,}\n"
                    f"- 監控標的: {total_symbols}\n"
                    "📌 資料品質:\n"
                    f"- Close 缺值: {missing_close:.1f}%\n"
                    f"- Volume 缺值: {missing_vol:.1f}%\n"
                    f"- Volume=0 比例: {zero_vol:.1f}%"
                )
            else:
                st.sidebar.warning("⚠️ 資料源為空 (yfinance 連線失敗或結構異常 / 休市)")

            # --- 熔斷保護 ---
            if top_10 is None or top_10.empty:
                st.warning("📉 本次掃描無符合 V15.5.1 戰略標準之標的。")
                if err_msg:
                    st.caption(f"排除原因參考: {err_msg}")
                else:
                    st.caption("可能原因：市場量縮、乖離過大、基本面惡化或資料品質不足。")
                st.stop()

            # --- 成功結果渲染 ---
            st.success(f"✅ V15.5.1 結構化數據構建完成 ({len(top_10)} 檔入選)")

            # 生成 JSON
            macro_dict = {
                "overview": {"amount": total_amount, "inst_net": total_inst},
                "indices": indices_df.to_dict(orient="records") if not indices_df.empty else [],
            }

            json_payload = analyzer.generate_ai_json(
                top_10, market=market, session=current_session, macro_data=macro_dict
            )

            st.subheader("🤖 AI 戰略數據包 (JSON V15.5.1)")
            st.caption(
                f"包含：技術評分、結構面 (OPM/QoQ/PE)、Kill Switch 狀態。Session: {current_session}"
            )
            st.code(json_payload, language="json")

            st.subheader("📊 關鍵標的指標")
            cols = ["Symbol", "Close", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
            if "Inst_Status" in top_10.columns:
                cols.insert(3, "Inst_Status")

            # 顯示前，確保欄位存在（避免上游資料缺欄造成 KeyError）
            safe_cols = [c for c in cols if c in top_10.columns]
            st.dataframe(top_10[safe_cols], use_container_width=True)

    except Exception as e:
        st.error("❌ 系統發生預期外錯誤")
        st.exception(e)
        st.stop()
