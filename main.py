# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator + UCC V19.1）
# FINAL HARDENED BUILD - 完整中文化與效能優化版
# =========================================================
import json
import time
import warnings
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz
import requests
import streamlit as st
import yfinance as yf

from ucc_v19_1 import UCCv19_1

warnings.filterwarnings("ignore")

# 設置網頁
st.set_page_config(page_title="Sunhero｜中控台", layout="wide", initial_sidebar_state="expanded")
APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator + UCC V19.1)"

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

SYMBOLS_TOP20 = [
    "2330.TW", "2317.TW", "2454.TW", "2308.TW", "2382.TW",
    "3231.TW", "2376.TW", "3017.TW", "3324.TW", "3661.TW",
    "2881.TW", "2882.TW", "2891.TW", "2886.TW", "2603.TW",
    "2609.TW", "1605.TW", "1513.TW", "1519.TW", "2002.TW"
]

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY", "2881.TW": "富邦金", "2882.TW": "國泰金",
    "2891.TW": "中信金", "2886.TW": "兆豐金", "2603.TW": "長榮", "2609.TW": "陽明",
    "1605.TW": "華新", "1513.TW": "中興電", "1519.TW": "華城", "2002.TW": "中鋼"
}

def get_taipei_now() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))

def last_trading_day(d: datetime) -> str:
    x = d
    while x.weekday() >= 5:
        x -= timedelta(days=1)
    return x.strftime("%Y-%m-%d")

# =========================================================
# 數據抓取 (加入 st.cache_data 避免重複抓取導致網頁無反應)
# =========================================================
@st.cache_data(ttl=300)
def fetch_twii_and_vix() -> Tuple[Optional[float], Optional[float], List[str]]:
    reasons: List[str] = []
    twii, vix = None, None
    try:
        twii_df = yf.download(TWII_SYMBOL, period="5d", progress=False)
        if not twii_df.empty:
            twii = float(twii_df["Close"].dropna().iloc[-1])
    except Exception:
        reasons.append("取得加權指數失敗")

    try:
        vix_df = yf.download(VIX_SYMBOL, period="5d", progress=False)
        if not vix_df.empty:
            vix = float(vix_df["Close"].dropna().iloc[-1])
    except Exception:
        reasons.append("取得VIX指數失敗")

    return twii, vix, reasons

@st.cache_data(ttl=300)
def fetch_market_institutional_summary() -> Tuple[str, str]:
    """獲取三大法人買賣超 (供儀表板顯示)"""
    try:
        now = get_taipei_now()
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        params = {
            "dataset": "TaiwanStockTotalInstitutionalInvestors",
            "start_date": start_date,
        }
        r = requests.get(FINMIND_URL, params=params, timeout=5)
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                df = pd.DataFrame(data)
                last_date = df["date"].iloc[-1]
                df_last = df[df["date"] == last_date]
                net_buy = df_last["buy"].sum() - df_last["sell"].sum()
                net_buy_b = net_buy / 1_000_000_000
                status = "買超" if net_buy_b > 0 else "賣超"
                return f"{last_date}", f"{status} {abs(net_buy_b):.1f} 億"
    except Exception:
        pass
    return "無資料", "API 連線失敗"

@st.cache_data(ttl=300)
def fetch_prices(symbols: List[str], strict_mode: bool = True) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]], List[str]]:
    warns: List[str] = []
    prices: Dict[str, Optional[float]] = {}
    vols: Dict[str, Optional[float]] = {}
    try:
        # 使用 group_by="ticker" 且加上 timeout 機制
        df = yf.download(symbols, period="5d", progress=False, group_by="ticker", auto_adjust=False, timeout=10)
        for sym in symbols:
            try:
                if isinstance(df.columns, pd.MultiIndex):
                    sub = df[sym]
                else:
                    sub = df
                px = float(sub["Close"].dropna().iloc[-1])
                prices[sym] = px
                vols[sym] = 1.0 # 簡化成交量比例避免錯誤
            except Exception:
                prices[sym] = None
                vols[sym] = None
                warns.append(f"無法取得價格: {sym}")
    except Exception as e:
        for s in symbols:
            prices[s] = None
            vols[s] = None
        warns.append(f"YF 下載失敗: {str(e)}")
    return prices, vols, warns

def compute_smr(vix: Optional[float], twii: Optional[float]) -> Tuple[Optional[float], List[str]]:
    reasons: List[str] = []
    if vix is None or twii is None:
        return None, ["缺乏計算 SMR 的基礎指標"]
    smr = float(vix) / float(twii) * 1000  # 調整比例以便閱讀
    return smr, reasons

# =========================================================
# UI 介面
# =========================================================
def main():
    st.title(APP_TITLE)
    
    # -------------------------
    # 市場即時儀表板 (Dashboard)
    # -------------------------
    st.markdown("### 📊 市場即時狀態")
    col1, col2, col3, col4 = st.columns(4)
    
    twii_val, vix_val, _ = fetch_twii_and_vix()
    inst_date, inst_val = fetch_market_institutional_summary()
    
    col1.metric("台灣加權指數 (TWII)", f"{twii_val:,.2f}" if twii_val else "資料載入中...")
    col2.metric("VIX 恐慌指數", f"{vix_val:.2f}" if vix_val else "資料載入中...")
    col3.metric("三大法人買賣超", inst_val, delta_color="normal")
    col4.metric("法人資料日期", inst_date)
    st.markdown("---")

    # -------------------------
    # Sidebar 控制列
    # -------------------------
    st.sidebar.header("UCC 指揮中心控制台")
    run_mode = st.sidebar.radio(
        "選擇執行模式",
        options=["L1", "L2", "L3"],
        index=0,
        format_func=lambda x: {"L1": "L1 (數據審計官)", "L2": "L2 (交易裁決官)", "L3": "L3 (回撤壓測官)"}[x],
        help="L1=只檢查資料品質；L2=交易裁決（必須 L1 通過）；L3=回撤壓測（需觸發過熱或恐慌條件）"
    )
    st.sidebar.markdown("---")
    st.sidebar.subheader("法人資料防護政策")
    allow_intraday_same_day_inst = st.sidebar.toggle("盤中允許使用「當日法人資料」", value=False)
    strict_price_mode = st.sidebar.toggle("嚴格價格污染防護模式", value=True)
    finmind_token = st.sidebar.text_input("FinMind Token（選填）", value="", type="password")

    # -------------------------
    # 內容區塊
    # -------------------------
    left, right = st.columns([1, 1])
    
    # 初始化 session state
    if "payload_text" not in st.session_state:
        st.session_state["payload_text"] = "{}"

    with left:
        st.subheader("輸入 JSON Payload")
        payload_input = st.text_area("請貼上系統資料 (JSON)", height=420, value=st.session_state["payload_text"], key="payload_input")
        
        b1, b2, b3 = st.columns([1, 1, 1])
        with b1:
            if st.button("載入標準範本"):
                st.session_state["payload_text"] = json.dumps({
                    "meta": {
                        "session": "INTRADAY",
                        "market_status": "DEGRADED",
                        "current_regime": "NORMAL",
                        "confidence_level": "LOW",
                        "is_using_previous_day": True,
                    },
                    "macro": {"overview": {"max_equity_allowed_pct": 0.05}},
                    "stocks": []
                }, ensure_ascii=False, indent=2)
                st.rerun()
        with b2:
            if st.button("格式化 JSON"):
                try:
                    obj = json.loads(payload_input)
                    st.session_state["payload_text"] = json.dumps(obj, ensure_ascii=False, indent=2)
                    st.rerun()
                except Exception:
                    st.warning("⚠️ JSON 格式錯誤，無法解析")
        with b3:
            run_btn = st.button("🚀 執行 UCC 裁決", type="primary")

    with right:
        st.subheader("UCC 輸出結果")
        if not run_btn:
            st.info("👈 請點擊「執行 UCC 裁決」以檢視結果")
            return

        # -------------------------
        # 執行區段
        # -------------------------
        patch_logs: List[str] = []
        try:
            raw = json.loads(payload_input)
        except Exception as e:
            st.error(f"❌ JSON 解析失敗：{str(e)}")
            return

        # 補齊基礎資料 (Meta)
        now = get_taipei_now()
        if not raw.get("meta"): raw["meta"] = {}
        if not raw["meta"].get("timestamp"):
            raw["meta"]["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
            patch_logs.append("【修補】已自動填寫 meta.timestamp")
        if not raw["meta"].get("effective_trade_date"):
            raw["meta"]["effective_trade_date"] = now.strftime("%Y-%m-%d")

        # 補齊大盤資料 (Macro)
        if "macro" not in raw: raw["macro"] = {}
        if "overview" not in raw["macro"]: raw["macro"]["overview"] = {}
        if "integrity" not in raw["macro"]: raw["macro"]["integrity"] = {"kill": False}
        
        if raw["macro"]["overview"].get("twii_close") is None:
            raw["macro"]["overview"]["twii_close"] = twii_val
            patch_logs.append("【修補】已自動帶入加權指數收盤價")
        if raw["macro"]["overview"].get("SMR") is None:
            smr, _ = compute_smr(vix_val, twii_val)
            raw["macro"]["overview"]["SMR"] = smr
            patch_logs.append("【修補】已自動計算 SMR 數值")
            
        # 補齊個股架構
        if not raw.get("stocks"):
            raw["stocks"] = []
            for i, sym in enumerate(SYMBOLS_TOP20, start=1):
                raw["stocks"].append({
                    "Symbol": sym,
                    "Name": STOCK_NAME_MAP.get(sym, sym),
                    "Price": None,
                    "Institutional": {"Inst_Status": "USING_T_MINUS_1", "Inst_Net_3d": None}
                })
            patch_logs.append("【修補】未偵測到個股，已自動建立 Top20 觀察清單")

        # 自動抓取價格
        symbols = [s.get("Symbol") for s in raw["stocks"] if isinstance(s, dict) and s.get("Symbol")]
        price_map, _, price_warns = fetch_prices(symbols, strict_mode=strict_price_mode)
        for w in price_warns: patch_logs.append(f"【警告】{w}")
        
        for s in raw["stocks"]:
            sym = s.get("Symbol")
            if s.get("Price") is None:
                s["Price"] = price_map.get(sym)

        # 顯示處理日誌
        with st.expander("🛠️ 系統資料預處理日誌 (Patch Logs)", expanded=False):
            if patch_logs:
                st.code("\n".join(patch_logs), language="text")
            else:
                st.write("無需修補，資料完整。")

        # 執行 UCC 裁決引擎
        st.markdown("### 🏛️ UCC 統一指揮中心裁決書")
        ucc = UCCv19_1()
        out = ucc.run(raw, run_mode=run_mode)
        
        if isinstance(out, dict):
            st.json(out)
        else:
            st.code(str(out), language="text")

if __name__ == "__main__":
    main()
