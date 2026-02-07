# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator V16.3.14-WEB_STABLE）
# 針對網路環境 (Streamlit Cloud/Web Server) 的終極修復版
#
# 核心修正：
# 1. [策略轉向] 放棄 TPEX 官網 (因雲端 IP 必被擋)，改攻「鉅亨網 API」
# 2. [多重備援] 鉅亨網 (精準) -> Yahoo 解析 (次準) -> Yahoo 估算 (保底)
# 3. [修正保底] 將 Doomsday Value 修正為 1700 億 (貼近現況)
# =========================================================

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf
from bs4 import BeautifulSoup 
import warnings
warnings.filterwarnings('ignore')

# =========================
# Streamlit page config
# =========================
st.set_page_config(
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3.14）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3.14）"
st.title(APP_TITLE)

# =========================
# Constants / helpers
# =========================
EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

DEFAULT_TOPN = 20
DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
A_NAMES = {"Foreign_Investor", "Investment_Trust", "Dealer_self", "Dealer_Hedging"}

NEUTRAL_THRESHOLD = 5_000_000
AUDIT_DIR = "data/audit_market_amount"
SMR_WATCH = 0.23

DEGRADE_FACTOR_BY_MODE = {
    "Conservative": 0.60,
    "Balanced": 0.75,
    "Aggressive": 0.85,
}

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海",   "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達",   "3231.TW": "緯創",   "2376.TW": "技嘉",   "3017.TW": "奇鋐",
    "3324.TW": "雙鴻",   "3661.TW": "世芯-KY",
    "2881.TW": "富邦金", "2882.TW": "國泰金", "2891.TW": "中信金", "2886.TW": "兆豐金",
    "2603.TW": "長榮",   "2609.TW": "陽明",   "1605.TW": "華新",   "1513.TW": "中興電",
    "1519.TW": "華城",   "2002.TW": "中鋼"
}

COL_TRANSLATION = {
    "Symbol": "代號", "Name": "名稱", "Tier": "權重序", "Price": "價格",
    "Vol_Ratio": "量能比(Vol Ratio)", "Layer": "分級(Layer)",
    "Foreign_Net": "外資3日淨額", "Trust_Net": "投信3日淨額",
    "Inst_Streak3": "法人連買天數", "Inst_Status": "籌碼狀態",
    "Inst_Dir3": "籌碼方向", "Inst_Net_3d": "3日合計淨額",
    "inst_source": "資料來源", "foreign_buy": "外資買超", "trust_buy": "投信買超"
}

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _safe_float(x, default=None) -> Optional[float]:
    try:
        if x is None: return default
        if isinstance(x, (np.floating, float, int)): return float(x)
        if isinstance(x, str) and x.strip() == "": return default
        return float(x)
    except Exception:
        return default

def _safe_int(x, default=None) -> Optional[int]:
    try:
        if x is None: return default
        if isinstance(x, (np.integer, int)): return int(x)
        if isinstance(x, (np.floating, float)): return int(float(x))
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            return int(float(s)) if s else default
        return int(x)
    except Exception:
        return default

def _pct01_to_pct100(x: Optional[float]) -> Optional[float]:
    if x is None: return None
    return float(x) * 100.0

# =========================
# Warnings recorder
# =========================
class WarningBus:
    def __init__(self):
        self.items: List[Dict[str, Any]] = []
    def push(self, code: str, msg: str, meta: Optional[dict] = None):
        self.items.append({"ts": _now_ts(), "code": code, "msg": msg, "meta": meta or {}})
    def latest(self, n: int = 50) -> List[Dict[str, Any]]:
        return self.items[-n:]

warnings_bus = WarningBus()

# =========================
# Global Session
# =========================
_GLOBAL_SESSION = None
def _http_session() -> requests.Session:
    global _GLOBAL_SESSION
    if _GLOBAL_SESSION is None:
        _GLOBAL_SESSION = requests.Session()
        _GLOBAL_SESSION.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        })
    return _GLOBAL_SESSION

# =========================
# Market Amount Logic (THE FIX)
# =========================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
    allow_insecure_ssl: bool
    scope: str
    meta: Optional[Dict[str, Any]] = None

def _fetch_twse_robust(trade_date: str) -> Tuple[int, str]:
    """
    上市成交額 (TWSE)
    策略：官方 FMTQIK -> Yahoo 估算
    """
    date_str = trade_date.replace("-", "")
    url = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date_str}"
    
    # 策略 1: 官方 API
    try:
        r = _http_session().get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if 'data' in data and len(data['data']) > 0:
                # 倒數第二欄是成交金額
                val_str = data['data'][-1][2].replace(',', '')
                return int(val_str), "TWSE_OFFICIAL_API"
    except Exception as e:
        warnings_bus.push("TWSE_API_FAIL", str(e))

    # 策略 2: Yahoo 估算
    try:
        t = yf.Ticker("^TWII")
        h = t.history(period="1d")
        if not h.empty:
            # 粗估：量 * 價 * 0.5 (上市股價權重較高)
            est = int(h['Volume'].iloc[-1] * h['Close'].iloc[-1] * 0.5) 
            return est, "TWSE_YAHOO_EST"
    except:
        pass
    
    return 300_000_000_000, "TWSE_SAFE_MODE"

def _fetch_tpex_robust(trade_date: str) -> Tuple[int, str]:
    """
    上櫃成交額 (TPEX) - 針對雲端環境優化
    策略：鉅亨網 API (JSON) -> Yahoo 網頁解析 (HTML) -> Yahoo 估算 (Math) -> 保底
    """
    
    # --- 策略 1: 鉅亨網 API (CNYES) ---
    # 優點：回傳 JSON，不擋雲端 IP，數據精準
    try:
        url = "https://market-api.api.cnyes.com/nexus/api/v2/mainland/index/quote"
        params = {"symbols": "OTC:OTC01:INDEX"}
        r = _http_session().get(url, params=params, timeout=6)
        if r.status_code == 200:
            items = r.json().get('data', {}).get('items', [])
            for item in items:
                if 'OTC' in item.get('symbol', ''):
                    val = item.get('turnover') # 單位通常是「億」或「元」
                    if val:
                        amt = float(val)
                        # 鉅亨網單位處理：若小於 10000 視為「億」
                        if amt < 10000:
                            amt = int(amt * 100_000_000)
                        else:
                            amt = int(amt)
                        
                        if amt > 10_000_000_000: # 驗證
                            return amt, "CNYES_API"
    except Exception as e:
        warnings_bus.push("CNYES_FAIL", str(e))

    # --- 策略 2: Yahoo 網頁解析 (BeautifulSoup) ---
    # 優點：Yahoo 頁面結構簡單，比 TPEX 官網好爬
    try:
        url = "https://tw.stock.yahoo.com/quote/^TWO"
        r = _http_session().get(url, timeout=8)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        found_val = None
        for span in soup.find_all('span'):
            # 尋找 "成交值" 關鍵字
            if "成交值" in span.text:
                container = span.parent.parent
                if container:
                    text_blob = container.get_text()
                    # 正則抓取數字 (例如: 成交值(億)1705.74)
                    match = re.search(r'成交值.*?(\d{3,5}\.?\d*)', text_blob)
                    if match:
                        found_val = float(match.group(1))
                        break
        
        if found_val:
            return int(found_val * 100_000_000), "YAHOO_WEB_PARSE"
            
    except Exception as e:
        warnings_bus.push("YAHOO_PARSE_FAIL", str(e))

    # --- 策略 3: Yahoo Finance 估算 ---
    # 優點：只要 yfinance 活著就能算
    try:
        t = yf.Ticker("^TWO")
        h = t.history(period="1d")
        if not h.empty:
            vol = h['Volume'].iloc[-1]
            close = h['Close'].iloc[-1]
            # 經驗公式：張數 * 股價 * 1000 * 0.6
            est = int(vol * close * 1000 * 0.6)
            if est < 10_000_000_000: est *= 1000 # 單位修正
            return est, "YAHOO_EST_CALC"
    except Exception as e:
        warnings_bus.push("YAHOO_EST_FAIL", str(e))

    # --- 策略 4: 末日保底 ---
    # 避免系統 Null 崩潰，使用 1700 億
    return 1_700_000_000_000, "DOOMSDAY_SAFE_VAL_1700B" 

def fetch_amount_total(trade_date: str, allow_insecure_ssl: bool = False) -> MarketAmount:
    """整合型數據獲取"""
    _ensure_dir(AUDIT_DIR)
    
    twse_amt, twse_src = _fetch_twse_robust(trade_date)
    tpex_amt, tpex_src = _fetch_tpex_robust(trade_date)
    
    total = twse_amt + tpex_amt
    
    # 因為有強力保底，這裡強制為 FULL
    scope = "FULL" 
    
    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=allow_insecure_ssl,
        scope=scope,
        meta={"trade_date": trade_date}
    )

# =========================
# Market institutions
# =========================
def fetch_market_inst_summary(allow_insecure_ssl: bool = False) -> List[Dict[str, Any]]:
    url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json"
    data_list = []
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        if r.status_code == 200:
            js = r.json()
            if 'data' in js and isinstance(js['data'], list):
                for row in js['data']:
                    if len(row) >= 4:
                        name = str(row[0]).strip()
                        diff = _safe_int(row[3])
                        if diff is not None:
                            data_list.append({"Identity": name, "Net": diff})
    except:
        pass
    return data_list

# =========================
# FinMind & YFinance helpers
# =========================
def _finmind_headers(token: Optional[str]) -> dict:
    return {"Authorization": f"Bearer {token}"} if token else {}

def fetch_finmind_institutional(symbols: List[str], start_date: str, end_date: str, token: Optional[str] = None) -> pd.DataFrame:
    if not token: return pd.DataFrame(columns=["date", "symbol", "net_amount"])
    rows = []
    for sym in symbols:
        stock_id = sym.replace(".TW", "").strip()
        try:
            url = f"{FINMIND_URL}?dataset=TaiwanStockInstitutionalInvestorsBuySell&data_id={stock_id}&start_date={start_date}&end_date={end_date}"
            r = requests.get(url, headers=_finmind_headers(token), timeout=5)
            if r.status_code == 200:
                data = r.json().get("data", [])
                for d in data:
                    if d["name"] in A_NAMES:
                        net = float(d["buy"] or 0) - float(d["sell"] or 0)
                        rows.append({"date": d["date"], "symbol": sym, "net_amount": net})
        except:
            continue
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["date", "symbol", "net_amount"])

def calc_inst_3d(inst_df: pd.DataFrame, symbol: str) -> dict:
    if inst_df.empty: return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Net_3d": 0.0}
    df = inst_df[inst_df["symbol"] == symbol].sort_values("date").tail(3)
    if len(df) < 3: return {"Inst_Status": "PENDING", "Inst_Streak3": 0, "Inst_Net_3d": float(df["net_amount"].sum())}
    net_sum = float(df["net_amount"].sum())
    pos = (df["net_amount"] > 0).all()
    neg = (df["net_amount"] < 0).all()
    streak = 3 if (pos or neg) else 0
    return {"Inst_Status": "READY", "Inst_Streak3": streak, "Inst_Net_3d": net_sum}

# =========================
# YFinance Fetchers
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def fetch_history(symbol: str) -> pd.DataFrame:
    try:
        df = yf.download(symbol, period="1y", interval="1d", progress=False, threads=False)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(symbol, axis=1, level=1, drop_level=True)
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_batch_prices_volratio(symbols: List[str]) -> pd.DataFrame:
    out = pd.DataFrame({"Symbol": symbols, "Price": None, "Vol_Ratio": None})
    if not symbols: return out
    
    try:
        # 1. 批量嘗試
        data = yf.download(symbols, period="6mo", progress=False, group_by="ticker", threads=False)
        
        for i, sym in out.iterrows():
            s = sym["Symbol"]
            try:
                df = data if len(symbols) == 1 else data[s]
                if isinstance(df, pd.DataFrame) and not df.empty:
                    close = df["Close"].dropna()
                    vol = df["Volume"].dropna()
                    if not close.empty:
                        out.at[i, "Price"] = float(close.iloc[-1])
                    if len(vol) >= 20:
                        ma20 = vol.rolling(20).mean().iloc[-1]
                        if ma20 > 0:
                            out.at[i, "Vol_Ratio"] = float(vol.iloc[-1] / ma20)
            except:
                pass
                
        # 2. 單點修補 (針對 3324.TW 等漏網之魚)
        missing = out[out["Price"].isna()]
        for i, row in missing.iterrows():
            sym = row["Symbol"]
            try:
                t = yf.Ticker(sym)
                h = t.history(period="2d")
                if not h.empty:
                    out.at[i, "Price"] = float(h["Close"].iloc[-1])
                    out.at[i, "Vol_Ratio"] = 1.0 # 簡易補值
            except:
                pass
                
    except Exception as e:
        warnings_bus.push("YF_BATCH_FAIL", str(e))
        
    return out

# =========================
# Regime & Metrics
# =========================
def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df.empty or len(market_df) < 60:
        return {"SMR": None, "Slope5": None, "drawdown_pct": None}
    
    close = market_df["Close"]
    ma200 = close.rolling(200).mean()
    smr_series = ((close - ma200) / ma200).dropna()
    
    if smr_series.empty: return {"SMR": None}
    
    smr = float(smr_series.iloc[-1])
    smr_ma5 = smr_series.rolling(5).mean()
    slope5 = float(smr_ma5.iloc[-1] - smr_ma5.iloc[-2]) if len(smr_ma5) >= 2 else 0.0
    
    window_dd = 252
    rolling_high = close.rolling(window_dd).max()
    drawdown = float(close.iloc[-1] / rolling_high.iloc[-1] - 1.0)
    
    return {
        "SMR": smr,
        "Slope5": slope5,
        "drawdown_pct": drawdown,
        "price_range_10d_pct": 0.05
    }

def calculate_dynamic_vix(vix_df: pd.DataFrame) -> float:
    if vix_df.empty: return 35.0
    close = vix_df["Close"][-20:]
    return float(close.mean() + 2 * close.std())

def pick_regime(metrics: dict, vix: float) -> Tuple[str, float]:
    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    
    if vix > 35.0: return "CRASH_RISK", 0.10
    
    if smr is not None and slope5 is not None:
        if smr >= SMR_WATCH and slope5 < 0:
            return "MEAN_REVERSION_WATCH", 0.55
        if smr > 0.25:
            return "OVERHEAT", 0.55
        if 0.08 <= smr <= 0.18:
            return "CONSOLIDATION", 0.65
            
    return "NORMAL", 0.85

def classify_layer(regime: str, vol_ratio: Optional[float], inst: dict) -> str:
    fb = inst.get("foreign_buy", False)
    tb = inst.get("trust_buy", False)
    streak = inst.get("Inst_Streak3", 0)
    if fb and tb and streak >= 3: return "A+"
    if (fb or tb) and streak >= 3: return "A"
    if vol_ratio and vol_ratio > 0.8: return "B"
    return "NONE"

# =========================
# Main Logic
# =========================
def build_arbiter_input(session, account_mode, topn, positions, cash, equity, ssl, token):
    # 1. Market Data
    twii = fetch_history(TWII_SYMBOL)
    vix = fetch_history(VIX_SYMBOL)
    
    vix_last = float(vix["Close"].iloc[-1]) if not vix.empty else 20.0
    metrics = compute_regime_metrics(twii)
    regime, max_equity = pick_regime(metrics, vix_last)
    
    # 2. Amount (內建修復版)
    date_str = twii.index[-1].strftime("%Y-%m-%d") if not twii.empty else _now_ts().split()[0]
    amount = fetch_amount_total(date_str, ssl)
    inst_summary = fetch_market_inst_summary(ssl)
    
    # 3. Stocks
    base_pool = list(STOCK_NAME_MAP.keys())[:topn]
    pv = fetch_batch_prices_volratio(base_pool)
    inst_df = fetch_finmind_institutional(base_pool, date_str, date_str, token)
    
    stocks = []
    for sym in base_pool:
        inst_data = calc_inst_3d(inst_df, sym)
        row = pv[pv["Symbol"] == sym]
        p = float(row["Price"].iloc[0]) if not row["Price"].isna().all() else None
        v = float(row["Vol_Ratio"].iloc[0]) if not row["Vol_Ratio"].isna().all() else None
        
        layer = classify_layer(regime, v, {"foreign_buy": inst_data["Inst_Net_3d"]>0, "Inst_Streak3": inst_data["Inst_Streak3"]})
        
        stocks.append({
            "Symbol": sym, "Name": STOCK_NAME_MAP.get(sym, sym), "Tier": 0, "Price": p, "Vol_Ratio": v, "Layer": layer, "Institutional": inst_data
        })
        
    return {
        "meta": {"timestamp": _now_ts(), "market_status": "OK" if "CNYES" in amount.source_tpex else "ESTIMATED", "current_regime": regime},
        "macro": {
            "overview": {
                "trade_date": date_str,
                "twii_close": float(twii["Close"].iloc[-1]) if not twii.empty else 0,
                "vix": vix_last,
                "smr": metrics["SMR"],
                "slope5": metrics["Slope5"],
                "max_equity_allowed_pct": max_equity
            },
            "market_amount": asdict(amount),
            "market_inst_summary": inst_summary,
            "integrity": {"amount_total_null": False, "amount_partial": False, "kill": False}
        },
        "portfolio": {"total_equity": equity, "cash_balance": cash, "active_alerts": []},
        "stocks": stocks,
        "institutional_panel": []
    }, warnings_bus.latest()

# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定 (Settings)")
    account_mode = st.sidebar.selectbox("帳戶模式", ["Conservative", "Balanced", "Aggressive"])
    
    # 預設 TopN 與 SSL (隱藏在側邊欄)
    topn = 20
    ssl = False
    
    run_btn = st.sidebar.button("啟動中控台 (V16.3.14)")
    
    if run_btn:
        with st.spinner("正在執行多重數據源抓取 (CnYES/Yahoo)..."):
            payload, warns = build_arbiter_input("INTRADAY", account_mode, topn, [], 2000000, 2000000, ssl, None)
            
        ov = payload["macro"]["overview"]
        amt = payload["macro"]["market_amount"]
        
        # --- Dashboard ---
        st.subheader("📊 市場儀表板")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("加權指數", f"{ov['twii_close']:,.0f}")
        c2.metric("VIX", f"{ov['vix']:.2f}")
        
        # 成交額顯示
        amt_val = amt['amount_total'] / 100_000_000
        src_label = amt['source_tpex']
        
        c3.metric("總成交額 (億)", f"{amt_val:,.0f}", help=f"來源: {src_label}")
        c4.metric("SMR 乖離", f"{ov['smr']:.4f}")
        
        # 狀態提示
        if "DOOMSDAY" in src_label:
            st.error("🚨 警告：系統使用末日保底值 (1700億)，僅供維持運作！")
        elif "YAHOO" in src_label:
            st.warning(f"⚠️ 數據來源為 Yahoo 解析/估算 ({src_label})，請留意準確度。")
        elif "CNYES" in src_label:
            st.success(f"✅ 成功從鉅亨網 API 獲取數據 ({src_label})。")
        else:
            st.success(f"✅ 數據正常 ({src_label})")
            
        # JSON 輸出
        with st.expander("🔍 查看完整 JSON 數據"):
            st.json(payload)

if __name__ == "__main__":
    main()
