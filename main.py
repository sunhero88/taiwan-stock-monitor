# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator + UCC V19.1）
# FINAL HARDENED BUILD (避免反覆改來改去)
# Date: 2026-02-24
#
# ✅ 目標：
# - SMR 計算穩健：抓不到就明確 reason，不用假數據
# - 價格抓取防污染：不沿用殘值 + 重試 + 同價群偵測
# - 成交金額估算留痕：TWSE/TPEX ESTIMATED 透明稽核
# - UI：RUN L1/L2/L3、盤中法人策略、嚴格價格污染模式、DATA_FAILURE 軟鎖 5%（可選）
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

st.set_page_config(page_title="Sunhero｜Predator + UCC", layout="wide", initial_sidebar_state="expanded")
APP_TITLE = "Sunhero｜股市智能超盤中控台 (Predator + UCC V19.1)"

TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

SMR_OVERHEAT = 0.30
SMR_BLOW_OFF = 0.33

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


# -----------------------------
# Time helpers
# -----------------------------
def get_taipei_now() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))


def last_trading_day(d: datetime) -> str:
    x = d
    while x.weekday() >= 5:
        x -= timedelta(days=1)
    return x.strftime("%Y-%m-%d")


def get_intraday_progress() -> float:
    now = get_taipei_now()
    start = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=13, minute=30, second=0, microsecond=0)
    if now < start:
        return 0.01
    if now > end:
        return 1.0
    return max(0.01, (now - start).total_seconds() / (end - start).total_seconds())


# -----------------------------
# Safe cast
# -----------------------------
def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        v = float(x)
        if np.isnan(v):
            return default
        return v
    except:
        return default


def _safe_int(x, default=0):
    try:
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return int(float(x))
    except:
        return default


def _to_roc_date(ymd: str) -> str:
    dt = pd.to_datetime(ymd)
    return f"{dt.year - 1911}/{dt.month:02d}/{dt.day:02d}"


# =========================================================
# 1) HARDENED: Index fetch + validation
# =========================================================
def validate_vix(v: Optional[float]) -> Optional[float]:
    # 合理範圍護欄：0 < VIX <= 100
    if v is None:
        return None
    try:
        v = float(v)
        if v <= 0 or v > 100:
            return None
        return v
    except:
        return None


def validate_twii_close(x: Optional[float]) -> Optional[float]:
    # 防污染：避免 0 / NaN / 明顯怪值
    if x is None:
        return None
    try:
        x = float(x)
        if x < 5000 or x > 100000:
            return None
        return x
    except:
        return None


def _yf_download_df(symbol: str, period: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period=period, progress=False)
        if df is None or df.empty:
            return None
        return df
    except:
        return None


def _yf_history_df(symbol: str, period: str) -> Optional[pd.DataFrame]:
    try:
        t = yf.Ticker(symbol)
        df = t.history(period=period, interval="1d", auto_adjust=False)
        if df is None or df.empty:
            return None
        # history 回傳 index 可能含 tz，統一
        df = df.reset_index(drop=False)
        # 重新整理成 download 類型的欄位結構
        if "Close" not in df.columns:
            return None
        df = df.set_index(df.columns[0])  # Date / Datetime
        return df
    except:
        return None


def _last_close(df: Optional[pd.DataFrame]) -> Optional[float]:
    if df is None or df.empty:
        return None
    try:
        c = df["Close"]
        if isinstance(c, pd.DataFrame):
            c = c.iloc[:, 0]
        c = pd.to_numeric(c, errors="coerce").dropna()
        if c.empty:
            return None
        return float(c.iloc[-1])
    except:
        return None


def fetch_twii_df_and_vix_hardened(retry: int = 3) -> Tuple[Optional[pd.DataFrame], Optional[float], List[str]]:
    """
    回 (twii_df, vix, reasons)
    reasons 用於 meta.market_status_reason
    """
    reasons: List[str] = []

    twii_df = None
    vix_val = None

    for k in range(retry):
        # TWII
        if twii_df is None:
            df = _yf_download_df(TWII_SYMBOL, period="5y")
            if df is None:
                df = _yf_history_df(TWII_SYMBOL, period="5y")
            # 確認可用性：Close 有效且筆數夠
            if df is not None:
                c = pd.to_numeric(df["Close"] if not isinstance(df["Close"], pd.DataFrame) else df["Close"].iloc[:, 0],
                                  errors="coerce").dropna()
                if len(c) >= 260:
                    twii_df = df
                else:
                    # 筆數不足，先保留 None，等下一次 retry
                    twii_df = None

        # VIX
        if vix_val is None:
            vix_df = _yf_download_df(VIX_SYMBOL, period="3mo")
            if vix_df is None:
                vix_df = _yf_history_df(VIX_SYMBOL, period="3mo")
            vix_raw = _last_close(vix_df)
            vix_val = validate_vix(vix_raw)

        if twii_df is not None and vix_val is not None:
            break

        time.sleep(0.25 * (k + 1))

    # reason 結案
    twii_close = validate_twii_close(_last_close(twii_df))
    if twii_df is None or twii_close is None:
        reasons.append("TWII_SERIES_TOO_SHORT_OR_INVALID")
        twii_df = None  # 直接視為失效

    if vix_val is None:
        reasons.append("VIX_MISSING_OR_INVALID")

    return twii_df, vix_val, reasons


# =========================================================
# 2) HARDENED: Regime metrics (SMR)
# =========================================================
def compute_regime_metrics_hardened(twii_df: Optional[pd.DataFrame]) -> Tuple[dict, List[str]]:
    reasons: List[str] = []

    if twii_df is None or twii_df.empty:
        reasons.append("SMR_INPUT_TWII_DF_MISSING")
        return {
            "twii_close": None, "SMR": None, "Slope5": None, "Acceleration": None,
            "Top_Divergence": False, "Blow_Off_Phase": False, "MOMENTUM_LOCK": False
        }, reasons

    try:
        c = twii_df["Close"]
        if isinstance(c, pd.DataFrame):
            c = c.iloc[:, 0]
        c = pd.to_numeric(c, errors="coerce").dropna()

        if len(c) < 260:
            reasons.append("TWII_SERIES_TOO_SHORT_FOR_SMR")
            twii_close = validate_twii_close(float(c.iloc[-1])) if len(c) else None
            return {
                "twii_close": twii_close, "SMR": None, "Slope5": None, "Acceleration": None,
                "Top_Divergence": False, "Blow_Off_Phase": False, "MOMENTUM_LOCK": False
            }, reasons

        twii_close = validate_twii_close(float(c.iloc[-1]))
        if twii_close is None:
            reasons.append("TWII_CLOSE_INVALID")
            return {
                "twii_close": None, "SMR": None, "Slope5": None, "Acceleration": None,
                "Top_Divergence": False, "Blow_Off_Phase": False, "MOMENTUM_LOCK": False
            }, reasons

        ma200 = c.rolling(200).mean()
        smr_series = (c - ma200) / ma200
        slope5_series = smr_series.diff(5)
        accel_series = slope5_series.diff(2)

        smr_valid = pd.to_numeric(smr_series, errors="coerce").dropna()
        slope5_valid = pd.to_numeric(slope5_series, errors="coerce").dropna()
        accel_valid = pd.to_numeric(accel_series, errors="coerce").dropna()

        smr = float(smr_valid.iloc[-1]) if not smr_valid.empty else None
        slope5 = float(slope5_valid.iloc[-1]) if not slope5_valid.empty else None
        accel = float(accel_valid.iloc[-1]) if not accel_valid.empty else None

        if smr is None:
            reasons.append("SMR_CALC_RESULT_NA")

        bop = bool(smr is not None and slope5 is not None and (smr >= SMR_BLOW_OFF) and (slope5 >= 0.08))
        top_div = bool(smr is not None and slope5 is not None and accel is not None and (smr > 0.15) and (slope5 > 0) and (accel < -0.01))
        mom_lock = bool(slope5 is not None and slope5 > 0)

        return {
            "twii_close": twii_close,
            "SMR": smr,
            "Slope5": slope5,
            "Acceleration": accel,
            "Top_Divergence": top_div,
            "Blow_Off_Phase": bop,
            "MOMENTUM_LOCK": mom_lock
        }, reasons

    except:
        reasons.append("SMR_EXCEPTION")
        return {
            "twii_close": None, "SMR": None, "Slope5": None, "Acceleration": None,
            "Top_Divergence": False, "Blow_Off_Phase": False, "MOMENTUM_LOCK": False
        }, reasons


# =========================================================
# 3) Market amount (TWSE/TPEX)
# =========================================================
@dataclass
class MarketAmount:
    amount_twse: int
    amount_tpex: int
    amount_total_raw: int
    amount_total_blended: int
    source_twse: str
    source_tpex: str
    status_twse: str
    status_tpex: str
    confidence_level: str


def fetch_blended_amount(trade_date: str) -> Tuple[MarketAmount, List[str]]:
    reasons: List[str] = []
    ymd = trade_date.replace("-", "")

    # TWSE：可能 timeout/SSL，被擋就 safe mode
    twse_amt = 0
    twse_src = "TWSE_FAIL"
    twse_sts = "FAIL"
    try:
        url_twse = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={ymd}"
        r = requests.get(url_twse, timeout=5, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        js = r.json()
        if "data" in js and js["data"]:
            twse_amt = sum(_safe_int(row[3], 0) for row in js["data"])
            twse_src = "TWSE_OK:AUDIT_SUM"
            twse_sts = "OK"
        else:
            raise RuntimeError("TWSE_EMPTY")
    except:
        twse_amt = 950_000_000_000
        twse_src = "TWSE_SAFE_MODE"
        twse_sts = "ESTIMATED"
        reasons.append("TWSE_ESTIMATED")

    # TPEX：常見 fail → safe mode 200B
    tpex_amt = 0
    tpex_src = "TPEX_FAIL"
    tpex_sts = "FAIL"
    try:
        roc = _to_roc_date(trade_date)
        url_tpex = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc}&se=EW"
        r = requests.get(url_tpex, timeout=5, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        js = r.json()
        if "aaData" in js and js["aaData"]:
            tpex_amt = _safe_int(js["aaData"][0][2], 0)
            tpex_src = "TPEX_OFFICIAL_OK"
            tpex_sts = "OK"
        else:
            raise RuntimeError("TPEX_EMPTY")
    except:
        tpex_amt = 200_000_000_000
        tpex_src = "TPEX_SAFE_MODE_200B"
        tpex_sts = "ESTIMATED"
        reasons.append("TPEX_ESTIMATED")

    conf = "HIGH" if (twse_sts == "OK" and tpex_sts == "OK") else "LOW"
    if conf == "LOW":
        reasons.append("AMOUNT_CONF_LOW")

    # raw_total：若任一市場估算，用「可稽核的保守定義」：raw=twse(官方或估算)，blended=twse+tpex(官方或估算)
    raw_total = twse_amt
    blended_total = twse_amt + tpex_amt

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total_raw=raw_total,
        amount_total_blended=blended_total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        status_twse=twse_sts,
        status_tpex=tpex_sts,
        confidence_level=conf
    ), reasons


# =========================================================
# 4) Stock price & vol ratio (anti-drift)
# =========================================================
def fetch_stock_price_vol_hardened(sym: str, retry: int = 2) -> Tuple[Optional[float], Optional[float], bool]:
    """
    回 (price, vol_ratio, success)
    - success=True 表示抓到合理 price
    """
    for k in range(retry + 1):
        # 方法 1：download
        df = _yf_download_df(sym, period="3mo")
        if df is None or df.empty:
            # 方法 2：history
            df = _yf_history_df(sym, period="3mo")

        if df is not None and not df.empty and "Close" in df.columns and "Volume" in df.columns:
            try:
                c = df["Close"]
                v = df["Volume"]
                if isinstance(c, pd.DataFrame):
                    c = c.iloc[:, 0]
                if isinstance(v, pd.DataFrame):
                    v = v.iloc[:, 0]

                c = pd.to_numeric(c, errors="coerce").dropna()
                v = pd.to_numeric(v, errors="coerce").dropna()

                if c.empty:
                    raise RuntimeError("EMPTY_CLOSE")

                price = float(c.iloc[-1])
                if not np.isfinite(price) or price <= 0:
                    raise RuntimeError("BAD_PRICE")

                # vol ratio：最後 20 筆有效值
                if len(v) >= 20:
                    v20 = v.tail(20)
                    ma20 = float(v20.mean()) if float(v20.mean()) > 0 else None
                    vr = float(v.iloc[-1] / ma20) if ma20 else 1.0
                else:
                    vr = 1.0

                return price, vr, True
            except:
                pass

        time.sleep(0.15 * (k + 1))

    return None, None, False


def detect_price_duplicate_cluster(stocks: List[Dict[str, Any]], min_cluster: int = 3, min_price: float = 50.0) -> Dict[float, List[str]]:
    """
    找出同價群（價格完全相同）且群聚 >= min_cluster，且 price >= min_price
    回 {price: [symbols]}
    """
    bucket: Dict[float, List[str]] = {}
    for s in stocks:
        p = s.get("Price")
        sym = s.get("Symbol")
        if p is None or sym is None:
            continue
        try:
            p = float(p)
            if p < min_price:
                continue
            bucket.setdefault(p, []).append(sym)
        except:
            continue

    return {p: syms for p, syms in bucket.items() if len(syms) >= min_cluster}


# =========================================================
# 5) FinMind Institutional
# =========================================================
def fetch_inst_3d(symbols: List[str], target_date: str, token: str) -> pd.DataFrame:
    rows = []
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    start_date = (pd.to_datetime(target_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")

    for sym in symbols:
        stock_id = sym.replace(".TW", "").replace(".TWO", "")
        try:
            params = {
                "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": target_date
            }
            r = requests.get(FINMIND_URL, headers=headers, params=params, timeout=4)
            js = r.json()
            data = js.get("data", [])
            if not data:
                continue
            df = pd.DataFrame(data)

            buy = pd.to_numeric(df.get("buy", 0), errors="coerce").fillna(0)
            sell = pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
            df["net"] = buy - sell
            net_sum = float(df.tail(3)["net"].sum())
            rows.append({"symbol": sym, "net_3d": net_sum})
        except:
            continue

    return pd.DataFrame(rows)


# =========================================================
# 6) Regime + max equity
# =========================================================
def pick_regime_and_limit(m: dict, vix_last: Optional[float]) -> Tuple[str, float, List[str]]:
    lock_reason: List[str] = []

    # 核心缺失：先判 DATA_FAILURE
    if m.get("SMR") is None or vix_last is None:
        lock_reason.append("DATA_FAILURE_CORE_METRIC_MISSING")
        return "DATA_FAILURE", 0.0, lock_reason

    smr = float(m["SMR"])
    bop = bool(m.get("Blow_Off_Phase"))

    if vix_last > 35:
        return "CRASH_RISK", 0.10, []
    if bop or smr >= SMR_BLOW_OFF or smr >= SMR_OVERHEAT:
        return "CRITICAL_OVERHEAT", 0.10, []
    if smr >= 0.25:
        return "OVERHEAT", 0.40, []
    return "NORMAL", 0.85, []


def price_conf_level(missing_cnt: int, total: int) -> str:
    if total <= 0:
        return "LOW"
    if missing_cnt == 0:
        return "HIGH"
    if missing_cnt <= max(1, total // 10):
        return "MEDIUM"
    return "LOW"


# =========================================================
# MAIN
# =========================================================
def main():
    st.title(APP_TITLE)

    with st.sidebar:
        st.header("⚙️ 系統參數")
        session = st.selectbox("時段", ["INTRADAY", "EOD"])
        topn = st.slider("監控 TopN", 5, 20, 20)
        token = st.text_input("FinMind Token（選填）", type="password")

        st.divider()
        st.header("🧾 盤中法人資料策略")
        allow_intraday_same_day_inst = st.toggle(
            "盤中允許當日法人資料（Inst=READY）",
            value=False
        )
        enforce_token_when_same_day = st.toggle(
            "盤中當日法人必須有 Token（否則退回 T-1）",
            value=True
        )

        st.divider()
        st.header("🧯 防污染護欄")
        strict_price_duplicate_guard = st.toggle(
            "嚴格模式：同價群（≥3 檔且價>50）視為疑似污染，除第一檔外改為 None",
            value=True
        )

        st.divider()
        st.header("🧱 DATA_FAILURE 行為")
        data_failure_softcap_5pct = st.toggle(
            "DATA_FAILURE 軟鎖：若 twii_close 有但 SMR 缺失，允許 max_equity=5%（仍保守）",
            value=False,
            help="關閉＝完全合憲『缺核心指標就 0%』。開啟＝盤中避免永遠卡死，但仍保守 5%。"
        )

        st.divider()
        st.header("🧠 UCC 裁決模式")
        run_mode = st.radio("RUN", ["L1", "L2", "L3"], index=0, horizontal=True)

        if st.button("🚀 啟動 / 更新"):
            st.session_state.run_trigger = True

    if not st.session_state.get("run_trigger", False):
        st.info("👈 請在左側設定參數並點擊「啟動 / 更新」。")
        return

    now = get_taipei_now()
    trade_date = now.strftime("%Y-%m-%d")
    progress = get_intraday_progress() if session == "INTRADAY" else 1.0

    market_status_reason: List[str] = []

    with st.spinner("資料抓取 + 稽核中..."):
        # 1) index hardened
        twii_df, vix_last, idx_reasons = fetch_twii_df_and_vix_hardened(retry=3)
        market_status_reason.extend(idx_reasons)

        # 2) SMR hardened
        m, smr_reasons = compute_regime_metrics_hardened(twii_df)
        # 若 SMR 仍缺，留下原因碼
        for r in smr_reasons:
            if r not in market_status_reason:
                market_status_reason.append(r)

        # 3) amount
        amt, amt_reasons = fetch_blended_amount(trade_date)
        market_status_reason.extend([r for r in amt_reasons if r not in market_status_reason])

        # 4) regime + max equity
        regime, base_limit, lock_reason = pick_regime_and_limit(m, vix_last)

        # DATA_FAILURE 軟鎖（可選）：twii_close 有但 SMR 缺失 → 5%
        if regime == "DATA_FAILURE" and data_failure_softcap_5pct:
            if m.get("twii_close") is not None and m.get("SMR") is None:
                base_limit = 0.05
                lock_reason = ["DATA_FAILURE_SMR_MISSING_SOFTCAP_5PCT"]

        # 成交金額信心懲罰（保守）
        conf_penalty = 0.5 if amt.confidence_level == "LOW" else 1.0
        final_limit = float(base_limit * conf_penalty)

        # 5) 盤中法人策略
        if session == "EOD":
            use_same_day_inst = True
        else:
            use_same_day_inst = bool(allow_intraday_same_day_inst)
            if enforce_token_when_same_day and use_same_day_inst and (not token):
                use_same_day_inst = False

        if use_same_day_inst:
            inst_effective_date = trade_date
            is_using_previous_day = False
        else:
            inst_effective_date = last_trading_day(now - timedelta(days=1))
            is_using_previous_day = True

        inst_source = "FinMind" if token else "FinMind_PUBLIC_OR_EMPTY_TOKEN"

        # 6) inst fetch
        symbols = SYMBOLS_TOP20[:topn]
        inst_df = fetch_inst_3d(symbols, inst_effective_date, token)

        # 7) stocks fetch hardened
        stocks_output: List[Dict[str, Any]] = []
        missing_price_cnt = 0
        no_inst_cnt = 0

        for i, sym in enumerate(symbols, 1):
            price, vr_raw, ok = fetch_stock_price_vol_hardened(sym, retry=2)
            if not ok:
                missing_price_cnt += 1

            vr = None
            if vr_raw is not None:
                vr = (vr_raw / progress) if session == "INTRADAY" else vr_raw

            has_inst = (not inst_df.empty) and (sym in inst_df["symbol"].values)
            net_val = float(inst_df[inst_df["symbol"] == sym]["net_3d"].iloc[0]) if has_inst else None

            # 法人狀態（嚴格一致）
            if use_same_day_inst:
                if has_inst:
                    inst_status = "READY"
                    inst_fresh = True
                else:
                    inst_status = "NO_UPDATE_TODAY"
                    inst_fresh = False
                    no_inst_cnt += 1
            else:
                # T-1 模式：一律 USING_T_MINUS_1（即使抓到值，也標示非新鮮）
                inst_status = "USING_T_MINUS_1"
                inst_fresh = False

            # tier_level（示例）：SMR 過熱強制弱化
            smr_val = m.get("SMR")
            if smr_val is not None and float(smr_val) >= SMR_OVERHEAT:
                tier_level = 2
            else:
                tier_level = 2

            stocks_output.append({
                "Symbol": sym,
                "Name": STOCK_NAME_MAP.get(sym, sym),
                "rank": i,
                "tier_level": tier_level,
                "Price": price,
                "Vol_Ratio": vr,
                "Institutional": {
                    "Inst_Status": inst_status,
                    "Inst_Net_3d": net_val,
                    "inst_unit": "shares",
                    "inst_data_fresh": inst_fresh,
                    "inst_effective_date": inst_effective_date,
                    "inst_source": inst_source
                }
            })

        # 8) 價格缺失 reason
        if missing_price_cnt > 0:
            market_status_reason.append(f"PRICE_MISSING_{missing_price_cnt}_OF_{len(stocks_output)}")

        # 9) 同價群污染偵測（可選嚴格）
        dup_clusters = detect_price_duplicate_cluster(stocks_output, min_cluster=3, min_price=50.0)
        if dup_clusters:
            for p, syms in dup_clusters.items():
                market_status_reason.append(f"PRICE_DUPLICATE_CLUSTER_{len(syms)}@{p}")
                if strict_price_duplicate_guard:
                    # 除第一個外都置 None（避免錯檔價帶入決策）
                    keep = syms[0]
                    for s in stocks_output:
                        if s["Symbol"] in syms and s["Symbol"] != keep:
                            s["Price"] = None
                            s["Vol_Ratio"] = None

        # 10) confidence 多維欄位
        price_conf = price_conf_level(missing_price_cnt, len(stocks_output))
        volume_conf = "MEDIUM" if session == "INTRADAY" else "HIGH"
        if session == "EOD":
            inst_conf = "HIGH" if no_inst_cnt == 0 else "MEDIUM"
        else:
            inst_conf = "LOW" if not use_same_day_inst else ("MEDIUM" if no_inst_cnt <= max(1, len(stocks_output)//5) else "LOW")

        # 11) market_status
        market_status = "DEGRADED" if len(market_status_reason) > 0 else "NORMAL"

        # 12) macro overview
        macro_overview = dict(m)
        macro_overview["vix"] = vix_last
        macro_overview["max_equity_allowed_pct"] = float(final_limit)
        macro_overview["calc_version"] = "V16.3.x+UCC_PATCH_2026-02-24_FINAL"
        macro_overview["slope5_def"] = "diff5_of_SMR"

        payload: Dict[str, Any] = {
            "meta": {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "session": session,
                "market_status": market_status,
                "market_status_reason": market_status_reason,
                "current_regime": regime,
                "confidence_level": amt.confidence_level,
                "is_using_previous_day": bool(is_using_previous_day),
                "effective_trade_date": inst_effective_date if is_using_previous_day else trade_date,
                "max_equity_lock_reason": lock_reason if lock_reason else [],
                "confidence": {
                    "price": price_conf,
                    "volume": volume_conf,
                    "institutional": inst_conf
                },
                "intraday_institutional_policy": {
                    "allow_same_day": bool(allow_intraday_same_day_inst),
                    "enforce_token_when_same_day": bool(enforce_token_when_same_day),
                    "resolved_use_same_day": bool(use_same_day_inst),
                    "inst_effective_date": inst_effective_date
                }
            },
            "macro": {
                "overview": macro_overview,
                "market_amount": asdict(amt),
                "integrity": {
                    "kill": False,
                    "vix_invalid": True if vix_last is None else False,
                    "reason": "OK" if vix_last is not None else "VIX_INVALID_OR_MISSING"
                }
            },
            "stocks": stocks_output
        }

    # =======================
    # UI: Summary
    # =======================
    st.subheader("📡 宏觀 / 風控摘要")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("market_status", payload["meta"]["market_status"], delta=";".join(payload["meta"]["market_status_reason"]) or "OK", delta_color="off")
    c2.metric("current_regime", payload["meta"]["current_regime"])
    c3.metric("max_equity_allowed_pct", f"{payload['macro']['overview']['max_equity_allowed_pct']*100:.1f}%")
    c4.metric("SMR", str(payload["macro"]["overview"]["SMR"]), delta=f"VIX={payload['macro']['overview']['vix']}", delta_color="off")

    st.caption(
        f"盤中法人策略：allow_same_day={allow_intraday_same_day_inst}, enforce_token={enforce_token_when_same_day}, "
        f"resolved_use_same_day={payload['meta']['intraday_institutional_policy']['resolved_use_same_day']}, "
        f"inst_effective_date={payload['meta']['intraday_institutional_policy']['inst_effective_date']}"
    )

    # =======================
    # UI: UCC
    # =======================
    st.markdown("---")
    st.subheader("🧠 UCC V19.1 輸出（RUN 單一層級）")
    ucc = UCCv19_1()
    ucc_out = ucc.run(payload, run_mode=run_mode)

    if isinstance(ucc_out, dict):
        st.json(ucc_out)
    else:
        st.code(str(ucc_out), language="text")

    # =======================
    # UI: Payload
    # =======================
    st.markdown("---")
    st.subheader("📦 最終 Payload（稽核可回放）")
    st.code(json.dumps(payload, ensure_ascii=False, indent=2), language="json")


if __name__ == "__main__":
    main()
