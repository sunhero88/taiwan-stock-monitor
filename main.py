# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（V16.3.x + UCC V19.1）修正版
# - 修正 current_regime 與 macro 指標矛盾
# - 修正 max_equity_allowed_pct=0.0 無原因
# - 修正 INTRADAY 法人新鮮度標記與稽核欄位
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

SMR_CRITICAL = 0.30
SMR_BLOW_OFF = 0.33

STOCK_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2308.TW": "台達電",
    "2382.TW": "廣達", "3231.TW": "緯創", "2376.TW": "技嘉", "3017.TW": "奇鋐",
    "3324.TW": "雙鴻", "3661.TW": "世芯-KY", "2881.TW": "富邦金", "2882.TW": "國泰金",
    "2891.TW": "中信金", "2886.TW": "兆豐金", "2603.TW": "長榮", "2609.TW": "陽明",
    "1605.TW": "華新", "1513.TW": "中興電", "1519.TW": "華城", "2002.TW": "中鋼"
}


def get_taipei_now() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))


def _safe_float(x, default=0.0):
    try:
        return float(x) if x is not None and not pd.isna(x) else default
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


def get_intraday_progress() -> float:
    now = get_taipei_now()
    start = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end = now.replace(hour=13, minute=30, second=0, microsecond=0)
    if now < start:
        return 0.01
    if now > end:
        return 1.0
    return max(0.01, (now - start).total_seconds() / (end - start).total_seconds())


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


def fetch_blended_amount(trade_date: str) -> MarketAmount:
    ymd = trade_date.replace("-", "")
    url_twse = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date={ymd}"
    twse_amt = 0
    twse_src = "TWSE_FAIL"
    twse_sts = "FAIL"
    try:
        r = requests.get(url_twse, timeout=5, verify=False)
        js = r.json()
        if "data" in js:
            twse_amt = sum(_safe_int(row[3], 0) for row in js["data"])
            twse_src = "TWSE_OK:AUDIT_SUM"
            twse_sts = "OK"
    except:
        twse_amt = 950_000_000_000
        twse_src = "TWSE_SAFE_MODE"
        twse_sts = "ESTIMATED"

    roc = _to_roc_date(trade_date)
    url_tpex = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc}&se=EW"
    tpex_amt = 0
    tpex_src = "TPEX_FAIL"
    tpex_sts = "FAIL"
    try:
        r = requests.get(url_tpex, headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
        js = r.json()
        if "aaData" in js and len(js["aaData"]) > 0:
            tpex_amt = _safe_int(js["aaData"][0][2])
            tpex_src = "TPEX_OFFICIAL_OK"
            tpex_sts = "OK"
    except:
        pass

    if tpex_sts == "FAIL":
        tpex_amt = 200_000_000_000
        tpex_src = "TPEX_SAFE_MODE_200B"
        tpex_sts = "ESTIMATED"

    conf = "HIGH" if (twse_sts == "OK" and tpex_sts == "OK") else "LOW"
    raw_total = twse_amt if tpex_sts != "OK" else (twse_amt + tpex_amt)
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
    )


def _single_fetch_price_vol(sym: str) -> Tuple[Optional[float], Optional[float]]:
    base = sym.split(".")[0]
    for suffix in [".TW", ".TWO"]:
        try:
            df = yf.download(f"{base}{suffix}", period="2mo", progress=False)
            if not df.empty:
                c = df["Close"].iloc[:, 0] if isinstance(df["Close"], pd.DataFrame) else df["Close"]
                v = df["Volume"].iloc[:, 0] if isinstance(df["Volume"], pd.DataFrame) else df["Volume"]
                if len(v) >= 20:
                    ma20 = v.rolling(20).mean().iloc[-1]
                    vr = float(v.iloc[-1] / ma20) if ma20 and ma20 > 0 else 1.0
                else:
                    vr = 1.0
                return float(c.iloc[-1]), vr
        except:
            continue
    return None, None


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
            r = requests.get(FINMIND_URL, headers=headers, params=params, timeout=3)
            data = r.json().get("data", [])
            if data:
                df = pd.DataFrame(data)
                df["net"] = pd.to_numeric(df.get("buy", 0), errors="coerce").fillna(0) - pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
                net_sum = float(df.tail(3)["net"].sum())
                rows.append({"symbol": sym, "net_3d": net_sum})
        except:
            pass
    return pd.DataFrame(rows)


def compute_regime_metrics(twii_df: pd.DataFrame) -> dict:
    if twii_df is None or twii_df.empty or len(twii_df) < 200:
        return {
            "twii_close": None,
            "SMR": None,
            "Slope5": None,
            "Acceleration": None,
            "Top_Divergence": False,
            "Blow_Off_Phase": False,
            "MOMENTUM_LOCK": False
        }

    close = twii_df["Close"].iloc[:, 0] if isinstance(twii_df["Close"], pd.DataFrame) else twii_df["Close"]
    twii_close = float(close.iloc[-1])

    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    slope5_series = smr_series.diff(5)
    accel_series = slope5_series.diff(2)

    smr = _safe_float(smr_series.iloc[-1], default=np.nan)
    slope5 = _safe_float(slope5_series.iloc[-1], default=np.nan)
    accel = _safe_float(accel_series.iloc[-1], default=np.nan)

    # NaN 保留 NaN，不硬補 0（避免 DATA_FAILURE 與有效數據混用）
    def _nan_to_none(x):
        return None if (x is None or (isinstance(x, float) and np.isnan(x))) else float(x)

    smr = _nan_to_none(smr)
    slope5 = _nan_to_none(slope5)
    accel = _nan_to_none(accel)

    bop = bool(smr is not None and smr >= SMR_BLOW_OFF and (slope5 is not None and slope5 >= 0.08))
    top_div = bool(smr is not None and smr > 0.15 and (slope5 is not None and slope5 > 0) and (accel is not None and accel < -0.01))
    mom_lock = bool(slope5 is not None and slope5 > 0)

    return {
        "twii_close": twii_close,
        "SMR": smr,
        "Slope5": slope5,
        "Acceleration": accel,
        "Top_Divergence": top_div,
        "Blow_Off_Phase": bop,
        "MOMENTUM_LOCK": mom_lock
    }


def pick_regime_and_limit(m: dict, vix_last: Optional[float]) -> Tuple[str, float, List[str]]:
    """
    回傳：
      - regime: 字串（不可與宏觀矛盾）
      - base_limit: 0~1
      - lock_reason: 若 base_limit=0，需給原因（稽核必備）
    """
    lock_reason: List[str] = []

    # DATA_FAILURE 只在「核心欄位不可用」時成立，避免跟 SMR/BlowOff 矛盾
    if m.get("twii_close") is None or m.get("SMR") is None or vix_last is None or vix_last <= 0 or vix_last > 100:
        lock_reason.append("DATA_FAILURE_CORE_METRIC_MISSING")
        return "DATA_FAILURE", 0.0, lock_reason

    smr = float(m["SMR"])
    bop = bool(m.get("Blow_Off_Phase"))

    # 風險體制
    if vix_last > 35:
        return "CRASH_RISK", 0.10, []
    if bop or smr >= SMR_BLOW_OFF or smr >= SMR_CRITICAL:
        # 末端過熱：仍可給低曝險上限（不必鎖死到 0）
        return "CRITICAL_OVERHEAT", 0.10, []
    if smr >= 0.25:
        return "OVERHEAT", 0.40, []
    return "NORMAL", 0.85, []


def main():
    st.title(APP_TITLE)

    with st.sidebar:
        st.header("⚙️ 系統參數")
        session = st.selectbox("時段", ["INTRADAY", "EOD"])
        topn = st.slider("監控 TopN", 5, 20, 20)
        token = st.text_input("FinMind Token (選填)", type="password")

        st.divider()
        st.header("🧭 UCC 裁決模式")
        run_mode = st.radio("RUN", ["L1", "L2", "L3"], index=0, horizontal=True)

        if st.button("🚀 啟動/更新引擎"):
            st.session_state.run_trigger = True

    if not st.session_state.get("run_trigger", False):
        st.info("👈 請設定參數並點擊左側「啟動/更新引擎」。")
        return

    now = get_taipei_now()
    trade_date = now.strftime("%Y-%m-%d")
    progress = get_intraday_progress() if session == "INTRADAY" else 1.0

    with st.spinner("雷達掃描中..."):
        twii_df = yf.download(TWII_SYMBOL, period="2y", progress=False)
        m = compute_regime_metrics(twii_df)

        vix_df = yf.download(VIX_SYMBOL, period="1mo", progress=False)
        if vix_df is None or vix_df.empty:
            vix_last = None
        else:
            v_s = vix_df["Close"].iloc[:, 0] if isinstance(vix_df["Close"], pd.DataFrame) else vix_df["Close"]
            vix_last = float(v_s.iloc[-1])

        amt = fetch_blended_amount(trade_date)

        # market_status 與原因（枚舉）
        market_status_reason: List[str] = []
        if amt.status_tpex != "OK":
            market_status_reason.append("TPEX_ESTIMATED")
        if amt.status_twse != "OK":
            market_status_reason.append("TWSE_ESTIMATED")
        if amt.confidence_level == "LOW":
            market_status_reason.append("AMOUNT_CONF_LOW")

        market_status = "DEGRADED" if market_status_reason else "NORMAL"
        conf_penalty = 0.5 if amt.confidence_level == "LOW" else 1.0

        regime, base_limit, lock_reason = pick_regime_and_limit(m, vix_last)
        final_limit = base_limit * conf_penalty

        # ✅ 若 regime=DATA_FAILURE，final_limit=0 合理，但必須給 lock_reason
        # ✅ 若非 DATA_FAILURE，final_limit 不應該被「無故變成 0」
        if regime != "DATA_FAILURE" and final_limit == 0.0:
            lock_reason.append("UNEXPECTED_ZERO_LIMIT_GUARD")
            # 給一個最小可稽核保守值，避免「矛盾鎖死」
            final_limit = 0.05

        # ----------- 法人日期：INTRADAY 預設採 T-1，避免 READY 假新鮮 -----------
        # 你若已能取得真正當日法人資料，可把 cutover_hour 改晚或加一個開關
        cutover_hour = 15  # 15:00 前使用 T-1
        is_using_previous_day = (session == "INTRADAY" and now.hour < cutover_hour)

        inst_effective_date = trade_date
        if is_using_previous_day:
            prev = now - timedelta(days=1)
            while prev.weekday() >= 5:
                prev -= timedelta(days=1)
            inst_effective_date = prev.strftime("%Y-%m-%d")

        inst_source = "FinMind" if token else "FinMind_PUBLIC_OR_EMPTY_TOKEN"
        inst_df = fetch_inst_3d(list(STOCK_NAME_MAP.keys())[:topn], inst_effective_date, token)

        stocks_output = []
        for i, sym in enumerate(list(STOCK_NAME_MAP.keys())[:topn], 1):
            price, vr_raw = _single_fetch_price_vol(sym)
            vr = (vr_raw / progress) if vr_raw else None

            has_inst = (not inst_df.empty) and (sym in inst_df["symbol"].values)
            net_val = float(inst_df[inst_df["symbol"] == sym]["net_3d"].iloc[0]) if has_inst else None

            # ✅ 盤中 T-1：不允許標 READY/inst_data_fresh=true
            if has_inst:
                if is_using_previous_day:
                    inst_status = "USING_T_MINUS_1"
                    inst_fresh = False
                else:
                    inst_status = "READY"
                    inst_fresh = True
            else:
                inst_status = "NO_UPDATE_TODAY"
                inst_fresh = False

            # tier_level 保留你原邏輯：過熱或非新鮮 => tier=2
            smr = m.get("SMR") if m.get("SMR") is not None else 0.0
            tier_level = 2 if (smr and smr > 0.25) or (not inst_fresh) else (1 if (vr and vr > 1.2) else 2)

            stocks_output.append({
                "Symbol": sym,
                "Name": STOCK_NAME_MAP[sym],
                "rank": i,
                "tier_level": tier_level,
                "Price": price,
                "Vol_Ratio": vr,
                "Institutional": {
                    "Inst_Status": inst_status,
                    "Inst_Net_3d": net_val,
                    "inst_unit": "shares",
                    "inst_data_fresh": inst_fresh,

                    # ✅ 稽核必備：法人資料到底是哪一天、哪個來源
                    "inst_effective_date": inst_effective_date,
                    "inst_source": inst_source
                }
            })

    # ===================== UI =====================
    st.subheader("📡 宏觀體制與風控摘要")
    c1, c2, c3, c4 = st.columns(4)

    c1.metric("市場狀態", market_status, delta=(";".join(market_status_reason) if market_status_reason else "OK"), delta_color="off")
    c2.metric("體制", regime)
    c3.metric("曝險上限", f"{final_limit*100:.1f}%", delta=f"base={base_limit*100:.1f}% × conf={conf_penalty}x", delta_color="off")
    c4.metric("SMR", f"{m.get('SMR')}", delta=f"BlowOff={m.get('Blow_Off_Phase')}", delta_color="off")

    # ===================== payload（修正版） =====================
    # meta.confidence_level：仍沿用 amt.confidence_level（LOW/HIGH）
    # meta.confidence：多維只作補充，不替代
    price_conf = "HIGH" if all(s.get("Price") is not None for s in stocks_output) else "LOW"
    volume_conf = "MEDIUM" if session == "INTRADAY" else "HIGH"
    inst_conf = "LOW" if is_using_previous_day else ("MEDIUM" if (not token) else "HIGH")

    macro_overview = dict(m)
    macro_overview["vix"] = vix_last
    macro_overview["max_equity_allowed_pct"] = float(final_limit)
    macro_overview["calc_version"] = "V16.3.x+UCC_PATCH_2026-02-24"
    macro_overview["slope5_def"] = "diff5_of_SMR"

    payload: Dict[str, Any] = {
        "meta": {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "session": session,
            "market_status": market_status,
            "market_status_reason": market_status_reason,
            "current_regime": regime,
            "confidence_level": amt.confidence_level,

            # ✅ 盤中法人多數是 T-1：誠實標註，避免 READY 假新鮮
            "is_using_previous_day": bool(is_using_previous_day),
            "effective_trade_date": inst_effective_date if is_using_previous_day else trade_date,

            # ✅ 若曝險被鎖死（或接近鎖死），必須留原因
            "max_equity_lock_reason": lock_reason if lock_reason else [],

            "confidence": {
                "price": price_conf,
                "volume": volume_conf,
                "institutional": inst_conf
            }
        },
        "macro": {
            "overview": macro_overview,
            "market_amount": asdict(amt),
            "integrity": {
                # ✅ 若 lock_reason 代表硬失效，你也可以把 kill=true；這裡先維持 false，讓 L1/L2 由欄位判斷
                "kill": False,
                "reason": "OK"
            }
        },
        "stocks": stocks_output
    }

    # ===================== UCC =====================
    st.markdown("---")
    st.subheader("🧠 UCC V19.1 輸出（RUN 單一層級）")
    ucc = UCCv19_1()
    ucc_out = ucc.run(payload, run_mode=run_mode)

    if isinstance(ucc_out, dict):
        st.json(ucc_out)
    else:
        st.code(ucc_out, language="text")

    st.markdown("---")
    st.subheader("📦 最終 Payload（已修正一致性）")
    st.code(json.dumps(payload, ensure_ascii=False, indent=2), language="json")


if __name__ == "__main__":
    main()
