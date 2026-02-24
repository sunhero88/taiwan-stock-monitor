# main.py
# =========================================================
# Sunhero｜股市智能超盤中控台（Predator + UCC V19.1）
# FINAL PATCH 2026-02-24
#
# ✅ 已完成：
# 1) UI：RUN L1/L2/L3 切換（顯示 UCC 單層輸出）
# 2) UI：盤中是否允許當日法人資料（allow_same_day_inst）
#    - 可選：盤中當日法人必須有 Token（enforce_token）
# 3) Payload：補齊 max_equity_allowed_pct + market_status_reason + confidence 多維欄位
# 4) 資料抓取層 Harden：
#    - 避免 VIX 被股票價格污染（vix 合理性驗證 0~100）
#    - 避免股票 Price 沿用上一檔殘值（失敗即 None）
#    - 自動補 market_status_reason：TWII_MISSING / VIX_INVALID / PRICE_MISSING_n
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

# 依你的體制定義
SMR_OVERHEAT = 0.30
SMR_BLOW_OFF = 0.33

# 你自己的 Top20（例）
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


def _safe_float(x, default=None):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return default
        return float(x)
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


def last_trading_day(d: datetime) -> str:
    x = d
    while x.weekday() >= 5:  # Sat/Sun
        x -= timedelta(days=1)
    return x.strftime("%Y-%m-%d")


# =========================
# HARDENED DATA FETCH (PATCH)
# =========================

def yf_download_df(symbol: str, period: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(symbol, period=period, progress=False)
        if df is None or df.empty:
            return None
        return df
    except:
        return None


def yf_last_close_from_df(df: pd.DataFrame) -> Optional[float]:
    try:
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        v = float(close.iloc[-1])
        if np.isnan(v):
            return None
        return v
    except:
        return None


def validate_vix(v: Optional[float]) -> Optional[float]:
    # 合理範圍：0 < VIX <= 100（超出多半是錯欄位/污染）
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
    # 只做防污染，不做預測：避免 0 / NaN / 明顯錯值
    if x is None:
        return None
    try:
        x = float(x)
        if x < 5000 or x > 100000:
            return None
        return x
    except:
        return None


def safe_fetch_twii_df_and_vix() -> Tuple[Optional[pd.DataFrame], Optional[float], List[str]]:
    """
    回 (twii_df, vix, reasons[])
    reasons：給 market_status_reason（稽核痕跡）
    """
    reasons: List[str] = []

    twii_df = yf_download_df(TWII_SYMBOL, period="2y")
    twii_close = validate_twii_close(yf_last_close_from_df(twii_df) if twii_df is not None else None)
    if twii_close is None:
        reasons.append("TWII_MISSING_OR_INVALID")
        twii_df = None  # 直接當失效，避免後續用空 df 算 SMR

    vix_df = yf_download_df(VIX_SYMBOL, period="1mo")
    vix_raw = yf_last_close_from_df(vix_df) if vix_df is not None else None
    vix = validate_vix(vix_raw)
    if vix is None:
        reasons.append("VIX_MISSING_OR_INVALID")

    return twii_df, vix, reasons


def safe_fetch_stock_price_vol(sym: str) -> Tuple[Optional[float], Optional[float]]:
    """
    嚴格：失敗就 (None, None)，不允許沿用上一檔殘值
    """
    try:
        df = yf.download(sym, period="2mo", progress=False)
        if df is None or df.empty:
            return None, None

        c = df["Close"]
        v = df["Volume"]
        if isinstance(c, pd.DataFrame):
            c = c.iloc[:, 0]
        if isinstance(v, pd.DataFrame):
            v = v.iloc[:, 0]

        px = float(c.iloc[-1])
        if np.isnan(px):
            return None, None

        if len(v) >= 20:
            ma20 = float(v.rolling(20).mean().iloc[-1])
            vr = float(v.iloc[-1] / ma20) if ma20 and ma20 > 0 else 1.0
        else:
            vr = 1.0

        return px, vr
    except:
        return None, None


# =========================
# Market amount
# =========================

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


# =========================
# Regime metrics (SMR)
# =========================

def compute_regime_metrics(twii_df: Optional[pd.DataFrame]) -> dict:
    if twii_df is None or twii_df.empty or len(twii_df) < 210:
        return {
            "twii_close": None, "SMR": None, "Slope5": None, "Acceleration": None,
            "Top_Divergence": False, "Blow_Off_Phase": False, "MOMENTUM_LOCK": False
        }

    close = twii_df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close = close.astype(float)

    twii_close = validate_twii_close(float(close.iloc[-1]))
    if twii_close is None:
        return {
            "twii_close": None, "SMR": None, "Slope5": None, "Acceleration": None,
            "Top_Divergence": False, "Blow_Off_Phase": False, "MOMENTUM_LOCK": False
        }

    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    slope5_series = smr_series.diff(5)
    accel_series = slope5_series.diff(2)

    smr = _safe_float(smr_series.iloc[-1], default=None)
    slope5 = _safe_float(slope5_series.iloc[-1], default=None)
    accel = _safe_float(accel_series.iloc[-1], default=None)

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
    lock_reason: List[str] = []

    # 核心指標缺失 -> DATA_FAILURE -> max_equity=0
    if m.get("twii_close") is None or m.get("SMR") is None or vix_last is None:
        lock_reason.append("DATA_FAILURE_CORE_METRIC_MISSING")
        return "DATA_FAILURE", 0.0, lock_reason

    smr = float(m["SMR"])
    bop = bool(m.get("Blow_Off_Phase"))

    # 示例：可依你自己的版本調整
    if vix_last > 35:
        return "CRASH_RISK", 0.10, []
    if bop or smr >= SMR_BLOW_OFF or smr >= SMR_OVERHEAT:
        return "CRITICAL_OVERHEAT", 0.10, []
    if smr >= 0.25:
        return "OVERHEAT", 0.40, []
    return "NORMAL", 0.85, []


# =========================
# FinMind Institutional
# =========================

def fetch_inst_3d(symbols: List[str], target_date: str, token: str) -> pd.DataFrame:
    """
    回傳 columns: symbol, net_3d
    若抓不到 -> empty df
    """
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
            # 有些資料集欄位不同，這裡保守處理
            buy = pd.to_numeric(df.get("buy", 0), errors="coerce").fillna(0)
            sell = pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
            df["net"] = buy - sell
            net_sum = float(df.tail(3)["net"].sum())
            rows.append({"symbol": sym, "net_3d": net_sum})
        except:
            continue

    return pd.DataFrame(rows)


# =========================
# Confidence helper
# =========================

def price_conf_level(missing_cnt: int, total: int) -> str:
    if total <= 0:
        return "LOW"
    if missing_cnt == 0:
        return "HIGH"
    if missing_cnt <= max(1, total // 10):
        return "MEDIUM"
    return "LOW"


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
            value=False,
            help="關閉：盤中預設使用 T-1（USING_T_MINUS_1 / inst_data_fresh=false）。\n"
                 "開啟：盤中嘗試當日法人（READY / inst_data_fresh=true）。"
        )
        enforce_token_when_same_day = st.toggle(
            "（建議）盤中當日法人必須有 Token（否則退回 T-1）",
            value=True,
            help="開啟：若未填 Token，會自動退回使用 T-1，避免 READY 假新鮮。"
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
        # 1) Hardened fetch: TWII df + VIX
        twii_df, vix_last, idx_reasons = safe_fetch_twii_df_and_vix()
        market_status_reason.extend(idx_reasons)

        # 2) Regime metrics
        m = compute_regime_metrics(twii_df)

        # 3) Amount
        amt = fetch_blended_amount(trade_date)
        if amt.status_tpex != "OK":
            market_status_reason.append("TPEX_ESTIMATED")
        if amt.status_twse != "OK":
            market_status_reason.append("TWSE_ESTIMATED")
        if amt.confidence_level == "LOW":
            market_status_reason.append("AMOUNT_CONF_LOW")

        market_status = "DEGRADED" if len(market_status_reason) > 0 else "NORMAL"
        conf_penalty = 0.5 if amt.confidence_level == "LOW" else 1.0

        # 4) regime + max_equity
        regime, base_limit, lock_reason = pick_regime_and_limit(m, vix_last)

        final_limit = base_limit * conf_penalty

        # 如果 DATA_FAILURE -> final_limit=0
        # 若非 DATA_FAILURE，但 final_limit==0（不合理），保守給 5% 但留下 lock_reason
        if regime != "DATA_FAILURE" and final_limit == 0.0:
            lock_reason.append("UNEXPECTED_ZERO_LIMIT_GUARD")
            final_limit = 0.05

        # 5) 盤中法人策略（UI 開關決定）
        use_same_day_inst = False
        if session == "EOD":
            use_same_day_inst = True
        else:
            use_same_day_inst = bool(allow_intraday_same_day_inst)
            if enforce_token_when_same_day and use_same_day_inst and (not token):
                use_same_day_inst = False  # 沒 token 退回 T-1

        if use_same_day_inst:
            inst_effective_date = trade_date
            is_using_previous_day = False
        else:
            inst_effective_date = last_trading_day(now - timedelta(days=1))
            is_using_previous_day = True

        inst_source = "FinMind" if token else "FinMind_PUBLIC_OR_EMPTY_TOKEN"

        # 6) 拉法人（用 inst_effective_date）
        symbols = SYMBOLS_TOP20[:topn]
        inst_df = fetch_inst_3d(symbols, inst_effective_date, token)

        # 7) 個股：嚴格抓價/量比（不允許沿用殘值）
        stocks_output: List[Dict[str, Any]] = []
        missing_price_cnt = 0
        no_inst_cnt = 0

        smr_val = m.get("SMR") if m.get("SMR") is not None else None

        for i, sym in enumerate(symbols, 1):
            price, vr_raw = safe_fetch_stock_price_vol(sym)
            if price is None:
                missing_price_cnt += 1

            vr = None
            if vr_raw is not None:
                vr = (vr_raw / progress) if (session == "INTRADAY") else vr_raw

            has_inst = (not inst_df.empty) and (sym in inst_df["symbol"].values)
            net_val = float(inst_df[inst_df["symbol"] == sym]["net_3d"].iloc[0]) if has_inst else None

            if has_inst:
                if use_same_day_inst:
                    inst_status = "READY"
                    inst_fresh = True
                else:
                    inst_status = "USING_T_MINUS_1"
                    inst_fresh = False
            else:
                inst_status = "NO_UPDATE_TODAY"
                inst_fresh = False
                no_inst_cnt += 1

            # tier_level（示例）：你可換回自己版本的 tier 判斷
            if smr_val is not None and smr_val >= SMR_OVERHEAT:
                tier_level = 2  # 過熱強制弱化
            else:
                tier_level = 1 if (vr is not None and vr >= 1.2 and inst_fresh) else 2

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

        # 8) 補充 market_status_reason：價格缺失
        if missing_price_cnt > 0:
            market_status_reason.append(f"PRICE_MISSING_{missing_price_cnt}_OF_{len(stocks_output)}")

        # 9) confidence 多維欄位（稽核可回放）
        price_conf = price_conf_level(missing_price_cnt, len(stocks_output))
        volume_conf = "MEDIUM" if session == "INTRADAY" else "HIGH"
        # 法人信心：盤中用 T-1 => LOW；若盤中當日但缺很多 => MEDIUM/LOW
        if session == "EOD":
            inst_conf = "HIGH" if no_inst_cnt == 0 else "MEDIUM"
        else:
            if use_same_day_inst:
                inst_conf = "MEDIUM" if no_inst_cnt <= max(1, len(stocks_output) // 5) else "LOW"
            else:
                inst_conf = "LOW"

        # 10) 若有任何原因 -> market_status 至少 DEGRADED
        market_status = "DEGRADED" if len(market_status_reason) > 0 else "NORMAL"

        # 11) macro.overview 組裝（保留 None 不硬補）
        macro_overview = dict(m)
        macro_overview["vix"] = vix_last
        macro_overview["max_equity_allowed_pct"] = float(final_limit)
        macro_overview["calc_version"] = "V16.3.x+UCC_PATCH_2026-02-24"
        macro_overview["slope5_def"] = "diff5_of_SMR"

        # 12) payload（最終一致版）
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
    # UI: 摘要
    # =======================
    st.subheader("📡 宏觀 / 風控摘要")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("market_status", payload["meta"]["market_status"], delta=";".join(payload["meta"]["market_status_reason"]) or "OK", delta_color="off")
    c2.metric("current_regime", payload["meta"]["current_regime"])
    c3.metric("max_equity_allowed_pct", f"{payload['macro']['overview']['max_equity_allowed_pct']*100:.1f}%")
    c4.metric("SMR", str(payload["macro"]["overview"]["SMR"]), delta=f"BlowOff={payload['macro']['overview']['Blow_Off_Phase']}", delta_color="off")

    st.caption(
        f"盤中法人策略：allow_same_day={allow_intraday_same_day_inst}, enforce_token={enforce_token_when_same_day}, "
        f"resolved_use_same_day={payload['meta']['intraday_institutional_policy']['resolved_use_same_day']}, "
        f"inst_effective_date={payload['meta']['intraday_institutional_policy']['inst_effective_date']}"
    )

    # =======================
    # UI: UCC 輸出
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
