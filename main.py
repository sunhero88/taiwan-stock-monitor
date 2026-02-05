# main.py
# =========================================================
# Sunhero | 股市智能超盤中控台 (TopN + 持倉監控 / Predator V16.3 Stable Hybrid)
# + V16.2 Enhanced Kill-Switch / SHELTER Mode
# Single-file Streamlit app (drop-in runnable)
# =========================================================

import json
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf

# =========================
# Streamlit page config
# =========================
st.set_page_config(
    page_title="Sunhero｜股市智能超盤中控台（Predator V16.3 Stable Hybrid）",
    layout="wide",
)

APP_TITLE = "Sunhero｜股市智能超盤中控台（TopN + 持倉監控 / Predator V16.3 Stable Hybrid）"
st.title(APP_TITLE)

# =========================
# Constants / helpers
# =========================
EPS = 1e-4
TWII_SYMBOL = "^TWII"
VIX_SYMBOL = "^VIX"

DEFAULT_CASH = 2_000_000
DEFAULT_EQUITY = 2_000_000

# Kill-Switch thresholds（可調）
KILL_PRICE_NULL_PCT = 0.50       # Price 缺失 >= 50% 觸發
KILL_VOLRATIO_NULL_PCT = 0.50    # Vol_Ratio 缺失 >= 50% 觸發
KILL_CORE_MISSING_PCT = 0.50     # 核心缺失率 >= 50% 觸發（你文內的硬規則）


def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _safe_float(x, default=None) -> Optional[float]:
    try:
        if x is None:
            return default
        if isinstance(x, (np.floating, float, int)):
            return float(x)
        if isinstance(x, str) and x.strip() == "":
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, (np.integer, int)):
            return int(x)
        if isinstance(x, (np.floating, float)):
            return int(float(x))
        if isinstance(x, str):
            s = x.replace(",", "").strip()
            if s == "":
                return default
            return int(float(s))
        return int(x)
    except Exception:
        return default


# =========================
# Warnings recorder
# =========================
class WarningBus:
    def __init__(self):
        self.items: List[Dict[str, Any]] = []

    def push(self, code: str, msg: str, meta: Optional[dict] = None):
        self.items.append(
            {
                "ts": _now_ts(),
                "code": code,
                "msg": msg,
                "meta": meta or {},
            }
        )

    def latest(self, n: int = 50) -> List[Dict[str, Any]]:
        return self.items[-n:]


warnings_bus = WarningBus()

# =========================
# Market amount (TWSE/TPEX)
# =========================
@dataclass
class MarketAmount:
    amount_twse: Optional[int]
    amount_tpex: Optional[int]
    amount_total: Optional[int]
    source_twse: str
    source_tpex: str
    allow_insecure_ssl: bool


def _fetch_twse_amount(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&type=ALLBUT0999"
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()
        js = r.json()

        candidate_tables = []
        for k in ["data9", "data1", "data2", "data3", "data4", "data5", "data6", "data7", "data8"]:
            v = js.get(k)
            if isinstance(v, list) and len(v) > 0:
                candidate_tables.append((k, v))

        if not candidate_tables:
            warnings_bus.push("TWSE_AMOUNT_PARSE_FAIL", "TWSE JSON missing tables", {"url": url})
            return None, "TWSE_FAIL:TABLE_MISSING"

        # 嘗試找 fields 中「成交金額」欄位索引
        fields = js.get("fields9") or js.get("fields1") or js.get("fields") or []
        fields = [str(x) for x in fields] if isinstance(fields, list) else []
        amt_idx = None
        for i, f in enumerate(fields):
            if "成交金額" in f:
                amt_idx = i
                break

        amount = None
        src = "TWSE_OK:MI_INDEX"

        if amt_idx is not None:
            data = js.get("data9") if isinstance(js.get("data9"), list) else candidate_tables[0][1]
            last = data[-1]
            if isinstance(last, list) and amt_idx < len(last):
                amount = _safe_int(last[amt_idx], default=None)

        if amount is None:
            # fallback：掃描末端數列的最大數字
            best = None
            for _, tbl in candidate_tables:
                for row in tbl[-8:]:
                    if not isinstance(row, list):
                        continue
                    for cell in row:
                        v = _safe_int(cell, default=None)
                        if v is None:
                            continue
                        if best is None or v > best:
                            best = v
            if best is None:
                warnings_bus.push("TWSE_AMOUNT_PARSE_FAIL", "TWSE amount parse none", {"url": url})
                return None, "TWSE_FAIL:PARSE_NONE"
            amount = best
            src = "TWSE_WARN:FALLBACK_MAXSCAN"
            warnings_bus.push("TWSE_AMOUNT_PARSE_WARN", "TWSE fallback max-scan used", {"amount": amount})

        return int(amount), src

    except requests.exceptions.SSLError as e:
        warnings_bus.push("TWSE_AMOUNT_SSL_ERROR", f"TWSE SSL error: {e}", {"url": url})
        return None, "TWSE_FAIL:SSLError"
    except Exception as e:
        warnings_bus.push("TWSE_AMOUNT_FETCH_FAIL", f"TWSE fetch fail: {e}", {"url": url})
        return None, "TWSE_FAIL:FETCH_ERROR"


def _fetch_tpex_amount(allow_insecure_ssl: bool) -> Tuple[Optional[int], str]:
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw"
    try:
        r = requests.get(url, timeout=10, verify=(not allow_insecure_ssl))
        r.raise_for_status()

        try:
            js = r.json()
        except Exception as e:
            warnings_bus.push("TPEX_AMOUNT_PARSE_FAIL", f"TPEX JSON decode error: {e}", {"url": url})
            return None, "TPEX_FAIL:JSONDecodeError"

        amount = None
        src = "TPEX_OK:st43_result"

        for key in ["totalAmount", "成交金額", "trade_value", "amt", "amount"]:
            if key in js:
                amount = _safe_int(js.get(key), default=None)
                if amount is not None:
                    break

        if amount is None:
            aa = js.get("aaData") or js.get("data")
            best = None
            if isinstance(aa, list):
                for row in aa[-12:]:
                    if isinstance(row, list):
                        for cell in row:
                            v = _safe_int(cell, default=None)
                            if v is None:
                                continue
                            if best is None or v > best:
                                best = v
            if best is None:
                warnings_bus.push("TPEX_AMOUNT_PARSE_FAIL", "TPEX amount parse none", {"url": url})
                return None, "TPEX_FAIL:PARSE_NONE"
            amount = best
            src = "TPEX_WARN:FALLBACK_MAXSCAN"
            warnings_bus.push("TPEX_AMOUNT_PARSE_WARN", "TPEX fallback max-scan used", {"amount": amount})

        return int(amount), src

    except requests.exceptions.SSLError as e:
        warnings_bus.push("TPEX_AMOUNT_SSL_ERROR", f"TPEX SSL error: {e}", {"url": url})
        return None, "TPEX_FAIL:SSLError"
    except Exception as e:
        warnings_bus.push("TPEX_AMOUNT_FETCH_FAIL", f"TPEX fetch fail: {e}", {"url": url})
        return None, "TPEX_FAIL:FETCH_ERROR"


def fetch_amount_total(allow_insecure_ssl: bool = False) -> MarketAmount:
    twse_amt, twse_src = _fetch_twse_amount(allow_insecure_ssl)
    tpex_amt, tpex_src = _fetch_tpex_amount(allow_insecure_ssl)

    total = None
    if twse_amt is not None and tpex_amt is not None:
        total = int(twse_amt) + int(tpex_amt)
    elif twse_amt is not None:
        total = int(twse_amt)
    elif tpex_amt is not None:
        total = int(tpex_amt)

    return MarketAmount(
        amount_twse=twse_amt,
        amount_tpex=tpex_amt,
        amount_total=total,
        source_twse=twse_src,
        source_tpex=tpex_src,
        allow_insecure_ssl=bool(allow_insecure_ssl),
    )


# =========================
# Regime metrics (簡化版)
# =========================
def _as_close_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("market_df is empty")
    if "Close" not in df.columns:
        raise ValueError("Close column missing")
    s = df["Close"]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    return s.astype(float)


def compute_regime_metrics(market_df: pd.DataFrame) -> dict:
    if market_df is None or market_df.empty or len(market_df) < 210:
        return {"smr": None, "slope5": None}

    close = _as_close_series(market_df)
    ma200 = close.rolling(200).mean()
    smr_series = (close - ma200) / ma200
    smr_series = smr_series.dropna()
    if len(smr_series) < 6:
        return {"smr": None, "slope5": None}

    smr = float(smr_series.iloc[-1])
    smr_ma5 = smr_series.rolling(5).mean().dropna()
    if len(smr_ma5) < 2:
        slope5 = 0.0
    else:
        slope5 = float(smr_ma5.iloc[-1] - smr_ma5.iloc[-2])
    return {"smr": smr, "slope5": slope5}


def pick_regime(metrics: dict, vix: Optional[float]) -> Tuple[str, float]:
    smr = metrics.get("smr")
    slope5 = metrics.get("slope5")

    if vix is not None and float(vix) > 35:
        return "CRASH_RISK", 0.10

    if smr is not None and slope5 is not None:
        if float(smr) > 0.25 and float(slope5) < -EPS:
            return "MEAN_REVERSION", 0.45
        if float(smr) > 0.25 and float(slope5) >= -EPS:
            return "OVERHEAT", 0.55

    return "NORMAL", 0.85


# =========================
# Data fetchers (yfinance)
# =========================
@st.cache_data(ttl=60 * 10, show_spinner=False)
def fetch_history(symbol: str, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
    df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "Datetime"})
    if "Datetime" not in df.columns:
        df.insert(0, "Datetime", pd.to_datetime(df.index))
    return df


# =========================
# Kill-Switch / Integrity
# =========================
def evaluate_integrity(stocks: List[dict], amount: MarketAmount) -> Dict[str, Any]:
    n = len(stocks)
    if n <= 0:
        return {
            "n": 0,
            "price_null": 0,
            "volratio_null": 0,
            "amount_null": 3,
            "core_missing_pct": 1.0,
            "kill": True,
            "reason": "NO_STOCKS",
        }

    price_null = sum(1 for s in stocks if s.get("Price") is None)
    volratio_null = sum(1 for s in stocks if s.get("Vol_Ratio") is None)

    amount_null = 0
    if amount.amount_twse is None:
        amount_null += 1
    if amount.amount_tpex is None:
        amount_null += 1
    if amount.amount_total is None:
        amount_null += 1

    # 核心欄位集合：Price、Vol_Ratio、Amount_total（你也可擴充）
    # 缺失率 = (price_null + volratio_null + (1 if amount_total null else 0)) / (n + n + 1)
    core_missing = price_null + volratio_null + (1 if amount.amount_total is None else 0)
    core_total = (n + n + 1)
    core_missing_pct = core_missing / max(1, core_total)

    kill = (
        (price_null / n) >= KILL_PRICE_NULL_PCT
        or (volratio_null / n) >= KILL_VOLRATIO_NULL_PCT
        or (core_missing_pct >= KILL_CORE_MISSING_PCT)
    )

    reason = "OK"
    if kill:
        reason = f"DATA_MISSING price_null={price_null}/{n}, volratio_null={volratio_null}/{n}, amount_total_null={amount.amount_total is None}, core_missing_pct={core_missing_pct:.2f}"

    return {
        "n": n,
        "price_null": price_null,
        "volratio_null": volratio_null,
        "amount_null": amount_null,
        "core_missing_pct": core_missing_pct,
        "kill": bool(kill),
        "reason": reason,
    }


# =========================
# JSON/UI helpers
# =========================
def _json_str(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def render_copy_button(label: str, text: str, height_px: int = 52):
    safe = text.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    html = f"""
    <div style="display:flex; align-items:center; gap:10px;">
      <button id="copyBtn"
        style="padding:8px 12px; border-radius:8px; border:1px solid #bbb; cursor:pointer; background:white;">
        {label}
      </button>
      <span id="copyMsg" style="font-size:13px; color:#555;"></span>
    </div>
    <script>
      const txt = `{safe}`;
      const btn = document.getElementById('copyBtn');
      const msg = document.getElementById('copyMsg');
      btn.addEventListener('click', async () => {{
        try {{
          await navigator.clipboard.writeText(txt);
          msg.textContent = '已複製到剪貼簿';
          setTimeout(()=>msg.textContent='', 1500);
        }} catch (e) {{
          msg.textContent = '複製失敗（瀏覽器限制）';
          setTimeout(()=>msg.textContent='', 2000);
        }}
      }});
    </script>
    """
    components.html(html, height=height_px)


# =========================
# Build payload
# =========================
def build_payload(
    session: str,
    account_mode: str,
    topn: int,
    positions: List[dict],
    cash_balance: int,
    total_equity: int,
    allow_insecure_ssl: bool,
) -> Tuple[dict, List[dict]]:

    # Macro
    twii_df = fetch_history(TWII_SYMBOL, period="3y", interval="1d")
    vix_df = fetch_history(VIX_SYMBOL, period="2y", interval="1d")

    trade_date = None
    twii_close = None
    if not twii_df.empty and "Datetime" in twii_df.columns and "Close" in twii_df.columns:
        trade_date = pd.to_datetime(twii_df["Datetime"].iloc[-1]).strftime("%Y-%m-%d")
        twii_close = float(twii_df["Close"].dropna().iloc[-1]) if len(twii_df["Close"].dropna()) else None
    trade_date = trade_date or time.strftime("%Y-%m-%d", time.localtime())

    vix_last = None
    if not vix_df.empty and "Close" in vix_df.columns:
        try:
            vix_last = float(vix_df["Close"].dropna().iloc[-1])
        except Exception:
            vix_last = None

    metrics = {"smr": None, "slope5": None}
    try:
        if not twii_df.empty and "Close" in twii_df.columns:
            mdf = twii_df[["Close"]].copy()
            metrics = compute_regime_metrics(mdf)
    except Exception:
        metrics = {"smr": None, "slope5": None}

    regime, max_equity = pick_regime(metrics, vix_last)

    # Amount
    amount = fetch_amount_total(allow_insecure_ssl=allow_insecure_ssl)

    # Symbols
    default_pool = ["2330.TW", "2317.TW", "2454.TW", "2308.TW", "2881.TW", "2882.TW", "2603.TW", "2609.TW"]
    symbols = default_pool[: max(1, int(topn))]

    # Stocks snapshot (Price / Vol_Ratio)
    stocks: List[dict] = []
    for i, sym in enumerate(symbols, start=1):
        px = None
        vol_ratio = None
        try:
            h = fetch_history(sym, period="6mo", interval="1d")
            if not h.empty and "Close" in h.columns:
                c = h["Close"].dropna()
                if len(c) > 0:
                    px = float(c.iloc[-1])
            if not h.empty and "Volume" in h.columns:
                v = h["Volume"].dropna()
                if len(v) >= 20:
                    ma20 = v.rolling(20).mean().iloc[-1]
                    if ma20 and not (pd.isna(ma20) or float(ma20) == 0.0):
                        vol_ratio = float(v.iloc[-1] / ma20)
        except Exception:
            px = None
            vol_ratio = None

        stocks.append(
            {
                "Symbol": sym,
                "Name": sym,
                "Tier": i,
                "Price": px,
                "Vol_Ratio": vol_ratio,
            }
        )

    integrity = evaluate_integrity(stocks, amount)

    # ===== Kill-Switch → SHELTER =====
    active_alerts: List[str] = []
    audit_log: List[dict] = []
    decisions: List[dict] = []

    market_status = "NORMAL"
    current_regime = regime
    max_equity_allowed_pct = max_equity

    if integrity["kill"]:
        market_status = "SHELTER"
        current_regime = "UNKNOWN"
        max_equity_allowed_pct = 0.0

        active_alerts.append("KILL_SWITCH_ACTIVATED")
        if amount.amount_total is None:
            active_alerts.append("DEGRADED_AMOUNT: 成交量數據完全缺失 (TWSE_FAIL + TPEX_FAIL)")
        if integrity["price_null"] == integrity["n"]:
            active_alerts.append("CRITICAL: 所有個股價格 = null (無法執行任何決策)")
        if integrity["volratio_null"] == integrity["n"]:
            active_alerts.append("CRITICAL: 所有個股 Vol_Ratio = null (Layer B 判定不可能)")
        active_alerts.append(f"DATA_INTEGRITY_FAILURE: 核心數據缺失率={integrity['core_missing_pct']:.2f}")
        active_alerts.append("FORCED_ALL_CASH: 資料品質不足，強制進入避險模式")

        audit_log = [
            {
                "symbol": "ALL",
                "event": "KILL_SWITCH_TRIGGERED",
                "attribution": "DATA_MISSING",
                "comment": integrity["reason"],
            },
            {
                "symbol": "ALL",
                "event": "DEGRADED_STATUS_CRITICAL",
                "attribution": "MARKET_AMOUNT_FAILURE",
                "comment": f"amount_twse={amount.amount_twse}, amount_tpex={amount.amount_tpex}, amount_total={amount.amount_total}, source_twse={amount.source_twse}, source_tpex={amount.source_tpex}",
            },
            {
                "symbol": "ALL",
                "event": "ALL_CASH_FORCED",
                "attribution": "SYSTEM_PROTECTION",
                "comment": "核心哲學: In Doubt → Cash. 當前數據品質無法支持任何進場決策",
            },
        ]

    # Portfolio summary（百分比）
    current_exposure_pct = 0.0  # 這版以 positions 之外的交易決策為準；Kill 時固定 0
    cash_pct = 100.0

    risk_level = "NORMAL"
    if market_status == "SHELTER":
        risk_level = "CRITICAL"

    payload = {
        "meta": {
            "timestamp": _now_ts(),
            "market_status": market_status,
            "current_regime": current_regime,
            "account_mode": account_mode,
            "session": session,
        },
        "portfolio_summary": {
            "total_equity": int(total_equity),
            "max_equity_allowed_pct": float(max_equity_allowed_pct),
            "current_exposure_pct": float(current_exposure_pct),
            "cash_pct": float(cash_pct),
            "risk_exposure_level": risk_level,
            "active_alerts": active_alerts,
        },
        "macro": {
            "trade_date": trade_date,
            "twii_close": twii_close,
            "vix": vix_last,
            "smr": metrics.get("smr"),
            "slope5": metrics.get("slope5"),
            "market_amount": asdict(amount),
            "integrity": integrity,
        },
        "stocks": stocks,
        "positions_input": positions,
        "decisions": decisions,
        "audit_log": audit_log,
        "audit_tag": "V16.2_ENHANCED_KILL_SWITCH_ACTIVATED" if market_status == "SHELTER" else "V16.3_STABLE_HYBRID",
    }

    return payload, warnings_bus.latest(50)


# =========================
# UI
# =========================
def main():
    st.sidebar.header("設定")
    session = st.sidebar.selectbox("Session", ["INTRADAY", "EOD"], index=0)
    account_mode = st.sidebar.selectbox("Account Mode", ["Conservative", "Balanced", "Aggressive"], index=0)
    topn = st.sidebar.selectbox("TopN（固定池化數量）", [8, 10, 15, 20], index=0)

    allow_insecure_ssl = st.sidebar.checkbox("允許不安全 SSL (verify=False)", value=False)

    st.sidebar.subheader("持倉（手動貼 JSON array）")
    positions_text = st.sidebar.text_area("positions", value="[]", height=120)

    cash_balance = st.sidebar.number_input("cash_balance (NTD)", min_value=0, value=DEFAULT_CASH, step=10000)
    total_equity = st.sidebar.number_input("total_equity (NTD)", min_value=0, value=DEFAULT_EQUITY, step=10000)

    run_btn = st.sidebar.button("Run")

    # positions parse
    positions = []
    try:
        positions = json.loads(positions_text) if positions_text.strip() else []
        if not isinstance(positions, list):
            raise ValueError("positions 必須是 JSON array")
    except Exception as e:
        st.sidebar.error(f"positions JSON 解析失敗：{e}")
        positions = []

    if run_btn or "auto_ran" not in st.session_state:
        st.session_state["auto_ran"] = True

        try:
            payload, warns = build_payload(
                session=session,
                account_mode=account_mode,
                topn=int(topn),
                positions=positions,
                cash_balance=int(cash_balance),
                total_equity=int(total_equity),
                allow_insecure_ssl=bool(allow_insecure_ssl),
            )
        except Exception as e:
            st.error("App 執行期間發生例外（已捕捉，不會白屏）。")
            st.exception(e)
            return

        meta = payload.get("meta", {})
        ps = payload.get("portfolio_summary", {})
        macro = payload.get("macro", {})
        integ = macro.get("integrity", {})

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("交易日", macro.get("trade_date", "-"))
        c2.metric("market_status", meta.get("market_status", "-"))
        c3.metric("regime", meta.get("current_regime", "-"))
        c4.metric("SMR", f"{_safe_float(macro.get('smr'), 0):.6f}" if macro.get("smr") is not None else "NA")
        c5.metric("Slope5", f"{_safe_float(macro.get('slope5'), 0):.6f}" if macro.get("slope5") is not None else "NA")
        c6.metric("Max Equity", f"{_safe_float(ps.get('max_equity_allowed_pct'), 0):.2f}")

        st.caption(
            f"Integrity｜Price null={integ.get('price_null')}/{integ.get('n')}｜"
            f"Vol_Ratio null={integ.get('volratio_null')}/{integ.get('n')}｜"
            f"core_missing_pct={_safe_float(integ.get('core_missing_pct'), 0):.2f}"
        )

        # alerts
        if ps.get("active_alerts"):
            st.subheader("Active Alerts")
            for a in ps["active_alerts"]:
                st.error(a)

        st.subheader("市場成交金額（best-effort / 可稽核）")
        st.json(macro.get("market_amount", {}))

        st.subheader("Stocks Snapshot")
        st.dataframe(pd.DataFrame(payload.get("stocks", [])), use_container_width=True)

        st.subheader("Audit Log")
        st.dataframe(pd.DataFrame(payload.get("audit_log", [])), use_container_width=True)

        st.subheader("Warnings（最新 50 條）")
        w_df = pd.DataFrame(warns)
        if not w_df.empty:
            st.dataframe(w_df, use_container_width=True)
        else:
            st.caption("（目前沒有 warnings）")

        st.subheader("AI JSON（Arbiter Input）— 可回溯（SIM-FREE）")
        json_text = _json_str(payload)
        render_copy_button("一鍵複製 AI JSON", json_text)
        st.code(json_text, language="json")
        st.download_button(
            "下載 JSON（payload.json）",
            data=json_text.encode("utf-8"),
            file_name="payload.json",
            mime="application/json",
        )


if __name__ == "__main__":
    main()
