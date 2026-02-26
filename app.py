# app.py
# -*- coding: utf-8 -*-
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Tuple, Optional
import concurrent.futures

import streamlit as st
import requests

try:
    import certifi  # 建議 requirements.txt 有 certifi
except Exception:
    certifi = None

# 你 repo 內的統一裁決入口
from arbiter import arbiter_run


# =========================
# Timezone
# =========================
TZ_TPE = timezone(timedelta(hours=8))


# =========================
# Stable HTTP Session
# =========================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    })
    return s


# =========================
# Utility
# =========================
def now_tpe() -> datetime:
    return datetime.now(TZ_TPE)

def yyyy_mm_dd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def safe_json(obj: Any) -> Any:
    """確保可被 st.json 顯示（避免 Streamlit renderer 內部炸）"""
    return json.loads(json.dumps(obj, ensure_ascii=False, default=str))

def run_with_timeout(fn, timeout_sec: float, fallback, tag: str) -> Tuple[Any, Dict[str, Any]]:
    """硬 timeout：永遠回傳 (data, audit)"""
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn)
        try:
            data = fut.result(timeout=timeout_sec)
            return data, {
                "name": tag,
                "status": "OK",
                "confidence": "HIGH",
                "error": None,
                "latency_ms": int((time.time() - t0) * 1000),
            }
        except concurrent.futures.TimeoutError:
            return fallback, {
                "name": tag,
                "status": "FAIL",
                "confidence": "LOW",
                "error": "TIMEOUT",
                "latency_ms": int((time.time() - t0) * 1000),
            }
        except Exception as e:
            return fallback, {
                "name": tag,
                "status": "FAIL",
                "confidence": "LOW",
                "error": type(e).__name__,
                "error_detail": str(e)[:200],
                "latency_ms": int((time.time() - t0) * 1000),
            }

def http_get_json(sess: requests.Session, url: str, params: Dict[str, Any], timeout=(2, 3)) -> Dict[str, Any]:
    """短 timeout + 可選 certifi verify"""
    verify = certifi.where() if certifi else True
    r = sess.get(url, params=params, timeout=timeout, verify=verify)
    r.raise_for_status()
    return r.json()


# =========================
# Data Layer (Stable / Tiered)
# =========================
TPEX_SAFE_AMOUNT = 200_000_000_000  # 2000 億（你既定 Safe Mode）

def fetch_twse_t86(sess: requests.Session, trade_date_yyyymmdd: str) -> Dict[str, Any]:
    """
    TWSE T86（三大法人）- 只回傳 summary（穩定/輕量）
    """
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    params = {"response": "json", "date": trade_date_yyyymmdd, "selectType": "ALL"}
    j = http_get_json(sess, url, params, timeout=(2, 3))

    rows = j.get("data") or []
    fields = j.get("fields") or []
    if not rows or not fields:
        raise RuntimeError("T86_EMPTY")

    # 欄位名稱可能會變，做關鍵字匹配
    idx_map = {name: i for i, name in enumerate(fields)}

    def find_col_idx(kw_list):
        for name, idx in idx_map.items():
            ok = True
            for kw in kw_list:
                if kw not in name:
                    ok = False
                    break
            if ok:
                return idx
        return None

    # 常見欄位口徑（股數/張數口徑；你 UI 目前只顯示「買超 xx 億」那是金額口徑，這裡先以淨買賣超合計數字呈現）
    i_total = find_col_idx(["三大法人", "買賣超"])
    i_foreign = find_col_idx(["外", "買賣超"])
    i_trust = find_col_idx(["投信", "買賣超"])
    i_dealer = find_col_idx(["自營商", "買賣超"])

    def to_int(x):
        try:
            s = str(x).replace(",", "").replace("--", "0").strip()
            return int(float(s))
        except:
            return 0

    summary = {}
    if i_foreign is not None:
        summary["外資"] = sum(to_int(r[i_foreign]) for r in rows)
    if i_trust is not None:
        summary["投信"] = sum(to_int(r[i_trust]) for r in rows)
    if i_dealer is not None:
        summary["自營商"] = sum(to_int(r[i_dealer]) for r in rows)
    if i_total is not None:
        summary["合計"] = sum(to_int(r[i_total]) for r in rows)

    return {"summary": summary, "asof": trade_date_yyyymmdd}

def fetch_twse_amount_stock_day_all(sess: requests.Session, trade_date_yyyymmdd: str) -> Dict[str, Any]:
    """
    TWSE 成交額：用 STOCK_DAY_ALL，做「總成交額加總」。
    注意：此 endpoint 偶發 SSL / 風控，故上層會短 timeout + fallback。
    """
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date_yyyymmdd}
    j = http_get_json(sess, url, params, timeout=(2, 3))

    rows = j.get("data") or []
    if not rows:
        raise RuntimeError("STOCK_DAY_ALL_EMPTY")

    # 保守：從尾端找可解析的大整數當「交易金額」
    def safe_int(x):
        try:
            s = str(x).replace(",", "").strip()
            return int(float(s))
        except:
            return None

    amount_sum = 0
    ok_rows = 0
    for row in rows:
        best = None
        for cell in reversed(row):
            v = safe_int(cell)
            if v is not None and v > 0:
                best = v
                break
        if best is not None:
            amount_sum += best
            ok_rows += 1

    # 合理性底線：1000 億（你之前也用過類似檢查）
    if amount_sum < 100_000_000_000:
        raise RuntimeError("AMOUNT_TOO_LOW")

    return {"amount_twse": amount_sum, "rows": len(rows), "ok_rows": ok_rows, "asof": trade_date_yyyymmdd}

def fetch_twii_via_yfinance_like(sess: requests.Session) -> Dict[str, Any]:
    """
    最穩的指數抓法本來應該用 TWSE 指數 endpoint，但你在 Cloud 上遇到 SSL，
    先給「穩定可用」方案：用 Stooq 的 TWII 指數（不保證永遠有，但常常比 TWSE 穩）
    你若已有 downloader_tw.py 的指數抓取函數，可直接替換這支。
    """
    # Stooq: Taiwan index 可能是 ^TWII 不一定，這裡做最保守：不硬依賴
    # => 若失敗，上層會 fallback 成 None，L1 就會 fail（符合你的稽核哲學）
    url = "https://stooq.com/q/l/?s=%5ETWII&i=d"
    # 這支回 CSV，不是 JSON，這裡簡化：只求能不 hang；若你要完全可靠請改你自家 TWSE index endpoint。
    r = sess.get(url, timeout=(2, 3))
    r.raise_for_status()
    text = r.text.strip().splitlines()
    if len(text) < 2:
        raise RuntimeError("TWII_CSV_EMPTY")
    # header: Date,Open,High,Low,Close,Volume
    parts = text[1].split(",")
    if len(parts) < 5:
        raise RuntimeError("TWII_CSV_BAD")
    close = float(parts[4])
    dt = parts[0]
    return {"twii_close": close, "asof": dt}

def resolve_effective_trade_date(target_date: datetime, session: str) -> Tuple[str, bool]:
    """
    EOD Guard：如果是 EOD 且現在在 00:00~15:30，資料多半還沒出，
    直接回退到前一日（你先前的 ADR 精神）。
    """
    t = now_tpe()
    if session.upper() == "EOD":
        if t.hour < 15 or (t.hour == 15 and t.minute < 30):
            prev = (target_date - timedelta(days=1))
            return yyyy_mm_dd(prev), True
    return yyyy_mm_dd(target_date), False


# =========================
# Build Snapshot (Parallel + Fast Fail)
# =========================
def get_snapshot_cached(sess: requests.Session, target_date: datetime, session: str, top_n: int) -> Dict[str, Any]:
    """
    最穩定版本：
    - 只做「市場快照」(TWII / TWSE amount / TPEX amount / T86 summary)
    - 並行抓取
    - 每支硬 timeout（避免一直 run）
    - 任何失敗都寫進 audit_modules
    """
    effective_date_iso, is_using_prev = resolve_effective_trade_date(target_date, session)
    trade_date = effective_date_iso.replace("-", "")

    # --- parallel fetch ---
    # 注意：TWSE SSL 常掛，這裡每支都短 timeout + 上層 fallback
    twii_data, twii_audit = run_with_timeout(
        lambda: fetch_twii_via_yfinance_like(sess),
        timeout_sec=4.5,
        fallback=None,
        tag="TWSE_TWII_INDEX"
    )

    twse_amt, twse_audit = run_with_timeout(
        lambda: fetch_twse_amount_stock_day_all(sess, trade_date),
        timeout_sec=4.5,
        fallback=None,
        tag="TWSE_STOCK_DAY_ALL"
    )

    t86_data, t86_audit = run_with_timeout(
        lambda: fetch_twse_t86(sess, trade_date),
        timeout_sec=4.5,
        fallback=None,
        tag="TWSE_T86"
    )

    # TPEX 仍採 Safe Mode（秒回）
    tpex_amount = TPEX_SAFE_AMOUNT
    tpex_audit = {
        "name": "TPEX_SAFE_CONSTANT",
        "status": "OK",
        "confidence": "LOW",
        "error": None,
        "latency_ms": 0
    }

    # assemble
    audit_modules = [twii_audit, twse_audit, tpex_audit, t86_audit]

    snapshot = {
        "meta": {
            "timestamp": now_tpe().strftime("%Y-%m-%d %H:%M:%S"),
            "session": session.upper(),
            "market_status": "NORMAL",
            "confidence_level": "HIGH",
            "is_using_previous_day": bool(is_using_prev),
            "effective_trade_date": effective_date_iso,
            "war_time_override": False,
            "audit_modules": audit_modules,
        },
        "macro": {
            "integrity": {"kill": False},
            "overview": {
                # 若 TWII 取不到，會是 None，Arbiter 的 L1 會擋下（符合你規則）
                "twii_close": (twii_data or {}).get("twii_close") if isinstance(twii_data, dict) else None,
                "vix": None,  # 若你有 VIX 資料源可補；沒有就維持 None，讓 L1/L2 自己處理
                "smr": None,  # 同上
                "daily_return_pct": None,
                "daily_return_pct_prev": None,
                "max_equity_allowed_pct": 0.05,  # 先給保守預設；你可改成從 system_params/配置檔讀
            }
        },
        "market_amount": {
            "amount_twse": (twse_amt or {}).get("amount_twse") if isinstance(twse_amt, dict) else None,
            "amount_tpex": tpex_amount,
            "amount_total": ((twse_amt or {}).get("amount_twse") if isinstance(twse_amt, dict) else 0) + tpex_amount,
            "source_twse": "TWSE_STOCK_DAY_ALL" if twse_audit.get("status") == "OK" else f"TWSE_FAIL:{twse_audit.get('error')}",
            "source_tpex": "TPEX_SAFE_CONSTANT",
            "tier": 4,
            "error_twse": twse_audit.get("error"),
        },
        "t86": {
            "ok": True if (isinstance(t86_data, dict) and t86_audit.get("status") == "OK") else False,
            "summary": (t86_data or {}).get("summary", {}) if isinstance(t86_data, dict) else {},
            "error": t86_audit.get("error"),
            "asof": (t86_data or {}).get("asof") if isinstance(t86_data, dict) else trade_date,
        },
    }
    return snapshot


# =========================
# Build Arbiter Payload (Minimal + Stable)
# =========================
def build_arbiter_payload(snapshot: Dict[str, Any], top_n: int) -> Dict[str, Any]:
    """
    產出「最小可運行 JSON」：先把 macro + meta + system_params 填好
    stocks 先留空（你可後續接 TopN 個股資料層再補）
    """
    meta = snapshot.get("meta", {})
    ov = snapshot.get("macro", {}).get("overview", {}) or {}
    # 兼容 snapshot 結構
    overview = snapshot.get("macro", {}).get("overview") or snapshot.get("macro", {}).get("overview", {})
    if not overview:
        overview = snapshot.get("macro", {}).get("overview", {})

    twii_close = snapshot.get("macro", {}).get("overview", {}).get("twii_close")

    payload = {
        "meta": {
            "timestamp": meta.get("timestamp"),
            "session": meta.get("session", "EOD"),
            "market_status": meta.get("market_status", "NORMAL"),
            "confidence_level": meta.get("confidence_level", "HIGH"),
            "is_using_previous_day": meta.get("is_using_previous_day", False),
            "effective_trade_date": meta.get("effective_trade_date"),
            "war_time_override": meta.get("war_time_override", False),
            "audit_modules": meta.get("audit_modules", []),
        },
        "macro": {
            "integrity": {"kill": False},
            "overview": {
                "twii_close": twii_close,
                "vix": snapshot.get("macro", {}).get("overview", {}).get("vix"),
                "smr": snapshot.get("macro", {}).get("overview", {}).get("smr"),
                "daily_return_pct": snapshot.get("macro", {}).get("overview", {}).get("daily_return_pct"),
                "daily_return_pct_prev": snapshot.get("macro", {}).get("overview", {}).get("daily_return_pct_prev"),
                "max_equity_allowed_pct": snapshot.get("macro", {}).get("overview", {}).get("max_equity_allowed_pct", 0.05),
            }
        },
        # 先給保守預設；你之後可以接真實 portfolio / monitoring
        "portfolio": {
            "equity": 2_000_000,
            "drawdown_pct": 0.0,
            "loss_streak": 0,
            "alpha_prev": 0.0
        },
        "monitoring": {
            "regime_predictive_score": 0.5,
            "regime_outcome_score": 0.5,
            "trade_count_20d": 0
        },
        "system_params": {
            "k_regime": 1.2,
            "lambda_drawdown": 2.0,
            "max_loss_per_trade_pct": 0.02,
            "stress_drawdown_trigger": 0.10,
            "min_trades_for_trust": 8,
            "trust_default_when_insufficient": 0.49,
            "trust_attack_scale_low": 0.30,
            "l1_price_min": 1,
            "l1_price_max": 5000,
            "l1_price_median_mult_hi": 50,
            "prev_day_allocation_scale": 0.70,
            "ai_enabled": True,
            "ai_confidence_threshold": 0.70
        },
        "stocks": []  # TopN 個股資料層之後再接
    }
    return payload


# =========================
# Streamlit UI (Stable)
# =========================
def main():
    st.set_page_config(page_title="Sunhero | 股市智能超盤中控台", layout="wide")
    st.title("Sunhero | 股市智能超盤中控台（Data-Layer + Arbiter Orchestrator）")

    sess = build_session()

    # ----- session_state -----
    if "snapshot" not in st.session_state:
        st.session_state["snapshot"] = None
    if "payload_text" not in st.session_state:
        st.session_state["payload_text"] = "{}"

    # ----- sidebar -----
    with st.sidebar:
        st.subheader("模式 / 交易日")

        run_mode = st.radio("RUN 模式", ["L1", "L2", "L3"], index=0)

        session = st.selectbox("Session", ["EOD", "INTRADAY"], index=0)

        d = st.date_input("目標日期（台北）", value=now_tpe().date())

        top_n = st.slider("TopN（上市成交額排序）", min_value=5, max_value=50, value=20, step=1)

        if st.button("立即更新", type="primary"):
            # 只在按鈕時抓資料，避免每次 rerun 都打 API
            with st.spinner("更新市場快照中（短 timeout + fallback，最差也會很快回來）..."):
                target_dt = datetime(d.year, d.month, d.day, tzinfo=TZ_TPE)
                snap = get_snapshot_cached(sess, target_dt, session=session, top_n=top_n)
                st.session_state["snapshot"] = snap

                # 同步生成 payload（避免 UI 另外再跑一次）
                payload = build_arbiter_payload(snap, top_n=top_n)
                st.session_state["payload_text"] = json.dumps(payload, ensure_ascii=False, indent=2)

            st.rerun()

    # ----- main area -----
    snap = st.session_state["snapshot"]

    # 市場狀態區
    st.subheader("市場狀態（以資料層輸出為準）")

    c1, c2, c3, c4 = st.columns(4)
    twii_close = None
    twse_amt = None
    tpex_amt = None
    total_amt = None
    t86_ok = False
    t86_sum = {}

    if isinstance(snap, dict):
        twii_close = snap.get("macro", {}).get("overview", {}).get("twii_close")
        ma = snap.get("market_amount", {}) or {}
        twse_amt = ma.get("amount_twse")
        tpex_amt = ma.get("amount_tpex")
        total_amt = ma.get("amount_total")
        t86 = snap.get("t86", {}) or {}
        t86_ok = bool(t86.get("ok"))
        t86_sum = t86.get("summary", {}) or {}

    def fmt_num(x):
        if x is None:
            return "—"
        try:
            return f"{int(x):,}"
        except:
            return str(x)

    with c1:
        st.metric("加權指數 TWII（TWSE）", "—" if twii_close is None else f"{float(twii_close):,.2f}")
        st.caption("TWII 讀取失敗（Arbiter 內部 L1 會直接擋下）" if twii_close is None else "")

    with c2:
        st.metric("上市成交額（TWSE）", fmt_num(twse_amt))
        src = (snap.get("market_amount") or {}).get("source_twse") if isinstance(snap, dict) else ""
        err = (snap.get("market_amount") or {}).get("error_twse") if isinstance(snap, dict) else ""
        st.caption(f"來源：{src}｜錯誤：{err}")

    with c3:
        st.metric("上櫃成交額（TPEX）", fmt_num(tpex_amt))
        st.caption("Tier=4｜來源：TPEX_SAFE_CONSTANT｜錯誤：—")

    with c4:
        st.metric("總成交額", fmt_num(total_amt))
        meta = (snap.get("meta") or {}) if isinstance(snap, dict) else {}
        st.caption(f"EOD Guard：is_using_previous_day={meta.get('is_using_previous_day')}｜effective_trade_date={meta.get('effective_trade_date')}")

    # 三大法人
    st.markdown("### 三大法人（TWSE T86）")
    if not t86_ok:
        err = (snap.get("t86") or {}).get("error") if isinstance(snap, dict) else "—"
        st.error(f"T86 讀取失敗：{err}")
    else:
        a, b, c, d = st.columns(4)
        a.metric("外資", fmt_num(t86_sum.get("外資")))
        b.metric("投信", fmt_num(t86_sum.get("投信")))
        c.metric("自營商", fmt_num(t86_sum.get("自營商")))
        d.metric("合計", fmt_num(t86_sum.get("合計")))

    # JSON Payload
    st.markdown("---")
    st.subheader("輸入 JSON Payload（可貼 Arbiter JSON 或用範本生成）")

    colL, colR = st.columns([1.2, 1.0])

    with colL:
        if st.button("載入標準範本（以 TopN + 市場資料層組裝）"):
            if not isinstance(snap, dict):
                st.warning("尚未更新市場快照，請先按左側「立即更新」。")
            else:
                payload = build_arbiter_payload(snap, top_n=top_n)
                st.session_state["payload_text"] = json.dumps(payload, ensure_ascii=False, indent=2)
                st.rerun()

        payload_text = st.text_area("JSON 內容", value=st.session_state["payload_text"], height=520)

    with colR:
        st.markdown("### 執行結果（統一入口：arbiter_run）")
        if st.button("執行（arbiter_run）", type="primary"):
            # parse
            try:
                payload = json.loads(payload_text)
            except Exception as e:
                st.error(f"JSON 解析錯誤：{e}")
                st.stop()

            # execute
            with st.spinner("裁決中..."):
                try:
                    result = arbiter_run(payload, run_mode)
                except Exception as e:
                    st.error(f"裁決引擎錯誤：{type(e).__name__}: {e}")
                    st.stop()

            # show
            no_trade = result.get("NO_TRADE", False)
            if no_trade:
                st.warning("NO_TRADE：已被 L1 Gate 阻擋（資料不可信）")
            st.markdown("#### ① Arbiter 統一輸出")
            st.json(safe_json(result))

    # Debug：audit modules
    if isinstance(snap, dict):
        with st.expander("（Debug）資料層 audit_modules", expanded=False):
            st.json(safe_json((snap.get("meta") or {}).get("audit_modules", [])))


if __name__ == "__main__":
    main()
