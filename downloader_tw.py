import os
import json
import time
import logging
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone

# =========================
# Streamlit UI
# =========================
import streamlit as st

# =========================
# 基本設定
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

TZ_TPE = timezone(timedelta(hours=8))
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

sess = requests.Session()
sess.headers.update(HEADERS)

# =========================
# 工具：交易日粗略判斷（用 TWSE 成功回應當作準）
# =========================
def today_tpe() -> datetime:
    return datetime.now(TZ_TPE)

def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def yyyy_mm_dd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def safe_int(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).replace(",", "").strip()
        return int(float(s))
    except:
        return default

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(",", "").strip()
        return float(s)
    except:
        return default

# =========================
# 1) TWII：用 yfinance 取 close + 前一日 close 計算漲跌
# =========================
@st.cache_data(ttl=60)
def fetch_twii_latest():
    """
    用 yfinance 取 ^TWII 最近 5 日，取最後兩筆計算 change/pct。
    """
    try:
        df = yf.download("^TWII", period="10d", progress=False)
        if df is None or df.empty:
            return None

        df = df.dropna()
        if len(df) < 1:
            return None

        last_close = float(df["Close"].iloc[-1])
        last_dt = df.index[-1].date()

        if len(df) >= 2:
            prev_close = float(df["Close"].iloc[-2])
            chg = last_close - prev_close
            pct = chg / prev_close if prev_close != 0 else None
        else:
            chg, pct = None, None

        return {
            "last_dt": str(last_dt),
            "close": last_close,
            "change": chg,
            "pct": pct,
        }
    except Exception as e:
        logging.warning(f"fetch_twii_latest fail: {type(e).__name__}")
        return None

# =========================
# 2) TWSE 成交額：用 STOCK_DAY_ALL 逐筆加總（你之前已驗證可用）
# =========================
def fetch_twse_amount_audit_sum(trade_date_yyyymmdd: str):
    """
    TWSE: https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json&date=YYYYMMDD
    回傳 (amount_sum, meta)
    """
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
    params = {"response": "json", "date": trade_date_yyyymmdd}
    meta = {
        "url": url,
        "params": params,
        "status_code": None,
        "rows": 0,
        "amount_sum": 0,
        "ok_rows": 0,
        "error": None,
        "final_url": None,
    }
    try:
        r = sess.get(url, params=params, timeout=15)
        meta["status_code"] = r.status_code
        meta["final_url"] = r.url
        if r.status_code != 200:
            meta["error"] = f"HTTP_{r.status_code}"
            return None, meta

        data = r.json()
        rows = data.get("data", []) or []
        meta["rows"] = len(rows)

        amount_sum = 0
        ok_rows = 0
        # 交易金額通常在最後一欄，或固定欄位；這裡用「從尾端找最大可解析整數」保守法
        for row in rows:
            best = None
            for cell in reversed(row):
                v = safe_int(cell, default=None)
                if v is not None and v > 0:
                    best = v
                    break
            if best is not None:
                amount_sum += best
                ok_rows += 1

        meta["amount_sum"] = amount_sum
        meta["ok_rows"] = ok_rows

        # 基本合理性：至少 1000 億
        if amount_sum < 100_000_000_000:
            meta["error"] = "AMOUNT_TOO_LOW"
            return None, meta

        return amount_sum, meta
    except Exception as e:
        meta["error"] = f"{type(e).__name__}"
        return None, meta

# =========================
# 3) TPEX 成交額：依 ADR-001/002，採 Safe Mode 常數
# =========================
def tpex_safe_mode_amount():
    # 你之前用 200B 當 safe mode；保留一致
    return 200_000_000_000, "TPEX_SAFE_MODE_200B"

# =========================
# 4) TWSE T86：三大法人買賣超（全市場）
# =========================
@st.cache_data(ttl=60)
def fetch_twse_t86(trade_date_yyyymmdd: str, select_type: str = "ALL"):
    """
    https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date=YYYYMMDD&selectType=ALL
    解析並回傳：
    - dataframe: 含 股票代號/名稱/外資/投信/自營商/合計（數值為張數或金額依 TWSE 欄位口徑）
    - summary: 外資/投信/自營商/合計 的 net
    """
    url = "https://www.twse.com.tw/rwd/zh/fund/T86"
    params = {"response": "json", "date": trade_date_yyyymmdd, "selectType": select_type}

    meta = {"url": url, "params": params, "status_code": None, "rows": 0, "error": None}
    try:
        r = sess.get(url, params=params, timeout=15)
        meta["status_code"] = r.status_code
        if r.status_code != 200:
            meta["error"] = f"HTTP_{r.status_code}"
            return None, None, meta

        j = r.json()
        data = j.get("data", []) or []
        fields = j.get("fields", []) or []
        meta["rows"] = len(data)

        if not data or not fields:
            meta["error"] = "EMPTY"
            return None, None, meta

        df = pd.DataFrame(data, columns=fields)

        # 嘗試抓關鍵欄位（不同語系/版本可能略有差異）
        # 常見欄位：證券代號、證券名稱、外陸資買賣超股數(不含外資自營商)、投信買賣超股數、自營商買賣超股數、三大法人買賣超股數
        col_code = next((c for c in df.columns if "代號" in c), None)
        col_name = next((c for c in df.columns if "名稱" in c), None)
        col_foreign = next((c for c in df.columns if "外" in c and "買賣超" in c and "不含外資自營商" in c), None)
        col_trust = next((c for c in df.columns if "投信" in c and "買賣超" in c), None)
        col_dealer = next((c for c in df.columns if "自營商" in c and "買賣超" in c), None)
        col_total = next((c for c in df.columns if "三大法人" in c and "買賣超" in c), None)

        for c in [col_foreign, col_trust, col_dealer, col_total]:
            if c and c in df.columns:
                df[c] = df[c].astype(str).str.replace(",", "").str.replace("--", "0")
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        summary = {}
        if col_foreign: summary["外資及陸資(不含外資自營商)"] = int(df[col_foreign].sum())
        if col_trust: summary["投信"] = int(df[col_trust].sum())
        if col_dealer: summary["自營商"] = int(df[col_dealer].sum())
        if col_total: summary["合計"] = int(df[col_total].sum())

        # 只保留核心欄位（UI 親和）
        keep = [c for c in [col_code, col_name, col_foreign, col_trust, col_dealer, col_total] if c]
        df_view = df[keep].copy()

        # 重新命名
        rename = {}
        if col_code: rename[col_code] = "代號"
        if col_name: rename[col_name] = "名稱"
        if col_foreign: rename[col_foreign] = "外資淨買賣超"
        if col_trust: rename[col_trust] = "投信淨買賣超"
        if col_dealer: rename[col_dealer] = "自營商淨買賣超"
        if col_total: rename[col_total] = "三大法人合計"
        df_view = df_view.rename(columns=rename)

        return df_view, summary, meta

    except Exception as e:
        meta["error"] = f"{type(e).__name__}"
        return None, None, meta

# =========================
# 5) 組合：市場概況（給 UI 與 JSON）
# =========================
def build_market_snapshot(target_date: datetime):
    trade_date = yyyymmdd(target_date)

    twii = fetch_twii_latest()

    # TWSE amount
    twse_amt, twse_meta = fetch_twse_amount_audit_sum(trade_date)
    twse_ok = twse_amt is not None

    # TPEX safe
    tpex_amt, tpex_src = tpex_safe_mode_amount()

    # T86
    t86_df, t86_sum, t86_meta = fetch_twse_t86(trade_date, "ALL")
    t86_ok = (t86_df is not None) and (t86_sum is not None) and (t86_meta.get("error") is None)

    snapshot = {
        "trade_date": trade_date,
        "trade_date_iso": target_date.strftime("%Y-%m-%d"),
        "twii": twii,
        "market_amount": {
            "amount_twse": twse_amt,
            "amount_tpex": tpex_amt,
            "amount_total": (twse_amt or 0) + (tpex_amt or 0),
            "source_twse": "TWSE_STOCK_DAY_ALL_AUDIT_SUM" if twse_ok else f"TWSE_FAIL:{twse_meta.get('error')}",
            "source_tpex": tpex_src,
            "status_twse": "OK" if twse_ok else "FAIL",
            "status_tpex": "ESTIMATED",
            "confidence_twse": "HIGH" if twse_ok else "LOW",
            "confidence_tpex": "LOW",
        },
        "t86": {
            "ok": t86_ok,
            "summary": t86_sum or {},
            "meta": t86_meta,
        },
        "integrity": {
            "twse_amount_ok": twse_ok,
            "t86_ok": t86_ok,
            "tpex_mode": "SAFE_MODE",
            "kill_switch": False,  # 依你的 ADR，這裡不因 TPEX 降級而 kill
        },
    }
    return snapshot, t86_df

# =========================
# UI
# =========================
def fmt_money(n):
    if n is None:
        return "—"
    return f"{int(n):,}"

def fmt_pct(x):
    if x is None:
        return "—"
    return f"{x*100:.2f}%"

def fmt_num(n):
    if n is None:
        return "—"
    return f"{int(n):,}"

def app():
    st.set_page_config(page_title="Sunhero 的股市智能超盤", layout="wide")
    st.title("Sunhero 的股市智能超盤（TWSE T86 優先 / TPEX Safe Mode）")

    with st.sidebar:
        st.subheader("更新設定")
        default_date = today_tpe().date()
        d = st.date_input("目標日期（交易日）", value=default_date)
        if st.button("立即更新", type="primary"):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption("規則：TPEX 成交額採 Safe Mode（200B）；三大法人先上 TWSE T86（免費穩）。")

    target_dt = datetime(d.year, d.month, d.day, tzinfo=TZ_TPE)
    snap, t86_df = build_market_snapshot(target_dt)

    # ====== 第一列：大盤 / 成交額 / 法人 ======
    c1, c2, c3, c4 = st.columns(4)

    twii = snap["twii"] or {}
    with c1:
        st.metric(
            "加權指數 TWII",
            f"{twii.get('close', '—'):.2f}" if twii.get("close") else "—",
            f"{twii.get('change', 0):+.2f}" if twii.get("change") is not None else None
        )
        st.caption(f"資料日：{twii.get('last_dt','—')}")

    ma = snap["market_amount"]
    with c2:
        st.metric("上市成交額（TWSE）", fmt_money(ma.get("amount_twse")))
        st.caption(f"來源：{ma.get('source_twse')}｜信心：{ma.get('confidence_twse')}")

    with c3:
        st.metric("上櫃成交額（TPEX）", fmt_money(ma.get("amount_tpex")))
        st.caption(f"來源：{ma.get('source_tpex')}｜信心：{ma.get('confidence_tpex')}")

    with c4:
        st.metric("總成交額", fmt_money(ma.get("amount_total")))
        st.caption(f"狀態：TWSE {ma.get('status_twse')} / TPEX {ma.get('status_tpex')}")

    st.divider()

    # ====== 三大法人摘要 ======
    st.subheader("三大法人（TWSE T86）")
    t86 = snap["t86"]
    if not t86.get("ok"):
        st.error(f"T86 讀取失敗：{t86.get('meta', {}).get('error')}")
    else:
        s = t86.get("summary", {})
        colA, colB, colC, colD = st.columns(4)
        colA.metric("外資淨買賣超", fmt_num(s.get("外資及陸資(不含外資自營商)")))
        colB.metric("投信淨買賣超", fmt_num(s.get("投信")))
        colC.metric("自營商淨買賣超", fmt_num(s.get("自營商")))
        colD.metric("三大法人合計", fmt_num(s.get("合計")))

        with st.expander("查看 T86 明細（可搜尋/排序）", expanded=False):
            st.dataframe(t86_df, use_container_width=True, height=520)

    st.divider()

    # ====== 稽核狀態 ======
    st.subheader("稽核狀態（Integrity）")
    integ = snap["integrity"]
    st.write({
        "trade_date": snap["trade_date_iso"],
        "twse_amount_ok": integ["twse_amount_ok"],
        "t86_ok": integ["t86_ok"],
        "tpex_mode": integ["tpex_mode"],
        "kill_switch": integ["kill_switch"],
    })

    with st.expander("查看 TWSE 成交額 audit meta", expanded=False):
        # 這裡把 meta 直接展示，有助你 debug（但 UI 不會是一堆代碼在首頁）
        st.json(snap["market_amount"])

if __name__ == "__main__":
    app()
