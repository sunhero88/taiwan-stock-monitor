# -*- coding: utf-8 -*-
"""
Filename: analyzer.py
Version: Predator V15.5.1 Patch (Bulletproof + Cloud Stable)
Notes:
- Interface: run_analysis(df, session) -> (top10_df, err_msg)
- Compatible with main.py V15.5.x
"""
import pandas as pd
import numpy as np
import json
import yfinance as yf
from datetime import datetime

# ======================================================
# 1. 固定參數區（參數集中管理）
# ======================================================

VOL_THRESHOLD_WEIGHTED = 1.2
VOL_THRESHOLD_SMALL = 1.8

MA_BIAS_GREEN_WEIGHTED = (0, 8)
MA_BIAS_GREEN_SMALL = (0, 12)

MA_BIAS_PENALTY_START = 10
MA_BIAS_PENALTY_FULL = 15
MA_BIAS_HARD_CAP = 20  # 硬剔除門檻 (乖離過大)

BODY_POWER_STRONG = 75
BODY_POWER_DISTRIBUTE = 20
DISTRIBUTE_VOL_RATIO = 2.5

SESSION_INTRADAY = "INTRADAY"
SESSION_EOD = "EOD"

# 準決賽名額：只對前 N 名做基本面抓取（降低 yfinance.info 次數）
FUNDAMENTAL_CANDIDATES = 15


# ======================================================
# 2. 工具函式（計算與防呆）
# ======================================================

def calc_body_power(row: dict) -> float:
    """計算 K 棒實體力道 (%)：|C-O| / (H-L)"""
    try:
        high = float(row.get("High", np.nan))
        low = float(row.get("Low", np.nan))
        close = float(row.get("Close", np.nan))
        open_ = float(row.get("Open", np.nan))

        if not np.isfinite(high) or not np.isfinite(low) or not np.isfinite(close) or not np.isfinite(open_):
            return 0.0

        high_low = high - low
        if high_low <= 0:
            return 0.0

        return abs(close - open_) / high_low * 100.0
    except Exception:
        return 0.0


def calc_ma_bias_penalty(ma_bias) -> float:
    """MA_Bias 噴出懲罰：10% 開始線性扣分，15% 到頂"""
    try:
        ma_bias = float(ma_bias)
    except Exception:
        return 0.0

    if ma_bias <= MA_BIAS_PENALTY_START:
        return 0.0
    if ma_bias >= MA_BIAS_PENALTY_FULL:
        return 1.0
    return (ma_bias - MA_BIAS_PENALTY_START) / (MA_BIAS_PENALTY_FULL - MA_BIAS_PENALTY_START)


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """欄位防呆：缺欄補 NaN，避免 rolling / Amount / BodyPower 爆掉"""
    required = ["Symbol", "Date", "Open", "High", "Low", "Close", "Volume"]
    for c in required:
        if c not in df.columns:
            df[c] = np.nan
    return df


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """型別防呆：轉數值欄位、Date 欄位"""
    num_cols = ["Open", "High", "Low", "Close", "Volume"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Date：允許字串、timestamp、tz-aware 混雜
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=False)
    return df


def _safe_get_fundamentals(symbol: str) -> dict:
    """
    結構面補強（降低 yfinance.info 卡死風險）
    優先用 fast_info（較快且穩），不夠再 fallback 到 info
    """
    data = {"OPM": 0, "QoQ": 0, "PE": 0, "Sector": "Unknown"}

    try:
        t = yf.Ticker(symbol)

        # 1) fast_info：通常較穩，但欄位少
        fi = getattr(t, "fast_info", None)
        # fast_info 通常沒有 operatingMargins / revenueGrowth / trailingPE / sector
        # 所以只作為「避免整體掛掉」的優先 path，不足就用 info

        # 2) info：欄位完整，但較慢且可能 timeout / 429
        info = {}
        try:
            info = t.info or {}
        except Exception:
            info = {}

        # 依你原本欄位
        opm = info.get("operatingMargins", 0) or 0
        qoq = info.get("revenueGrowth", 0) or 0
        pe = info.get("trailingPE", 0) or 0
        sector = info.get("sector", "Unknown") or "Unknown"

        data["OPM"] = round(float(opm) * 100, 2) if np.isfinite(float(opm)) else 0
        data["QoQ"] = round(float(qoq) * 100, 2) if np.isfinite(float(qoq)) else 0
        data["PE"] = round(float(pe), 2) if np.isfinite(float(pe)) else 0
        data["Sector"] = str(sector)

    except Exception:
        # 保持預設值即可
        pass

    return data


# ======================================================
# 3. 主分析流程（Run Analysis）
# ======================================================

def run_analysis(df: pd.DataFrame, session: str = SESSION_INTRADAY):
    """
    執行 V15.5.1 Patch 戰略篩選
    Return: (DataFrame, str_error_message)
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame(), "Input DataFrame is empty"

        # 若 Date 在 index，把它拉回欄位
        if "Date" not in df.columns:
            df = df.reset_index(drop=False)
            if "Date" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "Date"})

        df = df.copy()
        df = _ensure_columns(df)
        df = _coerce_types(df)

        # Date 全 NaT 直接回報（這通常代表 yfinance 回傳 index 沒帶進來）
        if df["Date"].isna().all():
            return pd.DataFrame(), "Date column invalid (all NaT after parsing)"

        # --------------------------------------------------
        # Step 1: 動態定義權值股（以最新交易日、成交額前50名門檻）
        # --------------------------------------------------
        latest_date = df["Date"].max()
        df_today = df[df["Date"] == latest_date].copy()
        if df_today.empty:
            return pd.DataFrame(), "No rows for latest_date"

        df_today["Amount"] = (df_today["Close"] * df_today["Volume"])
        df_today["Amount"] = pd.to_numeric(df_today["Amount"], errors="coerce").fillna(0)

        if len(df_today) > 50:
            top_50_amt_threshold = df_today["Amount"].nlargest(50).min()
        else:
            top_50_amt_threshold = 0

        weighted_symbols = set(
            df_today.loc[df_today["Amount"] >= top_50_amt_threshold, "Symbol"]
            .dropna()
            .astype(str)
            .tolist()
        )

        # --------------------------------------------------
        # Step 2: 技術面快篩（本地運算）
        # --------------------------------------------------
        results = []
        grouped = df.groupby("Symbol", dropna=False)

        for symbol, group in grouped:
            try:
                symbol = str(symbol)
            except Exception:
                continue

            group = group.sort_values("Date")
            if len(group) < 20:
                continue

            latest_row = group.iloc[-1]

            close_v = latest_row.get("Close", np.nan)
            vol_v = latest_row.get("Volume", np.nan)

            # 防呆：無有效 close / volume 或 volume=0
            if not np.isfinite(close_v) or not np.isfinite(vol_v) or float(vol_v) == 0.0:
                continue

            # MA20 / VOL_MA20
            ma20 = group["Close"].rolling(20).mean().iloc[-1]
            vol_ma20 = group["Volume"].rolling(20).mean().iloc[-1]

            if not np.isfinite(ma20) or not np.isfinite(vol_ma20) or vol_ma20 <= 0:
                continue

            latest = latest_row.to_dict()

            ma_bias = ((float(latest["Close"]) - float(ma20)) / float(ma20)) * 100.0
            vol_ratio = float(latest["Volume"]) / float(vol_ma20)

            latest["MA_Bias"] = ma_bias
            latest["Vol_Ratio"] = vol_ratio
            latest["Body_Power"] = calc_body_power(latest)

            # 權值股判斷
            is_weighted = symbol in weighted_symbols
            latest["_Is_Weighted"] = bool(is_weighted)

            # Kill Switch I：派貨陷阱（弱實體 + 超爆量）
            if latest["Body_Power"] < BODY_POWER_DISTRIBUTE and latest["Vol_Ratio"] > DISTRIBUTE_VOL_RATIO:
                continue

            # Kill Switch II：乖離過大硬剔除
            if latest["MA_Bias"] > MA_BIAS_HARD_CAP:
                continue

            # ERS 評分（你原本公式）
            penalty = calc_ma_bias_penalty(latest["MA_Bias"])
            ers = (
                (latest["Vol_Ratio"] * 20.0) +
                (max(0.0, 15.0 - abs(latest["MA_Bias"])) * 2.0)
            ) * (1.0 - 0.5 * penalty)

            latest["Score"] = round(float(ers), 2)

            # 保留籌碼視覺欄位（若 main.py 有傳 Inst_Status）
            if "Inst_Status" not in latest:
                # 讓下游 JSON 不至於 KeyError
                latest["Inst_Status"] = "N/A"

            results.append(latest)

        if not results:
            return pd.DataFrame(), "no_results_after_tech_filter (Check data source or market condition)"

        # --------------------------------------------------
        # Step 3: 準決賽（前 N 名做基本面聯網）
        # --------------------------------------------------
        candidates_df = pd.DataFrame(results).sort_values("Score", ascending=False).head(FUNDAMENTAL_CANDIDATES)
        candidates = candidates_df.to_dict("records")

        # --------------------------------------------------
        # Step 4: 基本面補強 + Kill Switch III + 打標籤
        # --------------------------------------------------
        final_list = []
        for row in candidates:
            symbol = str(row.get("Symbol", ""))

            fundamentals = _safe_get_fundamentals(symbol)
            row["Structure"] = fundamentals

            # Kill Switch III：結構惡化（QoQ < 0）
            # QoQ 已是百分比（例如 -3.2），因此用 0 判斷即可
            try:
                if fundamentals.get("QoQ", 0) is not None and float(fundamentals.get("QoQ", 0)) < 0:
                    continue
            except Exception:
                pass

            weighted = bool(row.get("_Is_Weighted", False))
            vol_threshold = VOL_THRESHOLD_WEIGHTED if weighted else VOL_THRESHOLD_SMALL
            green_range = MA_BIAS_GREEN_WEIGHTED if weighted else MA_BIAS_GREEN_SMALL

            ma_bias = float(row.get("MA_Bias", 0))
            vol_ratio = float(row.get("Vol_Ratio", 0))
            body_power = float(row.get("Body_Power", 0))

            tags = []
            # 🟢 綠燈（起漲）
            if green_range[0] < ma_bias <= green_range[1]:
                tags.append("🛡️起漲")
            # 🟡 黃燈（主力）
            if vol_ratio >= vol_threshold:
                tags.append("🔥主力")
            # 🟣 紫燈（真突破）
            if body_power >= BODY_POWER_STRONG:
                tags.append("⚡真突破")

            tag_suffix = "(觀望)" if session == SESSION_INTRADAY else "(確認)"
            row["Predator_Tag"] = (" ".join(tags) + tag_suffix) if tags else "○觀察"

            final_list.append(row)

        if not final_list:
            return pd.DataFrame(), "no_results_after_fundamental_filter (High risk market)"

        # --------------------------------------------------
        # Step 5: Top 10
        # --------------------------------------------------
        df_final = pd.DataFrame(final_list).sort_values("Score", ascending=False).head(10)

        # 確保下游顯示欄位存在（main.py dataframe cols）
        must_cols = ["Symbol", "Close", "MA_Bias", "Vol_Ratio", "Predator_Tag", "Score"]
        for c in must_cols:
            if c not in df_final.columns:
                df_final[c] = np.nan

        return df_final, ""

    except Exception as e:
        return pd.DataFrame(), f"Analyzer Crash: {type(e).__name__}: {str(e)}"


# ======================================================
# 4. JSON 輸出（Generate AI JSON）
# ======================================================

def generate_ai_json(df_top10: pd.DataFrame, market="tw-share", session=SESSION_INTRADAY, macro_data=None):
    """
    生成給 Gem 的戰略數據包
    """
    if df_top10 is None or df_top10.empty:
        return json.dumps({"error": "No data"}, indent=2)

    records = df_top10.to_dict("records")
    stocks = []

    for r in records:
        stocks.append({
            "Symbol": str(r.get("Symbol", "Unknown")),
            "Price": float(r.get("Close", 0) or 0),
            "Technical": {
                "MA_Bias": round(float(r.get("MA_Bias", 0) or 0), 2),
                "Vol_Ratio": round(float(r.get("Vol_Ratio", 0) or 0), 2),
                "Body_Power": round(float(r.get("Body_Power", 0) or 0), 1),
                "Score": round(float(r.get("Score", 0) or 0), 1),
                "Tag": r.get("Predator_Tag", "")
            },
            # main.py 有合併 Inst_Status 就會帶進來；沒有就維持 N/A
            "Inst_Visual": r.get("Inst_Status", "N/A"),
            "Structure": r.get("Structure", {})
        })

    payload = {
        "meta": {
            "system": "Predator V15.5.1 Patch (Bulletproof)",
            "market": market,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "session": session
        },
        "macro": macro_data if macro_data else {},
        "stocks": stocks
    }

    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
