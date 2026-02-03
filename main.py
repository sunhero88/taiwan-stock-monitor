# =========================================================
# Predator V16.3 Stable (Hybrid Edition)
# Replace these 4 functions (plus required helpers):
#   - compute_regime_metrics()
#   - pick_regime()
#   - inst_metrics_for_symbol()
#   - classify_layer()
# =========================================================

EPS = 1e-4  # 0.0001


def _as_series_close(df: pd.DataFrame) -> pd.Series:
    """
    yfinance 回來可能是 DataFrame/Series/欄位結構不同，這裡強制取得 Close 的 Series。
    """
    if df is None or len(df) == 0:
        return pd.Series(dtype="float64")

    if isinstance(df, pd.Series):
        return df.dropna()

    # DataFrame
    if "Close" in df.columns:
        s = df["Close"]
        if isinstance(s, pd.DataFrame):
            # 極少數情境 Close 仍是 DataFrame
            s = s.iloc[:, 0]
        return s.dropna()

    # MultiIndex 或其他：盡量猜一個 close 欄
    for c in df.columns:
        if str(c).lower().endswith("close"):
            s = df[c]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            return s.dropna()

    return pd.Series(dtype="float64")


def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float, np.floating)):
            return float(x)
        s = str(x).replace(",", "").strip()
        if s in ("", "nan", "None", "--"):
            return default
        return float(s)
    except Exception:
        return default


def _sign_dir(v: float) -> str:
    if v > 0:
        return "POSITIVE"
    if v < 0:
        return "NEGATIVE"
    return "NEUTRAL"


def _consecutive_true(flags: List[bool]) -> int:
    """
    從尾端開始算連續 True 幾天
    """
    c = 0
    for f in reversed(flags):
        if f:
            c += 1
        else:
            break
    return c


def compute_regime_metrics() -> dict:
    """
    V16.3 指標（最小可用版 / 可稽核）：

    - SMR = (Index - MA200) / MA200
    - SMR_MA5, Slope5
    - MOMENTUM_LOCK = (Slope5 > EPS) for 4 consecutive days
    - NEGATIVE_SLOPE_5D = (Slope5 < -EPS) for 5 consecutive days
    - drawdown_pct：近 250 交易日高點回撤（%），通常為負值（例如 -12.3）
    - MA14_Monthly：以月線收盤 resample('M').last() 取 14 個月均線
    - hibernation_below_ma_days：收盤 < MA14_Monthly*0.96 的連續天數（用於 2日×0.96）
    - consolidation_flag：SMR 在 0.08~0.18 且 15日價格振幅<5% 且最近10日 SMR 也都在範圍內
    - dynamic_vix_threshold = max(MA20 + 2*STD, 35)
    """
    out = {
        "SMR": None,
        "MA200": None,
        "SMR_MA5": None,
        "Slope5": None,

        "MOMENTUM_LOCK": False,
        "NEGATIVE_SLOPE_5D": False,

        "VIX": None,
        "vix_ma20": None,
        "vix_std20": None,
        "dynamic_vix_threshold": 35.0,

        "drawdown_pct": None,

        "MA14_Monthly": None,
        "hibernation_below_ma_days": 0,

        "consolidation_15d_vol": None,
        "consolidation_flag": False,
    }

    # 取足夠長度：MA200 + 月線均線 + 盤整判斷
    tw = yf.download("^TWII", period="900d", interval="1d", progress=False)
    vx = yf.download("^VIX", period="120d", interval="1d", progress=False)

    close = _as_series_close(tw)
    vix_s = _as_series_close(vx)

    if close.empty or len(close) < 260:
        return out

    # --- MA200 / SMR ---
    ma200_series = close.rolling(200).mean()
    ma200 = _safe_float(ma200_series.iloc[-1], None)
    last = _safe_float(close.iloc[-1], None)

    if ma200 is None or last is None or ma200 == 0:
        return out

    smr_series = (close - ma200_series) / ma200_series
    smr_series = smr_series.dropna()

    smr = _safe_float(smr_series.iloc[-1], None)
    if smr is None:
        return out

    # --- SMR_MA5 / Slope5 ---
    smr_ma5_series = smr_series.rolling(5).mean().dropna()
    if len(smr_ma5_series) < 6:
        # 資料太少就保守返回
        out.update({
            "SMR": round(float(smr), 6),
            "MA200": round(float(ma200), 2),
        })
        return out

    smr_ma5 = float(smr_ma5_series.iloc[-1])
    slope5 = float(smr_ma5_series.iloc[-1] - smr_ma5_series.iloc[-2])

    # --- MOMENTUM_LOCK / NEGATIVE_SLOPE_5D ---
    slopes = (smr_ma5_series.diff()).dropna()
    last4 = slopes.iloc[-4:].tolist() if len(slopes) >= 4 else []
    last5 = slopes.iloc[-5:].tolist() if len(slopes) >= 5 else []

    momentum_lock = (len(last4) == 4) and all((x is not None and x > EPS) for x in last4)
    negative_slope_5d = (len(last5) == 5) and all((x is not None and x < -EPS) for x in last5)

    # --- drawdown over 250 trading days ---
    lookback = close.iloc[-250:] if len(close) >= 250 else close
    peak = float(lookback.max())
    dd = ((last - peak) / peak * 100.0) if peak else None

    # --- consolidation 判斷 ---
    lb15 = close.iloc[-15:] if len(close) >= 15 else close
    vol15 = None
    if len(lb15) >= 10:
        vol15 = (float(lb15.max()) - float(lb15.min())) / float(lb15.mean()) * 100.0

    # 最近 10 日 SMR 是否都在 0.08~0.18（V16.3 的盤整定義要求「持續」）
    recent_smr10 = smr_series.iloc[-10:].tolist() if len(smr_series) >= 10 else []
    smr_in_range_10d = (len(recent_smr10) == 10) and all(0.08 <= float(x) <= 0.18 for x in recent_smr10)

    consolidation_flag = (
        (vol15 is not None) and (vol15 < 5.0)
        and (0.08 <= float(smr) <= 0.18)
        and smr_in_range_10d
    )

    # --- MA14_Monthly（用月末收盤） ---
    # yfinance 的 daily index 是 tz-naive；resample 以日曆月末取最後一筆
    monthly_close = close.resample("M").last().dropna()
    ma14_monthly = None
    if len(monthly_close) >= 14:
        ma14_monthly = float(monthly_close.rolling(14).mean().iloc[-1])

    # --- HIBERNATION：close < MA14_Monthly*0.96 連續天數 ---
    below_days = 0
    if ma14_monthly is not None and ma14_monthly > 0:
        thresh = ma14_monthly * 0.96
        flags = (close.iloc[-10:] < thresh).tolist()  # 看最近10天足夠算連續
        below_days = _consecutive_true(flags)

    # --- VIX / dynamic threshold ---
    vix = None
    vix_ma20 = None
    vix_std20 = None
    dyn_thr = 35.0

    if not vix_s.empty:
        vix = float(vix_s.iloc[-1])
        if len(vix_s) >= 20:
            last20 = vix_s.iloc[-20:]
            vix_ma20 = float(last20.mean())
            vix_std20 = float(last20.std(ddof=0))
            calc = vix_ma20 + 2.0 * vix_std20
            dyn_thr = float(max(calc, 35.0))
        else:
            dyn_thr = 40.0  # 資料不足時預設較保守

    out.update({
        "SMR": round(float(smr), 6),
        "MA200": round(float(ma200), 2),
        "SMR_MA5": round(float(smr_ma5), 6),
        "Slope5": round(float(slope5), 6),

        "MOMENTUM_LOCK": bool(momentum_lock),
        "NEGATIVE_SLOPE_5D": bool(negative_slope_5d),

        "VIX": round(float(vix), 2) if vix is not None else None,
        "vix_ma20": round(float(vix_ma20), 2) if vix_ma20 is not None else None,
        "vix_std20": round(float(vix_std20), 2) if vix_std20 is not None else None,
        "dynamic_vix_threshold": round(float(dyn_thr), 2),

        "drawdown_pct": round(float(dd), 2) if dd is not None else None,

        "MA14_Monthly": round(float(ma14_monthly), 2) if ma14_monthly is not None else None,
        "hibernation_below_ma_days": int(below_days),

        "consolidation_15d_vol": round(float(vol15), 2) if vol15 is not None else None,
        "consolidation_flag": bool(consolidation_flag),
    })
    return out


def pick_regime(metrics: dict) -> Tuple[str, float]:
    """
    V16.3 Regime 優先序（由高到低）：
      CRASH_RISK > HIBERNATION > MEAN_REVERSION > OVERHEAT > CONSOLIDATION > NORMAL

    V16.3 門檻（照你文檔）：
      CRASH_RISK      : VIX > 35 或 drawdown_pct >= 18%
      HIBERNATION     : close < MA14_Monthly * 0.96 連續 2 個交易日（用 hibernation_below_ma_days >= 2 近似）
      MEAN_REVERSION  : SMR > 0.25 且 Slope5 < -0.0001
      OVERHEAT        : SMR > 0.25 且 Slope5 >= -0.0001
      CONSOLIDATION   : consolidation_flag == True
      NORMAL          : 其他

    回傳：
      (regime_name, max_equity_pct)
    """
    max_equity_map = {
        "CRASH_RISK": 10.0,
        "HIBERNATION": 20.0,
        "MEAN_REVERSION": 45.0,
        "OVERHEAT": 55.0,
        "CONSOLIDATION": 65.0,
        "NORMAL": 85.0,
    }

    smr = metrics.get("SMR")
    slope5 = metrics.get("Slope5")
    vix = metrics.get("VIX")
    dd = metrics.get("drawdown_pct")
    cons = bool(metrics.get("consolidation_flag"))
    below_days = int(metrics.get("hibernation_below_ma_days") or 0)

    # 缺資料 → 保守 NORMAL（並交給 Data Health Gate 決定是否 degraded）
    if smr is None or slope5 is None:
        return "NORMAL", max_equity_map["NORMAL"]

    # 1) CRASH_RISK
    dd_abs = abs(float(dd)) if dd is not None else None
    if (vix is not None and float(vix) > 35.0) or (dd_abs is not None and dd_abs >= 18.0 and float(dd) < 0):
        return "CRASH_RISK", max_equity_map["CRASH_RISK"]

    # 2) HIBERNATION：2日×0.96
    # 這裡用 hibernation_below_ma_days>=2 觸發
    if below_days >= 2:
        return "HIBERNATION", max_equity_map["HIBERNATION"]

    # 3) MEAN_REVERSION
    if float(smr) > 0.25 and float(slope5) < -EPS:
        return "MEAN_REVERSION", max_equity_map["MEAN_REVERSION"]

    # 4) OVERHEAT
    if float(smr) > 0.25 and float(slope5) >= -EPS:
        return "OVERHEAT", max_equity_map["OVERHEAT"]

    # 5) CONSOLIDATION
    if cons:
        return "CONSOLIDATION", max_equity_map["CONSOLIDATION"]

    # 6) NORMAL
    return "NORMAL", max_equity_map["NORMAL"]


def inst_metrics_for_symbol(panel: pd.DataFrame, symbol_tw: str) -> dict:
    """
    V16.3 法人指標（供 Layer A+/A 使用）

    你的 panel 來源仍沿用 build_institutional_panel() 的輸出（T86 concat 後的 DataFrame）
    預期欄位至少包含：code, date, foreign_net, it_net

    回傳（兼容舊欄位 + 新欄位）：
      - inst_streak3 : 連續法人(外資+投信)淨買超天數（上限>=3就視為3）
      - inst_dir3    : 最近 3 日法人合計方向 POSITIVE/NEGATIVE/NEUTRAL
      - foreign_buy  : 當日外資淨買超 >0
      - trust_buy    : 當日投信淨買超 >0
      - inst_net_3d  : 最近三日法人合計（股數）
      - inst_status  : READY / UNAVAILABLE
      - inst_dates_5 : 最近可用日期（最多5）
      - foreign_dir  : 最近三日外資方向（兼容舊 UI）
      - inst_streak5 : 兼容舊版本（用於展示，不作 V16.3 Layer A+ 判斷）
    """
    out = {
        "inst_status": "UNAVAILABLE",

        "inst_streak3": 0,
        "inst_streak5": 0,
        "inst_dir3": "MISSING",
        "foreign_dir": "MISSING",

        "foreign_buy": False,
        "trust_buy": False,

        "inst_net_3d": 0,
        "inst_dates_5": [],
    }

    if panel is None or panel.empty:
        return out

    code = symbol_tw.replace(".TW", "")
    df = panel[panel["code"] == code].copy()
    if df.empty:
        return out

    # 日期排序（重要：連續判斷靠這個）
    df = df.sort_values("date")

    # 轉成 int
    df["foreign_net"] = df["foreign_net"].fillna(0).astype(int)
    df["it_net"] = df["it_net"].fillna(0).astype(int)
    df["inst_net"] = df["foreign_net"] + df["it_net"]

    # 最近 N 日資料
    dates = df["date"].tolist()
    inst = df["inst_net"].tolist()
    foreign = df["foreign_net"].tolist()
    it = df["it_net"].tolist()

    out["inst_dates_5"] = dates[-5:]

    # READY：至少要有 3 日
    if len(inst) >= 3:
        out["inst_status"] = "READY"
    else:
        out["inst_status"] = "UNAVAILABLE"
        return out

    # inst_dir3
    last3_inst = inst[-3:]
    s3 = int(np.sum(last3_inst))
    out["inst_dir3"] = _sign_dir(float(s3))
    out["inst_net_3d"] = int(s3)

    # foreign_dir（兼容）
    last3_f = foreign[-3:]
    sf3 = int(np.sum(last3_f))
    out["foreign_dir"] = _sign_dir(float(sf3))

    # foreign_buy / trust_buy（V16.3 Layer A+/A 的核心）
    out["foreign_buy"] = bool(foreign[-1] > 0) if foreign else False
    out["trust_buy"] = bool(it[-1] > 0) if it else False

    # inst_streak3：連續法人淨買超（>0）天數（最多回傳 3）
    streak = 0
    for v in reversed(inst):
        if v > 0:
            streak += 1
        else:
            break
    out["inst_streak3"] = int(min(streak, 3))
    out["inst_streak5"] = int(min(streak, 5))  # 兼容顯示用

    return out


def classify_layer(regime: str, momentum_lock: bool, vol_ratio: Optional[float], inst: dict) -> str:
    """
    V16.3 Layer（A+ → A → B → NONE）

    A+ : foreign_buy == True AND trust_buy == True AND inst_streak3 >= 3
    A  : (foreign_buy OR trust_buy) AND inst_streak3 >= 3
    B  : momentum_lock AND vol_ratio > 0.8 AND regime in ["NORMAL","OVERHEAT","CONSOLIDATION"]
    NONE: 其他
    """
    foreign_buy = bool(inst.get("foreign_buy", False))
    trust_buy = bool(inst.get("trust_buy", False))
    streak3 = int(inst.get("inst_streak3", 0))

    if foreign_buy and trust_buy and streak3 >= 3:
        return "A+"

    if (foreign_buy or trust_buy) and streak3 >= 3:
        return "A"

    vr = _safe_float(vol_ratio, None)
    if momentum_lock and (vr is not None) and (vr > 0.8) and (regime in ("NORMAL", "OVERHEAT", "CONSOLIDATION")):
        return "B"

    return "NONE"
