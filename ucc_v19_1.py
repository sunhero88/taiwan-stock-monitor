# main.py
# =========================================================
# Streamlit UI for Predator UCC V19.1
# - RUN L1/L2/L3 selector
# - Intraday same-day institutional policy toggle
# - Payload patching (safe, deterministic)
# =========================================================

import json
import math
from copy import deepcopy
from datetime import datetime
import streamlit as st

from ucc_v19_1 import UCCv19_1


APP_TITLE = "Predator UCC V19.1 — Streamlit Console"
DEFAULT_PAYLOAD = {
    "meta": {
        "timestamp": "",
        "session": "INTRADAY",
        "market_status": "DEGRADED",
        "market_status_reason": [],
        "current_regime": "DATA_FAILURE",
        "confidence_level": "LOW",
        "is_using_previous_day": True,
        "effective_trade_date": "",
        "max_equity_lock_reason": [],
        "confidence": {"price": "LOW", "volume": "LOW", "institutional": "LOW"},
        "intraday_institutional_policy": {
            "allow_same_day": False,
            "enforce_token_when_same_day": True,
            "resolved_use_same_day": False,
            "inst_effective_date": ""
        }
    },
    "macro": {
        "overview": {
            "twii_close": None,
            "SMR": None,
            "Slope5": None,
            "Acceleration": None,
            "Top_Divergence": False,
            "Blow_Off_Phase": False,
            "MOMENTUM_LOCK": False,
            "vix": None,
            "max_equity_allowed_pct": 0.0,
            "calc_version": "V16.3.x+UCC_PATCH_UI",
            "slope5_def": "diff5_of_SMR"
        },
        "market_amount": {
            "amount_twse": None,
            "amount_tpex": None,
            "amount_total_raw": None,
            "amount_total_blended": None,
            "source_twse": "",
            "source_tpex": "",
            "status_twse": "",
            "status_tpex": "",
            "confidence_level": "LOW"
        },
        "integrity": {
            "kill": False,
            "vix_invalid": False,
            "reason": "OK"
        }
    },
    "stocks": []
}


# -------------------------
# Utility
# -------------------------
def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _is_nan(x) -> bool:
    try:
        return isinstance(x, float) and math.isnan(x)
    except Exception:
        return False


def normalize_json(obj):
    """Convert NaN to None recursively."""
    if isinstance(obj, dict):
        return {k: normalize_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_json(v) for v in obj]
    if _is_nan(obj):
        return None
    return obj


def ensure_path(d: dict, keys: list):
    cur = d
    for k in keys:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    return d


def count_missing_prices(payload: dict) -> int:
    n = 0
    for s in payload.get("stocks", []) or []:
        if (s or {}).get("Price", None) is None:
            n += 1
    return n


def patch_payload(payload_in: dict,
                  allow_same_day_inst: bool,
                  enforce_token_when_same_day: bool,
                  inst_effective_date_override: str | None) -> tuple[dict, list[str]]:
    """
    Deterministic patching:
    - Ensure meta/macro required structure exists
    - Ensure max_equity_allowed_pct exists (if missing -> 0.0 + lock reason)
    - Intraday institutional policy toggle:
      if not allow same-day: force USING_T_MINUS_1 and inst_data_fresh=False
    - Add PRICE_MISSING_X_OF_N market_status_reason when needed
    """
    payload = deepcopy(payload_in)
    notes: list[str] = []

    # Ensure base structure
    ensure_path(payload, ["meta"])
    ensure_path(payload, ["macro"])
    ensure_path(payload, ["macro", "overview"])
    ensure_path(payload, ["macro", "market_amount"])
    ensure_path(payload, ["macro", "integrity"])

    meta = payload["meta"]
    ov = payload["macro"]["overview"]

    # Fill timestamp if empty
    if not meta.get("timestamp"):
        meta["timestamp"] = _now_str()
        notes.append("PATCH: $.meta.timestamp filled with now()")

    # Fill effective_trade_date if empty (use date part of timestamp)
    if not meta.get("effective_trade_date"):
        meta["effective_trade_date"] = meta["timestamp"][:10]
        notes.append("PATCH: $.meta.effective_trade_date filled from timestamp")

    # Ensure max_equity_allowed_pct exists
    if "max_equity_allowed_pct" not in ov or ov.get("max_equity_allowed_pct") is None:
        ov["max_equity_allowed_pct"] = 0.0
        ensure_path(meta, ["max_equity_lock_reason"])
        if not isinstance(meta.get("max_equity_lock_reason"), list):
            meta["max_equity_lock_reason"] = []
        meta["max_equity_lock_reason"].append("MAX_EQUITY_MISSING_DEFAULT_0")
        notes.append("PATCH: $.macro.overview.max_equity_allowed_pct missing -> set 0.0 + lock_reason")

    # Ensure market_status_reason list
    if "market_status_reason" not in meta or not isinstance(meta.get("market_status_reason"), list):
        meta["market_status_reason"] = []
        notes.append("PATCH: $.meta.market_status_reason initialized []")

    # Normalize NaN -> None
    payload = normalize_json(payload)

    # Intraday institutional policy node
    ensure_path(meta, ["intraday_institutional_policy"])
    pol = meta["intraday_institutional_policy"]
    pol["allow_same_day"] = bool(allow_same_day_inst)
    pol["enforce_token_when_same_day"] = bool(enforce_token_when_same_day)

    # Resolve inst_effective_date
    inst_eff = inst_effective_date_override or meta.get("effective_trade_date")
    pol["inst_effective_date"] = inst_eff
    pol["resolved_use_same_day"] = bool(allow_same_day_inst)

    # Apply policy to each stock institutional block
    stocks = payload.get("stocks", []) or []
    for i, s in enumerate(stocks):
        if not isinstance(s, dict):
            continue
        inst = s.get("Institutional")
        if not isinstance(inst, dict):
            inst = {}
            s["Institutional"] = inst

        if allow_same_day_inst:
            # keep READY if provided; if absent -> READY
            if inst.get("Inst_Status") is None:
                inst["Inst_Status"] = "READY"
            if inst.get("inst_data_fresh") is None:
                inst["inst_data_fresh"] = True
            inst["inst_effective_date"] = inst_eff
        else:
            # force T-1 semantics (deterministic)
            inst["Inst_Status"] = "USING_T_MINUS_1"
            inst["inst_data_fresh"] = False
            inst["inst_effective_date"] = inst_eff

    # Add price-missing reason
    missing = count_missing_prices(payload)
    total = len(payload.get("stocks", []) or [])
    if total > 0 and missing > 0:
        reason = f"PRICE_MISSING_{missing}_OF_{total}"
        if reason not in meta["market_status_reason"]:
            meta["market_status_reason"].append(reason)
            notes.append(f"PATCH: $.meta.market_status_reason add {reason}")

    return payload, notes


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.header("UCC 控制台")

    run_mode = st.radio(
        "RUN 模式（一次只輸出一層）",
        options=["L1", "L2", "L3"],
        index=0,
        help="L1=只做資料稽核；L2=交易裁決（需 L1=PASS）；L3=回撤壓測（需符合觸發條件）"
    )

    st.divider()
    st.subheader("法人資料政策（UI 開關）")
    allow_same_day_inst = st.toggle(
        "盤中是否允許「當日法人資料」",
        value=False,
        help="關閉：一律視為 T-1（USING_T_MINUS_1, inst_data_fresh=false）。開啟：允許 READY + inst_data_fresh=true。"
    )

    enforce_token_when_same_day = st.toggle(
        "若允許當日法人，是否強制需要 Token 才能當日？",
        value=True,
        help="目前只會寫入 payload 的 policy 欄位，實際抓取端可依此決定是否要 token。"
    )

    inst_effective_date_override = st.text_input(
        "法人資料有效日（不填=用 effective_trade_date）",
        value="",
        help="建議用 YYYY-MM-DD；不填則自動取 meta.effective_trade_date。"
    )
    inst_effective_date_override = inst_effective_date_override.strip() or None

    st.divider()
    st.caption("提示：若 L2/L3 被拒絕，通常是 L1 沒 PASS（合憲護欄會回退 L1 FAIL）。")

col1, col2 = st.columns(2)

with col1:
    st.subheader("輸入 Payload（JSON）")
    if "payload_text" not in st.session_state:
        st.session_state.payload_text = json.dumps(DEFAULT_PAYLOAD, ensure_ascii=False, indent=2)

    payload_text = st.text_area(
        "貼上你的 payload JSON",
        value=st.session_state.payload_text,
        height=520
    )

    btns = st.columns(3)
    with btns[0]:
        if st.button("載入範本", use_container_width=True):
            st.session_state.payload_text = json.dumps(DEFAULT_PAYLOAD, ensure_ascii=False, indent=2)
            st.rerun()
    with btns[1]:
        if st.button("格式化 JSON", use_container_width=True):
            try:
                obj = json.loads(payload_text)
                st.session_state.payload_text = json.dumps(obj, ensure_ascii=False, indent=2)
                st.rerun()
            except Exception as e:
                st.error(f"JSON 格式錯誤：{e}")
    with btns[2]:
        run_clicked = st.button("執行 UCC", type="primary", use_container_width=True)

with col2:
    st.subheader("UCC 輸出結果")
    st.caption("會先做 payload patch（安全補齊 + 法人政策寫入），再送進 UCC。")

    if "last_output" not in st.session_state:
        st.session_state.last_output = None
    if "last_patch_notes" not in st.session_state:
        st.session_state.last_patch_notes = []
    if "last_payload_patched" not in st.session_state:
        st.session_state.last_payload_patched = None

    if run_clicked:
        try:
            payload_obj = json.loads(payload_text)
        except Exception as e:
            st.error(f"JSON 解析失敗：{e}")
            st.stop()

        patched, notes = patch_payload(
            payload_obj,
            allow_same_day_inst=allow_same_day_inst,
            enforce_token_when_same_day=enforce_token_when_same_day,
            inst_effective_date_override=inst_effective_date_override
        )

        ucc = UCCv19_1()
        out = ucc.run(patched, run_mode=run_mode)

        st.session_state.last_output = out
        st.session_state.last_patch_notes = notes
        st.session_state.last_payload_patched = patched

    # Show patch notes
    if st.session_state.last_patch_notes:
        st.markdown("**Patch 紀錄（送入 UCC 前的修補）**")
        st.code("\n".join(st.session_state.last_patch_notes), language="text")

    # Show output
    if st.session_state.last_output is None:
        st.info("尚未執行。請在左側貼上 JSON 後按「執行 UCC」。")
    else:
        out = st.session_state.last_output
        if isinstance(out, dict):
            st.json(out)
        else:
            st.code(str(out), language="text")

    st.divider()
    with st.expander("查看送入 UCC 的最終 Payload（patched）", expanded=False):
        if st.session_state.last_payload_patched is not None:
            st.json(st.session_state.last_payload_patched)
        else:
            st.caption("尚未產生 patched payload。")
