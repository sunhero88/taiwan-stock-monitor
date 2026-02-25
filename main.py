# main.py
# =========================================================
# Streamlit UI for Predator UCC V19.1
# - RUN L1/L2/L3 selector
# - 「盤中是否允許當日法人資料」開關
# - payload patch（補欄位/寫政策/統計缺漏）
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


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _is_nan(x) -> bool:
    try:
        return isinstance(x, float) and math.isnan(x)
    except Exception:
        return False


def normalize_json(obj):
    """遞迴把 NaN 轉 None，避免 JSON 序列化/比較問題。"""
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
                  inst_effective_date_override: str | None):
    """
    送入 UCC 前的 deterministic patch：
    - 補齊 meta/macro 結構
    - 補 max_equity_allowed_pct（缺就設 0.0 + lock_reason）
    - UI 法人政策寫入 meta.intraday_institutional_policy
    - allow_same_day_inst=false 時，強制全股 Inst_Status=USING_T_MINUS_1, inst_data_fresh=false
    - 追加 PRICE_MISSING_X_OF_N 到 market_status_reason
    """
    payload = deepcopy(payload_in)
    notes = []

    ensure_path(payload, ["meta"])
    ensure_path(payload, ["macro"])
    ensure_path(payload, ["macro", "overview"])
    ensure_path(payload, ["macro", "market_amount"])
    ensure_path(payload, ["macro", "integrity"])

    meta = payload["meta"]
    ov = payload["macro"]["overview"]

    if not meta.get("timestamp"):
        meta["timestamp"] = _now_str()
        notes.append("PATCH: $.meta.timestamp filled with now()")

    if not meta.get("effective_trade_date"):
        meta["effective_trade_date"] = meta["timestamp"][:10]
        notes.append("PATCH: $.meta.effective_trade_date filled from timestamp")

    if "max_equity_allowed_pct" not in ov or ov.get("max_equity_allowed_pct") is None:
        ov["max_equity_allowed_pct"] = 0.0
        if not isinstance(meta.get("max_equity_lock_reason"), list):
            meta["max_equity_lock_reason"] = []
        meta["max_equity_lock_reason"].append("MAX_EQUITY_MISSING_DEFAULT_0")
        notes.append("PATCH: $.macro.overview.max_equity_allowed_pct missing -> set 0.0 + lock_reason")

    if "market_status_reason" not in meta or not isinstance(meta.get("market_status_reason"), list):
        meta["market_status_reason"] = []
        notes.append("PATCH: $.meta.market_status_reason initialized []")

    payload = normalize_json(payload)

    ensure_path(meta, ["intraday_institutional_policy"])
    pol = meta["intraday_institutional_policy"]
    pol["allow_same_day"] = bool(allow_same_day_inst)
    pol["enforce_token_when_same_day"] = bool(enforce_token_when_same_day)

    inst_eff = inst_effective_date_override or meta.get("effective_trade_date")
    pol["inst_effective_date"] = inst_eff
    pol["resolved_use_same_day"] = bool(allow_same_day_inst)

    stocks = payload.get("stocks", []) or []
    for s in stocks:
        if not isinstance(s, dict):
            continue
        inst = s.get("Institutional")
        if not isinstance(inst, dict):
            inst = {}
            s["Institutional"] = inst

        if allow_same_day_inst:
            inst.setdefault("Inst_Status", "READY")
            inst.setdefault("inst_data_fresh", True)
            inst["inst_effective_date"] = inst_eff
        else:
            inst["Inst_Status"] = "USING_T_MINUS_1"
            inst["inst_data_fresh"] = False
            inst["inst_effective_date"] = inst_eff

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
        help="L1=只做資料稽核；L2=交易裁決（需 L1=PASS）；L3=回撤壓測（需觸發條件）"
    )

    st.divider()
    st.subheader("法人資料政策（UI 開關）")
    allow_same_day_inst = st.toggle(
        "盤中是否允許「當日法人資料」",
        value=False,
        help="關閉：強制 USING_T_MINUS_1 + inst_data_fresh=false。開啟：允許 READY + inst_data_fresh=true。"
    )
    enforce_token_when_same_day = st.toggle(
        "允許當日法人時：是否強制 Token？（寫入 policy 欄位）",
        value=True
    )
    inst_effective_date_override = st.text_input(
        "法人資料有效日（不填=用 effective_trade_date）",
        value="",
        help="YYYY-MM-DD；不填則自動取 meta.effective_trade_date"
    ).strip() or None

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

    b1, b2, b3 = st.columns(3)
    with b1:
        if st.button("載入範本", use_container_width=True):
            st.session_state.payload_text = json.dumps(DEFAULT_PAYLOAD, ensure_ascii=False, indent=2)
            st.rerun()
    with b2:
        if st.button("格式化 JSON", use_container_width=True):
            try:
                obj = json.loads(payload_text)
                st.session_state.payload_text = json.dumps(obj, ensure_ascii=False, indent=2)
                st.rerun()
            except Exception as e:
                st.error(f"JSON 格式錯誤：{e}")
    with b3:
        run_clicked = st.button("執行 UCC", type="primary", use_container_width=True)

with col2:
    st.subheader("UCC 輸出結果")
    st.caption("會先做 payload patch（補欄位/法人政策/缺漏統計），再送進 UCC。")

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

    if st.session_state.last_patch_notes:
        st.markdown("**Patch 紀錄（送入 UCC 前的修補）**")
        st.code("\n".join(st.session_state.last_patch_notes), language="text")

    if st.session_state.last_output is None:
        st.info("尚未執行。請貼上 JSON 後按「執行 UCC」。")
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
