import streamlit as st
import json
from arbiter import arbiter_run

st.set_page_config(layout="wide")
st.title("Sunhero | 股市智能超盤中控台 (V20 Stable / arbiter_run)")

# ===== 模式選擇 =====
run_mode = st.sidebar.radio("模式選擇", ["L1", "L2", "L3"], index=1)

# ===== JSON 輸入 =====
st.subheader("輸入 JSON Payload")
json_input = st.text_area("JSON 數據內容", height=420)

def safe_json(obj):
    """確保任何物件都能被 json 序列化（避免 st.json 內部爆炸）"""
    return json.loads(json.dumps(obj, ensure_ascii=False, default=str))

def action_counts(ucc: dict) -> dict:
    """永不 index，僅統計數量"""
    keys = ["OPEN", "ADD", "HOLD", "REDUCE", "CLOSE", "NO_TRADE"]
    out = {k: len(ucc.get(k, [])) if isinstance(ucc.get(k), list) else 0 for k in keys}
    out["DECISION"] = ucc.get("DECISION", "—")
    out["CONFIDENCE"] = ucc.get("CONFIDENCE", "—")
    return out

if st.button("執行裁決", type="primary"):

    # 1) JSON parse
    try:
        payload = json.loads(json_input)
    except Exception as e:
        st.error(f"JSON 解析錯誤: {e}")
        st.stop()

    # 2) Execute arbiter
    try:
        result = arbiter_run(payload, run_mode)
    except Exception as e:
        st.error(f"裁決引擎錯誤: {e}")
        st.stop()

    st.subheader("裁決結果（摘要）")

    # 3) 摘要顯示：只用穩定 key（MODE/RUN/VERDICT/NO_TRADE/AUDIT/UCC）
    mode = result.get("MODE", "—")
    run = result.get("RUN", "—")
    verdict = result.get("VERDICT", "—")
    no_trade = result.get("NO_TRADE", None)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("MODE", mode)
    c2.metric("RUN", run)
    c3.metric("VERDICT", verdict)
    c4.metric("NO_TRADE", str(no_trade))

    # 4) L1 稽核（永遠存在）
    l1 = ((result.get("AUDIT") or {}).get("L1")) or {}
    with st.expander("L1 稽核（AUDIT.L1）", expanded=True):
        st.json(safe_json(l1))

    # 5) UCC 行動摘要（永遠不 index）
    ucc = result.get("UCC") or {}
    with st.expander("UCC 動作摘要（Counts）", expanded=True):
        st.json(action_counts(ucc))

    # 6) UCC 動作明細（只顯示存在且非空的 list）
    st.markdown("### 交易動作（明細）")
    for k in ["OPEN", "ADD", "HOLD", "REDUCE", "CLOSE", "NO_TRADE"]:
        v = ucc.get(k, [])
        if isinstance(v, list) and len(v) > 0:
            st.write(f"**{k}**")
            st.json(safe_json(v))

    # 7) 完整輸出：放在 expander，避免 st.json 在頁面主區塊渲染時爆
    with st.expander("完整輸出（Debug / Raw Result）", expanded=False):
        st.json(safe_json(result))
