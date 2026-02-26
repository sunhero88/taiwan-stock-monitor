import streamlit as st
import json
from arbiter import arbiter_run

st.set_page_config(layout="wide")

st.title("Sunhero | 股市智能超盤中控台 (V20 Stable)")

# ===== 模式選擇 =====
mode = st.sidebar.radio("模式選擇", ["L1", "L2", "L3"])

# ===== JSON 輸入 =====
st.subheader("輸入 JSON Payload")
json_input = st.text_area("JSON 數據內容", height=400)

if st.button("執行裁決"):

    try:
        payload = json.loads(json_input)
    except Exception as e:
        st.error(f"JSON 解析錯誤: {e}")
        st.stop()

    try:
        result = arbiter_run(payload, mode)
    except Exception as e:
        st.error(f"裁決引擎錯誤: {e}")
        st.stop()

    st.subheader("裁決結果")

    # 安全顯示整體結果
    st.json(result)

    # ===== L2 特別安全顯示 =====
    if result.get("mode") == "L2":

        decision = result.get("decision", {})
        open_list = decision.get("OPEN", [])
        add_list = decision.get("ADD", [])
        hold_list = decision.get("HOLD", [])
        reduce_list = decision.get("REDUCE", [])
        close_list = decision.get("CLOSE", [])

        st.markdown("### 交易動作")

        if open_list:
            st.write("OPEN:", open_list)
        if add_list:
            st.write("ADD:", add_list)
        if hold_list:
            st.write("HOLD:", hold_list)
        if reduce_list:
            st.write("REDUCE:", reduce_list)
        if close_list:
            st.write("CLOSE:", close_list)

        if not any([open_list, add_list, hold_list, reduce_list, close_list]):
            st.write("NO_TRADE")
