import json
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Predator 外圍監控｜航運估值", layout="wide")
st.title("Predator V15.6.3 外圍監控｜航運估值整合（不改核心 Arbiter）")

# ---------- Load JSON ----------
def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

eod_path = st.sidebar.text_input("EOD JSON 路徑", "data/eod_2026-01-23.json")
val_path = st.sidebar.text_input("航運估值 JSON 路徑", "data/shipping_valuation.json")

try:
    predator_eod = load_json(eod_path)
    shipping_val = load_json(val_path)
except Exception as e:
    st.error(f"讀檔失敗：{e}")
    st.stop()

# ---------- Meta ----------
meta = predator_eod.get("meta", {})
overview = predator_eod.get("macro", {}).get("overview", {})

st.caption(
    f"資料時間：{meta.get('timestamp','-')}｜交易日：{overview.get('trade_date','-')}｜"
    f"成交金額：{overview.get('amount','-')}｜估值資料日：{shipping_val.get('asof','-')}"
)
st.caption(f"估值來源註記：{shipping_val.get('source_note','-')}")

# ---------- Tables ----------
pred_df = pd.json_normalize(predator_eod.get("stocks", []))
val_df = pd.DataFrame(shipping_val.get("items", []))

# 估值端：用 Price_asof / EPS_TTM 重算 PE，避免人工欄位出錯
if not val_df.empty:
    val_df["PE_calc_asof"] = (val_df["Price_asof"] / val_df["EPS_TTM"]).round(2)

# 保留 Predator 關鍵欄位（你後續擴充可再加）
keep_cols = [
    "Symbol", "Price",
    "ranking.rank", "ranking.tier", "ranking.top20_flag",
    "Technical.Tag", "Technical.Score", "Technical.Vol_Ratio", "Technical.MA_Bias",
    "Institutional.Inst_Streak3", "Institutional.Inst_Dir3", "Institutional.Inst_Net_3d",
]
pred_df = pred_df[[c for c in keep_cols if c in pred_df.columns]]

# Merge：把估值資料掛回 Predator stocks
merged = pred_df.merge(val_df, on="Symbol", how="left")

# ---------- 外圍提示（保守，不下交易指令） ----------
def outer_note(r):
    if pd.isna(r.get("PE_calc_asof")):
        return ""
    pe = r.get("PE_calc_asof", 999)
    vol = r.get("Technical.Vol_Ratio", None)
    streak = r.get("Institutional.Inst_Streak3", 0)

    # 你目前的現況：2603 Vol=0.54、2609 Vol=0.67 且 streak=0
    if pe <= 8 and vol is not None and vol < 1.0 and streak < 3:
        return "估值便宜，但量能/法人未確認 → 列入追蹤，不放大倉位"
    if pe <= 8 and vol is not None and vol >= 1.0 and streak < 3:
        return "估值便宜 + 量能改善 → 可試單觀察，但仍需等法人連續"
    if pe <= 8 and streak >= 3:
        return "估值便宜 + 法人連續 → 可提升候選等級（仍由 Arbiter 定奪）"
    return "估值一般 → 以技術/法人為主"

merged["外圍提示"] = merged.apply(outer_note, axis=1)

# ---------- UI ----------
st.subheader("航運估值股（2603/2609）合併表")
only_shipping = st.checkbox("只顯示有估值資料者", value=True)
view = merged[merged["PE_calc_asof"].notna()].copy() if only_shipping else merged.copy()

# 排序：先把估值便宜者排前，再看 Technical.Score
if "PE_calc_asof" in view.columns and "Technical.Score" in view.columns:
    view = view.sort_values(["PE_calc_asof", "Technical.Score"], ascending=[True, False])

st.dataframe(view, use_container_width=True)

st.subheader("30 秒摘要（給指揮官）")
for _, r in view.iterrows():
    st.write(
        f"- {r['Symbol']}｜現價={r.get('Price','-')}｜"
        f"PE({shipping_val.get('asof','-')})={r.get('PE_calc_asof','-')}｜"
        f"OPM={r.get('OPM_latest_q','-')}%({r.get('OPM_q','-')})｜"
        f"Vol={r.get('Technical.Vol_Ratio','-')}｜法人Streak3={r.get('Institutional.Inst_Streak3','-')}｜"
        f"{r.get('Valuation_Tag','')}｜{r.get('外圍提示','')}"
    )
