from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="TTC Pulse", layout="wide")

st.title("TTC Pulse")
st.caption("A reliability story for TTC routes and stations.")

lead_left, lead_right = st.columns([1.2, 1], vertical_alignment="center")
with lead_left:
    st.subheader("Start here")
    st.markdown("Use the story flow in the sidebar, or jump directly:")
    st.page_link("pages/01_Story_Overview.py", label="Story Overview")
    st.page_link("pages/02_Recurring_Hotspots.py", label="Recurring Hotspots")
    st.page_link("pages/03_Time_Patterns.py", label="Time Patterns")
with lead_right:
    st.subheader("Deep dive")
    st.page_link("pages/05_Drill_Down_Explorer.py", label="Drill-Down Explorer")
    st.page_link("pages/06_Live_Alert_Alignment.py", label="Live Alert Alignment")
    st.page_link("pages/07_QA_Methodology.py", label="QA / Methodology")
