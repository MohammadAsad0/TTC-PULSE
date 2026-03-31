from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st


def _bootstrap_src_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        src_dir = parent / "src"
        if (src_dir / "ttc_pulse").exists():
            if str(src_dir) not in sys.path:
                sys.path.insert(0, str(src_dir))
            return


_bootstrap_src_path()

from ttc_pulse.dashboard.formatting import fmt_int
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_spatial_hotspot():
    return query_table(
        table_name="gold_spatial_hotspot",
        query_template="""
        SELECT
            mode,
            spatial_unit_type,
            spatial_unit_id,
            centroid_lat,
            centroid_lon,
            frequency,
            severity_p90,
            regularity_p90,
            composite_score,
            confidence_score
        FROM {source}
        ORDER BY composite_score DESC NULLS LAST
        """,
    )


st.title("Spatial Hotspot Map")
st.caption("Gold table: `gold_spatial_hotspot`")

result = _load_spatial_hotspot()
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()

if result.status == "empty":
    st.info(
        "Spatial hotspot remains deferred. Confidence-gated `gold_spatial_hotspot` is currently empty, "
        "so map publication is disabled for this run."
    )
    st.stop()

frame = result.frame.copy()
frame = frame.dropna(subset=["centroid_lat", "centroid_lon"])
if frame.empty:
    st.info("Spatial rows are present but all centroid coordinates are null. Map rendering is deferred.")
    st.stop()

mode_options = sorted(frame["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)
top_n = st.slider("Top N Hotspots by Composite Score", min_value=10, max_value=500, value=100, step=10)

filtered = frame[frame["mode"] == selected_mode].copy()
filtered = filtered.sort_values("composite_score", ascending=False).head(top_n)
if filtered.empty:
    st.info("No mapped hotspots for the selected mode.")
    st.stop()

st.metric("Mapped Hotspots", fmt_int(len(filtered)))
st.map(
    filtered.rename(columns={"centroid_lat": "lat", "centroid_lon": "lon"})[
        ["lat", "lon", "composite_score", "frequency"]
    ]
)
st.dataframe(filtered, use_container_width=True, hide_index=True)
