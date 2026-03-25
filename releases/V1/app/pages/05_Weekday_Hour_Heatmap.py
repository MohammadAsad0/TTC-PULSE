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

from ttc_pulse.dashboard.charts import heatmap_chart
from ttc_pulse.dashboard.formatting import sort_day_name
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_time_heatmap_frame():
    return query_table(
        table_name="gold_time_reliability",
        query_template="""
        SELECT
            mode,
            day_name,
            hour_bin,
            SUM(frequency)::DOUBLE AS frequency,
            AVG(severity_p90) AS severity_p90,
            AVG(regularity_p90) AS regularity_p90,
            AVG(composite_score) AS composite_score
        FROM {source}
        GROUP BY 1, 2, 3
        ORDER BY mode, day_name, hour_bin
        """,
    )


st.title("Weekday Hour Heatmap")
st.caption("Gold table: `gold_time_reliability`")

result = _load_time_heatmap_frame()
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("gold_time_reliability is available but empty.")
    st.stop()

frame = result.frame.copy()
frame = sort_day_name(frame, column="day_name")
frame["hour_bin"] = pd.to_numeric(frame["hour_bin"], errors="coerce").fillna(0).astype(int)

mode_options = sorted(frame["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)
metric_map = {
    "Frequency": "frequency",
    "Severity P90": "severity_p90",
    "Regularity P90": "regularity_p90",
    "Composite Score": "composite_score",
}
metric_label = st.selectbox("Heat Metric", options=list(metric_map.keys()), index=0)
metric_column = metric_map[metric_label]

filtered = frame[frame["mode"] == selected_mode].copy()
if filtered.empty:
    st.info("No rows available for the selected mode.")
    st.stop()

chart = heatmap_chart(
    frame=filtered,
    x="hour_bin:O",
    y="day_name:N",
    color=f"{metric_column}:Q",
    title=f"{selected_mode.title()} {metric_label} by Weekday and Hour",
    tooltip=["day_name:N", "hour_bin:O", f"{metric_column}:Q"],
)
if chart is not None:
    st.altair_chart(chart, use_container_width=True)

st.dataframe(
    filtered.sort_values(["day_name", "hour_bin"]),
    use_container_width=True,
    hide_index=True,
)
