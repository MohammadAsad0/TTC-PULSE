from __future__ import annotations

from pathlib import Path
import sys

import altair as alt
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

from ttc_pulse.dashboard.formatting import sort_day_name
from ttc_pulse.dashboard.metric_config import (
    METRIC_OPTIONS,
    metric_axis_title,
    resolve_metric_choice,
)
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
            AVG(cause_mix_score) AS cause_mix_score,
            AVG(composite_score) AS composite_score
        FROM {source}
        GROUP BY 1, 2, 3
        ORDER BY mode, day_name, hour_bin
        """,
    )


st.title("Weekday Hour Heatmap")
st.caption("Gold table: `gold_time_reliability`")
selected_metric_label = st.selectbox("Metric to analyze", options=METRIC_OPTIONS, index=0, key="heatmap_metric")

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

filtered = frame[frame["mode"] == selected_mode].copy()
if filtered.empty:
    st.info("No rows available for the selected mode.")
    st.stop()

metric_resolution = resolve_metric_choice(filtered, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)

metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
chart_title = f"{selected_mode.title()} {metric_title} by Weekday and Hour"

chart = (
    alt.Chart(filtered)
    .mark_rect()
    .encode(
        x=alt.X("hour_bin:O", title="Hour of Day"),
        y=alt.Y("day_name:N", title="Day of Week"),
        color=alt.Color(f"{metric_column}:Q", title=metric_axis_title(metric_title), scale=alt.Scale(scheme="tealblues")),
        tooltip=["day_name:N", "hour_bin:O", f"{metric_column}:Q"],
    )
    .properties(title=chart_title, height=380)
)
st.altair_chart(chart, use_container_width=True)

st.dataframe(
    filtered.sort_values(["day_name", "hour_bin"]),
    use_container_width=True,
    hide_index=True,
)
