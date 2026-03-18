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

from ttc_pulse.dashboard.metric_config import (
    METRIC_OPTIONS,
    metric_axis_title,
    resolve_metric_choice,
)
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_route_monthly():
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        SELECT
            mode,
            'route' AS entity_type,
            CAST(DATE_TRUNC('month', service_date) AS DATE) AS month_start,
            SUM(frequency)::DOUBLE AS frequency,
            AVG(severity_p90) AS severity_p90,
            AVG(regularity_p90) AS regularity_p90,
            AVG(cause_mix_score) AS cause_mix_score,
            AVG(composite_score) AS composite_score
        FROM {source}
        WHERE service_date IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY month_start, mode
        """,
    )


@st.cache_data(ttl=120)
def _load_station_monthly():
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        SELECT
            'subway' AS mode,
            'station' AS entity_type,
            CAST(DATE_TRUNC('month', service_date) AS DATE) AS month_start,
            SUM(frequency)::DOUBLE AS frequency,
            AVG(severity_p90) AS severity_p90,
            AVG(regularity_p90) AS regularity_p90,
            AVG(cause_mix_score) AS cause_mix_score,
            AVG(composite_score) AS composite_score
        FROM {source}
        WHERE service_date IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY month_start
        """,
    )


st.title("Monthly Trends")
selected_metric_label = st.selectbox("Metric to analyze", options=METRIC_OPTIONS, index=0, key="monthly_metric")

route_result = _load_route_monthly()
station_result = _load_station_monthly()

if route_result.status in {"missing", "error"} and station_result.status in {"missing", "error"}:
    st.error("Both source marts are missing or failed to load.")
    st.stop()

frames: list[pd.DataFrame] = []
if route_result.status in {"ok", "empty"}:
    frames.append(route_result.frame.copy())
if station_result.status in {"ok", "empty"}:
    frames.append(station_result.frame.copy())

if not frames:
    st.info("No monthly trend sources are available.")
    st.stop()

frame = pd.concat(frames, ignore_index=True)
if frame.empty:
    st.info("Monthly trends tables are available but contain zero rows.")
    st.stop()

frame["month_start"] = pd.to_datetime(frame["month_start"])
frame["series"] = frame["mode"].astype(str) + " - " + frame["entity_type"].astype(str)

mode_options = sorted(frame["mode"].dropna().unique().tolist())
entity_options = sorted(frame["entity_type"].dropna().unique().tolist())

selected_modes = st.multiselect("Mode", options=mode_options, default=mode_options)
selected_entities = st.multiselect("Entity Type", options=entity_options, default=entity_options)

filtered = frame[
    frame["mode"].isin(selected_modes) & frame["entity_type"].isin(selected_entities)
].copy()
if filtered.empty:
    st.info("No rows remain after applying mode/entity filters.")
    st.stop()

metric_resolution = resolve_metric_choice(filtered, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)

metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
composite_selected = metric_resolution.requested_label == "Composite Score" and metric_column == "composite_score"

chart_title = f"Monthly {metric_column} trend" if composite_selected else f"Monthly {metric_title} trend"

chart = (
    alt.Chart(filtered)
    .mark_line(point=True)
    .encode(
        x=alt.X("month_start:T", title="Month"),
        y=alt.Y(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        color=alt.Color("series:N", title="Series"),
        tooltip=["month_start:T", "mode:N", "entity_type:N", f"{metric_column}:Q"],
    )
    .properties(title=chart_title, height=360)
)
st.altair_chart(chart, use_container_width=True)

table_frame = filtered.sort_values(["month_start", "mode", "entity_type"], ascending=[False, True, True])
st.dataframe(table_frame, use_container_width=True, hide_index=True)
