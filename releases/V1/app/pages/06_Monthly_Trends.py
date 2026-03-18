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

from ttc_pulse.dashboard.charts import line_chart
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
            AVG(composite_score) AS composite_score
        FROM {source}
        WHERE service_date IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY month_start
        """,
    )


st.title("Monthly Trends")
st.caption("Gold tables: `gold_route_time_metrics`, `gold_station_time_metrics`")

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
metric_options = ["frequency", "severity_p90", "regularity_p90", "composite_score"]

selected_modes = st.multiselect("Mode", options=mode_options, default=mode_options)
selected_entities = st.multiselect("Entity Type", options=entity_options, default=entity_options)
selected_metric = st.selectbox("Trend Metric", options=metric_options, index=0)

filtered = frame[
    frame["mode"].isin(selected_modes) & frame["entity_type"].isin(selected_entities)
].copy()
if filtered.empty:
    st.info("No rows remain after applying mode/entity filters.")
    st.stop()

chart = line_chart(
    frame=filtered,
    x="month_start:T",
    y=f"{selected_metric}:Q",
    color="series:N",
    title=f"Monthly {selected_metric} trend",
    tooltip=["month_start:T", "mode:N", "entity_type:N", f"{selected_metric}:Q"],
)
if chart is not None:
    st.altair_chart(chart, use_container_width=True)

table_frame = filtered.sort_values(["month_start", "mode", "entity_type"], ascending=[False, True, True])
st.dataframe(table_frame, use_container_width=True, hide_index=True)
