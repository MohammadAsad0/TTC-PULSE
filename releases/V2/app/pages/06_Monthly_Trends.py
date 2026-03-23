from __future__ import annotations

from datetime import date
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


def _normalize_date_range(selection: object, min_date: date, max_date: date) -> tuple[date, date]:
    if isinstance(selection, tuple) and len(selection) == 2:
        start_date, end_date = selection
    else:
        start_date = end_date = selection if isinstance(selection, date) else min_date

    if start_date is None:
        start_date = min_date
    if end_date is None:
        end_date = max_date
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


def _series_label(mode: object, entity_type: object) -> str:
    mode_norm = str(mode).strip().lower()
    entity_norm = str(entity_type).strip().lower()
    mapping = {
        ("bus", "route"): "Bus Routes",
        ("subway", "route"): "Subway Routes",
        ("subway", "station"): "Subway Stations",
    }
    return mapping.get((mode_norm, entity_norm), f"{str(mode).title()} {str(entity_type).title()}")


st.title("Monthly Trends")
st.caption("Gold tables: `gold_route_time_metrics`, `gold_station_time_metrics`")
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

frame["month_start"] = pd.to_datetime(frame["month_start"], errors="coerce")
frame = frame.dropna(subset=["month_start"])
if frame.empty:
    st.info("Monthly rows are present but month values are invalid.")
    st.stop()

full_start_month = frame["month_start"].min().date()
full_end_month = frame["month_start"].max().date()

date_selection = st.date_input(
    "Month range",
    value=(full_start_month, full_end_month),
    min_value=full_start_month,
    max_value=full_end_month,
    key="monthly_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(
    date_selection,
    min_date=full_start_month,
    max_date=full_end_month,
)

selected_start_month = pd.Timestamp(selected_start_date).to_period("M").to_timestamp()
selected_end_month = pd.Timestamp(selected_end_date).to_period("M").to_timestamp()

st.caption(
    "Data coverage (`service_date`): "
    f"{full_start_month:%Y-%m} to {full_end_month:%Y-%m} | "
    f"Selected: {selected_start_month:%Y-%m} to {selected_end_month:%Y-%m}"
)

frame = frame[(frame["month_start"] >= selected_start_month) & (frame["month_start"] <= selected_end_month)].copy()
if frame.empty:
    st.info("No monthly rows remain after applying date range.")
    st.stop()

frame["series_label"] = frame.apply(lambda row: _series_label(row["mode"], row["entity_type"]), axis=1)

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

axis_format = ",.0f" if metric_column == "frequency" else ".3f"
chart_title = f"Monthly {metric_title} Trend ({selected_start_month:%Y-%m} to {selected_end_month:%Y-%m})"

chart = (
    alt.Chart(filtered)
    .mark_line(point=alt.OverlayMarkDef(filled=True, size=70), strokeWidth=2.5)
    .encode(
        x=alt.X("month_start:T", title="Month", axis=alt.Axis(format="%Y-%m", labelAngle=-30)),
        y=alt.Y(f"{metric_column}:Q", title=metric_axis_title(metric_title), axis=alt.Axis(format=axis_format)),
        color=alt.Color("series_label:N", title="Series"),
        strokeDash=alt.StrokeDash("entity_type:N", title="Entity Type"),
        tooltip=[
            alt.Tooltip("month_start:T", title="Month"),
            alt.Tooltip("series_label:N", title="Series"),
            alt.Tooltip("frequency:Q", title="Frequency", format=",.0f"),
            alt.Tooltip("severity_p90:Q", title="Severity (P90 Delay)", format=".3f"),
            alt.Tooltip("regularity_p90:Q", title="Regularity (P90 Gap)", format=".3f"),
            alt.Tooltip("cause_mix_score:Q", title="Cause Mix", format=".3f"),
            alt.Tooltip("composite_score:Q", title="Composite Score", format=".3f"),
        ],
    )
    .properties(title=chart_title, height=380)
)
st.altair_chart(chart, use_container_width=True)

# Keep component context visible regardless of selected metric.
table_frame = filtered.sort_values(["month_start", "series_label"], ascending=[False, True]).copy()
table_frame["Month"] = table_frame["month_start"].dt.strftime("%Y-%m")
table_frame = table_frame.rename(
    columns={
        "series_label": "Series",
        "frequency": "Frequency",
        "severity_p90": "Severity (P90 Delay)",
        "regularity_p90": "Regularity (P90 Gap)",
        "cause_mix_score": "Cause Mix",
        "composite_score": "Composite Score",
    }
)

st.dataframe(
    table_frame[
        [
            "Month",
            "Series",
            "Frequency",
            "Severity (P90 Delay)",
            "Regularity (P90 Gap)",
            "Cause Mix",
            "Composite Score",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)
