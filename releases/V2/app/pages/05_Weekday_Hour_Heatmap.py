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

from ttc_pulse.dashboard.formatting import sort_day_name
from ttc_pulse.dashboard.metric_config import (
    METRIC_OPTIONS,
    metric_axis_title,
    resolve_metric_choice,
)
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_time_heatmap_frame(start_date: str, end_date: str):
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        WITH filtered AS (
            SELECT
                mode,
                service_date,
                hour_bin,
                incident_category,
                SUM(event_count)::DOUBLE AS category_event_count,
                SUM(event_count * min_delay_p90)
                    / NULLIF(SUM(CASE WHEN min_delay_p90 IS NOT NULL THEN event_count ELSE 0 END), 0) AS severity_p90,
                SUM(event_count * min_gap_p90)
                    / NULLIF(SUM(CASE WHEN min_gap_p90 IS NOT NULL THEN event_count ELSE 0 END), 0) AS regularity_p90
            FROM {source}
            WHERE service_date IS NOT NULL
                AND hour_bin IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1, 2, 3, 4
        ),
        category_counts AS (
            SELECT
                mode,
                STRFTIME(service_date, '%A') AS day_name,
                hour_bin,
                incident_category,
                SUM(category_event_count)::DOUBLE AS category_count,
                SUM(category_event_count * severity_p90)
                    / NULLIF(SUM(CASE WHEN severity_p90 IS NOT NULL THEN category_event_count ELSE 0 END), 0) AS severity_p90,
                SUM(category_event_count * regularity_p90)
                    / NULLIF(SUM(CASE WHEN regularity_p90 IS NOT NULL THEN category_event_count ELSE 0 END), 0) AS regularity_p90
            FROM filtered
            GROUP BY 1, 2, 3, 4
        ),
        metrics AS (
            SELECT
                mode,
                day_name,
                hour_bin,
                SUM(category_count)::DOUBLE AS frequency,
                SUM(category_count * severity_p90)
                    / NULLIF(SUM(CASE WHEN severity_p90 IS NOT NULL THEN category_count ELSE 0 END), 0) AS severity_p90,
                SUM(category_count * regularity_p90)
                    / NULLIF(SUM(CASE WHEN regularity_p90 IS NOT NULL THEN category_count ELSE 0 END), 0) AS regularity_p90
            FROM category_counts
            GROUP BY 1, 2, 3
        ),
        cause_mix AS (
            SELECT
                mode,
                day_name,
                hour_bin,
                1.0 - SUM(POWER(category_count / NULLIF(total_count, 0.0), 2)) AS cause_mix_score
            FROM (
                SELECT
                    mode,
                    day_name,
                    hour_bin,
                    category_count,
                    SUM(category_count) OVER (PARTITION BY mode, day_name, hour_bin) AS total_count
                FROM category_counts
            ) AS x
            GROUP BY 1, 2, 3
        ),
        scored AS (
            SELECT
                m.mode,
                m.day_name,
                m.hour_bin,
                m.frequency,
                m.severity_p90,
                m.regularity_p90,
                COALESCE(c.cause_mix_score, 0.0) AS cause_mix_score
            FROM metrics AS m
            LEFT JOIN cause_mix AS c
                ON m.mode = c.mode
                AND m.day_name = c.day_name
                AND m.hour_bin = c.hour_bin
        )
        SELECT
            mode,
            day_name,
            hour_bin,
            frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            (0.35 * COALESCE(
                (frequency - AVG(frequency) OVER (PARTITION BY mode))
                    / NULLIF(STDDEV_SAMP(frequency) OVER (PARTITION BY mode), 0),
                0.0
            )) +
            (0.30 * COALESCE(
                (severity_p90 - AVG(severity_p90) OVER (PARTITION BY mode))
                    / NULLIF(STDDEV_SAMP(severity_p90) OVER (PARTITION BY mode), 0),
                0.0
            )) +
            (0.20 * COALESCE(
                (regularity_p90 - AVG(regularity_p90) OVER (PARTITION BY mode))
                    / NULLIF(STDDEV_SAMP(regularity_p90) OVER (PARTITION BY mode), 0),
                0.0
            )) +
            (0.15 * COALESCE(cause_mix_score, 0.0)) AS composite_score
        FROM scored
        ORDER BY mode, day_name, hour_bin
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_time_heatmap_coverage():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE service_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1
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


st.title("Weekday Hour Heatmap")
st.caption("Source: `gold_delay_events_core` (date-windowed aggregation)")
selected_metric_label = st.selectbox("Metric to analyze", options=METRIC_OPTIONS, index=0, key="heatmap_metric")

coverage_result = _load_time_heatmap_coverage()
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty":
    st.info("gold_delay_events_core is available but has no `service_date` rows.")
    st.stop()

coverage = coverage_result.frame.copy()
coverage["min_service_date"] = pd.to_datetime(coverage["min_service_date"], errors="coerce")
coverage["max_service_date"] = pd.to_datetime(coverage["max_service_date"], errors="coerce")
coverage = coverage.dropna(subset=["min_service_date", "max_service_date"])
if coverage.empty:
    st.info("No valid service-date coverage found in `gold_delay_events_core`.")
    st.stop()

mode_options = sorted(coverage["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)

selected_coverage = coverage[coverage["mode"] == selected_mode]
if selected_coverage.empty:
    st.info("No coverage rows found for selected mode.")
    st.stop()

min_service_date = selected_coverage["min_service_date"].iloc[0].date()
max_service_date = selected_coverage["max_service_date"].iloc[0].date()

date_selection = st.date_input(
    "Service date range",
    value=(min_service_date, max_service_date),
    min_value=min_service_date,
    max_value=max_service_date,
    key="heatmap_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(
    date_selection,
    min_date=min_service_date,
    max_date=max_service_date,
)

st.caption(
    "Data coverage "
    f"(`service_date`, {selected_mode}): {min_service_date:%Y-%m} to {max_service_date:%Y-%m} "
    f"({min_service_date:%Y-%m-%d} to {max_service_date:%Y-%m-%d}) | "
    f"Selected: {selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d}"
)

result = _load_time_heatmap_frame(selected_start_date.isoformat(), selected_end_date.isoformat())
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("No rows available for the selected date range.")
    st.stop()

frame = result.frame.copy()
frame = sort_day_name(frame, column="day_name")
frame["hour_bin"] = pd.to_numeric(frame["hour_bin"], errors="coerce").fillna(0).astype(int)

filtered = frame[frame["mode"] == selected_mode].copy()
if filtered.empty:
    st.info("No rows available for the selected mode and date range.")
    st.stop()

metric_resolution = resolve_metric_choice(filtered, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)

metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
chart_title = (
    f"{selected_mode.title()} {metric_title} by Weekday and Hour "
    f"({selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d})"
)

chart = (
    alt.Chart(filtered)
    .mark_rect()
    .encode(
        x=alt.X("hour_bin:O", title="Hour of Day"),
        y=alt.Y("day_name:N", title="Day of Week"),
        color=alt.Color(f"{metric_column}:Q", title=metric_axis_title(metric_title), scale=alt.Scale(scheme="tealblues")),
        tooltip=[
            alt.Tooltip("day_name:N", title="Day"),
            alt.Tooltip("hour_bin:O", title="Hour"),
            alt.Tooltip("frequency:Q", title="Frequency", format=",.0f"),
            alt.Tooltip("severity_p90:Q", title="Severity (P90 Delay)", format=".2f"),
            alt.Tooltip("regularity_p90:Q", title="Regularity (P90 Gap)", format=".2f"),
            alt.Tooltip("cause_mix_score:Q", title="Cause Mix", format=".3f"),
            alt.Tooltip("composite_score:Q", title="Composite Score", format=".3f"),
        ],
    )
    .properties(title=chart_title, height=380)
)
st.altair_chart(chart, use_container_width=True)

st.dataframe(
    filtered.sort_values(["day_name", "hour_bin"]),
    use_container_width=True,
    hide_index=True,
)
