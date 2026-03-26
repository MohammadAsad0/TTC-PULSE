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
from ttc_pulse.dashboard.loaders import query_table
from ttc_pulse.dashboard.metric_config import METRIC_OPTIONS, metric_axis_title, resolve_metric_choice
from ttc_pulse.dashboard.storytelling import is_presentation_mode, next_question_hint, page_story_header, story_mode_selector


@st.cache_data(ttl=120)
def _load_heatmap_coverage():
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


@st.cache_data(ttl=120)
def _load_heatmap_metrics(start_date: str, end_date: str):
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
def _load_route_monthly(start_date: str, end_date: str):
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
        WHERE mode = 'bus'
            AND service_date IS NOT NULL
            AND service_date BETWEEN ? AND ?
        GROUP BY 1, 2, 3
        ORDER BY month_start, mode
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_station_monthly(start_date: str, end_date: str):
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
            AND service_date BETWEEN ? AND ?
        GROUP BY 1, 2, 3
        ORDER BY month_start
        """,
        params=[start_date, end_date],
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


st.title("Time Patterns")
st.caption("Understand how disruptions repeat themselves and the way their patterns shift and change over time.")

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

page_story_header(
    audience_question="When do disruptions recur most often?",
    takeaway="Disruptions are not uniform, they cluster in specific weekday-hour windows and monthly periods.",
)

coverage_result = _load_heatmap_coverage()
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty":
    st.info("No service-date coverage is available for time-pattern analysis.")
    st.stop()

coverage = coverage_result.frame.copy()
coverage["min_service_date"] = pd.to_datetime(coverage["min_service_date"], errors="coerce")
coverage["max_service_date"] = pd.to_datetime(coverage["max_service_date"], errors="coerce")
coverage = coverage.dropna(subset=["min_service_date", "max_service_date"])
if coverage.empty:
    st.info("Coverage dates are unavailable.")
    st.stop()

mode_options = sorted(coverage["mode"].dropna().unique().tolist())
if not mode_options:
    st.info("No mode coverage is available.")
    st.stop()
selected_mode = st.selectbox("Mode", options=mode_options, index=0)
selected_coverage = coverage[coverage["mode"] == selected_mode]
if selected_coverage.empty:
    st.info("No coverage rows for selected mode.")
    st.stop()

min_date = selected_coverage["min_service_date"].iloc[0].date()
max_date = selected_coverage["max_service_date"].iloc[0].date()

selected_metric_label = st.selectbox("Heatmap Metric", options=METRIC_OPTIONS, index=0)

date_selection = st.date_input(
    "Service date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key="time_patterns_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(date_selection, min_date=min_date, max_date=max_date)
selected_start_iso = selected_start_date.isoformat()
selected_end_iso = selected_end_date.isoformat()

heatmap_result = _load_heatmap_metrics(selected_start_iso, selected_end_iso)
if heatmap_result.status in {"missing", "error"}:
    st.error(heatmap_result.message)
    st.stop()
if heatmap_result.status == "empty":
    st.info("No heatmap rows are available for selected window.")
    st.stop()

heatmap = heatmap_result.frame.copy()
heatmap = sort_day_name(heatmap, column="day_name")
heatmap["hour_bin"] = pd.to_numeric(heatmap["hour_bin"], errors="coerce").fillna(0).astype(int)
heatmap = heatmap[heatmap["mode"] == selected_mode].copy()
if heatmap.empty:
    st.info("No heatmap rows for selected mode/window.")
    st.stop()

metric_resolution = resolve_metric_choice(heatmap, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)
metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label

heatmap_chart = (
    alt.Chart(heatmap)
    .mark_rect()
    .encode(
        x=alt.X("hour_bin:O", title="Hour of Day"),
        y=alt.Y("day_name:N", title="Weekday"),
        color=alt.Color(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        tooltip=["day_name:N", "hour_bin:Q", f"{metric_column}:Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"],
    )
    .properties(
        title=f"{selected_mode.title()} {metric_title} by Weekday and Hour",
        height=320,
    )
)
st.altair_chart(heatmap_chart, use_container_width=True)

route_monthly_result = _load_route_monthly(selected_start_iso, selected_end_iso)
station_monthly_result = _load_station_monthly(selected_start_iso, selected_end_iso)
if route_monthly_result.status in {"missing", "error"} and station_monthly_result.status in {"missing", "error"}:
    st.info("Monthly support trend is unavailable.")
    next_question_hint("Why do these disruptions happen? Open: Cause Signatures.")
    st.stop()

frames: list[pd.DataFrame] = []
if route_monthly_result.status in {"ok", "empty"}:
    frames.append(route_monthly_result.frame.copy())
if station_monthly_result.status in {"ok", "empty"}:
    frames.append(station_monthly_result.frame.copy())
if not frames:
    st.info("Monthly support trend is unavailable.")
    next_question_hint("Why do these disruptions happen? Open: Cause Signatures.")
    st.stop()

monthly = pd.concat(frames, ignore_index=True)
monthly["month_start"] = pd.to_datetime(monthly["month_start"], errors="coerce")
monthly = monthly.dropna(subset=["month_start"])
monthly = monthly[monthly["mode"] == selected_mode].copy()
if monthly.empty:
    st.info("No monthly support trend rows for selected mode/window.")
    next_question_hint("Why do these disruptions happen? Open: Cause Signatures.")
    st.stop()

monthly["series_label"] = monthly.apply(lambda row: _series_label(row["mode"], row["entity_type"]), axis=1)
trend_metric = "frequency" if presentation else metric_column
axis_title = metric_axis_title("Frequency") if trend_metric == "frequency" else metric_axis_title(metric_title)
monthly_chart = (
    alt.Chart(monthly)
    .mark_line(point=alt.OverlayMarkDef(filled=True, size=65), strokeWidth=2.2)
    .encode(
        x=alt.X("month_start:T", title="Month", axis=alt.Axis(format="%Y-%m", labelAngle=-30)),
        y=alt.Y(f"{trend_metric}:Q", title=axis_title),
        color=alt.Color("series_label:N", title="Series"),
        tooltip=["month_start:T", "series_label:N", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"],
    )
    .properties(title="Monthly Supporting Trend", height=300)
    .interactive()
)
st.altair_chart(monthly_chart, use_container_width=True)

if not presentation:
    monthly_table = monthly.sort_values(["month_start", "series_label"], ascending=[False, True]).copy()
    st.dataframe(monthly_table, use_container_width=True, hide_index=True)

next_question_hint("What causes dominate these hotspots? Open: Cause Signatures.")
