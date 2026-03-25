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

from ttc_pulse.dashboard.formatting import fmt_date, fmt_float, fmt_int
from ttc_pulse.dashboard.metric_config import (
    METRIC_OPTIONS,
    metric_axis_title,
    metric_chart_title,
    resolve_metric_choice,
)
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_bus_coverage_window():
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE mode = 'bus'
            AND route_id_gtfs IS NOT NULL
            AND service_date IS NOT NULL
        """,
    )


@st.cache_data(ttl=120)
def _load_bus_route_rankings(start_date: str, end_date: str):
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        WITH route_agg AS (
            SELECT
                route_id_gtfs AS route_id,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE mode = 'bus'
                AND route_id_gtfs IS NOT NULL
                AND service_date IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                route_id,
                frequency,
                severity_p90,
                regularity_p90,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (
                    0.35 * COALESCE((frequency - AVG(frequency) OVER()) / NULLIF(STDDEV_SAMP(frequency) OVER(), 0.0), 0.0)
                    + 0.30 * COALESCE((severity_p90 - AVG(severity_p90) OVER()) / NULLIF(STDDEV_SAMP(severity_p90) OVER(), 0.0), 0.0)
                    + 0.20 * COALESCE((regularity_p90 - AVG(regularity_p90) OVER()) / NULLIF(STDDEV_SAMP(regularity_p90) OVER(), 0.0), 0.0)
                    + 0.15 * COALESCE(cause_mix_score, 0.0)
                ) AS composite_score
            FROM route_agg
        )
        SELECT
            route_id,
            RANK() OVER (ORDER BY composite_score DESC NULLS LAST, frequency DESC) AS rank_position,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM scored
        ORDER BY rank_position ASC, route_id
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


st.title("Bus Route Ranking")
st.caption("Source: `gold_route_time_metrics` (date-windowed recomputed ranking)")
selected_metric_label = st.selectbox("Metric to analyze", options=METRIC_OPTIONS, index=0, key="bus_route_metric")

coverage_result = _load_bus_coverage_window()
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty" or coverage_result.frame.empty:
    st.info("No bus route rows with `service_date` are available.")
    st.stop()

coverage_row = coverage_result.frame.iloc[0]
min_service_date = pd.to_datetime(coverage_row["min_service_date"], errors="coerce")
max_service_date = pd.to_datetime(coverage_row["max_service_date"], errors="coerce")
if pd.isna(min_service_date) or pd.isna(max_service_date):
    st.info("Service-date coverage is unavailable for bus route metrics.")
    st.stop()

min_date = min_service_date.date()
max_date = max_service_date.date()

date_selection = st.date_input(
    "Service date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key="bus_route_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(date_selection, min_date=min_date, max_date=max_date)

st.caption(
    "Data coverage (`service_date`): "
    f"{min_date:%Y-%m} to {max_date:%Y-%m} ({min_date:%Y-%m-%d} to {max_date:%Y-%m-%d}) | "
    f"Selected: {selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d}"
)

top_n = st.slider("Top N Routes", min_value=5, max_value=100, value=25, step=5)

result = _load_bus_route_rankings(selected_start_date.isoformat(), selected_end_date.isoformat())
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("No bus route rankings are available for the selected date range.")
    st.stop()

filtered = result.frame.copy()
metric_resolution = resolve_metric_choice(filtered, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)

metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
composite_selected = metric_resolution.requested_label == "Composite Score" and metric_column == "composite_score"

if composite_selected:
    filtered = filtered.sort_values(["rank_position", "route_id"]).head(top_n)
    chart_title = (
        "Bus Routes by Composite Reliability Risk Score "
        f"({selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d})"
    )
else:
    filtered = filtered.sort_values([metric_column, "rank_position"], ascending=[False, True]).head(top_n)
    chart_title = (
        f"{metric_chart_title('Bus Routes', metric_title)} "
        f"({selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d})"
    )

kpi_a, kpi_b, kpi_c = st.columns(3)
kpi_a.metric("Selected Window", f"{fmt_date(selected_start_date)} to {fmt_date(selected_end_date)}")
kpi_b.metric("Routes Displayed", fmt_int(len(filtered)))
metric_value = filtered[metric_column].max()
kpi_label = "Top Composite Score" if composite_selected else f"Top {metric_title}"
metric_formatter = fmt_int if metric_title == "Frequency" else fmt_float
kpi_c.metric(kpi_label, metric_formatter(metric_value, digits=3) if metric_formatter is fmt_float else metric_formatter(metric_value))

chart = (
    alt.Chart(filtered)
    .mark_bar()
    .encode(
        x=alt.X(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        y=alt.Y("route_id:N", sort="-x", title="Route"),
        tooltip=[
            "route_id:N",
            "rank_position:Q",
            f"{metric_column}:Q",
            "frequency:Q",
            "severity_p90:Q",
            "regularity_p90:Q",
            "cause_mix_score:Q",
            "composite_score:Q",
        ],
    )
    .properties(title=chart_title, height=360)
)
st.altair_chart(chart, use_container_width=True)

display_frame = filtered[
    [
        "route_id",
        "rank_position",
        "frequency",
        "severity_p90",
        "regularity_p90",
        "cause_mix_score",
        "composite_score",
    ]
].copy()
st.dataframe(display_frame, use_container_width=True, hide_index=True)
