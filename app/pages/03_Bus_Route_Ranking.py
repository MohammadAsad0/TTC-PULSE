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
def _load_bus_date_bounds():
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE LOWER(mode) = 'bus'
            AND route_id_gtfs IS NOT NULL
            AND service_date IS NOT NULL
        """,
    )


@st.cache_data(ttl=120)
def _load_bus_route_rankings(start_date: date, end_date: date):
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
            WHERE LOWER(mode) = 'bus'
                AND route_id_gtfs IS NOT NULL
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
                (frequency - AVG(frequency) OVER()) / NULLIF(STDDEV_POP(frequency) OVER(), 0.0) AS z_freq,
                (severity_p90 - AVG(severity_p90) OVER()) / NULLIF(STDDEV_POP(severity_p90) OVER(), 0.0) AS z_sev,
                (regularity_p90 - AVG(regularity_p90) OVER()) / NULLIF(STDDEV_POP(regularity_p90) OVER(), 0.0) AS z_reg,
                (COALESCE(cause_mix_score, 0.0) - AVG(COALESCE(cause_mix_score, 0.0)) OVER())
                    / NULLIF(STDDEV_POP(COALESCE(cause_mix_score, 0.0)) OVER(), 0.0) AS z_cause
            FROM route_agg
        )
        SELECT
            route_id,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            (
                0.35 * COALESCE(z_freq, 0.0) +
                0.30 * COALESCE(z_sev, 0.0) +
                0.20 * COALESCE(z_reg, 0.0) +
                0.15 * COALESCE(z_cause, 0.0)
            ) AS composite_score,
            RANK() OVER (
                ORDER BY
                    (
                        0.35 * COALESCE(z_freq, 0.0) +
                        0.30 * COALESCE(z_sev, 0.0) +
                        0.20 * COALESCE(z_reg, 0.0) +
                        0.15 * COALESCE(z_cause, 0.0)
                    ) DESC,
                    frequency DESC
            ) AS rank_position
        FROM scored
        ORDER BY rank_position ASC, route_id
        """,
        params=[start_date, end_date],
    )


st.title("Bus Route Ranking")
selected_metric_label = st.selectbox("Metric to analyze", options=METRIC_OPTIONS, index=0, key="bus_route_metric")

bounds_result = _load_bus_date_bounds()
if bounds_result.status in {"missing", "error"}:
    st.error(bounds_result.message)
    st.stop()
if bounds_result.status == "empty" or bounds_result.frame.empty:
    st.info("No bus route date bounds are available.")
    st.stop()

bounds_row = bounds_result.frame.iloc[0]
min_service_date = pd.to_datetime(bounds_row["min_service_date"]).date()
max_service_date = pd.to_datetime(bounds_row["max_service_date"]).date()

ctrl_a, ctrl_b, ctrl_c = st.columns([2, 2, 1])
start_date = ctrl_a.date_input("Start date", value=min_service_date, min_value=min_service_date, max_value=max_service_date)
end_date = ctrl_b.date_input("End date", value=max_service_date, min_value=min_service_date, max_value=max_service_date)
top_n = int(ctrl_c.slider("Top N Routes", min_value=5, max_value=100, value=25, step=5))

if start_date > end_date:
    st.error("Start date must be on or before end date.")
    st.stop()

result = _load_bus_route_rankings(start_date=start_date, end_date=end_date)
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("No bus route rows were found for the selected date range.")
    st.stop()

filtered = result.frame.copy()
metric_resolution = resolve_metric_choice(filtered, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)

metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
composite_selected = metric_resolution.requested_label == "Composite Score" and metric_column == "composite_score"

if composite_selected:
    filtered = filtered.sort_values("rank_position").head(top_n).reset_index(drop=True)
    chart_title = "Bus Routes by Composite Reliability Risk Score"
else:
    filtered = filtered.sort_values([metric_column, "rank_position"], ascending=[False, True]).head(top_n).reset_index(drop=True)
    filtered["rank_position"] = range(1, len(filtered) + 1)
    chart_title = metric_chart_title("Bus Routes", metric_title)

kpi_a, kpi_b, kpi_c = st.columns(3)
kpi_a.metric("Date Range", f"{fmt_date(start_date)} to {fmt_date(end_date)}")
kpi_b.metric("Routes Displayed", fmt_int(len(filtered)))
kpi_c.metric(
    "Top Composite Score" if composite_selected else f"Top {metric_title}",
    fmt_int(filtered[metric_column].max()) if metric_title == "Frequency" else fmt_float(filtered[metric_column].max(), digits=3),
)

chart_height = max(360, min(2200, 26 * max(len(filtered), 1)))
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
    .properties(title=chart_title, height=chart_height)
)

with st.container(height=640):
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
display_frame = display_frame.sort_values("rank_position")
st.dataframe(display_frame, use_container_width=True, hide_index=True)
