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

from ttc_pulse.dashboard.formatting import fmt_date, fmt_float, fmt_int, fmt_pct
from ttc_pulse.dashboard.metric_config import (
    METRIC_OPTIONS,
    metric_axis_title,
    metric_chart_title,
    resolve_metric_choice,
)
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_station_coverage_window():
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE station_canonical IS NOT NULL
            AND service_date IS NOT NULL
        """,
    )


@st.cache_data(ttl=120)
def _load_station_rankings(start_date: str, end_date: str):
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        WITH station_agg AS (
            SELECT
                station_canonical AS station_name,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE station_canonical IS NOT NULL
                AND service_date IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                station_name,
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
            FROM station_agg
        )
        SELECT
            station_name,
            RANK() OVER (ORDER BY composite_score DESC NULLS LAST, frequency DESC) AS rank_position,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM scored
        ORDER BY rank_position ASC, station_name
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_subway_linkage_context():
    return query_table(
        table_name="gold_linkage_quality",
        query_template="""
        SELECT
            period_start,
            SUM(row_count)::DOUBLE AS total_rows,
            SUM(
                CASE WHEN link_status = 'matched' THEN row_count ELSE 0 END
            )::DOUBLE AS matched_rows
        FROM {source}
        WHERE mode = 'subway'
        GROUP BY 1
        ORDER BY period_start DESC
        LIMIT 1
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


st.title("Subway Station Ranking")
st.caption("Sources: `gold_station_time_metrics`, `gold_linkage_quality`")
selected_metric_label = st.selectbox("Metric to analyze", options=METRIC_OPTIONS, index=0, key="subway_station_metric")

coverage_result = _load_station_coverage_window()
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty" or coverage_result.frame.empty:
    st.info("No station rows with `service_date` are available.")
    st.stop()

coverage_row = coverage_result.frame.iloc[0]
min_service_date = pd.to_datetime(coverage_row["min_service_date"], errors="coerce")
max_service_date = pd.to_datetime(coverage_row["max_service_date"], errors="coerce")
if pd.isna(min_service_date) or pd.isna(max_service_date):
    st.info("Service-date coverage is unavailable for station metrics.")
    st.stop()

min_date = min_service_date.date()
max_date = max_service_date.date()

date_selection = st.date_input(
    "Service date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key="subway_station_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(date_selection, min_date=min_date, max_date=max_date)

st.caption(
    "Data coverage (`service_date`): "
    f"{min_date:%Y-%m} to {max_date:%Y-%m} ({min_date:%Y-%m-%d} to {max_date:%Y-%m-%d}) | "
    f"Selected: {selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d}"
)

top_n = st.slider("Top N Stations", min_value=5, max_value=75, value=25, step=5)
min_frequency = st.slider("Minimum Frequency", min_value=0, max_value=500, value=0, step=5)

result = _load_station_rankings(selected_start_date.isoformat(), selected_end_date.isoformat())
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("No subway station rankings are available for the selected date range.")
    st.stop()

filtered = result.frame.copy()
filtered = filtered[filtered["frequency"] >= min_frequency]
if filtered.empty:
    st.info("No rows match the selected filters.")
    st.stop()

metric_resolution = resolve_metric_choice(filtered, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)

metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
composite_selected = metric_resolution.requested_label == "Composite Score" and metric_column == "composite_score"

if composite_selected:
    filtered = filtered.sort_values(["rank_position", "station_name"]).head(top_n)
    chart_title = (
        "Subway Stations by Composite Reliability Risk Score "
        f"({selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d})"
    )
else:
    filtered = filtered.sort_values([metric_column, "rank_position"], ascending=[False, True]).head(top_n)
    chart_title = (
        f"{metric_chart_title('Subway Stations', metric_title)} "
        f"({selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d})"
    )

linkage_result = _load_subway_linkage_context()
if linkage_result.status == "ok" and not linkage_result.frame.empty:
    linkage_row = linkage_result.frame.iloc[0]
    total_rows = float(linkage_row["total_rows"])
    matched_share = float(linkage_row["matched_rows"] / total_rows) if total_rows else 0.0
    st.caption(
        "Latest subway linkage matched share "
        f"({fmt_date(pd.to_datetime(linkage_row['period_start']).date())}): "
        f"{fmt_pct(matched_share)}"
    )

kpi_a, kpi_b, kpi_c = st.columns(3)
kpi_a.metric("Selected Window", f"{fmt_date(selected_start_date)} to {fmt_date(selected_end_date)}")
kpi_b.metric("Stations Displayed", fmt_int(len(filtered)))
metric_value = filtered[metric_column].max()
kpi_label = "Highest Composite Score" if composite_selected else f"Highest {metric_title}"
metric_formatter = fmt_int if metric_title == "Frequency" else fmt_float
kpi_c.metric(kpi_label, metric_formatter(metric_value, digits=3) if metric_formatter is fmt_float else metric_formatter(metric_value))

chart = (
    alt.Chart(filtered)
    .mark_bar()
    .encode(
        x=alt.X(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        y=alt.Y("station_name:N", sort="-x", title="Station"),
        tooltip=[
            "station_name:N",
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
        "station_name",
        "rank_position",
        "frequency",
        "severity_p90",
        "regularity_p90",
        "cause_mix_score",
        "composite_score",
    ]
].copy()
st.dataframe(display_frame, use_container_width=True, hide_index=True)
