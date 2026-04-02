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

from ttc_pulse.dashboard.formatting import fmt_float, fmt_int
from ttc_pulse.dashboard.ai_explain import render_ai_explain_block
from ttc_pulse.dashboard.loaders import query_table
from ttc_pulse.dashboard.storytelling import is_presentation_mode, next_question_hint, page_story_header, story_mode_selector

ALL_MODES = ("bus", "streetcar", "subway")
METRIC_DISPLAY_NAMES = {
    "frequency": "Frequency",
    "severity_p90": "Severity P90",
    "regularity_p90": "Regularity P90",
    "cause_mix_score": "Cause Mix Score",
}
MODE_METRIC_COLORS = {
    ("bus", "frequency"): "#1f77b4",
    ("bus", "severity_p90"): "#4e79a7",
    ("bus", "regularity_p90"): "#76a5d8",
    ("bus", "cause_mix_score"): "#9ec1e6",
    ("streetcar", "frequency"): "#d64f4f",
    ("streetcar", "severity_p90"): "#e07a5f",
    ("streetcar", "regularity_p90"): "#f1a46f",
    ("streetcar", "cause_mix_score"): "#f5c28b",
    ("subway", "frequency"): "#2ca02c",
    ("subway", "severity_p90"): "#4cb35f",
    ("subway", "regularity_p90"): "#7ccf84",
    ("subway", "cause_mix_score"): "#a8e0aa",
}


@st.cache_data(ttl=120)
def _load_core_coverage():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE service_date IS NOT NULL
        """,
    )


@st.cache_data(ttl=120)
def _load_monthly_reliability(start_date: str, end_date: str):
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        WITH filtered AS (
            SELECT
                mode,
                service_date,
                incident_category,
                SUM(event_count)::DOUBLE AS category_event_count,
                SUM(event_count * min_delay_p90)
                    / NULLIF(SUM(CASE WHEN min_delay_p90 IS NOT NULL THEN event_count ELSE 0 END), 0) AS severity_p90,
                SUM(event_count * min_gap_p90)
                    / NULLIF(SUM(CASE WHEN min_gap_p90 IS NOT NULL THEN event_count ELSE 0 END), 0) AS regularity_p90
            FROM {source}
            WHERE service_date IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1, 2, 3
        ),
        month_category AS (
            SELECT
                mode,
                CAST(DATE_TRUNC('month', service_date) AS DATE) AS month_start,
                incident_category,
                SUM(category_event_count)::DOUBLE AS category_count,
                SUM(category_event_count * severity_p90)
                    / NULLIF(SUM(CASE WHEN severity_p90 IS NOT NULL THEN category_event_count ELSE 0 END), 0) AS severity_p90,
                SUM(category_event_count * regularity_p90)
                    / NULLIF(SUM(CASE WHEN regularity_p90 IS NOT NULL THEN category_event_count ELSE 0 END), 0) AS regularity_p90
            FROM filtered
            GROUP BY 1, 2, 3
        ),
        monthly AS (
            SELECT
                mode,
                month_start,
                SUM(category_count)::BIGINT AS frequency,
                SUM(category_count * severity_p90)
                    / NULLIF(SUM(CASE WHEN severity_p90 IS NOT NULL THEN category_count ELSE 0 END), 0) AS severity_p90,
                SUM(category_count * regularity_p90)
                    / NULLIF(SUM(CASE WHEN regularity_p90 IS NOT NULL THEN category_count ELSE 0 END), 0) AS regularity_p90
            FROM month_category
            GROUP BY 1, 2
        ),
        cause_mix AS (
            SELECT
                mode,
                month_start,
                1.0 - SUM(POWER(category_count / NULLIF(total_count, 0.0), 2)) AS cause_mix_score
            FROM (
                SELECT
                    mode,
                    month_start,
                    category_count,
                    SUM(category_count) OVER (PARTITION BY mode, month_start) AS total_count
                FROM month_category
            ) AS x
            GROUP BY 1, 2
        )
        SELECT
            m.mode,
            m.month_start,
            m.frequency,
            m.severity_p90,
            m.regularity_p90,
            COALESCE(c.cause_mix_score, 0.0) AS cause_mix_score
        FROM monthly AS m
        LEFT JOIN cause_mix AS c
            ON m.mode = c.mode
            AND m.month_start = c.month_start
        ORDER BY m.month_start, m.mode
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_mode_hotspot_snapshot(mode: str, start_date: str, end_date: str):
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        SELECT
            mode,
            route_id_gtfs AS entity_id,
            SUM(frequency)::DOUBLE AS frequency,
            quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
            quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
            AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score,
            AVG(composite_score) FILTER (WHERE composite_score IS NOT NULL) AS composite_score
        FROM {source}
        WHERE mode = ?
            AND route_id_gtfs IS NOT NULL
            AND service_date BETWEEN ? AND ?
        GROUP BY 1, 2
        ORDER BY composite_score DESC NULLS LAST, frequency DESC
        LIMIT 8
        """,
        params=[mode, start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_subway_hotspot_snapshot(start_date: str, end_date: str):
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        SELECT
            'subway' AS mode,
            station_canonical AS entity_id,
            SUM(frequency)::DOUBLE AS frequency,
            quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
            quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
            AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score,
            AVG(composite_score) FILTER (WHERE composite_score IS NOT NULL) AS composite_score
        FROM {source}
        WHERE station_canonical IS NOT NULL
            AND service_date BETWEEN ? AND ?
        GROUP BY 1, 2
        ORDER BY composite_score DESC NULLS LAST, frequency DESC
        LIMIT 8
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


st.title("Story Overview")
st.caption("Understand the TTC's service reliability story.")

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

page_story_header(
    audience_question="What is the TTC reliability problem, and why should we care?",
    takeaway=(
        "TTC reliability is a recurring pattern across place and time."
        " A small set of entities repeatedly drives high disruption risk."
    ),
)

coverage_result = _load_core_coverage()
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty" or coverage_result.frame.empty:
    st.info("No service-date coverage is available in `gold_delay_events_core`.")
    st.stop()

coverage_row = coverage_result.frame.iloc[0]
min_service_date = pd.to_datetime(coverage_row["min_service_date"], errors="coerce")
max_service_date = pd.to_datetime(coverage_row["max_service_date"], errors="coerce")
if pd.isna(min_service_date) or pd.isna(max_service_date):
    st.info("Coverage dates are unavailable.")
    st.stop()

min_date = min_service_date.date()
max_date = max_service_date.date()

selected_modes = st.multiselect("Modes", options=list(ALL_MODES), default=list(ALL_MODES))
if not selected_modes:
    st.info("Select at least one mode to render Story Overview.")
    st.stop()

date_selection = st.date_input(
    "Service date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key="story_overview_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(date_selection, min_date=min_date, max_date=max_date)
selected_start_iso = selected_start_date.isoformat()
selected_end_iso = selected_end_date.isoformat()

monthly_result = _load_monthly_reliability(selected_start_iso, selected_end_iso)
if monthly_result.status in {"missing", "error"}:
    st.error(monthly_result.message)
    st.stop()
if monthly_result.status == "empty":
    st.info("No monthly reliability rows are available for this window.")
    st.stop()

monthly = monthly_result.frame.copy()
monthly["month_start"] = pd.to_datetime(monthly["month_start"], errors="coerce")
monthly = monthly[(monthly["mode"].isin(selected_modes)) & monthly["month_start"].notna()].copy()
if monthly.empty:
    st.info("No monthly reliability rows are available for selected modes.")
    st.stop()

kpi_a, kpi_b, kpi_c, kpi_d = st.columns(4)
kpi_a.metric("Months in Window", fmt_int(monthly["month_start"].nunique()))
kpi_b.metric("Avg Frequency", fmt_int(monthly["frequency"].mean()))
kpi_c.metric("Avg Severity P90", fmt_float(monthly["severity_p90"].mean(), digits=2))
kpi_d.metric("Avg Regularity P90", fmt_float(monthly["regularity_p90"].mean(), digits=2))

trend_frame = monthly.melt(
    id_vars=["mode", "month_start"],
    value_vars=["frequency", "severity_p90", "regularity_p90", "cause_mix_score"],
    var_name="metric",
    value_name="value",
)

metric_focus = ["frequency", "severity_p90"] if presentation else ["frequency", "severity_p90", "regularity_p90", "cause_mix_score"]
trend_plot = trend_frame[trend_frame["metric"].isin(metric_focus)].copy()
trend_plot["metric_label"] = trend_plot["metric"].map(METRIC_DISPLAY_NAMES).fillna(trend_plot["metric"])
trend_plot["mode_label"] = trend_plot["mode"].str.title()
trend_plot["mode_metric"] = trend_plot["mode_label"] + " - " + trend_plot["metric_label"]

color_domain = []
color_range = []
for mode_name in selected_modes:
    for metric_name in metric_focus:
        color_domain.append(f"{mode_name.title()} - {METRIC_DISPLAY_NAMES.get(metric_name, metric_name)}")
        color_range.append(MODE_METRIC_COLORS.get((mode_name, metric_name), "#7f7f7f"))

selected_mode_title = ", ".join(mode_name.title() for mode_name in selected_modes)

chart = (
    alt.Chart(trend_plot)
    .mark_line(point=True)
    .encode(
        x=alt.X("month_start:T", title="Month", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("value:Q", title="Metric Value"),
        color=alt.Color("mode_metric:N", title="Mode + Metric", scale=alt.Scale(domain=color_domain, range=color_range)),
        tooltip=[
            alt.Tooltip("month_start:T", title="Month"),
            alt.Tooltip("mode_label:N", title="Mode"),
            alt.Tooltip("metric_label:N", title="Metric"),
            alt.Tooltip("value:Q", title="Value", format=".3f"),
        ],
    )
    .properties(
        title=f"Reliability Pattern Over Time ({selected_mode_title})",
        height=340,
    )
    .interactive()
)
st.altair_chart(chart, use_container_width=True)
render_ai_explain_block(
    page_name="Story Overview",
    chart_id="monthly_reliability_trend",
    chart_title=f"Reliability Pattern Over Time ({selected_mode_title})",
    filters={
        "mode": ",".join(selected_modes),
        "start_date": selected_start_iso,
        "end_date": selected_end_iso,
        "presentation_mode": presentation,
        "metrics": ",".join(metric_focus),
    },
    frame=trend_plot,
)

route_frames: list[pd.DataFrame] = []
for mode_name in selected_modes:
    mode_snapshot = _load_mode_hotspot_snapshot(mode_name, selected_start_iso, selected_end_iso)
    if mode_snapshot.status == "ok" and not mode_snapshot.frame.empty:
        route_frames.append(mode_snapshot.frame.copy())

if route_frames:
    route_hotspots = pd.concat(route_frames, ignore_index=True)
    route_hotspots = route_hotspots.sort_values(
        by=["composite_score", "frequency"], ascending=[False, False], na_position="last"
    ).head(12)
else:
    route_hotspots = pd.DataFrame()

subway_hotspot_result = _load_subway_hotspot_snapshot(selected_start_iso, selected_end_iso)
streetcar_hotspot_result = _load_mode_hotspot_snapshot("streetcar", selected_start_iso, selected_end_iso)

st.markdown("**Top Route Hotspots in Window (Selected Modes)**")
if not route_hotspots.empty:
    route_view = route_hotspots[
        ["mode", "entity_id", "frequency", "severity_p90", "regularity_p90", "cause_mix_score", "composite_score"]
    ].copy()
    route_view = route_view.rename(columns={"entity_id": "route_id"})
    route_view["mode"] = route_view["mode"].str.title()
    st.dataframe(route_view, use_container_width=True, hide_index=True)
else:
    st.caption("No route hotspot snapshot available for selected modes.")

subway_col, streetcar_col = st.columns(2)
with subway_col:
    st.markdown("**Top Subway Stations in Window**")
    if subway_hotspot_result.status == "ok" and not subway_hotspot_result.frame.empty:
        subway_view = subway_hotspot_result.frame[["entity_id", "frequency", "severity_p90", "regularity_p90", "cause_mix_score", "composite_score"]].copy()
        subway_view = subway_view.rename(columns={"entity_id": "station_name"})
        st.dataframe(subway_view, use_container_width=True, hide_index=True)
    else:
        st.caption("No subway hotspot snapshot available.")

with streetcar_col:
    st.markdown("**Top Streetcar Routes in Window**")
    if streetcar_hotspot_result.status == "ok" and not streetcar_hotspot_result.frame.empty:
        streetcar_view = streetcar_hotspot_result.frame[
            ["entity_id", "frequency", "severity_p90", "regularity_p90", "cause_mix_score", "composite_score"]
        ].copy()
        streetcar_view = streetcar_view.rename(columns={"entity_id": "route_id"})
        st.dataframe(streetcar_view, use_container_width=True, hide_index=True)
    else:
        st.caption("No streetcar hotspot snapshot available.")

if presentation:
    st.caption("Presentation mode keeps only summary evidence. Switch to Exploration for full tables and controls.")

next_question_hint("Which routes/stations repeatedly dominate risk? Open: Recurring Hotspots.")
