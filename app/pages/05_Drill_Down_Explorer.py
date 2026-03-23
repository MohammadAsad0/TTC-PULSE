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

from ttc_pulse.dashboard.formatting import DAY_NAME_ORDER, fmt_float, fmt_int
from ttc_pulse.dashboard.loaders import query_table
from ttc_pulse.dashboard.metric_config import METRIC_OPTIONS, metric_axis_title, resolve_metric_choice
from ttc_pulse.dashboard.storytelling import is_presentation_mode, next_question_hint, page_story_header, story_mode_selector


def _mode_config(mode: str) -> tuple[str, str, str, str]:
    if mode == "bus":
        return "gold_route_time_metrics", "route_id_gtfs", "Route", "AND mode = 'bus'"
    return "gold_station_time_metrics", "station_canonical", "Station", ""


@st.cache_data(ttl=120)
def _load_coverage(mode: str):
    table_name, id_col, _, where_mode = _mode_config(mode)
    return query_table(
        table_name=table_name,
        query_template=f"""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {{source}}
        WHERE {id_col} IS NOT NULL
            AND service_date IS NOT NULL
            {where_mode}
        """,
    )


@st.cache_data(ttl=120)
def _load_rankings(mode: str, start_date: str, end_date: str):
    table_name, id_col, _, where_mode = _mode_config(mode)
    return query_table(
        table_name=table_name,
        query_template=f"""
        WITH entity_agg AS (
            SELECT
                {id_col} AS entity_id,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {{source}}
            WHERE {id_col} IS NOT NULL
                AND service_date IS NOT NULL
                {where_mode}
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                entity_id,
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
            FROM entity_agg
        )
        SELECT
            entity_id,
            RANK() OVER (ORDER BY composite_score DESC NULLS LAST, frequency DESC) AS rank_position,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM scored
        ORDER BY rank_position ASC, entity_id
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_year_metrics(mode: str, entity_id: str, start_date: str, end_date: str):
    table_name, id_col, _, where_mode = _mode_config(mode)
    return query_table(
        table_name=table_name,
        query_template=f"""
        WITH year_agg AS (
            SELECT
                CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) AS year,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {{source}}
            WHERE {id_col} = ?
                {where_mode}
                AND service_date IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                year,
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
            FROM year_agg
        )
        SELECT * FROM scored ORDER BY year ASC
        """,
        params=[entity_id, start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_month_metrics(mode: str, entity_id: str, year: int, start_date: str, end_date: str):
    table_name, id_col, _, where_mode = _mode_config(mode)
    return query_table(
        table_name=table_name,
        query_template=f"""
        WITH months AS (
            SELECT range AS month_num
            FROM range(1, 13)
        ),
        month_agg AS (
            SELECT
                CAST(EXTRACT(MONTH FROM service_date) AS INTEGER) AS month_num,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {{source}}
            WHERE {id_col} = ?
                {where_mode}
                AND service_date IS NOT NULL
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        month_filled AS (
            SELECT
                m.month_num,
                COALESCE(a.frequency, 0.0) AS frequency,
                COALESCE(a.severity_p90, 0.0) AS severity_p90,
                COALESCE(a.regularity_p90, 0.0) AS regularity_p90,
                COALESCE(a.cause_mix_score, 0.0) AS cause_mix_score
            FROM months AS m
            LEFT JOIN month_agg AS a
                ON m.month_num = a.month_num
        ),
        scored AS (
            SELECT
                month_num,
                frequency,
                severity_p90,
                regularity_p90,
                cause_mix_score,
                (
                    0.35 * COALESCE((frequency - AVG(frequency) OVER()) / NULLIF(STDDEV_SAMP(frequency) OVER(), 0.0), 0.0)
                    + 0.30 * COALESCE((severity_p90 - AVG(severity_p90) OVER()) / NULLIF(STDDEV_SAMP(severity_p90) OVER(), 0.0), 0.0)
                    + 0.20 * COALESCE((regularity_p90 - AVG(regularity_p90) OVER()) / NULLIF(STDDEV_SAMP(regularity_p90) OVER(), 0.0), 0.0)
                    + 0.15 * cause_mix_score
                ) AS composite_score
            FROM month_filled
        )
        SELECT
            month_num,
            frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM scored
        ORDER BY month_num ASC
        """,
        params=[entity_id, year, start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_weekday_metrics(
    mode: str,
    entity_id: str,
    year: int,
    month_num: int | None,
    start_date: str,
    end_date: str,
):
    table_name, id_col, _, where_mode = _mode_config(mode)
    return query_table(
        table_name=table_name,
        query_template=f"""
        WITH weekday_agg AS (
            SELECT
                STRFTIME(service_date, '%A') AS day_name,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {{source}}
            WHERE {id_col} = ?
                {where_mode}
                AND service_date IS NOT NULL
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND (? IS NULL OR CAST(EXTRACT(MONTH FROM service_date) AS INTEGER) = ?)
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                day_name,
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
            FROM weekday_agg
        )
        SELECT * FROM scored
        """,
        params=[entity_id, year, month_num, month_num, start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_time_bin_metrics(
    mode: str,
    entity_id: str,
    year: int,
    month_num: int | None,
    day_name: str | None,
    start_date: str,
    end_date: str,
):
    table_name, id_col, _, where_mode = _mode_config(mode)
    return query_table(
        table_name=table_name,
        query_template=f"""
        WITH binned AS (
            SELECT
                CASE
                    WHEN hour_bin BETWEEN 4 AND 6 THEN 'Early morning (4-6)'
                    WHEN hour_bin BETWEEN 7 AND 9 THEN 'Morning peak (7-9)'
                    WHEN hour_bin BETWEEN 10 AND 12 THEN 'Post-peak morning (10-12)'
                    WHEN hour_bin BETWEEN 13 AND 14 THEN 'Lunch time (13-14)'
                    WHEN hour_bin BETWEEN 15 AND 16 THEN 'Post-lunch (15-16)'
                    WHEN hour_bin BETWEEN 17 AND 19 THEN 'Evening peak (17-19)'
                    WHEN hour_bin BETWEEN 20 AND 22 THEN 'Night (20-22)'
                    WHEN hour_bin IN (23, 0, 1) THEN 'Late night (23-1)'
                    WHEN hour_bin BETWEEN 2 AND 3 THEN 'Overnight (2-3)'
                    ELSE 'Other'
                END AS time_bin,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {{source}}
            WHERE {id_col} = ?
                {where_mode}
                AND service_date IS NOT NULL
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND (? IS NULL OR CAST(EXTRACT(MONTH FROM service_date) AS INTEGER) = ?)
                AND (? IS NULL OR STRFTIME(service_date, '%A') = ?)
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                time_bin,
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
            FROM binned
        )
        SELECT * FROM scored
        ORDER BY CASE time_bin
            WHEN 'Early morning (4-6)' THEN 1
            WHEN 'Morning peak (7-9)' THEN 2
            WHEN 'Post-peak morning (10-12)' THEN 3
            WHEN 'Lunch time (13-14)' THEN 4
            WHEN 'Post-lunch (15-16)' THEN 5
            WHEN 'Evening peak (17-19)' THEN 6
            WHEN 'Night (20-22)' THEN 7
            WHEN 'Late night (23-1)' THEN 8
            WHEN 'Overnight (2-3)' THEN 9
            ELSE 99
        END
        """,
        params=[entity_id, year, month_num, month_num, day_name, day_name, start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_weekly_heatmap_metrics(
    mode: str,
    entity_id: str,
    year: int,
    month_num: int,
    start_date: str,
    end_date: str,
):
    table_name, id_col, _, where_mode = _mode_config(mode)
    return query_table(
        table_name=table_name,
        query_template=f"""
        WITH week_day_agg AS (
            SELECT
                1 + CAST((EXTRACT(DAY FROM service_date) - 1) / 7 AS INTEGER) AS week_of_month,
                STRFTIME(service_date, '%A') AS day_name,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {{source}}
            WHERE {id_col} = ?
                {where_mode}
                AND service_date IS NOT NULL
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND CAST(EXTRACT(MONTH FROM service_date) AS INTEGER) = ?
                AND service_date BETWEEN ? AND ?
            GROUP BY 1, 2
        ),
        scored AS (
            SELECT
                week_of_month,
                day_name,
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
            FROM week_day_agg
        )
        SELECT * FROM scored
        ORDER BY week_of_month, day_name
        """,
        params=[entity_id, year, month_num, start_date, end_date],
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


def _coerce_year(value: object) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_field_from_chart_event(event_state: object, selection_name: str, field_name: str) -> object | None:
    if event_state is None:
        return None

    selection = None
    try:
        selection = event_state.selection  # type: ignore[attr-defined]
    except Exception:
        if isinstance(event_state, dict):
            selection = event_state.get("selection")

    if not selection:
        return None

    candidate = None
    if isinstance(selection, dict):
        candidate = selection.get(selection_name)
    else:
        try:
            candidate = selection[selection_name]  # type: ignore[index]
        except Exception:
            candidate = None

    if candidate is None:
        return None

    if isinstance(candidate, list):
        if not candidate:
            return None
        first = candidate[0]
        if isinstance(first, dict):
            return first.get(field_name)
        return first

    if isinstance(candidate, dict):
        if field_name in candidate:
            return candidate.get(field_name)
        if "value" in candidate and isinstance(candidate.get("value"), list):
            values = candidate.get("value") or []
            if values and isinstance(values[0], dict):
                return values[0].get(field_name)
        if "values" in candidate and isinstance(candidate.get("values"), list):
            values = candidate.get("values") or []
            if values and isinstance(values[0], dict):
                return values[0].get(field_name)
        return None

    return candidate


def _extract_year_from_chart_event(event_state: object, selection_name: str = "year_pick") -> int | None:
    return _coerce_year(_extract_field_from_chart_event(event_state, selection_name=selection_name, field_name="year"))


def _extract_month_from_chart_event(event_state: object, selection_name: str = "month_pick") -> int | None:
    return _coerce_year(_extract_field_from_chart_event(event_state, selection_name=selection_name, field_name="month_num"))


def _extract_day_name_from_chart_event(event_state: object, selection_name: str = "weekday_pick") -> str | None:
    raw = _extract_field_from_chart_event(event_state, selection_name=selection_name, field_name="day_name")
    if raw is None:
        return None
    day_name = str(raw).strip()
    if not day_name:
        return None
    if day_name not in DAY_NAME_ORDER:
        return None
    return day_name


st.title("Drill-Down Explorer")
st.caption("Inspect one route/station deeply before taking action.")

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

page_story_header(
    audience_question="For one hotspot, how do reliability patterns evolve across year, month, weekday, and time-of-day?",
    takeaway="The selected hotspot shows consistent temporal signatures that support targeted interventions.",
)

selected_mode = st.radio("Explorer Mode", options=["bus", "subway"], horizontal=True)
table_name, _, entity_label, _ = _mode_config(selected_mode)

coverage_result = _load_coverage(selected_mode)
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty" or coverage_result.frame.empty:
    st.info("No coverage rows are available for selected mode.")
    st.stop()

coverage_row = coverage_result.frame.iloc[0]
min_service_date = pd.to_datetime(coverage_row["min_service_date"], errors="coerce")
max_service_date = pd.to_datetime(coverage_row["max_service_date"], errors="coerce")
if pd.isna(min_service_date) or pd.isna(max_service_date):
    st.info("Coverage dates are unavailable.")
    st.stop()

min_date = min_service_date.date()
max_date = max_service_date.date()
selected_metric_label = st.selectbox("Metric", options=METRIC_OPTIONS, index=0)

date_selection = st.date_input(
    "Service date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key="explorer_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(date_selection, min_date=min_date, max_date=max_date)
selected_start_iso = selected_start_date.isoformat()
selected_end_iso = selected_end_date.isoformat()

ranking_result = _load_rankings(selected_mode, selected_start_iso, selected_end_iso)
if ranking_result.status in {"missing", "error"}:
    st.error(ranking_result.message)
    st.stop()
if ranking_result.status == "empty" or ranking_result.frame.empty:
    st.info(f"No {entity_label.lower()} rows available for selected window.")
    st.stop()

ranking = ranking_result.frame.copy()
metric_resolution = resolve_metric_choice(ranking, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)
metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label

ranking = ranking.sort_values([metric_column, "rank_position"], ascending=[False, True])
entity_options = ranking["entity_id"].astype(str).tolist()
default_entity = entity_options[0]
selected_entity = st.selectbox(f"Selected {entity_label}", options=entity_options, index=0)
selected_row = ranking[ranking["entity_id"].astype(str) == str(selected_entity)].iloc[0]

kpi_a, kpi_b, kpi_c, kpi_d, kpi_e = st.columns(5)
kpi_a.metric("Composite", fmt_float(selected_row["composite_score"], digits=3))
kpi_b.metric("Frequency", fmt_int(selected_row["frequency"]))
kpi_c.metric("Severity P90", fmt_float(selected_row["severity_p90"], digits=2))
kpi_d.metric("Regularity P90", fmt_float(selected_row["regularity_p90"], digits=2))
kpi_e.metric("Cause Mix", fmt_float(selected_row["cause_mix_score"], digits=3))

year_result = _load_year_metrics(selected_mode, str(selected_entity), selected_start_iso, selected_end_iso)
if year_result.status in {"missing", "error"}:
    st.error(year_result.message)
    st.stop()
if year_result.status == "empty" or year_result.frame.empty:
    st.info("No year-level slices available for selected entity.")
    next_question_hint("Do live alerts align with these hotspots? Open: Live Alert Alignment.")
    st.stop()

year_frame = year_result.frame.copy()
year_frame["year"] = year_frame["year"].astype(int)
year_options = sorted(year_frame["year"].dropna().astype(int).unique().tolist(), reverse=True)
if not year_options:
    st.info("No year options are available for the selected entity.")
    next_question_hint("Do live alerts align with these hotspots? Open: Live Alert Alignment.")
    st.stop()

selection_name = "year_pick"
year_click = alt.selection_point(name=selection_name, fields=["year"], empty=True, on="click")
year_chart = (
    alt.Chart(year_frame)
    .mark_bar()
    .encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        color=alt.condition(year_click, alt.value("#1f77b4"), alt.value("#b5c3d7")),
        tooltip=["year:Q", f"{metric_column}:Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"],
    )
    .add_params(year_click)
    .properties(title=f"{entity_label} {selected_entity}: Year Profile (click a bar to select year)", height=280)
)

chart_key = f"drill_year_chart_{selected_mode}_{selected_entity}_{metric_column}"
year_event = st.altair_chart(
    year_chart,
    use_container_width=True,
    key=chart_key,
    on_select="rerun",
    selection_mode=selection_name,
)

selected_year_from_chart = _extract_year_from_chart_event(year_event, selection_name=selection_name)
year_state_key = f"drill_selected_year_{selected_mode}_{selected_entity}"
if selected_year_from_chart in year_options:
    st.session_state[year_state_key] = int(selected_year_from_chart)

default_year = st.session_state.get(year_state_key, year_options[0])
if default_year not in year_options:
    default_year = year_options[0]
selected_year = st.selectbox(
    "Selected Year (click chart or use dropdown)",
    options=year_options,
    index=year_options.index(default_year),
    key=f"drill_year_select_{selected_mode}_{selected_entity}",
)
st.session_state[year_state_key] = int(selected_year)


month_result = _load_month_metrics(selected_mode, str(selected_entity), int(selected_year), selected_start_iso, selected_end_iso)
if month_result.status in {"missing", "error"}:
    st.error(month_result.message)
    st.stop()

month_frame = month_result.frame.copy() if month_result.status == "ok" else pd.DataFrame()
if month_frame.empty:
    month_frame = pd.DataFrame({"month_num": list(range(1, 13))})
    for metric_name in ["frequency", "severity_p90", "regularity_p90", "cause_mix_score", "composite_score"]:
        month_frame[metric_name] = 0.0

month_frame["month_num"] = pd.to_numeric(month_frame["month_num"], errors="coerce").fillna(0).astype(int)
month_frame = month_frame[month_frame["month_num"].between(1, 12)].sort_values("month_num")
month_frame["month_start"] = pd.to_datetime(
    {"year": int(selected_year), "month": month_frame["month_num"], "day": 1},
    errors="coerce",
)
month_frame = month_frame.dropna(subset=["month_start"])
month_frame["month_choice"] = month_frame["month_start"].dt.strftime("%b (%m)")
month_frame["month_label"] = month_frame["month_start"].dt.strftime("%b")

month_selection_name = "month_pick"
month_click = alt.selection_point(name=month_selection_name, fields=["month_num"], empty=True, on="click")
month_chart = (
    alt.Chart(month_frame)
    .mark_bar()
    .encode(
        x=alt.X("month_start:T", title="Month", axis=alt.Axis(format="%b", labelAngle=0)),
        y=alt.Y(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        color=alt.condition(month_click, alt.value("#1f77b4"), alt.value("#b5c3d7")),
        tooltip=["month_start:T", f"{metric_column}:Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"],
    )
    .add_params(month_click)
    .properties(title=f"Monthly Profile ({selected_year}) (click a bar to select month)", height=260)
)

month_chart_key = f"drill_month_chart_{selected_mode}_{selected_entity}_{selected_year}_{metric_column}"
month_event = st.altair_chart(
    month_chart,
    use_container_width=True,
    key=month_chart_key,
    on_select="rerun",
    selection_mode=month_selection_name,
)

month_choice_to_num = dict(zip(month_frame["month_choice"], month_frame["month_num"]))
month_num_to_choice = {month_num: month_choice for month_choice, month_num in month_choice_to_num.items()}
month_options = ["All months"] + month_frame["month_choice"].tolist()
selected_month_from_chart = _extract_month_from_chart_event(month_event, selection_name=month_selection_name)
month_state_key = f"drill_selected_month_{selected_mode}_{selected_entity}_{selected_year}"
if selected_month_from_chart in month_num_to_choice:
    st.session_state[month_state_key] = int(selected_month_from_chart)

default_month_num = st.session_state.get(month_state_key)
default_month_choice = month_num_to_choice.get(default_month_num, "All months")
if default_month_choice not in month_options:
    default_month_choice = "All months"
selected_month_choice = st.selectbox(
    "Selected Month (click chart or use dropdown)",
    options=month_options,
    index=month_options.index(default_month_choice),
    key=f"drill_month_select_{selected_mode}_{selected_entity}_{selected_year}",
)
selected_month_num = month_choice_to_num.get(selected_month_choice)
st.session_state[month_state_key] = int(selected_month_num) if selected_month_num is not None else None
selected_month_label = selected_month_choice.split(" (")[0] if selected_month_num is not None else "All months"

weekday_result = _load_weekday_metrics(
    selected_mode,
    str(selected_entity),
    int(selected_year),
    selected_month_num,
    selected_start_iso,
    selected_end_iso,
)
if weekday_result.status in {"missing", "error"}:
    st.error(weekday_result.message)
    st.stop()

weekday_frame = weekday_result.frame.copy() if weekday_result.status == "ok" else pd.DataFrame(columns=["day_name"])
if "day_name" not in weekday_frame.columns:
    weekday_frame = pd.DataFrame(columns=["day_name"])
weekday_base = pd.DataFrame({"day_name": DAY_NAME_ORDER})
weekday_frame = weekday_base.merge(weekday_frame, on="day_name", how="left")
for metric_name in ["frequency", "severity_p90", "regularity_p90", "cause_mix_score", "composite_score"]:
    weekday_frame[metric_name] = pd.to_numeric(weekday_frame[metric_name], errors="coerce").fillna(0.0)
weekday_frame["day_name"] = pd.Categorical(weekday_frame["day_name"], categories=DAY_NAME_ORDER, ordered=True)
weekday_frame = weekday_frame.sort_values("day_name")

weekday_scope = f"{selected_month_label} {selected_year}" if selected_month_num is not None else str(selected_year)
weekday_selection_name = "weekday_pick"
weekday_click = alt.selection_point(name=weekday_selection_name, fields=["day_name"], empty=True, on="click")
weekday_chart = (
    alt.Chart(weekday_frame)
    .mark_bar()
    .encode(
        x=alt.X("day_name:N", sort=DAY_NAME_ORDER, title="Weekday"),
        y=alt.Y(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        color=alt.condition(weekday_click, alt.value("#1f77b4"), alt.value("#b5c3d7")),
        tooltip=["day_name:N", f"{metric_column}:Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"],
    )
    .add_params(weekday_click)
    .properties(title=f"Weekday Profile ({weekday_scope}) (click a bar to select weekday)", height=260)
)

weekday_chart_key = (
    f"drill_weekday_chart_{selected_mode}_{selected_entity}_{selected_year}_"
    f"{selected_month_num if selected_month_num is not None else 'all'}_{metric_column}"
)
weekday_event = st.altair_chart(
    weekday_chart,
    use_container_width=True,
    key=weekday_chart_key,
    on_select="rerun",
    selection_mode=weekday_selection_name,
)

weekday_options = ["All weekdays"] + DAY_NAME_ORDER
selected_weekday_from_chart = _extract_day_name_from_chart_event(weekday_event, selection_name=weekday_selection_name)
weekday_scope_token = selected_month_num if selected_month_num is not None else "all_months"
weekday_state_key = f"drill_selected_weekday_{selected_mode}_{selected_entity}_{selected_year}_{weekday_scope_token}"
if selected_weekday_from_chart in DAY_NAME_ORDER:
    st.session_state[weekday_state_key] = selected_weekday_from_chart

default_weekday_choice = st.session_state.get(weekday_state_key, "All weekdays")
if default_weekday_choice not in weekday_options:
    default_weekday_choice = "All weekdays"
selected_weekday_choice = st.selectbox(
    "Selected Weekday (click chart or use dropdown)",
    options=weekday_options,
    index=weekday_options.index(default_weekday_choice),
    key=f"drill_weekday_select_{selected_mode}_{selected_entity}_{selected_year}_{weekday_scope_token}",
)
selected_weekday = None if selected_weekday_choice == "All weekdays" else selected_weekday_choice
st.session_state[weekday_state_key] = selected_weekday_choice

if selected_month_num is not None:
    weekly_heatmap_result = _load_weekly_heatmap_metrics(
        selected_mode,
        str(selected_entity),
        int(selected_year),
        int(selected_month_num),
        selected_start_iso,
        selected_end_iso,
    )
    if weekly_heatmap_result.status == "ok" and not weekly_heatmap_result.frame.empty:
        weekly_heatmap = weekly_heatmap_result.frame.copy()
        weekly_heatmap["week_of_month"] = pd.to_numeric(weekly_heatmap["week_of_month"], errors="coerce").fillna(0).astype(int)
        weekly_heatmap["day_name"] = pd.Categorical(weekly_heatmap["day_name"], categories=DAY_NAME_ORDER, ordered=True)
        weekly_heatmap = weekly_heatmap.sort_values(["week_of_month", "day_name"])
        heatmap_chart = (
            alt.Chart(weekly_heatmap)
            .mark_rect()
            .encode(
                x=alt.X("day_name:N", sort=DAY_NAME_ORDER, title="Weekday"),
                y=alt.Y("week_of_month:O", title="Week of Month"),
                color=alt.Color(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
                tooltip=["week_of_month:Q", "day_name:N", f"{metric_column}:Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"],
            )
            .properties(title=f"Weekly Heatmap ({selected_month_label} {selected_year})", height=280)
        )
        st.altair_chart(heatmap_chart, use_container_width=True)
    elif weekly_heatmap_result.status in {"missing", "error"}:
        st.info("Weekly heatmap is unavailable in the current environment.")
    else:
        st.info("No weekly heatmap rows are available for the selected month.")

time_bin_result = _load_time_bin_metrics(
    selected_mode,
    str(selected_entity),
    int(selected_year),
    selected_month_num,
    selected_weekday,
    selected_start_iso,
    selected_end_iso,
)
if time_bin_result.status == "ok" and not time_bin_result.frame.empty:
    time_bin_frame = time_bin_result.frame.copy()
    if selected_month_num is None and selected_weekday is None:
        time_scope = f"{selected_year} (all months, all weekdays)"
    elif selected_month_num is not None and selected_weekday is None:
        time_scope = f"{selected_month_label} {selected_year} (all weekdays)"
    elif selected_month_num is None and selected_weekday is not None:
        time_scope = f"{selected_weekday}s in {selected_year}"
    else:
        time_scope = f"{selected_weekday}s in {selected_month_label} {selected_year}"

    time_chart = (
        alt.Chart(time_bin_frame)
        .mark_bar()
        .encode(
            x=alt.X("time_bin:N", title="Time Bin"),
            y=alt.Y(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
            tooltip=["time_bin:N", f"{metric_column}:Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"],
        )
        .properties(title=f"Time-of-Day Profile ({time_scope})", height=280)
    )
    st.altair_chart(time_chart, use_container_width=True)
elif time_bin_result.status in {"missing", "error"}:
    st.info("Time-of-day profile is unavailable in the current environment.")
else:
    st.info("No time-of-day rows are available for the selected scope.")

if not presentation:
    with st.expander("Exploration Tables"):
        st.dataframe(ranking, use_container_width=True, hide_index=True)
        st.dataframe(year_frame, use_container_width=True, hide_index=True)

next_question_hint("Do live alerts currently align with historical hotspots? Open: Live Alert Alignment.")
