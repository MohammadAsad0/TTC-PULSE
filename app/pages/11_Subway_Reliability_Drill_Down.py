from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

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
from ttc_pulse.dashboard.metric_config import METRIC_COLUMN_MAP, METRIC_OPTIONS
from ttc_pulse.dashboard.loaders import query_table


STATE_STATION = "subway_drill_station"
STATE_YEAR = "subway_drill_year"
STATE_MONTH = "subway_drill_month"
STATE_DAY = "subway_drill_day"
STATE_WEEKDAY = "subway_drill_weekday"
STATE_TIME_BIN = "subway_drill_time_bin"

SUMMARY_METRIC_ALIASES: dict[str, list[str]] = {
    label: [column] for label, column in METRIC_COLUMN_MAP.items()
}

DRILL_METRIC_ALIASES: dict[str, list[str]] = {
    "Composite Score": [METRIC_COLUMN_MAP["Composite Score"]],
    "Frequency": ["incident_count", METRIC_COLUMN_MAP["Frequency"]],
    "Severity": ["p90_delay", METRIC_COLUMN_MAP["Severity"], "avg_delay", "severity_median"],
    "Regularity": ["p90_gap", METRIC_COLUMN_MAP["Regularity"]],
    "Cause Mix": [METRIC_COLUMN_MAP["Cause Mix"]],
}

SUMMARY_FALLBACK_COLUMNS = ["composite_score", "frequency", "severity_p90", "regularity_p90", "cause_mix_score"]
DRILL_FALLBACK_COLUMNS = ["incident_count", "p90_delay", "avg_delay", "p90_gap", "cause_mix_score", "composite_score"]

TIME_BIN_ORDER: list[tuple[int, str]] = [
    (1, "Early morning (4-6)"),
    (2, "Morning peak (7-9)"),
    (3, "Post-peak morning (10-12)"),
    (4, "Lunch time (13-14)"),
    (5, "Post-lunch (15-16)"),
    (6, "Evening peak (17-19)"),
    (7, "Night (20-22)"),
    (8, "Late night (23-1)"),
    (9, "Overnight (2-3)"),
]
TIME_BIN_LABELS = [label for _, label in TIME_BIN_ORDER]
TIME_BIN_ORDER_MAP = {label: order for order, label in TIME_BIN_ORDER}


def _init_state() -> None:
    for key in [STATE_STATION, STATE_YEAR, STATE_MONTH, STATE_DAY, STATE_WEEKDAY, STATE_TIME_BIN]:
        if key not in st.session_state:
            st.session_state[key] = None


def _clear_from(level: str) -> None:
    if level == "station":
        st.session_state[STATE_YEAR] = None
        st.session_state[STATE_MONTH] = None
        st.session_state[STATE_DAY] = None
        st.session_state[STATE_WEEKDAY] = None
        st.session_state[STATE_TIME_BIN] = None
    elif level == "year":
        st.session_state[STATE_MONTH] = None
        st.session_state[STATE_DAY] = None
        st.session_state[STATE_WEEKDAY] = None
        st.session_state[STATE_TIME_BIN] = None
    elif level == "month":
        st.session_state[STATE_DAY] = None
        st.session_state[STATE_WEEKDAY] = None
        st.session_state[STATE_TIME_BIN] = None
    elif level == "day":
        st.session_state[STATE_TIME_BIN] = None
    elif level == "weekday":
        st.session_state[STATE_TIME_BIN] = None


def _reset_all() -> None:
    for key in [STATE_STATION, STATE_YEAR, STATE_MONTH, STATE_DAY, STATE_WEEKDAY, STATE_TIME_BIN]:
        st.session_state[key] = None


def _step_back() -> None:
    if st.session_state[STATE_TIME_BIN] is not None:
        st.session_state[STATE_TIME_BIN] = None
        return
    if st.session_state[STATE_WEEKDAY] is not None:
        st.session_state[STATE_WEEKDAY] = None
        return
    if st.session_state[STATE_DAY] is not None:
        st.session_state[STATE_DAY] = None
        return
    if st.session_state[STATE_MONTH] is not None:
        st.session_state[STATE_MONTH] = None
        return
    if st.session_state[STATE_YEAR] is not None:
        st.session_state[STATE_YEAR] = None
        return
    if st.session_state[STATE_STATION] is not None:
        st.session_state[STATE_STATION] = None


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, list):
        if not value:
            return None
        return _normalize_scalar(value[0])
    if isinstance(value, tuple):
        if not value:
            return None
        return _normalize_scalar(value[0])
    if isinstance(value, dict):
        for key in ["value", "values", "datum", "items"]:
            if key in value:
                found = _normalize_scalar(value[key])
                if found is not None:
                    return found
        return None
    return value


def _extract_field_from_event(node: Any, field_name: str) -> Any:
    if node is None:
        return None
    if isinstance(node, dict):
        if field_name in node:
            return _normalize_scalar(node[field_name])
        for value in node.values():
            found = _extract_field_from_event(value, field_name)
            if found is not None:
                return found
        return None
    if isinstance(node, list):
        for item in node:
            found = _extract_field_from_event(item, field_name)
            if found is not None:
                return found
        return None
    return None


def _extract_selection_value(event_state: Any, selection_name: str, field_name: str) -> Any:
    if event_state is None:
        return None
    if isinstance(event_state, dict):
        selection = event_state.get("selection")
    else:
        selection = getattr(event_state, "selection", None)
    if selection is None:
        return None
    if isinstance(selection, dict):
        payload = selection.get(selection_name)
    else:
        payload = getattr(selection, selection_name, None)
    return _extract_field_from_event(payload, field_name)


def _is_composite_unstable(frame: pd.DataFrame, min_points: int = 8) -> bool:
    if frame.empty or "composite_score" not in frame.columns:
        return True
    values = pd.to_numeric(frame["composite_score"], errors="coerce").dropna()
    if len(values) < min_points:
        return True
    if values.nunique() <= 1:
        return True
    return False


def _metric_candidates(metric_label: str, level: str) -> list[str]:
    alias_map = SUMMARY_METRIC_ALIASES if level == "summary" else DRILL_METRIC_ALIASES
    return alias_map.get(metric_label, alias_map["Composite Score"])


def _resolve_metric_column(
    frame: pd.DataFrame,
    metric_label: str,
    level: str,
    composite_unstable: bool = False,
) -> tuple[str, str | None]:
    candidates = _metric_candidates(metric_label, level)
    fallback_columns = SUMMARY_FALLBACK_COLUMNS if level == "summary" else DRILL_FALLBACK_COLUMNS

    if metric_label == "Composite Score" and composite_unstable:
        for fallback in fallback_columns:
            if fallback != "composite_score" and fallback in frame.columns:
                return fallback, (
                    "Composite is unstable at fine granularity for this slice. "
                    f"Fallback metric is active: {fallback.replace('_', ' ').title()}."
                )

    for candidate in candidates:
        if candidate in frame.columns:
            return candidate, None

    for fallback in fallback_columns:
        if fallback in frame.columns:
            return fallback, (
                f"{metric_label} is unavailable at this level; "
                f"falling back to {fallback.replace('_', ' ').title()}."
            )

    return candidates[0], f"No usable metric columns found; defaulting to {candidates[0]}."


def _metric_display_fields(metric_label: str, level: str) -> list[str]:
    fields = list(_metric_candidates(metric_label, level))
    if level == "summary":
        fields.extend(["frequency", "severity_p90", "regularity_p90", "cause_mix_score", "composite_score"])
    else:
        fields.extend(["incident_count", "avg_delay", "p90_delay", "p90_gap", "cause_mix_score", "composite_score"])
    seen: list[str] = []
    for field in fields:
        if field not in seen:
            seen.append(field)
    return seen


def _sort_ranking_frame(frame: pd.DataFrame, metric_label: str, metric_column: str) -> pd.DataFrame:
    output = frame.copy()
    if metric_label == "Composite Score":
        return output.sort_values(["rank_position", "station_name"]).reset_index(drop=True)

    sort_cols = [metric_column]
    ascending = [False]
    if "frequency" in output.columns and metric_column != "frequency":
        sort_cols.append("frequency")
        ascending.append(False)
    if "station_name" in output.columns:
        sort_cols.append("station_name")
        ascending.append(True)
    output = output.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)
    output["rank_position"] = range(1, len(output) + 1)
    return output


@st.cache_data(ttl=180)
def _load_top_stations_full_history() -> Any:
    return query_table(
        table_name="gold_top_offender_ranking",
        query_template="""
        SELECT
            ranking_date,
            entity_id AS station_name,
            rank_position,
            frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM {source}
        WHERE entity_type = 'station'
            AND mode = 'subway'
        ORDER BY rank_position ASC, station_name
        """,
    )


@st.cache_data(ttl=180)
def _load_station_year_metrics(station_name: str) -> Any:
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        WITH year_agg AS (
            SELECT
                CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) AS year,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE station_canonical = ?
                AND service_date IS NOT NULL
            GROUP BY 1
        ),
        scored AS (
            SELECT
                year,
                frequency,
                severity_p90,
                regularity_p90,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (frequency - AVG(frequency) OVER()) / NULLIF(STDDEV_POP(frequency) OVER(), 0.0) AS z_freq,
                (severity_p90 - AVG(severity_p90) OVER()) / NULLIF(STDDEV_POP(severity_p90) OVER(), 0.0) AS z_sev,
                (regularity_p90 - AVG(regularity_p90) OVER()) / NULLIF(STDDEV_POP(regularity_p90) OVER(), 0.0) AS z_reg,
                (COALESCE(cause_mix_score, 0.0) - AVG(COALESCE(cause_mix_score, 0.0)) OVER())
                    / NULLIF(STDDEV_POP(COALESCE(cause_mix_score, 0.0)) OVER(), 0.0) AS z_cause
            FROM year_agg
        )
        SELECT
            year,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            (
                0.35 * COALESCE(z_freq, 0.0) +
                0.30 * COALESCE(z_sev, 0.0) +
                0.20 * COALESCE(z_reg, 0.0) +
                0.15 * COALESCE(z_cause, 0.0)
            ) AS composite_score
        FROM scored
        ORDER BY year ASC
        """,
        params=[station_name],
    )


@st.cache_data(ttl=180)
def _load_station_month_metrics(station_name: str, year: int) -> Any:
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        WITH month_agg AS (
            SELECT
                CAST(EXTRACT(MONTH FROM service_date) AS INTEGER) AS month_num,
                CAST(DATE_TRUNC('month', service_date) AS DATE) AS month_start,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE station_canonical = ?
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND service_date IS NOT NULL
            GROUP BY 1, 2
        ),
        scored AS (
            SELECT
                month_num,
                month_start,
                frequency,
                severity_p90,
                regularity_p90,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (frequency - AVG(frequency) OVER()) / NULLIF(STDDEV_POP(frequency) OVER(), 0.0) AS z_freq,
                (severity_p90 - AVG(severity_p90) OVER()) / NULLIF(STDDEV_POP(severity_p90) OVER(), 0.0) AS z_sev,
                (regularity_p90 - AVG(regularity_p90) OVER()) / NULLIF(STDDEV_POP(regularity_p90) OVER(), 0.0) AS z_reg,
                (COALESCE(cause_mix_score, 0.0) - AVG(COALESCE(cause_mix_score, 0.0)) OVER())
                    / NULLIF(STDDEV_POP(COALESCE(cause_mix_score, 0.0)) OVER(), 0.0) AS z_cause
            FROM month_agg
        )
        SELECT
            month_num,
            month_start,
            STRFTIME(month_start, '%b') AS month_label,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            (
                0.35 * COALESCE(z_freq, 0.0) +
                0.30 * COALESCE(z_sev, 0.0) +
                0.20 * COALESCE(z_reg, 0.0) +
                0.15 * COALESCE(z_cause, 0.0)
            ) AS composite_score
        FROM scored
        ORDER BY month_num ASC
        """,
        params=[station_name, year],
    )


@st.cache_data(ttl=180)
def _load_station_daily_month_metrics(station_name: str, year: int, month: int) -> Any:
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        WITH day_agg AS (
            SELECT
                service_date,
                SUM(frequency)::DOUBLE AS incident_count,
                SUM(COALESCE(severity_p90, 0.0) * frequency) / NULLIF(SUM(frequency), 0.0) AS avg_delay,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS p90_delay,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS p90_gap,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE station_canonical = ?
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND CAST(EXTRACT(MONTH FROM service_date) AS INTEGER) = ?
                AND service_date IS NOT NULL
            GROUP BY 1
        ),
        scored AS (
            SELECT
                service_date,
                incident_count,
                avg_delay,
                p90_delay,
                p90_gap,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (incident_count - AVG(incident_count) OVER()) / NULLIF(STDDEV_POP(incident_count) OVER(), 0.0) AS z_freq,
                (p90_delay - AVG(p90_delay) OVER()) / NULLIF(STDDEV_POP(p90_delay) OVER(), 0.0) AS z_sev,
                (p90_gap - AVG(p90_gap) OVER()) / NULLIF(STDDEV_POP(p90_gap) OVER(), 0.0) AS z_reg,
                (COALESCE(cause_mix_score, 0.0) - AVG(COALESCE(cause_mix_score, 0.0)) OVER())
                    / NULLIF(STDDEV_POP(COALESCE(cause_mix_score, 0.0)) OVER(), 0.0) AS z_cause
            FROM day_agg
        )
        SELECT
            service_date,
            CAST(incident_count AS BIGINT) AS incident_count,
            avg_delay,
            p90_delay,
            p90_gap,
            cause_mix_score,
            (
                0.35 * COALESCE(z_freq, 0.0) +
                0.30 * COALESCE(z_sev, 0.0) +
                0.20 * COALESCE(z_reg, 0.0) +
                0.15 * COALESCE(z_cause, 0.0)
            ) AS composite_score
        FROM scored
        ORDER BY service_date ASC
        """,
        params=[station_name, year, month],
    )


@st.cache_data(ttl=180)
def _load_station_weekday_metrics(station_name: str, year: int) -> Any:
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        WITH weekday_agg AS (
            SELECT
                STRFTIME(service_date, '%A') AS day_name,
                CASE STRFTIME(service_date, '%A')
                    WHEN 'Monday' THEN 1
                    WHEN 'Tuesday' THEN 2
                    WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4
                    WHEN 'Friday' THEN 5
                    WHEN 'Saturday' THEN 6
                    WHEN 'Sunday' THEN 7
                    ELSE 99
                END AS day_order,
                SUM(frequency)::DOUBLE AS incident_count,
                SUM(COALESCE(severity_p90, 0.0) * frequency) / NULLIF(SUM(frequency), 0.0) AS avg_delay,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS p90_delay,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS p90_gap,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE station_canonical = ?
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND service_date IS NOT NULL
            GROUP BY 1, 2
        ),
        scored AS (
            SELECT
                day_name,
                day_order,
                incident_count,
                avg_delay,
                p90_delay,
                p90_gap,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (incident_count - AVG(incident_count) OVER()) / NULLIF(STDDEV_POP(incident_count) OVER(), 0.0) AS z_freq,
                (p90_delay - AVG(p90_delay) OVER()) / NULLIF(STDDEV_POP(p90_delay) OVER(), 0.0) AS z_sev,
                (p90_gap - AVG(p90_gap) OVER()) / NULLIF(STDDEV_POP(p90_gap) OVER(), 0.0) AS z_reg,
                (COALESCE(cause_mix_score, 0.0) - AVG(COALESCE(cause_mix_score, 0.0)) OVER())
                    / NULLIF(STDDEV_POP(COALESCE(cause_mix_score, 0.0)) OVER(), 0.0) AS z_cause
            FROM weekday_agg
        )
        SELECT
            day_name,
            day_order,
            CAST(incident_count AS BIGINT) AS incident_count,
            avg_delay,
            p90_delay,
            p90_gap,
            cause_mix_score,
            (
                0.35 * COALESCE(z_freq, 0.0) +
                0.30 * COALESCE(z_sev, 0.0) +
                0.20 * COALESCE(z_reg, 0.0) +
                0.15 * COALESCE(z_cause, 0.0)
            ) AS composite_score
        FROM scored
        ORDER BY day_order ASC
        """,
        params=[station_name, year],
    )


@st.cache_data(ttl=180)
def _load_station_time_bin_metrics(station_name: str, year: int, day_name: str) -> Any:
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
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
                SUM(frequency)::DOUBLE AS incident_count,
                SUM(COALESCE(severity_p90, 0.0) * frequency) / NULLIF(SUM(frequency), 0.0) AS avg_delay,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS p90_delay,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS p90_gap,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE station_canonical = ?
                AND CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) = ?
                AND STRFTIME(service_date, '%A') = ?
                AND service_date IS NOT NULL
            GROUP BY 1
        ),
        scored AS (
            SELECT
                time_bin,
                incident_count,
                avg_delay,
                p90_delay,
                p90_gap,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (incident_count - AVG(incident_count) OVER()) / NULLIF(STDDEV_POP(incident_count) OVER(), 0.0) AS z_freq,
                (p90_delay - AVG(p90_delay) OVER()) / NULLIF(STDDEV_POP(p90_delay) OVER(), 0.0) AS z_sev,
                (p90_gap - AVG(p90_gap) OVER()) / NULLIF(STDDEV_POP(p90_gap) OVER(), 0.0) AS z_reg,
                (COALESCE(cause_mix_score, 0.0) - AVG(COALESCE(cause_mix_score, 0.0)) OVER())
                    / NULLIF(STDDEV_POP(COALESCE(cause_mix_score, 0.0)) OVER(), 0.0) AS z_cause
            FROM binned
        )
        SELECT
            time_bin,
            CAST(incident_count AS BIGINT) AS incident_count,
            avg_delay,
            p90_delay,
            p90_gap,
            cause_mix_score,
            (
                0.35 * COALESCE(z_freq, 0.0) +
                0.30 * COALESCE(z_sev, 0.0) +
                0.20 * COALESCE(z_reg, 0.0) +
                0.15 * COALESCE(z_cause, 0.0)
            ) AS composite_score
        FROM scored
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
        params=[station_name, year, day_name],
    )


def _render_clickable_horizontal(
    frame: pd.DataFrame,
    x_field: str,
    y_field: str,
    selected_value: Any,
    selection_name: str,
    title: str,
    tooltip: list[str],
) -> Any:
    plot = frame.copy()
    plot["is_selected"] = plot[y_field].astype(str) == str(selected_value)
    selector = alt.selection_point(name=selection_name, fields=[y_field], on="click", clear="dblclick")

    chart = (
        alt.Chart(plot)
        .mark_bar()
        .encode(
            x=alt.X(f"{x_field}:Q"),
            y=alt.Y(f"{y_field}:N", sort="-x"),
            color=alt.Color(
                "is_selected:N",
                legend=None,
                scale=alt.Scale(domain=[True, False], range=["#ea580c", "#2563eb"]),
            ),
            tooltip=tooltip,
        )
        .add_params(selector)
        .properties(title=title, height=380)
    )
    for selection_mode in (selection_name, [selection_name]):
        try:
            return st.altair_chart(
                chart,
                use_container_width=True,
                on_select="rerun",
                selection_mode=selection_mode,
            )
        except Exception:
            continue
    return st.altair_chart(chart, use_container_width=True)


def _render_clickable_bar(
    frame: pd.DataFrame,
    x_field: str,
    y_field: str,
    selected_value: Any,
    selected_field: str,
    selection_name: str,
    title: str,
    tooltip: list[str],
    x_sort: Any | None = None,
) -> Any:
    plot = frame.copy()
    plot["is_selected"] = plot[selected_field].astype(str) == str(selected_value)
    selector = alt.selection_point(name=selection_name, fields=[selected_field], on="click", clear="dblclick")

    x_encoding = alt.X(f"{x_field}:N")
    if x_sort is not None:
        x_encoding = alt.X(f"{x_field}:N", sort=x_sort)

    chart = (
        alt.Chart(plot)
        .mark_bar()
        .encode(
            x=x_encoding,
            y=alt.Y(f"{y_field}:Q"),
            color=alt.Color(
                "is_selected:N",
                legend=None,
                scale=alt.Scale(domain=[True, False], range=["#ea580c", "#0f766e"]),
            ),
            tooltip=tooltip,
        )
        .add_params(selector)
        .properties(title=title, height=300)
    )
    for selection_mode in (selection_name, [selection_name]):
        try:
            return st.altair_chart(
                chart,
                use_container_width=True,
                on_select="rerun",
                selection_mode=selection_mode,
            )
        except Exception:
            continue
    return st.altair_chart(chart, use_container_width=True)


def _render_line_chart(frame: pd.DataFrame, x_field: str, y_field: str, title: str, tooltip: list[str]) -> None:
    if frame.empty:
        st.info("No rows available for this slice.")
        return
    chart = (
        alt.Chart(frame)
        .mark_line(point=True)
        .encode(
            x=alt.X(x_field),
            y=alt.Y(y_field),
            tooltip=tooltip,
        )
        .properties(title=title, height=280)
    )
    st.altair_chart(chart, use_container_width=True)


def _build_breadcrumb() -> str:
    parts = ["Top stations"]
    station = st.session_state[STATE_STATION]
    year = st.session_state[STATE_YEAR]
    month = st.session_state[STATE_MONTH]
    day = st.session_state[STATE_DAY]
    weekday = st.session_state[STATE_WEEKDAY]
    time_bin = st.session_state[STATE_TIME_BIN]

    if station is not None:
        parts.append(f"Station {station}")
    if year is not None:
        parts.append(f"Year {year}")
    if month is not None:
        parts.append(f"Month {month:02d}")
    if day is not None:
        parts.append(f"Day {day}")
    if weekday is not None:
        parts.append(f"Weekday {weekday}")
    if time_bin is not None:
        parts.append(f"Time bin {time_bin}")
    return " > ".join(parts)


def _show_station_summary(station_row: pd.Series, station_name: str) -> None:
    st.markdown("#### Selected Station Summary")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Overall Composite", fmt_float(station_row["composite_score"], digits=3))
    c2.metric("Total Incidents", fmt_int(station_row["frequency"]))
    c3.metric("P90 Delay (min)", fmt_float(station_row["severity_p90"], digits=1))
    c4.metric("P90 Gap (min)", fmt_float(station_row["regularity_p90"], digits=1))
    c5.metric("Cause Mix", fmt_float(station_row.get("cause_mix_score"), digits=3))


def _station_by_id(frame: pd.DataFrame, station_name: str) -> pd.Series | None:
    matched = frame[frame["station_name"].astype(str) == str(station_name)]
    if matched.empty:
        return None
    return matched.iloc[0]


def _prep_status_frame(result: Any) -> pd.DataFrame:
    frame = result.frame.copy()
    if "ranking_date" in frame.columns:
        frame["ranking_date"] = pd.to_datetime(frame["ranking_date"])
    if "month_start" in frame.columns:
        frame["month_start"] = pd.to_datetime(frame["month_start"])
    if "service_date" in frame.columns:
        frame["service_date"] = pd.to_datetime(frame["service_date"])
    return frame


def _require_result(result: Any, missing_message: str) -> pd.DataFrame:
    if result.status in {"missing", "error"}:
        st.error(result.message)
        return pd.DataFrame()
    if result.status == "empty":
        st.info(missing_message)
        return pd.DataFrame()
    return _prep_status_frame(result)


_init_state()

st.title("Subway Reliability Drill-Down")
st.caption(
    "Guided drill-down for TTC subway station reliability (full history): station ranking -> year -> month -> "
    "day or weekday -> time bins."
)
selected_metric_label = st.selectbox(
    "Metric to analyze",
    options=METRIC_OPTIONS,
    index=0,
    key="subway_metric_selector",
)

ctrl_left, ctrl_mid, ctrl_right = st.columns([2, 1, 1])
ctrl_left.info(f"Breadcrumb: {_build_breadcrumb()}")
if ctrl_mid.button("Back One Level"):
    _step_back()
if ctrl_right.button("Reset Drill-Down"):
    _reset_all()

top_k = st.slider("Top K Stations", min_value=5, max_value=100, value=20, step=5)

ranking_result = _load_top_stations_full_history()
ranking_frame = _require_result(ranking_result, "No subway station reliability rows available in Gold ranking data.")
if ranking_frame.empty:
    st.stop()

ranking_metric_column, ranking_metric_notice = _resolve_metric_column(
    ranking_frame,
    selected_metric_label,
    level="summary",
)
if ranking_metric_notice is not None:
    st.info(ranking_metric_notice)

ranking_frame = _sort_ranking_frame(ranking_frame, selected_metric_label, ranking_metric_column)

if st.session_state[STATE_STATION] is None:
    st.session_state[STATE_STATION] = str(ranking_frame.iloc[0]["station_name"])
else:
    station_in_data = ranking_frame["station_name"].astype(str).eq(str(st.session_state[STATE_STATION])).any()
    if not station_in_data:
        st.session_state[STATE_STATION] = str(ranking_frame.iloc[0]["station_name"])
        _clear_from("station")

top_frame = ranking_frame.head(top_k).copy()
st.markdown("### 1. Top K Subway Stations Across Full History")
if selected_metric_label == "Composite Score":
    station_chart_title = "Worst Subway Stations by Full-History Composite Reliability Score"
    station_tooltip = [
        "station_name:N",
        "rank_position:Q",
        "composite_score:Q",
        "frequency:Q",
        "severity_p90:Q",
        "regularity_p90:Q",
    ]
else:
    station_chart_title = f"Worst Subway Stations by Full-History {selected_metric_label}"
    station_tooltip = ["station_name:N", "rank_position:Q"] + [
        f"{field}:Q" for field in _metric_display_fields(selected_metric_label, "summary")
    ]

station_event = _render_clickable_horizontal(
    frame=top_frame,
    x_field=ranking_metric_column,
    y_field="station_name",
    selected_value=st.session_state[STATE_STATION],
    selection_name="station_pick",
    title=station_chart_title,
    tooltip=station_tooltip,
)
station_clicked = _extract_selection_value(station_event, "station_pick", "station_name")
if station_clicked is not None and str(station_clicked) != str(st.session_state[STATE_STATION]):
    st.session_state[STATE_STATION] = str(station_clicked)
    _clear_from("station")

station_options = top_frame["station_name"].astype(str).tolist()
station_index = (
    station_options.index(str(st.session_state[STATE_STATION]))
    if str(st.session_state[STATE_STATION]) in station_options
    else 0
)
station_select = st.selectbox("Selected station", options=station_options, index=station_index)
if station_select != str(st.session_state[STATE_STATION]):
    st.session_state[STATE_STATION] = station_select
    _clear_from("station")

selected_station = str(st.session_state[STATE_STATION])
station_row = _station_by_id(ranking_frame, selected_station)
if station_row is not None:
    _show_station_summary(station_row, selected_station)
if selected_metric_label != "Composite Score":
    st.caption(f"Primary metric selected: {selected_metric_label}")

st.markdown("---")
st.markdown("### 2. Selected Station by Year")
year_result = _load_station_year_metrics(selected_station)
year_frame = _require_result(year_result, "No yearly slices available for the selected station.")
if year_frame.empty:
    st.stop()

year_frame["year"] = year_frame["year"].astype(int)
year_frame = year_frame.sort_values("year")
year_metric_column, year_metric_notice = _resolve_metric_column(year_frame, selected_metric_label, level="summary")
if year_metric_notice is not None and year_metric_notice != ranking_metric_notice:
    st.info(year_metric_notice)

if st.session_state[STATE_YEAR] is None:
    st.session_state[STATE_YEAR] = int(year_frame.iloc[0]["year"])
else:
    if int(st.session_state[STATE_YEAR]) not in set(year_frame["year"].tolist()):
        st.session_state[STATE_YEAR] = int(year_frame.iloc[0]["year"])
        _clear_from("year")

year_event = _render_clickable_bar(
    frame=year_frame,
    x_field="year",
    y_field=year_metric_column,
    selected_value=st.session_state[STATE_YEAR],
    selected_field="year",
    selection_name="year_pick",
    title=(
        f"Station {selected_station} Reliability by Year"
        if selected_metric_label == "Composite Score"
        else f"Station {selected_station} Reliability by Year ({selected_metric_label})"
    ),
    tooltip=(
        [
            "year:N",
            "composite_score:Q",
            "frequency:Q",
            "severity_p90:Q",
            "regularity_p90:Q",
        ]
        if selected_metric_label == "Composite Score"
        else ["year:N", year_metric_column + ":Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"]
    ),
)
year_clicked = _extract_selection_value(year_event, "year_pick", "year")
if year_clicked is not None:
    year_clicked_int = int(float(year_clicked))
    if year_clicked_int != int(st.session_state[STATE_YEAR]):
        st.session_state[STATE_YEAR] = year_clicked_int
        _clear_from("year")

year_options = [int(v) for v in year_frame["year"].tolist()]
year_index = year_options.index(int(st.session_state[STATE_YEAR]))
year_select = st.selectbox("Selected year", options=year_options, index=year_index)
if int(year_select) != int(st.session_state[STATE_YEAR]):
    st.session_state[STATE_YEAR] = int(year_select)
    _clear_from("year")

selected_year = int(st.session_state[STATE_YEAR])
st.info(f"Context: Station {selected_station} | Year {selected_year}")

st.markdown("---")
st.markdown("### 3. Selected Station Within Year by Month")
month_result = _load_station_month_metrics(selected_station, selected_year)
month_frame = _require_result(month_result, "No month-level slices available for the selected station/year.")
if month_frame.empty:
    st.stop()

month_frame["month_num"] = month_frame["month_num"].astype(int)
month_frame = month_frame.sort_values("month_num")
month_frame["month_axis"] = month_frame["month_label"] + " (" + month_frame["month_num"].astype(str).str.zfill(2) + ")"
month_metric_column, month_metric_notice = _resolve_metric_column(month_frame, selected_metric_label, level="summary")
if month_metric_notice is not None and month_metric_notice not in {ranking_metric_notice, year_metric_notice}:
    st.info(month_metric_notice)

if st.session_state[STATE_MONTH] is None:
    st.session_state[STATE_MONTH] = int(month_frame.iloc[0]["month_num"])
else:
    if int(st.session_state[STATE_MONTH]) not in set(month_frame["month_num"].tolist()):
        st.session_state[STATE_MONTH] = int(month_frame.iloc[0]["month_num"])
        _clear_from("month")

month_event = _render_clickable_bar(
    frame=month_frame,
    x_field="month_axis",
    y_field=month_metric_column,
    selected_value=st.session_state[STATE_MONTH],
    selected_field="month_num",
    selection_name="month_pick",
    title=(
        f"Station {selected_station} Reliability by Month in {selected_year}"
        if selected_metric_label == "Composite Score"
        else f"Station {selected_station} Reliability by Month in {selected_year} ({selected_metric_label})"
    ),
    tooltip=(
        [
            "month_axis:N",
            "composite_score:Q",
            "frequency:Q",
            "severity_p90:Q",
            "regularity_p90:Q",
        ]
        if selected_metric_label == "Composite Score"
        else ["month_axis:N", month_metric_column + ":Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"]
    ),
    x_sort=month_frame["month_axis"].tolist(),
)
month_clicked = _extract_selection_value(month_event, "month_pick", "month_num")
if month_clicked is not None:
    month_clicked_int = int(float(month_clicked))
    if month_clicked_int != int(st.session_state[STATE_MONTH]):
        st.session_state[STATE_MONTH] = month_clicked_int
        _clear_from("month")

month_options = [int(v) for v in month_frame["month_num"].tolist()]
month_names = {
    int(row["month_num"]): str(row["month_label"])
    for _, row in month_frame[["month_num", "month_label"]].drop_duplicates().iterrows()
}
month_index = month_options.index(int(st.session_state[STATE_MONTH]))
month_select = st.selectbox(
    "Selected month",
    options=month_options,
    index=month_index,
    format_func=lambda m: f"{month_names.get(m, 'M')} ({m:02d})",
)
if int(month_select) != int(st.session_state[STATE_MONTH]):
    st.session_state[STATE_MONTH] = int(month_select)
    _clear_from("month")

selected_month = int(st.session_state[STATE_MONTH])

branch_left, branch_right = st.columns(2)

with branch_left:
    st.markdown("### 4A. Selected Month by Day")
    daily_result = _load_station_daily_month_metrics(selected_station, selected_year, selected_month)
    daily_frame = _require_result(daily_result, "No day-level rows available for this station/year/month.")
    if not daily_frame.empty:
        daily_frame["service_date"] = pd.to_datetime(daily_frame["service_date"])
        daily_frame["day_key"] = daily_frame["service_date"].dt.date.astype(str)
        daily_frame = daily_frame.sort_values("service_date")
        unstable_daily = selected_metric_label == "Composite Score" and _is_composite_unstable(daily_frame, min_points=7)
        daily_metric_column, daily_metric_notice = _resolve_metric_column(
            daily_frame,
            selected_metric_label,
            level="drill",
            composite_unstable=unstable_daily,
        )
        if daily_metric_notice is not None:
            month_context = month_frame[month_frame["month_num"] == selected_month]
            context_score = month_context["composite_score"].iloc[0] if not month_context.empty else pd.NA
            if unstable_daily and selected_metric_label == "Composite Score":
                st.caption(
                    "Composite is unstable at daily granularity for this slice. "
                    f"Fallback metric is active. Month-level composite context: {fmt_float(context_score, digits=3)}."
                )
            else:
                st.info(daily_metric_notice)

        if st.session_state[STATE_DAY] is None:
            st.session_state[STATE_DAY] = str(daily_frame.iloc[0]["day_key"])
        else:
            if str(st.session_state[STATE_DAY]) not in set(daily_frame["day_key"].astype(str).tolist()):
                st.session_state[STATE_DAY] = str(daily_frame.iloc[0]["day_key"])
                _clear_from("day")

        daily_event = _render_clickable_bar(
            frame=daily_frame,
            x_field="day_key",
            y_field=daily_metric_column,
            selected_value=st.session_state[STATE_DAY],
            selected_field="day_key",
            selection_name="day_pick",
            title=(
                f"Station {selected_station} Daily Pattern in {selected_year}-{selected_month:02d}"
                if selected_metric_label == "Composite Score"
                else f"Station {selected_station} Daily Pattern in {selected_year}-{selected_month:02d} ({selected_metric_label})"
            ),
            tooltip=(
                [
                    "day_key:N",
                    "incident_count:Q",
                    "avg_delay:Q",
                    "p90_delay:Q",
                    "p90_gap:Q",
                    "composite_score:Q",
                ]
                if selected_metric_label == "Composite Score"
                else ["day_key:N", daily_metric_column + ":Q", "incident_count:Q", "avg_delay:Q", "p90_delay:Q", "p90_gap:Q", "cause_mix_score:Q", "composite_score:Q"]
            ),
            x_sort=daily_frame["day_key"].tolist(),
        )
        day_clicked = _extract_selection_value(daily_event, "day_pick", "day_key")
        if day_clicked is not None and str(day_clicked) != str(st.session_state[STATE_DAY]):
            st.session_state[STATE_DAY] = str(day_clicked)
            _clear_from("day")

        day_options = daily_frame["day_key"].astype(str).tolist()
        day_index = day_options.index(str(st.session_state[STATE_DAY])) if str(st.session_state[STATE_DAY]) in day_options else 0
        day_select = st.selectbox("Selected day", options=day_options, index=day_index)
        if day_select != str(st.session_state[STATE_DAY]):
            st.session_state[STATE_DAY] = day_select
            _clear_from("day")

        _render_line_chart(
            frame=daily_frame,
            x_field="service_date:T",
            y_field=f"{daily_metric_column}:Q",
            title=(
                f"Station {selected_station} Daily Pattern in {selected_year}-{selected_month:02d}"
                if selected_metric_label == "Composite Score"
                else f"Station {selected_station} Daily Pattern in {selected_year}-{selected_month:02d} ({selected_metric_label})"
            ),
            tooltip=[
                "service_date:T",
                "incident_count:Q",
                "avg_delay:Q",
                "p90_delay:Q",
                "p90_gap:Q",
                "cause_mix_score:Q",
                "composite_score:Q",
            ],
        )

with branch_right:
    st.markdown("### 4B. Selected Year by Weekday")
    weekday_result = _load_station_weekday_metrics(selected_station, selected_year)
    weekday_frame = _require_result(weekday_result, "No weekday-level rows available for this station/year.")
    if not weekday_frame.empty:
        weekday_frame["day_name"] = pd.Categorical(
            weekday_frame["day_name"],
            categories=DAY_NAME_ORDER,
            ordered=True,
        )
        weekday_frame = weekday_frame.sort_values("day_name")
        weekday_metric_column, weekday_metric_notice = _resolve_metric_column(weekday_frame, selected_metric_label, level="summary")
        if weekday_metric_notice is not None and weekday_metric_notice not in {ranking_metric_notice, year_metric_notice, month_metric_notice}:
            st.info(weekday_metric_notice)

        if st.session_state[STATE_WEEKDAY] is None:
            st.session_state[STATE_WEEKDAY] = str(weekday_frame.iloc[0]["day_name"])
        else:
            if str(st.session_state[STATE_WEEKDAY]) not in set(weekday_frame["day_name"].astype(str).tolist()):
                st.session_state[STATE_WEEKDAY] = str(weekday_frame.iloc[0]["day_name"])
                _clear_from("weekday")

        weekday_event = _render_clickable_bar(
            frame=weekday_frame,
            x_field="day_name",
            y_field=weekday_metric_column,
            selected_value=st.session_state[STATE_WEEKDAY],
            selected_field="day_name",
            selection_name="weekday_pick",
            title=(
                f"Station {selected_station} Reliability by Weekday in {selected_year}"
                if selected_metric_label == "Composite Score"
                else f"Station {selected_station} Reliability by Weekday in {selected_year} ({selected_metric_label})"
            ),
            tooltip=(
                [
                    "day_name:N",
                    "composite_score:Q",
                    "incident_count:Q",
                    "p90_delay:Q",
                    "p90_gap:Q",
                ]
                if selected_metric_label == "Composite Score"
                else ["day_name:N", weekday_metric_column + ":Q", "incident_count:Q", "avg_delay:Q", "p90_delay:Q", "p90_gap:Q", "cause_mix_score:Q", "composite_score:Q"]
            ),
            x_sort=DAY_NAME_ORDER,
        )
        weekday_clicked = _extract_selection_value(weekday_event, "weekday_pick", "day_name")
        if weekday_clicked is not None and str(weekday_clicked) != str(st.session_state[STATE_WEEKDAY]):
            st.session_state[STATE_WEEKDAY] = str(weekday_clicked)
            _clear_from("weekday")

        weekday_options = [str(v) for v in weekday_frame["day_name"].astype(str).tolist()]
        weekday_index = (
            weekday_options.index(str(st.session_state[STATE_WEEKDAY]))
            if str(st.session_state[STATE_WEEKDAY]) in weekday_options
            else 0
        )
        weekday_select = st.selectbox("Selected weekday", options=weekday_options, index=weekday_index)
        if weekday_select != str(st.session_state[STATE_WEEKDAY]):
            st.session_state[STATE_WEEKDAY] = weekday_select
            _clear_from("weekday")

if st.session_state[STATE_WEEKDAY] is not None:
    selected_weekday = str(st.session_state[STATE_WEEKDAY])
    st.markdown("---")
    st.markdown("### 5. Weekday to Time Bins")
    st.info(f"Context: Station {selected_station} | Year {selected_year} | Weekday {selected_weekday}")

    time_bin_result = _load_station_time_bin_metrics(selected_station, selected_year, selected_weekday)
    time_bin_frame = _require_result(time_bin_result, "No time-bin rows available for this station/year/weekday.")
    if not time_bin_frame.empty:
        time_bin_frame["bin_order"] = time_bin_frame["time_bin"].map(TIME_BIN_ORDER_MAP).fillna(99).astype(int)
        time_bin_frame = time_bin_frame.sort_values("bin_order")
        unstable_time = selected_metric_label == "Composite Score" and _is_composite_unstable(time_bin_frame, min_points=6)
        time_metric_column, time_metric_notice = _resolve_metric_column(
            time_bin_frame,
            selected_metric_label,
            level="drill",
            composite_unstable=unstable_time,
        )
        if time_metric_notice is not None:
            if unstable_time and selected_metric_label == "Composite Score":
                st.caption(
                    "Composite is unstable at fine time-bin granularity for this slice. "
                    "Fallback metrics are shown while preserving higher-level composite context."
                )
            else:
                st.info(time_metric_notice)

        if st.session_state[STATE_TIME_BIN] is None:
            st.session_state[STATE_TIME_BIN] = str(time_bin_frame.iloc[0]["time_bin"])
        else:
            if str(st.session_state[STATE_TIME_BIN]) not in set(time_bin_frame["time_bin"].astype(str).tolist()):
                st.session_state[STATE_TIME_BIN] = str(time_bin_frame.iloc[0]["time_bin"])
                _clear_from("weekday")

        time_bin_event = _render_clickable_bar(
            frame=time_bin_frame,
            x_field="time_bin",
            y_field=time_metric_column,
            selected_value=st.session_state[STATE_TIME_BIN],
            selected_field="time_bin",
            selection_name="time_bin_pick",
            title=(
                f"Station {selected_station} Time-Bin Reliability on {selected_weekday} ({selected_year})"
                if selected_metric_label == "Composite Score"
                else f"Station {selected_station} Time-Bin Reliability on {selected_weekday} ({selected_year}) ({selected_metric_label})"
            ),
            tooltip=(
                [
                    "time_bin:N",
                    "incident_count:Q",
                    "avg_delay:Q",
                    "p90_delay:Q",
                    "p90_gap:Q",
                    "composite_score:Q",
                ]
                if selected_metric_label == "Composite Score"
                else ["time_bin:N", time_metric_column + ":Q", "incident_count:Q", "avg_delay:Q", "p90_delay:Q", "p90_gap:Q", "cause_mix_score:Q", "composite_score:Q"]
            ),
            x_sort=TIME_BIN_LABELS,
        )
        time_bin_clicked = _extract_selection_value(time_bin_event, "time_bin_pick", "time_bin")
        if time_bin_clicked is not None and str(time_bin_clicked) != str(st.session_state[STATE_TIME_BIN]):
            st.session_state[STATE_TIME_BIN] = str(time_bin_clicked)
            _clear_from("weekday")

        time_bin_options = [str(v) for v in time_bin_frame["time_bin"].astype(str).tolist()]
        time_bin_index = (
            time_bin_options.index(str(st.session_state[STATE_TIME_BIN]))
            if str(st.session_state[STATE_TIME_BIN]) in time_bin_options
            else 0
        )
        time_bin_select = st.selectbox("Selected time bin", options=time_bin_options, index=time_bin_index)
        if time_bin_select != str(st.session_state[STATE_TIME_BIN]):
            st.session_state[STATE_TIME_BIN] = time_bin_select
