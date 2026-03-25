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
from ttc_pulse.dashboard.loaders import query_table


STATE_ROUTE = "bus_drill_route_id"
STATE_YEAR = "bus_drill_year"
STATE_MONTH = "bus_drill_month"
STATE_WEEKDAY = "bus_drill_weekday"

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
    for key in [STATE_ROUTE, STATE_YEAR, STATE_MONTH, STATE_WEEKDAY]:
        if key not in st.session_state:
            st.session_state[key] = None


def _clear_from(level: str) -> None:
    if level == "route":
        st.session_state[STATE_YEAR] = None
        st.session_state[STATE_MONTH] = None
        st.session_state[STATE_WEEKDAY] = None
    elif level == "year":
        st.session_state[STATE_MONTH] = None
        st.session_state[STATE_WEEKDAY] = None
    elif level == "month":
        pass
    elif level == "weekday":
        pass


def _reset_all() -> None:
    st.session_state[STATE_ROUTE] = None
    st.session_state[STATE_YEAR] = None
    st.session_state[STATE_MONTH] = None
    st.session_state[STATE_WEEKDAY] = None


def _step_back() -> None:
    if st.session_state[STATE_WEEKDAY] is not None:
        st.session_state[STATE_WEEKDAY] = None
        return
    if st.session_state[STATE_MONTH] is not None:
        st.session_state[STATE_MONTH] = None
        return
    if st.session_state[STATE_YEAR] is not None:
        st.session_state[STATE_YEAR] = None
        return
    if st.session_state[STATE_ROUTE] is not None:
        st.session_state[STATE_ROUTE] = None


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


@st.cache_data(ttl=180)
def _load_top_routes_full_history() -> Any:
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
            CAST(frequency AS BIGINT) AS total_incidents,
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
    )


@st.cache_data(ttl=180)
def _load_route_year_metrics(route_id: str) -> Any:
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        WITH year_agg AS (
            SELECT
                CAST(EXTRACT(YEAR FROM service_date) AS INTEGER) AS year,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE mode = 'bus'
                AND route_id_gtfs = ?
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
        params=[route_id],
    )


@st.cache_data(ttl=180)
def _load_route_month_metrics(route_id: str, year: int) -> Any:
    return query_table(
        table_name="gold_route_time_metrics",
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
            WHERE mode = 'bus'
                AND route_id_gtfs = ?
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
        params=[route_id, year],
    )


@st.cache_data(ttl=180)
def _load_route_daily_month_metrics(route_id: str, year: int, month: int) -> Any:
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        WITH day_agg AS (
            SELECT
                service_date,
                SUM(frequency)::DOUBLE AS incident_count,
                SUM(COALESCE(severity_median, 0.0) * frequency) / NULLIF(SUM(frequency), 0.0) AS avg_delay,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS p90_delay,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS p90_gap,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE mode = 'bus'
                AND route_id_gtfs = ?
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
        params=[route_id, year, month],
    )


@st.cache_data(ttl=180)
def _load_route_weekday_metrics(route_id: str, year: int) -> Any:
    return query_table(
        table_name="gold_route_time_metrics",
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
                SUM(COALESCE(severity_median, 0.0) * frequency) / NULLIF(SUM(frequency), 0.0) AS avg_delay,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS p90_delay,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS p90_gap,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE mode = 'bus'
                AND route_id_gtfs = ?
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
        params=[route_id, year],
    )


@st.cache_data(ttl=180)
def _load_route_time_bin_metrics(route_id: str, year: int, day_name: str) -> Any:
    return query_table(
        table_name="gold_route_time_metrics",
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
                SUM(COALESCE(severity_median, 0.0) * frequency) / NULLIF(SUM(frequency), 0.0) AS avg_delay,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS p90_delay,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS p90_gap,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE mode = 'bus'
                AND route_id_gtfs = ?
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
        """,
        params=[route_id, year, day_name],
    )


@st.cache_data(ttl=180)
def _load_route_top_cause(route_id: str) -> Any:
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            incident_category,
            SUM(event_count)::BIGINT AS incident_count
        FROM {source}
        WHERE mode = 'bus'
            AND route_id_gtfs = ?
        GROUP BY 1
        ORDER BY incident_count DESC, incident_category
        LIMIT 1
        """,
        params=[route_id],
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
            color=alt.Color("is_selected:N", legend=None, scale=alt.Scale(domain=[True, False], range=["#ea580c", "#2563eb"])),
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
            color=alt.Color("is_selected:N", legend=None, scale=alt.Scale(domain=[True, False], range=["#ea580c", "#0f766e"])),
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
    parts = ["Top routes"]
    route = st.session_state[STATE_ROUTE]
    year = st.session_state[STATE_YEAR]
    month = st.session_state[STATE_MONTH]
    weekday = st.session_state[STATE_WEEKDAY]

    if route is not None:
        parts.append(f"Route {route}")
    if year is not None:
        parts.append(f"Year {year}")
    if month is not None:
        parts.append(f"Month {month:02d}")
    if weekday is not None:
        parts.append(f"Weekday {weekday}")
    return " > ".join(parts)


def _show_route_summary(route_row: pd.Series, route_id: str) -> None:
    cause_result = _load_route_top_cause(route_id)
    top_cause = "-"
    if cause_result.status == "ok" and not cause_result.frame.empty:
        cause_value = cause_result.frame.iloc[0]["incident_category"]
        top_cause = str(cause_value) if pd.notna(cause_value) else "-"

    st.markdown("#### Selected Route Summary")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Overall Composite", fmt_float(route_row["composite_score"], digits=3))
    c2.metric("Total Incidents", fmt_int(route_row["total_incidents"]))
    c3.metric("P90 Delay (min)", fmt_float(route_row["severity_p90"], digits=1))
    c4.metric("P90 Gap (min)", fmt_float(route_row["regularity_p90"], digits=1))
    c5.metric("Top Cause Category", top_cause)


def _route_by_id(frame: pd.DataFrame, route_id: str) -> pd.Series | None:
    matched = frame[frame["route_id"].astype(str) == str(route_id)]
    if matched.empty:
        return None
    return matched.iloc[0]


def _prep_status_frame(result: Any) -> pd.DataFrame:
    frame = result.frame.copy()
    if "service_date" in frame.columns:
        frame["service_date"] = pd.to_datetime(frame["service_date"])
    if "month_start" in frame.columns:
        frame["month_start"] = pd.to_datetime(frame["month_start"])
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

st.title("Bus Reliability Drill-Down")
st.caption(
    "Guided drill-down for TTC bus reliability (full history): route ranking -> year -> month -> day or weekday -> time-of-day."
)

ctrl_left, ctrl_mid, ctrl_right = st.columns([2, 1, 1])
ctrl_left.info(f"Breadcrumb: {_build_breadcrumb()}")
if ctrl_mid.button("Back One Level"):
    _step_back()
if ctrl_right.button("Reset Drill-Down"):
    _reset_all()

top_k = st.slider("Top K Routes", min_value=5, max_value=100, value=20, step=5)

ranking_result = _load_top_routes_full_history()
ranking_frame = _require_result(ranking_result, "No bus route reliability rows available in Gold route metrics.")
if ranking_frame.empty:
    st.stop()

ranking_frame = ranking_frame.sort_values(["rank_position", "route_id"]).reset_index(drop=True)

if st.session_state[STATE_ROUTE] is None:
    st.session_state[STATE_ROUTE] = str(ranking_frame.iloc[0]["route_id"])
else:
    route_in_data = ranking_frame["route_id"].astype(str).eq(str(st.session_state[STATE_ROUTE])).any()
    if not route_in_data:
        st.session_state[STATE_ROUTE] = str(ranking_frame.iloc[0]["route_id"])
        _clear_from("route")

top_frame = ranking_frame.head(top_k).copy()
st.markdown("### 1. Top K Bus Routes Across Full History")
route_event = _render_clickable_horizontal(
    frame=top_frame,
    x_field="composite_score",
    y_field="route_id",
    selected_value=st.session_state[STATE_ROUTE],
    selection_name="route_pick",
    title="Worst Routes by Full-History Composite Reliability Score",
    tooltip=[
        "route_id:N",
        "rank_position:Q",
        "composite_score:Q",
        "total_incidents:Q",
        "severity_p90:Q",
        "regularity_p90:Q",
    ],
)
route_clicked = _extract_selection_value(route_event, "route_pick", "route_id")
if route_clicked is not None and str(route_clicked) != str(st.session_state[STATE_ROUTE]):
    st.session_state[STATE_ROUTE] = str(route_clicked)
    _clear_from("route")

route_options = top_frame["route_id"].astype(str).tolist()
route_index = route_options.index(str(st.session_state[STATE_ROUTE])) if str(st.session_state[STATE_ROUTE]) in route_options else 0
route_select = st.selectbox("Selected route", options=route_options, index=route_index)
if route_select != str(st.session_state[STATE_ROUTE]):
    st.session_state[STATE_ROUTE] = route_select
    _clear_from("route")

selected_route = str(st.session_state[STATE_ROUTE])
route_row = _route_by_id(ranking_frame, selected_route)
if route_row is not None:
    _show_route_summary(route_row, selected_route)

st.markdown("---")
st.markdown("### 2. Selected Route by Year")
year_result = _load_route_year_metrics(selected_route)
year_frame = _require_result(year_result, "No yearly slices available for the selected route.")
if year_frame.empty:
    st.stop()

year_frame["year"] = year_frame["year"].astype(int)
year_frame = year_frame.sort_values("year")

if st.session_state[STATE_YEAR] is None:
    st.session_state[STATE_YEAR] = int(year_frame.iloc[0]["year"])
else:
    if int(st.session_state[STATE_YEAR]) not in set(year_frame["year"].tolist()):
        st.session_state[STATE_YEAR] = int(year_frame.iloc[0]["year"])
        _clear_from("year")

year_event = _render_clickable_bar(
    frame=year_frame,
    x_field="year",
    y_field="composite_score",
    selected_value=st.session_state[STATE_YEAR],
    selected_field="year",
    selection_name="year_pick",
    title=f"Route {selected_route} Reliability by Year",
    tooltip=[
        "year:N",
        "composite_score:Q",
        "frequency:Q",
        "severity_p90:Q",
        "regularity_p90:Q",
    ],
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
st.info(f"Context: Route {selected_route} | Year {selected_year}")

st.markdown("---")
st.markdown("### 3. Selected Route Within Year by Month")
month_result = _load_route_month_metrics(selected_route, selected_year)
month_frame = _require_result(month_result, "No month-level slices available for the selected route/year.")
if month_frame.empty:
    st.stop()

month_frame["month_num"] = month_frame["month_num"].astype(int)
month_frame = month_frame.sort_values("month_num")
month_frame["month_axis"] = month_frame["month_label"] + " (" + month_frame["month_num"].astype(str).str.zfill(2) + ")"

if st.session_state[STATE_MONTH] is None:
    st.session_state[STATE_MONTH] = int(month_frame.iloc[0]["month_num"])
else:
    if int(st.session_state[STATE_MONTH]) not in set(month_frame["month_num"].tolist()):
        st.session_state[STATE_MONTH] = int(month_frame.iloc[0]["month_num"])

month_event = _render_clickable_bar(
    frame=month_frame,
    x_field="month_axis",
    y_field="composite_score",
    selected_value=st.session_state[STATE_MONTH],
    selected_field="month_num",
    selection_name="month_pick",
    title=f"Route {selected_route} Reliability by Month in {selected_year}",
    tooltip=[
        "month_axis:N",
        "composite_score:Q",
        "frequency:Q",
        "severity_p90:Q",
        "regularity_p90:Q",
    ],
    x_sort=month_frame["month_axis"].tolist(),
)
month_clicked = _extract_selection_value(month_event, "month_pick", "month_num")
if month_clicked is not None:
    month_clicked_int = int(float(month_clicked))
    if month_clicked_int != int(st.session_state[STATE_MONTH]):
        st.session_state[STATE_MONTH] = month_clicked_int

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

selected_month = int(st.session_state[STATE_MONTH])

branch_left, branch_right = st.columns(2)

with branch_left:
    st.markdown("### 4A. Selected Month by Day")
    daily_result = _load_route_daily_month_metrics(selected_route, selected_year, selected_month)
    daily_frame = _require_result(daily_result, "No day-level rows available for this route/year/month.")
    if not daily_frame.empty:
        daily_frame["service_date"] = pd.to_datetime(daily_frame["service_date"])
        unstable_daily = _is_composite_unstable(daily_frame, min_points=7)
        daily_metric_options = {
            "composite_score": "Composite score",
            "incident_count": "Incident count",
            "avg_delay": "Average delay (min)",
            "p90_delay": "P90 delay (min)",
        }
        default_daily = "incident_count" if unstable_daily else "composite_score"
        daily_metric = st.selectbox(
            "Daily metric",
            options=list(daily_metric_options.keys()),
            index=list(daily_metric_options.keys()).index(default_daily),
            format_func=lambda k: daily_metric_options[k],
            key="bus_drill_daily_metric",
        )
        if unstable_daily:
            month_context = month_frame[month_frame["month_num"] == selected_month]
            context_score = (
                month_context["composite_score"].iloc[0] if not month_context.empty else pd.NA
            )
            st.caption(
                "Composite is unstable at daily granularity for this slice. "
                f"Fallback metric is active. Month-level composite context: {fmt_float(context_score, digits=3)}."
            )
            if daily_metric == "composite_score":
                daily_metric = "incident_count"

        _render_line_chart(
            frame=daily_frame,
            x_field="service_date:T",
            y_field=f"{daily_metric}:Q",
            title=f"Route {selected_route} Daily Pattern in {selected_year}-{selected_month:02d}",
            tooltip=[
                "service_date:T",
                "incident_count:Q",
                "avg_delay:Q",
                "p90_delay:Q",
                "p90_gap:Q",
                "composite_score:Q",
            ],
        )

with branch_right:
    st.markdown("### 4B. Selected Year by Weekday")
    weekday_result = _load_route_weekday_metrics(selected_route, selected_year)
    weekday_frame = _require_result(weekday_result, "No weekday-level rows available for this route/year.")
    if not weekday_frame.empty:
        weekday_frame["day_name"] = pd.Categorical(
            weekday_frame["day_name"],
            categories=DAY_NAME_ORDER,
            ordered=True,
        )
        weekday_frame = weekday_frame.sort_values("day_name")

        if st.session_state[STATE_WEEKDAY] is None:
            st.session_state[STATE_WEEKDAY] = str(weekday_frame.iloc[0]["day_name"])
        else:
            if str(st.session_state[STATE_WEEKDAY]) not in set(weekday_frame["day_name"].astype(str).tolist()):
                st.session_state[STATE_WEEKDAY] = str(weekday_frame.iloc[0]["day_name"])

        weekday_event = _render_clickable_bar(
            frame=weekday_frame,
            x_field="day_name",
            y_field="composite_score",
            selected_value=st.session_state[STATE_WEEKDAY],
            selected_field="day_name",
            selection_name="weekday_pick",
            title=f"Route {selected_route} Reliability by Weekday in {selected_year}",
            tooltip=[
                "day_name:N",
                "composite_score:Q",
                "incident_count:Q",
                "p90_delay:Q",
                "p90_gap:Q",
            ],
            x_sort=DAY_NAME_ORDER,
        )
        weekday_clicked = _extract_selection_value(weekday_event, "weekday_pick", "day_name")
        if weekday_clicked is not None and str(weekday_clicked) != str(st.session_state[STATE_WEEKDAY]):
            st.session_state[STATE_WEEKDAY] = str(weekday_clicked)

        weekday_options = [str(v) for v in weekday_frame["day_name"].astype(str).tolist()]
        weekday_index = (
            weekday_options.index(str(st.session_state[STATE_WEEKDAY]))
            if str(st.session_state[STATE_WEEKDAY]) in weekday_options
            else 0
        )
        weekday_select = st.selectbox("Selected weekday", options=weekday_options, index=weekday_index)
        if weekday_select != str(st.session_state[STATE_WEEKDAY]):
            st.session_state[STATE_WEEKDAY] = weekday_select

if st.session_state[STATE_WEEKDAY] is not None:
    selected_weekday = str(st.session_state[STATE_WEEKDAY])
    st.markdown("---")
    st.markdown("### 5. Weekday to Time-of-Day Bins")
    st.info(f"Context: Route {selected_route} | Year {selected_year} | Weekday {selected_weekday}")

    time_bin_result = _load_route_time_bin_metrics(selected_route, selected_year, selected_weekday)
    time_bin_frame = _require_result(time_bin_result, "No time-bin rows available for this route/year/weekday.")
    if not time_bin_frame.empty:
        time_bin_frame["bin_order"] = time_bin_frame["time_bin"].map(TIME_BIN_ORDER_MAP).fillna(99).astype(int)
        time_bin_frame = time_bin_frame.sort_values("bin_order")

        unstable_time = _is_composite_unstable(time_bin_frame, min_points=6)
        time_metric_options = {
            "composite_score": "Composite score",
            "incident_count": "Incident count",
            "avg_delay": "Average delay (min)",
            "p90_delay": "P90 delay (min)",
        }
        default_time_metric = "incident_count" if unstable_time else "composite_score"
        time_metric = st.selectbox(
            "Time-bin metric",
            options=list(time_metric_options.keys()),
            index=list(time_metric_options.keys()).index(default_time_metric),
            format_func=lambda k: time_metric_options[k],
            key="bus_drill_time_metric",
        )
        if unstable_time:
            st.caption(
                "Composite is unstable at fine time-bin granularity for this slice. "
                "Fallback metrics are shown while preserving higher-level composite context."
            )
            if time_metric == "composite_score":
                time_metric = "incident_count"

        chart = (
            alt.Chart(time_bin_frame)
            .mark_bar()
            .encode(
                x=alt.X("time_bin:N", sort=TIME_BIN_LABELS),
                y=alt.Y(f"{time_metric}:Q"),
                tooltip=[
                    "time_bin:N",
                    "incident_count:Q",
                    "avg_delay:Q",
                    "p90_delay:Q",
                    "p90_gap:Q",
                    "composite_score:Q",
                ],
            )
            .properties(
                title=f"Route {selected_route} Time-of-Day Reliability on {selected_weekday} ({selected_year})",
                height=320,
            )
        )
        st.altair_chart(chart, use_container_width=True)

st.caption(
    "Data source policy: this page reads only DuckDB/Gold marts with parquet fallback (`gold_route_time_metrics`, "
    "`gold_delay_events_core`)."
)
