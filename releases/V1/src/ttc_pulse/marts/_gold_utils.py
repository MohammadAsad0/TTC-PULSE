"""Shared utilities for Step 3 Gold mart builders."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ttc_pulse.utils.project_setup import quote_identifier, sql_literal

STEP2_TABLE_FALLBACKS: dict[str, str] = {
    "fact_delay_events_norm": "silver/fact_delay_events_norm.parquet",
    "fact_gtfsrt_alerts_norm": "silver/fact_gtfsrt_alerts_norm.parquet",
    "dim_route_gtfs": "dimensions/dim_route_gtfs.parquet",
    "dim_stop_gtfs": "dimensions/dim_stop_gtfs.parquet",
    "gold_route_time_metrics": "gold/gold_route_time_metrics.parquet",
    "gold_station_time_metrics": "gold/gold_station_time_metrics.parquet",
}


def table_exists(connection: Any, table_name: str) -> bool:
    """Check whether a table exists in the main schema."""
    query = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'main' AND table_name = ?
    LIMIT 1
    """
    return connection.execute(query, [table_name]).fetchone() is not None


def get_table_columns(connection: Any, table_name: str) -> set[str]:
    """Return a set of columns for table_name (empty if table does not exist)."""
    if not table_exists(connection, table_name):
        return set()
    rows = connection.execute(
        f"PRAGMA table_info({sql_literal(table_name)})"
    ).fetchall()
    return {str(row[1]) for row in rows}


def ensure_table_from_parquet(
    connection: Any,
    table_name: str,
    project_root: Path,
    caveats: list[str],
) -> str:
    """Ensure a source table is available, loading from fallback parquet when needed."""
    if table_exists(connection, table_name):
        return "existing"

    relative_fallback = STEP2_TABLE_FALLBACKS.get(table_name)
    if relative_fallback is None:
        caveats.append(f"{table_name}: no fallback parquet configured; using empty source.")
        return "missing"

    parquet_path = (project_root / relative_fallback).resolve()
    if not parquet_path.exists():
        caveats.append(
            f"{table_name}: fallback parquet not found at {parquet_path.as_posix()}; using empty source."
        )
        return "missing"

    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {quote_identifier(table_name)} AS
        SELECT * FROM read_parquet({sql_literal(parquet_path.as_posix())})
        """
    )
    caveats.append(
        f"{table_name}: table was missing in DuckDB and was loaded from {relative_fallback}."
    )
    return "loaded_from_parquet"


def _text_expr(columns: set[str], column_name: str) -> str:
    if column_name not in columns:
        return "NULL::VARCHAR"
    return f"NULLIF(TRIM(CAST({quote_identifier(column_name)} AS VARCHAR)), '')"


def _cast_expr(columns: set[str], column_name: str, sql_type: str) -> str:
    if column_name not in columns:
        return f"NULL::{sql_type}"
    return f"CAST({quote_identifier(column_name)} AS {sql_type})"


def create_empty_delay_events_view(connection: Any, view_name: str = "delay_events_src") -> None:
    """Create an empty canonical delay-events source view."""
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {quote_identifier(view_name)} AS
        SELECT
            CAST(NULL AS VARCHAR) AS mode,
            CAST(NULL AS DATE) AS service_date,
            CAST(NULL AS TIMESTAMP) AS event_ts,
            CAST(NULL AS VARCHAR) AS day_name,
            CAST(NULL AS INTEGER) AS hour_bin,
            CAST(NULL AS DATE) AS month_bin,
            CAST(NULL AS VARCHAR) AS route_id_gtfs,
            CAST(NULL AS VARCHAR) AS route_short_name_norm,
            CAST(NULL AS VARCHAR) AS line_code_norm,
            CAST(NULL AS VARCHAR) AS station_canonical,
            CAST(NULL AS VARCHAR) AS station_text_raw,
            CAST(NULL AS VARCHAR) AS location_text_raw,
            CAST(NULL AS VARCHAR) AS incident_category,
            CAST(NULL AS DOUBLE) AS min_delay,
            CAST(NULL AS DOUBLE) AS min_gap,
            CAST(NULL AS VARCHAR) AS match_method,
            CAST(NULL AS VARCHAR) AS link_status,
            CAST(NULL AS DOUBLE) AS match_confidence,
            CAST(NULL AS VARCHAR) AS confidence_tier
        WHERE FALSE
        """
    )


def create_delay_events_view(
    connection: Any,
    project_root: Path,
    caveats: list[str],
    source_table_name: str = "fact_delay_events_norm",
    view_name: str = "delay_events_src",
) -> dict[str, Any]:
    """Create a canonical source view for delay-event marts with graceful fallbacks."""
    source_status = ensure_table_from_parquet(
        connection=connection,
        table_name=source_table_name,
        project_root=project_root,
        caveats=caveats,
    )
    columns = get_table_columns(connection, source_table_name)

    if not columns:
        create_empty_delay_events_view(connection=connection, view_name=view_name)
        caveats.append(f"{source_table_name}: unavailable; delay marts were built as empty outputs.")
        return {"source_status": source_status, "row_count": 0, "columns": []}

    expected_columns = {
        "mode",
        "source_mode",
        "service_date",
        "event_ts",
        "day_name",
        "hour_bin",
        "route_id_gtfs",
        "route_short_name_norm",
        "line_code_norm",
        "station_canonical",
        "station_text_raw",
        "location_text_raw",
        "incident_category",
        "incident_code_raw",
        "min_delay",
        "min_gap",
        "match_method",
        "link_status",
        "match_confidence",
    }
    missing_columns = sorted(expected_columns - columns)
    if missing_columns:
        caveats.append(
            f"{source_table_name}: missing expected columns {', '.join(missing_columns)}; NULL defaults were used."
        )

    mode_expr = _text_expr(columns, "mode")
    source_mode_expr = _text_expr(columns, "source_mode")
    service_date_expr = _cast_expr(columns, "service_date", "DATE")
    event_ts_expr = _cast_expr(columns, "event_ts", "TIMESTAMP")
    day_name_expr = _text_expr(columns, "day_name")
    hour_bin_expr = _cast_expr(columns, "hour_bin", "INTEGER")
    route_id_expr = _text_expr(columns, "route_id_gtfs")
    route_short_expr = _text_expr(columns, "route_short_name_norm")
    line_code_expr = _text_expr(columns, "line_code_norm")
    station_canonical_expr = _text_expr(columns, "station_canonical")
    station_text_expr = _text_expr(columns, "station_text_raw")
    location_text_expr = _text_expr(columns, "location_text_raw")
    incident_category_expr = _text_expr(columns, "incident_category")
    incident_code_expr = _text_expr(columns, "incident_code_raw")
    min_delay_expr = _cast_expr(columns, "min_delay", "DOUBLE")
    min_gap_expr = _cast_expr(columns, "min_gap", "DOUBLE")
    match_method_expr = _text_expr(columns, "match_method")
    link_status_expr = _text_expr(columns, "link_status")
    match_confidence_expr = _cast_expr(columns, "match_confidence", "DOUBLE")

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {quote_identifier(view_name)} AS
        SELECT
            COALESCE({mode_expr}, {source_mode_expr}, 'unknown') AS mode,
            COALESCE({service_date_expr}, CAST({event_ts_expr} AS DATE)) AS service_date,
            COALESCE({event_ts_expr}, CAST({service_date_expr} AS TIMESTAMP)) AS event_ts,
            COALESCE(
                {day_name_expr},
                STRFTIME(COALESCE({event_ts_expr}, CAST({service_date_expr} AS TIMESTAMP)), '%A')
            ) AS day_name,
            COALESCE(
                {hour_bin_expr},
                CAST(EXTRACT(HOUR FROM COALESCE({event_ts_expr}, CAST({service_date_expr} AS TIMESTAMP))) AS INTEGER)
            ) AS hour_bin,
            CAST(
                DATE_TRUNC('month', COALESCE({service_date_expr}, CAST({event_ts_expr} AS DATE)))
                AS DATE
            ) AS month_bin,
            COALESCE({route_id_expr}, {route_short_expr}, {line_code_expr}) AS route_id_gtfs,
            {route_short_expr} AS route_short_name_norm,
            COALESCE({line_code_expr}, {route_short_expr}, {route_id_expr}) AS line_code_norm,
            COALESCE({station_canonical_expr}, {station_text_expr}, {location_text_expr}) AS station_canonical,
            {station_text_expr} AS station_text_raw,
            {location_text_expr} AS location_text_raw,
            COALESCE({incident_category_expr}, {incident_code_expr}, 'Uncategorized') AS incident_category,
            CASE
                WHEN {min_delay_expr} IS NULL THEN NULL
                WHEN {min_delay_expr} < 0 THEN 0
                ELSE {min_delay_expr}
            END AS min_delay,
            CASE
                WHEN {min_gap_expr} IS NULL THEN NULL
                WHEN {min_gap_expr} < 0 THEN 0
                ELSE {min_gap_expr}
            END AS min_gap,
            COALESCE({match_method_expr}, 'unknown') AS match_method,
            COALESCE({link_status_expr}, 'unknown') AS link_status,
            {match_confidence_expr} AS match_confidence,
            CASE
                WHEN LOWER(COALESCE({link_status_expr}, 'unknown')) = 'unmatched_review' THEN 'unmatched_review'
                WHEN LOWER(COALESCE({link_status_expr}, 'unknown')) = 'ambiguous_review' THEN 'ambiguous_review'
                WHEN LOWER(COALESCE({match_method_expr}, 'unknown')) = 'exact' THEN 'exact_gtfs_match'
                WHEN LOWER(COALESCE({match_method_expr}, 'unknown')) = 'token' THEN 'token_gtfs_match'
                WHEN LOWER(COALESCE({match_method_expr}, 'unknown')) = 'alias' THEN 'alias_match'
                WHEN LOWER(COALESCE({match_method_expr}, 'unknown')) IN ('route_only', 'fallback')
                    AND COALESCE({route_id_expr}, {route_short_expr}, {line_code_expr}) IS NOT NULL
                    THEN 'route_only_match'
                WHEN LOWER(COALESCE({link_status_expr}, 'unknown')) = 'matched' THEN 'alias_match'
                ELSE 'unmatched_review'
            END AS confidence_tier
        FROM {quote_identifier(source_table_name)}
        """
    )

    row_count = connection.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(view_name)}"
    ).fetchone()[0]
    return {"source_status": source_status, "row_count": int(row_count), "columns": sorted(columns)}


def create_empty_alert_events_view(connection: Any, view_name: str = "alert_events_src") -> None:
    """Create an empty canonical GTFS-RT alerts source view."""
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {quote_identifier(view_name)} AS
        SELECT
            CAST(NULL AS TIMESTAMP) AS snapshot_ts,
            CAST(NULL AS TIMESTAMP) AS feed_ts,
            CAST(NULL AS VARCHAR) AS alert_id,
            CAST(NULL AS TIMESTAMP) AS active_start_ts,
            CAST(NULL AS TIMESTAMP) AS active_end_ts,
            CAST(NULL AS VARCHAR) AS cause,
            CAST(NULL AS VARCHAR) AS effect,
            CAST(NULL AS VARCHAR) AS header_text,
            CAST(NULL AS VARCHAR) AS description_text,
            CAST(NULL AS VARCHAR) AS route_id_gtfs,
            CAST(NULL AS VARCHAR) AS stop_id_gtfs,
            CAST(NULL AS VARCHAR) AS trip_id_gtfs,
            CAST(NULL AS VARCHAR) AS selector_scope,
            CAST(NULL AS VARCHAR) AS match_status,
            CAST(NULL AS VARCHAR) AS match_notes
        WHERE FALSE
        """
    )


def create_alert_events_view(
    connection: Any,
    project_root: Path,
    caveats: list[str],
    source_table_name: str = "fact_gtfsrt_alerts_norm",
    view_name: str = "alert_events_src",
) -> dict[str, Any]:
    """Create a canonical source view for alert-validation marts with graceful fallbacks."""
    source_status = ensure_table_from_parquet(
        connection=connection,
        table_name=source_table_name,
        project_root=project_root,
        caveats=caveats,
    )
    columns = get_table_columns(connection, source_table_name)

    if not columns:
        create_empty_alert_events_view(connection=connection, view_name=view_name)
        caveats.append(f"{source_table_name}: unavailable; alert validation mart was built as empty output.")
        return {"source_status": source_status, "row_count": 0, "columns": []}

    expected_columns = {
        "snapshot_ts",
        "feed_ts",
        "alert_id",
        "active_start_ts",
        "active_end_ts",
        "cause",
        "effect",
        "header_text",
        "description_text",
        "route_id_gtfs",
        "stop_id_gtfs",
        "trip_id_gtfs",
        "selector_scope",
        "match_status",
        "match_notes",
    }
    missing_columns = sorted(expected_columns - columns)
    if missing_columns:
        caveats.append(
            f"{source_table_name}: missing expected columns {', '.join(missing_columns)}; NULL defaults were used."
        )

    snapshot_ts_expr = _cast_expr(columns, "snapshot_ts", "TIMESTAMP")
    feed_ts_expr = _cast_expr(columns, "feed_ts", "TIMESTAMP")
    alert_id_expr = _text_expr(columns, "alert_id")
    active_start_expr = _cast_expr(columns, "active_start_ts", "TIMESTAMP")
    active_end_expr = _cast_expr(columns, "active_end_ts", "TIMESTAMP")
    cause_expr = _text_expr(columns, "cause")
    effect_expr = _text_expr(columns, "effect")
    header_expr = _text_expr(columns, "header_text")
    description_expr = _text_expr(columns, "description_text")
    route_expr = _text_expr(columns, "route_id_gtfs")
    stop_expr = _text_expr(columns, "stop_id_gtfs")
    trip_expr = _text_expr(columns, "trip_id_gtfs")
    selector_scope_expr = _text_expr(columns, "selector_scope")
    match_status_expr = _text_expr(columns, "match_status")
    match_notes_expr = _text_expr(columns, "match_notes")

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {quote_identifier(view_name)} AS
        SELECT
            COALESCE({snapshot_ts_expr}, {feed_ts_expr}, {active_start_expr}) AS snapshot_ts,
            {feed_ts_expr} AS feed_ts,
            COALESCE({alert_id_expr}, 'unknown_alert') AS alert_id,
            {active_start_expr} AS active_start_ts,
            {active_end_expr} AS active_end_ts,
            {cause_expr} AS cause,
            {effect_expr} AS effect,
            {header_expr} AS header_text,
            {description_expr} AS description_text,
            {route_expr} AS route_id_gtfs,
            {stop_expr} AS stop_id_gtfs,
            {trip_expr} AS trip_id_gtfs,
            COALESCE(
                {selector_scope_expr},
                CASE
                    WHEN {route_expr} IS NOT NULL AND {stop_expr} IS NOT NULL THEN 'route_stop'
                    WHEN {route_expr} IS NOT NULL THEN 'route'
                    WHEN {stop_expr} IS NOT NULL THEN 'stop'
                    WHEN {trip_expr} IS NOT NULL THEN 'trip'
                    ELSE 'unknown'
                END
            ) AS selector_scope,
            COALESCE(
                {match_status_expr},
                CASE
                    WHEN {route_expr} IS NULL AND {stop_expr} IS NULL AND {trip_expr} IS NULL THEN 'missing_selector'
                    ELSE 'unknown'
                END
            ) AS match_status,
            {match_notes_expr} AS match_notes
        FROM {quote_identifier(source_table_name)}
        """
    )

    row_count = connection.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(view_name)}"
    ).fetchone()[0]
    return {"source_status": source_status, "row_count": int(row_count), "columns": sorted(columns)}


def materialize_query_to_gold(
    connection: Any,
    query_sql: str,
    table_name: str,
    parquet_path: Path,
) -> int:
    """Materialize a SQL query to parquet and register the resulting table in DuckDB."""
    temp_table_name = f"tmp_{table_name}"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {quote_identifier(temp_table_name)} AS
        {query_sql}
        """
    )
    row_count = connection.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(temp_table_name)}"
    ).fetchone()[0]

    connection.execute(
        f"""
        COPY {quote_identifier(temp_table_name)}
        TO {sql_literal(parquet_path.as_posix())}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {quote_identifier(table_name)} AS
        SELECT * FROM read_parquet({sql_literal(parquet_path.as_posix())})
        """
    )
    return int(row_count)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    """Write a CSV file with explicit field ordering."""
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
