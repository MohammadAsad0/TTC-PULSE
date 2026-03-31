"""Load parsed GTFS-RT Service Alert CSV rows into DuckDB bronze tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from ttc_pulse.utils.project_setup import (
    ensure_duckdb_connection,
    quote_identifier,
    resolve_project_paths,
    sql_literal,
    utc_now_iso,
)

DEFAULT_PARSED_RELATIVE_PATH = "alerts/parsed/service_alert_entities.csv"


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return (
        connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
        > 0
    )


def _table_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not _table_exists(connection, table_name):
        return 0
    return int(connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0])


def create_gtfsrt_shell_tables(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Create empty schema-ready GTFS-RT bronze tables."""
    connection.execute(
        """
        CREATE OR REPLACE TABLE bronze_gtfsrt_alerts (
            snapshot_source_file VARCHAR,
            alert_id VARCHAR,
            cause VARCHAR,
            effect VARCHAR,
            severity_level VARCHAR,
            header_text VARCHAR,
            description_text VARCHAR,
            starts_at VARCHAR,
            ends_at VARCHAR,
            ingested_at TIMESTAMP,
            row_hash VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TABLE bronze_gtfsrt_entities (
            snapshot_source_file VARCHAR,
            alert_id VARCHAR,
            agency_id VARCHAR,
            route_id VARCHAR,
            route_type VARCHAR,
            stop_id VARCHAR,
            trip_id VARCHAR,
            direction_id VARCHAR,
            start_time VARCHAR,
            end_time VARCHAR,
            ingested_at TIMESTAMP,
            row_hash VARCHAR
        )
        """
    )
    return {
        "bronze_gtfsrt_alerts": _table_count(connection, "bronze_gtfsrt_alerts"),
        "bronze_gtfsrt_entities": _table_count(connection, "bronze_gtfsrt_entities"),
    }


def _column_set(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({sql_literal(table_name)})").fetchall()
    return {str(row[1]) for row in rows}


def _text_expr(stage_columns: set[str], column_name: str) -> str:
    if column_name not in stage_columns:
        return "NULL::VARCHAR"
    return f"NULLIF(TRIM(CAST({quote_identifier(column_name)} AS VARCHAR)), '')"


def _build_alerts_sql(stage_columns: set[str], ingested_at: str) -> str:
    snapshot_rel_expr = _text_expr(stage_columns, "snapshot_rel_path")
    snapshot_path_expr = _text_expr(stage_columns, "snapshot_path")
    snapshot_file_expr = _text_expr(stage_columns, "snapshot_file")
    alert_id_expr = _text_expr(stage_columns, "alert_id")
    cause_expr = _text_expr(stage_columns, "cause")
    effect_expr = _text_expr(stage_columns, "effect")
    header_expr = _text_expr(stage_columns, "header_text")
    description_expr = _text_expr(stage_columns, "description_text")
    active_start_expr = _text_expr(stage_columns, "active_start_utc")
    active_end_expr = _text_expr(stage_columns, "active_end_utc")

    return f"""
    CREATE OR REPLACE TABLE bronze_gtfsrt_alerts AS
    WITH parsed AS (
        SELECT
            COALESCE({snapshot_rel_expr}, {snapshot_path_expr}, {snapshot_file_expr}) AS snapshot_source_file,
            {alert_id_expr} AS alert_id,
            {cause_expr} AS cause,
            {effect_expr} AS effect,
            {header_expr} AS header_text,
            {description_expr} AS description_text,
            {active_start_expr} AS starts_at,
            {active_end_expr} AS ends_at
        FROM tmp_parsed_gtfsrt_alert_entities
    )
    SELECT DISTINCT
        snapshot_source_file,
        alert_id,
        cause,
        effect,
        NULL::VARCHAR AS severity_level,
        header_text,
        description_text,
        starts_at,
        ends_at,
        CAST({sql_literal(ingested_at)} AS TIMESTAMP) AS ingested_at,
        MD5(
            TO_JSON(
                STRUCT_PACK(
                    snapshot_source_file := snapshot_source_file,
                    alert_id := alert_id,
                    cause := cause,
                    effect := effect,
                    header_text := header_text,
                    description_text := description_text,
                    starts_at := starts_at,
                    ends_at := ends_at
                )
            )
        ) AS row_hash
    FROM parsed
    """


def _build_entities_sql(stage_columns: set[str], ingested_at: str) -> str:
    snapshot_rel_expr = _text_expr(stage_columns, "snapshot_rel_path")
    snapshot_path_expr = _text_expr(stage_columns, "snapshot_path")
    snapshot_file_expr = _text_expr(stage_columns, "snapshot_file")
    alert_id_expr = _text_expr(stage_columns, "alert_id")
    agency_expr = _text_expr(stage_columns, "agency_id")
    route_expr = _text_expr(stage_columns, "route_id")
    route_type_expr = _text_expr(stage_columns, "route_type")
    stop_expr = _text_expr(stage_columns, "stop_id")
    trip_expr = _text_expr(stage_columns, "trip_id")
    direction_expr = _text_expr(stage_columns, "direction_id")
    trip_start_time_expr = _text_expr(stage_columns, "trip_start_time")
    active_start_expr = _text_expr(stage_columns, "active_start_utc")
    active_end_expr = _text_expr(stage_columns, "active_end_utc")
    entity_index_expr = _text_expr(stage_columns, "entity_index")
    informed_entity_index_expr = _text_expr(stage_columns, "informed_entity_index")

    return f"""
    CREATE OR REPLACE TABLE bronze_gtfsrt_entities AS
    WITH parsed AS (
        SELECT
            COALESCE({snapshot_rel_expr}, {snapshot_path_expr}, {snapshot_file_expr}) AS snapshot_source_file,
            {alert_id_expr} AS alert_id,
            {agency_expr} AS agency_id,
            {route_expr} AS route_id,
            {route_type_expr} AS route_type,
            {stop_expr} AS stop_id,
            {trip_expr} AS trip_id,
            {direction_expr} AS direction_id,
            COALESCE({trip_start_time_expr}, {active_start_expr}) AS start_time,
            {active_end_expr} AS end_time,
            {entity_index_expr} AS entity_index_raw,
            {informed_entity_index_expr} AS informed_entity_index_raw
        FROM tmp_parsed_gtfsrt_alert_entities
    )
    SELECT
        snapshot_source_file,
        alert_id,
        agency_id,
        route_id,
        route_type,
        stop_id,
        trip_id,
        direction_id,
        start_time,
        end_time,
        CAST({sql_literal(ingested_at)} AS TIMESTAMP) AS ingested_at,
        MD5(
            TO_JSON(
                STRUCT_PACK(
                    snapshot_source_file := snapshot_source_file,
                    alert_id := alert_id,
                    agency_id := agency_id,
                    route_id := route_id,
                    route_type := route_type,
                    stop_id := stop_id,
                    trip_id := trip_id,
                    direction_id := direction_id,
                    start_time := start_time,
                    end_time := end_time,
                    entity_index_raw := entity_index_raw,
                    informed_entity_index_raw := informed_entity_index_raw
                )
            )
        ) AS row_hash
    FROM parsed
    """


def load_parsed_alerts_into_bronze(
    *,
    connection: duckdb.DuckDBPyConnection,
    parsed_csv_path: Path,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Populate GTFS-RT bronze tables from parsed service-alert CSV."""
    effective_ingested_at = ingested_at or utc_now_iso()
    resolved_csv = parsed_csv_path.resolve()

    create_gtfsrt_shell_tables(connection)
    if not resolved_csv.exists():
        return {
            "status": "missing_parsed_csv",
            "parsed_csv_path": resolved_csv.as_posix(),
            "input_rows": 0,
            "bronze_alerts_row_count": _table_count(connection, "bronze_gtfsrt_alerts"),
            "bronze_entities_row_count": _table_count(connection, "bronze_gtfsrt_entities"),
            "ingested_at": effective_ingested_at,
        }

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE tmp_parsed_gtfsrt_alert_entities AS
        SELECT *
        FROM read_csv_auto(
            {sql_literal(resolved_csv.as_posix())},
            all_varchar = TRUE,
            union_by_name = TRUE,
            ignore_errors = TRUE
        )
        """
    )
    input_rows = int(
        connection.execute("SELECT COUNT(*) FROM tmp_parsed_gtfsrt_alert_entities").fetchone()[0]
    )
    if input_rows == 0:
        return {
            "status": "parsed_csv_empty",
            "parsed_csv_path": resolved_csv.as_posix(),
            "input_rows": 0,
            "bronze_alerts_row_count": _table_count(connection, "bronze_gtfsrt_alerts"),
            "bronze_entities_row_count": _table_count(connection, "bronze_gtfsrt_entities"),
            "ingested_at": effective_ingested_at,
        }

    stage_columns = _column_set(connection, "tmp_parsed_gtfsrt_alert_entities")
    connection.execute(_build_alerts_sql(stage_columns, effective_ingested_at))
    connection.execute(_build_entities_sql(stage_columns, effective_ingested_at))

    return {
        "status": "loaded",
        "parsed_csv_path": resolved_csv.as_posix(),
        "input_rows": input_rows,
        "bronze_alerts_row_count": _table_count(connection, "bronze_gtfsrt_alerts"),
        "bronze_entities_row_count": _table_count(connection, "bronze_gtfsrt_entities"),
        "ingested_at": effective_ingested_at,
    }


def run_load_parsed_alerts_into_bronze(
    *,
    db_path: Path | None = None,
    parsed_csv_path: Path | None = None,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Standalone runner for loading parsed alerts into GTFS-RT bronze tables."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_csv_path = (parsed_csv_path or (paths.project_root / DEFAULT_PARSED_RELATIVE_PATH)).resolve()

    connection = ensure_duckdb_connection(resolved_db_path)
    try:
        result = load_parsed_alerts_into_bronze(
            connection=connection,
            parsed_csv_path=resolved_csv_path,
            ingested_at=ingested_at,
        )
    finally:
        connection.close()

    result["duckdb_path"] = resolved_db_path.as_posix()
    return result


def main() -> None:
    result = run_load_parsed_alerts_into_bronze()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
