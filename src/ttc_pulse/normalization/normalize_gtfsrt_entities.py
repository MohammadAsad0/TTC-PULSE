"""Step 2 GTFS-RT alert entity normalization into Silver parquet."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from ttc_pulse.utils.project_setup import resolve_project_paths, sql_literal

OUTPUT_FILENAME = "silver_gtfsrt_alert_entities.parquet"


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


def _column_set(connection: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({sql_literal(table_name)})").fetchall()
    return {str(row[1]) for row in rows}


def _safe_text_expr(column_set: set[str], column_name: str) -> str:
    if column_name not in column_set:
        return "NULL::VARCHAR"
    return f"NULLIF(TRIM(CAST({column_name} AS VARCHAR)), '')"


def _prepare_gtfsrt_source_views(connection: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(connection, "bronze_gtfsrt_alerts"):
        _create_empty_output_table(connection)
        connection.execute(
            """
            CREATE OR REPLACE TEMP VIEW src_bronze_gtfsrt_alerts AS
            SELECT
                CAST(NULL AS VARCHAR) AS snapshot_source_file,
                CAST(NULL AS VARCHAR) AS alert_id,
                CAST(NULL AS VARCHAR) AS cause,
                CAST(NULL AS VARCHAR) AS effect,
                CAST(NULL AS VARCHAR) AS header_text,
                CAST(NULL AS VARCHAR) AS description_text,
                CAST(NULL AS VARCHAR) AS starts_at,
                CAST(NULL AS VARCHAR) AS ends_at,
                CAST(NULL AS TIMESTAMP) AS ingested_at,
                CAST(NULL AS VARCHAR) AS row_hash
            WHERE FALSE
            """
        )
    else:
        alert_cols = _column_set(connection, "bronze_gtfsrt_alerts")
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW src_bronze_gtfsrt_alerts AS
            SELECT
                {_safe_text_expr(alert_cols, 'snapshot_source_file')} AS snapshot_source_file,
                {_safe_text_expr(alert_cols, 'alert_id')} AS alert_id,
                {_safe_text_expr(alert_cols, 'cause')} AS cause,
                {_safe_text_expr(alert_cols, 'effect')} AS effect,
                {_safe_text_expr(alert_cols, 'header_text')} AS header_text,
                {_safe_text_expr(alert_cols, 'description_text')} AS description_text,
                {_safe_text_expr(alert_cols, 'starts_at')} AS starts_at,
                {_safe_text_expr(alert_cols, 'ends_at')} AS ends_at,
                ingested_at,
                row_hash
            FROM bronze_gtfsrt_alerts
            """
        )

    if not _table_exists(connection, "bronze_gtfsrt_entities"):
        connection.execute(
            """
            CREATE OR REPLACE TEMP VIEW src_bronze_gtfsrt_entities AS
            SELECT
                CAST(NULL AS VARCHAR) AS snapshot_source_file,
                CAST(NULL AS VARCHAR) AS alert_id,
                CAST(NULL AS VARCHAR) AS agency_id,
                CAST(NULL AS VARCHAR) AS route_id,
                CAST(NULL AS VARCHAR) AS stop_id,
                CAST(NULL AS VARCHAR) AS trip_id,
                CAST(NULL AS VARCHAR) AS route_type,
                CAST(NULL AS VARCHAR) AS direction_id,
                CAST(NULL AS VARCHAR) AS start_time,
                CAST(NULL AS VARCHAR) AS end_time,
                CAST(NULL AS TIMESTAMP) AS ingested_at,
                CAST(NULL AS VARCHAR) AS row_hash
            WHERE FALSE
            """
        )
    else:
        entity_cols = _column_set(connection, "bronze_gtfsrt_entities")
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW src_bronze_gtfsrt_entities AS
            SELECT
                {_safe_text_expr(entity_cols, 'snapshot_source_file')} AS snapshot_source_file,
                {_safe_text_expr(entity_cols, 'alert_id')} AS alert_id,
                {_safe_text_expr(entity_cols, 'agency_id')} AS agency_id,
                {_safe_text_expr(entity_cols, 'route_id')} AS route_id,
                {_safe_text_expr(entity_cols, 'stop_id')} AS stop_id,
                {_safe_text_expr(entity_cols, 'trip_id')} AS trip_id,
                {_safe_text_expr(entity_cols, 'route_type')} AS route_type,
                {_safe_text_expr(entity_cols, 'direction_id')} AS direction_id,
                {_safe_text_expr(entity_cols, 'start_time')} AS start_time,
                {_safe_text_expr(entity_cols, 'end_time')} AS end_time,
                ingested_at,
                row_hash
            FROM bronze_gtfsrt_entities
            """
        )


def _create_empty_output_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE tmp_silver_gtfsrt_alert_entities AS
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
            CAST(NULL AS VARCHAR) AS match_notes,
            CAST(NULL AS VARCHAR) AS match_method,
            CAST(NULL AS DOUBLE) AS match_confidence,
            CAST(NULL AS VARCHAR) AS link_status,
            CAST(NULL AS VARCHAR) AS source_file,
            CAST(NULL AS BIGINT) AS source_row_id,
            CAST(NULL AS TIMESTAMP) AS ingested_at,
            CAST(NULL AS VARCHAR) AS row_hash
        WHERE FALSE
        """
    )


def _normalize_gtfsrt_sql() -> str:
    return """
    WITH alerts AS (
        SELECT
            NULLIF(TRIM(snapshot_source_file), '') AS snapshot_source_file,
            NULLIF(TRIM(alert_id), '') AS alert_id,
            NULLIF(TRIM(cause), '') AS cause,
            NULLIF(TRIM(effect), '') AS effect,
            NULLIF(TRIM(header_text), '') AS header_text,
            NULLIF(TRIM(description_text), '') AS description_text,
            TRY_CAST(NULLIF(TRIM(starts_at), '') AS TIMESTAMP) AS active_start_ts,
            TRY_CAST(NULLIF(TRIM(ends_at), '') AS TIMESTAMP) AS active_end_ts,
            ingested_at AS alert_ingested_at,
            row_hash AS alert_row_hash
        FROM src_bronze_gtfsrt_alerts
    ),
    entities AS (
        SELECT
            NULLIF(TRIM(snapshot_source_file), '') AS snapshot_source_file,
            NULLIF(TRIM(alert_id), '') AS alert_id,
            NULLIF(TRIM(agency_id), '') AS agency_id,
            NULLIF(TRIM(route_id), '') AS route_id_gtfs,
            NULLIF(TRIM(stop_id), '') AS stop_id_gtfs,
            NULLIF(TRIM(trip_id), '') AS trip_id_gtfs,
            NULLIF(TRIM(route_type), '') AS route_type_raw,
            NULLIF(TRIM(direction_id), '') AS direction_id_raw,
            NULLIF(TRIM(start_time), '') AS selector_start_raw,
            NULLIF(TRIM(end_time), '') AS selector_end_raw,
            ingested_at AS entity_ingested_at,
            row_hash AS entity_row_hash
        FROM src_bronze_gtfsrt_entities
    ),
    joined AS (
        SELECT
            COALESCE(e.snapshot_source_file, a.snapshot_source_file) AS source_file,
            COALESCE(e.alert_id, a.alert_id) AS alert_id,
            a.active_start_ts,
            a.active_end_ts,
            a.cause,
            a.effect,
            a.header_text,
            a.description_text,
            e.agency_id,
            e.route_id_gtfs,
            e.stop_id_gtfs,
            e.trip_id_gtfs,
            e.route_type_raw,
            e.direction_id_raw,
            e.selector_start_raw,
            e.selector_end_raw,
            COALESCE(e.entity_ingested_at, a.alert_ingested_at) AS ingested_at,
            COALESCE(a.alert_ingested_at, e.entity_ingested_at) AS feed_ts,
            COALESCE(e.entity_ingested_at, a.alert_ingested_at) AS snapshot_ts,
            COALESCE(
                e.entity_row_hash,
                a.alert_row_hash,
                MD5(
                    TO_JSON(
                        STRUCT_PACK(
                            source_file := COALESCE(e.snapshot_source_file, a.snapshot_source_file),
                            alert_id := COALESCE(e.alert_id, a.alert_id),
                            route_id := e.route_id_gtfs,
                            stop_id := e.stop_id_gtfs,
                            trip_id := e.trip_id_gtfs
                        )
                    )
                )
            ) AS row_hash
        FROM entities e
        LEFT JOIN alerts a
            ON a.alert_id = e.alert_id
            AND (
                a.snapshot_source_file = e.snapshot_source_file
                OR a.snapshot_source_file IS NULL
                OR e.snapshot_source_file IS NULL
            )
    ),
    final AS (
        SELECT
            snapshot_ts,
            feed_ts,
            alert_id,
            active_start_ts,
            active_end_ts,
            cause,
            effect,
            header_text,
            description_text,
            route_id_gtfs,
            stop_id_gtfs,
            trip_id_gtfs,
            CASE
                WHEN route_id_gtfs IS NOT NULL AND stop_id_gtfs IS NOT NULL AND trip_id_gtfs IS NOT NULL
                    THEN 'route_stop_trip'
                WHEN trip_id_gtfs IS NOT NULL THEN 'trip'
                WHEN route_id_gtfs IS NOT NULL AND stop_id_gtfs IS NOT NULL THEN 'route_stop'
                WHEN route_id_gtfs IS NOT NULL THEN 'route'
                WHEN stop_id_gtfs IS NOT NULL THEN 'stop'
                ELSE 'agency_or_unknown'
            END AS selector_scope,
            CASE
                WHEN route_id_gtfs IS NOT NULL OR stop_id_gtfs IS NOT NULL OR trip_id_gtfs IS NOT NULL
                    THEN 'matched'
                ELSE 'unmatched_review'
            END AS match_status,
            CASE
                WHEN route_id_gtfs IS NOT NULL OR stop_id_gtfs IS NOT NULL OR trip_id_gtfs IS NOT NULL
                    THEN 'selector ids retained from GTFS-RT entity'
                WHEN agency_id IS NOT NULL
                    THEN 'agency selector present but route/stop/trip ids absent'
                ELSE 'entity has no mappable selector ids'
            END AS match_notes,
            CASE
                WHEN route_id_gtfs IS NOT NULL OR stop_id_gtfs IS NOT NULL OR trip_id_gtfs IS NOT NULL
                    THEN 'exact'
                ELSE 'fallback'
            END AS match_method,
            CASE
                WHEN route_id_gtfs IS NOT NULL AND stop_id_gtfs IS NOT NULL AND trip_id_gtfs IS NOT NULL THEN 1.00
                WHEN trip_id_gtfs IS NOT NULL THEN 0.98
                WHEN route_id_gtfs IS NOT NULL AND stop_id_gtfs IS NOT NULL THEN 0.95
                WHEN route_id_gtfs IS NOT NULL OR stop_id_gtfs IS NOT NULL THEN 0.90
                WHEN agency_id IS NOT NULL THEN 0.35
                ELSE 0.0
            END AS match_confidence,
            CASE
                WHEN route_id_gtfs IS NOT NULL OR stop_id_gtfs IS NOT NULL OR trip_id_gtfs IS NOT NULL
                    THEN 'matched'
                ELSE 'unmatched_review'
            END AS link_status,
            source_file,
            ROW_NUMBER() OVER (
                ORDER BY
                    source_file,
                    alert_id,
                    COALESCE(route_id_gtfs, ''),
                    COALESCE(stop_id_gtfs, ''),
                    COALESCE(trip_id_gtfs, '')
            ) AS source_row_id,
            ingested_at,
            row_hash
        FROM joined
    )
    SELECT
        snapshot_ts,
        feed_ts,
        alert_id,
        active_start_ts,
        active_end_ts,
        cause,
        effect,
        header_text,
        description_text,
        route_id_gtfs,
        stop_id_gtfs,
        trip_id_gtfs,
        selector_scope,
        match_status,
        match_notes,
        match_method,
        match_confidence,
        link_status,
        source_file,
        source_row_id,
        ingested_at,
        row_hash
    FROM final
    """


def run_normalize_gtfsrt_entities(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build silver GTFS-RT alert entity parquet with schema-safe empty fallback."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output = (output_path or (paths.project_root / "silver" / OUTPUT_FILENAME)).resolve()
    resolved_output.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(resolved_db_path))
    connection.execute("PRAGMA disable_progress_bar")
    _prepare_gtfsrt_source_views(connection)

    entity_count = connection.execute("SELECT COUNT(*) FROM src_bronze_gtfsrt_entities").fetchone()[0]
    if entity_count == 0:
        _create_empty_output_table(connection)
    else:
        connection.execute(
            f"CREATE OR REPLACE TEMP TABLE tmp_silver_gtfsrt_alert_entities AS {_normalize_gtfsrt_sql()}"
        )

    row_count = connection.execute("SELECT COUNT(*) FROM tmp_silver_gtfsrt_alert_entities").fetchone()[0]
    connection.execute(
        """
        CREATE OR REPLACE TABLE silver_gtfsrt_alert_entities AS
        SELECT * FROM tmp_silver_gtfsrt_alert_entities
        """
    )
    table_row_count = connection.execute("SELECT COUNT(*) FROM silver_gtfsrt_alert_entities").fetchone()[0]
    connection.execute(
        f"""
        COPY tmp_silver_gtfsrt_alert_entities
        TO {sql_literal(resolved_output.as_posix())}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    connection.close()

    return {
        "mode": "gtfsrt_alert_entities",
        "duckdb_path": resolved_db_path.as_posix(),
        "output_path": resolved_output.as_posix(),
        "row_count": row_count,
        "table_name": "silver_gtfsrt_alert_entities",
        "table_row_count": table_row_count,
        "entities_input_row_count": entity_count,
    }


def main() -> None:
    result = run_normalize_gtfsrt_entities()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
