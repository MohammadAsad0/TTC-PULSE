"""Build station alias dimension from bronze station tokens to GTFS stop references."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import ensure_duckdb_connection, quote_identifier, resolve_project_paths

REQUIRED_TABLES = [
    "bronze_bus",
    "bronze_subway",
    "bronze_gtfs_stops",
    "bronze_gtfs_trips",
    "bronze_gtfs_stop_times",
    "bronze_gtfs_routes",
]


def _table_exists(connection: Any, table_name: str) -> bool:
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


def _ensure_required_tables(connection: Any, table_names: list[str]) -> None:
    missing = [table_name for table_name in table_names if not _table_exists(connection, table_name)]
    if missing:
        joined = ", ".join(sorted(missing))
        raise RuntimeError(f"Missing required tables for station alias build: {joined}")


def _copy_table_to_parquet(connection: Any, table_name: str, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    escaped_path = output_path.resolve().as_posix().replace("'", "''")
    connection.execute(
        f"COPY {quote_identifier(table_name)} TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]


def _station_key_expression(column_name: str) -> str:
    return f"""
    UPPER(
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REGEXP_REPLACE(
                                                        REPLACE(COALESCE({column_name}, ''), '&', ' AND '),
                                                        '[^A-Za-z0-9 ]',
                                                        ' ',
                                                        'g'
                                                    ),
                                                    '(?i)\\bSCARB\\b',
                                                    'SCARBOROUGH',
                                                    'g'
                                                ),
                                                '(?i)\\bCTR\\b',
                                                'CENTRE',
                                                'g'
                                            ),
                                            '(?i)\\bCENTR\\b',
                                            'CENTRE',
                                            'g'
                                        ),
                                        '(?i)\\bVMC\\b',
                                        'VAUGHAN METROPOLITAN CENTRE',
                                        'g'
                                    ),
                                    '(?i)\\bMC\\b',
                                    'METROPOLITAN CENTRE',
                                    'g'
                                ),
                                '(?i)\\b(STATIO|STN|STA)\\b',
                                ' STATION ',
                                'g'
                            ),
                            '(?i)\\b(YUS|YU|BD|SHP|SHEP|SRT|RT)\\b',
                            ' ',
                            'g'
                        ),
                        '(?i)\\b(SUBWAY|LINE|LINES|PLATFORM|NORTHBOUND|SOUTHBOUND|EASTBOUND|WESTBOUND|BUS BAY)\\b',
                        ' ',
                        'g'
                    ),
                    '(?i)\\bSTATION\\b',
                    ' ',
                    'g'
                ),
                '\\s+',
                ' ',
                'g'
            )
        )
    )
    """


def _build_station_alias_table(connection: Any) -> int:
    stop_station_key_sql = _station_key_expression("trim(s.stop_name)")
    alias_station_key_sql = _station_key_expression("alias_token")

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE station_reference AS
        WITH stop_base AS (
            SELECT DISTINCT
                trim(s.stop_id) AS stop_id,
                NULLIF(trim(s.stop_name), '') AS stop_name,
                {stop_station_key_sql} AS station_key
            FROM bronze_gtfs_stops s
            WHERE s.stop_id IS NOT NULL
              AND trim(s.stop_id) <> ''
              AND s.stop_name IS NOT NULL
              AND trim(s.stop_name) <> ''
        ),
        stop_route_type AS (
            SELECT DISTINCT
                trim(st.stop_id) AS stop_id,
                trim(r.route_type) AS route_type
            FROM bronze_gtfs_stop_times st
            JOIN bronze_gtfs_trips t
                ON trim(st.trip_id) = trim(t.trip_id)
            JOIN bronze_gtfs_routes r
                ON trim(t.route_id) = trim(r.route_id)
            WHERE st.stop_id IS NOT NULL
              AND trim(st.stop_id) <> ''
              AND r.route_type IS NOT NULL
              AND trim(r.route_type) <> ''
        )
        SELECT
            sb.station_key,
            MIN(sb.stop_name) AS canonical_stop_name,
            MIN(sb.stop_id) AS canonical_stop_id,
            COUNT(DISTINCT sb.stop_id) AS candidate_stop_count,
            string_agg(DISTINCT sb.stop_id, '|' ORDER BY sb.stop_id) AS candidate_stop_ids,
            MAX(CASE WHEN lower(sb.stop_name) LIKE '%station%' THEN 1 ELSE 0 END) AS has_station_keyword,
            MAX(CASE WHEN srt.route_type = '1' THEN 1 ELSE 0 END) AS serves_subway,
            string_agg(DISTINCT srt.route_type, ',' ORDER BY srt.route_type) AS route_types_served
        FROM stop_base sb
        LEFT JOIN stop_route_type srt
            ON sb.stop_id = srt.stop_id
        WHERE sb.station_key IS NOT NULL
          AND sb.station_key <> ''
        GROUP BY sb.station_key
        """
    )

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE dim_station_alias AS
        WITH raw_station_tokens AS (
            SELECT
                trim(Station) AS alias_token,
                'subway' AS source_mode,
                'Station' AS source_field,
                COUNT(*) AS observed_rows
            FROM bronze_subway
            WHERE Station IS NOT NULL
              AND trim(Station) <> ''
              AND upper(trim(Station)) NOT IN ('NONE', 'NULL', 'N/A')
            GROUP BY 1
            UNION ALL
            SELECT
                trim(Station) AS alias_token,
                'bus' AS source_mode,
                'Station' AS source_field,
                COUNT(*) AS observed_rows
            FROM bronze_bus
            WHERE Station IS NOT NULL
              AND trim(Station) <> ''
              AND upper(trim(Station)) NOT IN ('NONE', 'NULL', 'N/A')
            GROUP BY 1
        ),
        normalized_tokens AS (
            SELECT
                alias_token,
                source_mode,
                source_field,
                observed_rows,
                {alias_station_key_sql} AS alias_station_key
            FROM raw_station_tokens
        )
        SELECT
            row_number() OVER (ORDER BY source_mode, observed_rows DESC, alias_token) AS station_alias_sk,
            nt.alias_token,
            nt.alias_station_key,
            nt.source_mode,
            nt.source_field,
            nt.observed_rows,
            CASE
                WHEN nt.alias_station_key IS NULL OR nt.alias_station_key = '' THEN 'unresolved'
                WHEN sr.station_key IS NULL THEN 'unresolved'
                WHEN sr.candidate_stop_count > 1 THEN 'ambiguous'
                ELSE 'resolved'
            END AS mapping_status,
            CASE
                WHEN nt.alias_station_key IS NULL OR nt.alias_station_key = '' THEN 'empty_after_normalization'
                WHEN sr.station_key IS NULL THEN 'no_reference_station_key_match'
                WHEN sr.candidate_stop_count > 1 THEN 'station_key_matches_multiple_stops'
                ELSE 'normalized_station_key_match'
            END AS mapping_rule,
            sr.station_key AS mapped_station_key,
            sr.canonical_stop_name AS mapped_stop_name,
            CASE
                WHEN sr.candidate_stop_count = 1 THEN sr.canonical_stop_id
                ELSE NULL
            END AS mapped_stop_id,
            COALESCE(sr.candidate_stop_count, 0) AS candidate_stop_count,
            sr.candidate_stop_ids,
            COALESCE(sr.serves_subway, 0) = 1 AS mapped_serves_subway,
            sr.route_types_served,
            TRUE AS reference_gtfs_bridge_family,
            CURRENT_TIMESTAMP AS generated_at
        FROM normalized_tokens nt
        LEFT JOIN station_reference sr
            ON nt.alias_station_key = sr.station_key
        ORDER BY source_mode, observed_rows DESC, alias_token
        """
    )

    return connection.execute("SELECT COUNT(*) FROM dim_station_alias").fetchone()[0]


def run() -> dict[str, Any]:
    """Build station alias dimension parquet and return row metrics."""
    paths = resolve_project_paths()
    dimensions_root = paths.project_root / "dimensions"
    output_path = dimensions_root / "dim_station_alias.parquet"

    connection = ensure_duckdb_connection(paths.db_path)
    _ensure_required_tables(connection, REQUIRED_TABLES)
    alias_rows = _build_station_alias_table(connection)
    alias_rows = _copy_table_to_parquet(connection, "dim_station_alias", output_path)

    unresolved_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_station_alias
        WHERE mapping_status IN ('unresolved', 'ambiguous')
        """
    ).fetchone()[0]
    ambiguous_count = connection.execute(
        "SELECT COUNT(*) FROM dim_station_alias WHERE mapping_status = 'ambiguous'"
    ).fetchone()[0]

    connection.close()
    return {
        "duckdb_path": paths.db_path.resolve().as_posix(),
        "outputs": {
            "dim_station_alias": output_path.resolve().as_posix(),
        },
        "row_counts": {
            "dim_station_alias": alias_rows,
        },
        "unresolved_counts": {
            "station_alias_unresolved_like": unresolved_count,
            "station_alias_ambiguous": ambiguous_count,
        },
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
