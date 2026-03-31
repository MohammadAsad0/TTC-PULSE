"""Build GTFS reference dimensions for Step 2 alias/bridge work."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import ensure_duckdb_connection, quote_identifier, resolve_project_paths

REQUIRED_GTFS_TABLES = [
    "bronze_gtfs_routes",
    "bronze_gtfs_stops",
    "bronze_gtfs_trips",
    "bronze_gtfs_stop_times",
    "bronze_gtfs_calendar",
    "bronze_gtfs_calendar_dates",
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
        raise RuntimeError(f"Missing required GTFS bronze tables: {joined}")


def _copy_table_to_parquet(connection: Any, table_name: str, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    escaped_path = output_path.resolve().as_posix().replace("'", "''")
    connection.execute(
        f"COPY {quote_identifier(table_name)} TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]


def _build_dim_route_gtfs(connection: Any) -> int:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE dim_route_gtfs AS
        WITH route_base AS (
            SELECT DISTINCT
                trim(route_id) AS route_id,
                NULLIF(trim(route_short_name), '') AS route_short_name,
                NULLIF(trim(route_long_name), '') AS route_long_name,
                NULLIF(trim(route_desc), '') AS route_desc,
                TRY_CAST(NULLIF(trim(route_type), '') AS INTEGER) AS route_type,
                NULLIF(trim(route_url), '') AS route_url,
                NULLIF(trim(route_color), '') AS route_color,
                NULLIF(trim(route_text_color), '') AS route_text_color,
                ingested_at
            FROM bronze_gtfs_routes
            WHERE route_id IS NOT NULL AND trim(route_id) <> ''
        )
        SELECT
            row_number() OVER (ORDER BY route_id) AS route_sk,
            route_id,
            route_short_name,
            route_long_name,
            route_desc,
            route_type,
            CASE route_type
                WHEN 0 THEN 'streetcar'
                WHEN 1 THEN 'subway'
                WHEN 2 THEN 'rail'
                WHEN 3 THEN 'bus'
                WHEN 4 THEN 'ferry'
                ELSE 'other'
            END AS route_mode,
            route_url,
            route_color,
            route_text_color,
            MAX(ingested_at) OVER () AS latest_ingested_at
        FROM route_base
        ORDER BY route_id
        """
    )
    return connection.execute("SELECT COUNT(*) FROM dim_route_gtfs").fetchone()[0]


def _build_dim_stop_gtfs(connection: Any) -> int:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE dim_stop_gtfs AS
        WITH stop_base AS (
            SELECT DISTINCT
                trim(stop_id) AS stop_id,
                NULLIF(trim(stop_code), '') AS stop_code,
                NULLIF(trim(stop_name), '') AS stop_name,
                NULLIF(trim(stop_desc), '') AS stop_desc,
                TRY_CAST(NULLIF(trim(stop_lat), '') AS DOUBLE) AS stop_lat,
                TRY_CAST(NULLIF(trim(stop_lon), '') AS DOUBLE) AS stop_lon,
                NULLIF(trim(zone_id), '') AS zone_id,
                NULLIF(trim(stop_url), '') AS stop_url,
                TRY_CAST(NULLIF(trim(location_type), '') AS INTEGER) AS location_type,
                NULLIF(trim(parent_station), '') AS parent_station,
                NULLIF(trim(stop_timezone), '') AS stop_timezone,
                TRY_CAST(NULLIF(trim(wheelchair_boarding), '') AS INTEGER) AS wheelchair_boarding,
                ingested_at
            FROM bronze_gtfs_stops
            WHERE stop_id IS NOT NULL AND trim(stop_id) <> ''
        ),
        stop_service_stats AS (
            SELECT
                trim(st.stop_id) AS stop_id,
                COUNT(*) AS stop_time_rows,
                COUNT(DISTINCT trim(st.trip_id)) AS trip_count,
                COUNT(DISTINCT trim(t.route_id)) AS route_count,
                MAX(CASE WHEN trim(r.route_type) = '1' THEN 1 ELSE 0 END) AS serves_subway,
                string_agg(DISTINCT trim(r.route_type), ',' ORDER BY trim(r.route_type)) AS route_types_served
            FROM bronze_gtfs_stop_times st
            JOIN bronze_gtfs_trips t
                ON trim(st.trip_id) = trim(t.trip_id)
            JOIN bronze_gtfs_routes r
                ON trim(t.route_id) = trim(r.route_id)
            WHERE st.stop_id IS NOT NULL
              AND trim(st.stop_id) <> ''
              AND st.trip_id IS NOT NULL
              AND trim(st.trip_id) <> ''
            GROUP BY 1
        )
        SELECT
            row_number() OVER (ORDER BY sb.stop_id) AS stop_sk,
            sb.stop_id,
            sb.stop_code,
            sb.stop_name,
            UPPER(
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REPLACE(COALESCE(sb.stop_name, ''), '&', ' AND '),
                                        '[^A-Za-z0-9 ]',
                                        ' ',
                                        'g'
                                    ),
                                    '(?i)\\b(CTR)\\b',
                                    'CENTRE',
                                    'g'
                                ),
                                '(?i)\\b(STN|STA|STATIO)\\b',
                                ' STATION ',
                                'g'
                            ),
                            '(?i)\\b(STATION|SUBWAY|PLATFORM|NORTHBOUND|SOUTHBOUND|EASTBOUND|WESTBOUND)\\b',
                            ' ',
                            'g'
                        ),
                        '\\s+',
                        ' ',
                        'g'
                    )
                )
            ) AS station_key,
            sb.stop_desc,
            sb.stop_lat,
            sb.stop_lon,
            sb.zone_id,
            sb.stop_url,
            sb.location_type,
            sb.parent_station,
            sb.stop_timezone,
            sb.wheelchair_boarding,
            COALESCE(ss.stop_time_rows, 0) AS stop_time_rows,
            COALESCE(ss.trip_count, 0) AS trip_count,
            COALESCE(ss.route_count, 0) AS route_count,
            COALESCE(ss.serves_subway, 0) = 1 AS serves_subway,
            ss.route_types_served,
            (
                LOWER(COALESCE(sb.stop_name, '')) LIKE '%station%'
                OR COALESCE(ss.serves_subway, 0) = 1
            ) AS is_station_like,
            MAX(sb.ingested_at) OVER () AS latest_ingested_at
        FROM stop_base sb
        LEFT JOIN stop_service_stats ss
            ON sb.stop_id = ss.stop_id
        ORDER BY sb.stop_id
        """
    )
    return connection.execute("SELECT COUNT(*) FROM dim_stop_gtfs").fetchone()[0]


def _build_dim_service_gtfs(connection: Any) -> int:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE dim_service_gtfs AS
        WITH services AS (
            SELECT DISTINCT trim(service_id) AS service_id
            FROM bronze_gtfs_calendar
            WHERE service_id IS NOT NULL AND trim(service_id) <> ''
            UNION
            SELECT DISTINCT trim(service_id) AS service_id
            FROM bronze_gtfs_calendar_dates
            WHERE service_id IS NOT NULL AND trim(service_id) <> ''
        ),
        calendar_rollup AS (
            SELECT
                trim(service_id) AS service_id,
                MAX(TRY_CAST(NULLIF(trim(monday), '') AS INTEGER)) AS monday,
                MAX(TRY_CAST(NULLIF(trim(tuesday), '') AS INTEGER)) AS tuesday,
                MAX(TRY_CAST(NULLIF(trim(wednesday), '') AS INTEGER)) AS wednesday,
                MAX(TRY_CAST(NULLIF(trim(thursday), '') AS INTEGER)) AS thursday,
                MAX(TRY_CAST(NULLIF(trim(friday), '') AS INTEGER)) AS friday,
                MAX(TRY_CAST(NULLIF(trim(saturday), '') AS INTEGER)) AS saturday,
                MAX(TRY_CAST(NULLIF(trim(sunday), '') AS INTEGER)) AS sunday,
                MIN(TRY_CAST(NULLIF(trim(start_date), '') AS BIGINT)) AS start_date_int,
                MAX(TRY_CAST(NULLIF(trim(end_date), '') AS BIGINT)) AS end_date_int
            FROM bronze_gtfs_calendar
            WHERE service_id IS NOT NULL AND trim(service_id) <> ''
            GROUP BY 1
        ),
        exception_rollup AS (
            SELECT
                trim(service_id) AS service_id,
                COUNT(*) AS exception_rows,
                SUM(CASE WHEN trim(exception_type) = '1' THEN 1 ELSE 0 END) AS added_dates_count,
                SUM(CASE WHEN trim(exception_type) = '2' THEN 1 ELSE 0 END) AS removed_dates_count,
                MIN(TRY_CAST(NULLIF(trim(date), '') AS BIGINT)) AS first_exception_date_int,
                MAX(TRY_CAST(NULLIF(trim(date), '') AS BIGINT)) AS last_exception_date_int
            FROM bronze_gtfs_calendar_dates
            WHERE service_id IS NOT NULL AND trim(service_id) <> ''
            GROUP BY 1
        )
        SELECT
            row_number() OVER (ORDER BY s.service_id) AS service_sk,
            s.service_id,
            COALESCE(c.monday, 0) = 1 AS monday,
            COALESCE(c.tuesday, 0) = 1 AS tuesday,
            COALESCE(c.wednesday, 0) = 1 AS wednesday,
            COALESCE(c.thursday, 0) = 1 AS thursday,
            COALESCE(c.friday, 0) = 1 AS friday,
            COALESCE(c.saturday, 0) = 1 AS saturday,
            COALESCE(c.sunday, 0) = 1 AS sunday,
            TRY_CAST(TRY_STRPTIME(CAST(c.start_date_int AS VARCHAR), '%Y%m%d') AS DATE) AS start_date,
            TRY_CAST(TRY_STRPTIME(CAST(c.end_date_int AS VARCHAR), '%Y%m%d') AS DATE) AS end_date,
            COALESCE(e.exception_rows, 0) AS exception_rows,
            COALESCE(e.added_dates_count, 0) AS added_dates_count,
            COALESCE(e.removed_dates_count, 0) AS removed_dates_count,
            TRY_CAST(TRY_STRPTIME(CAST(e.first_exception_date_int AS VARCHAR), '%Y%m%d') AS DATE)
                AS first_exception_date,
            TRY_CAST(TRY_STRPTIME(CAST(e.last_exception_date_int AS VARCHAR), '%Y%m%d') AS DATE)
                AS last_exception_date,
            (
                COALESCE(c.monday, 0)
                + COALESCE(c.tuesday, 0)
                + COALESCE(c.wednesday, 0)
                + COALESCE(c.thursday, 0)
                + COALESCE(c.friday, 0)
                + COALESCE(c.saturday, 0)
                + COALESCE(c.sunday, 0)
            ) AS weekly_active_days
        FROM services s
        LEFT JOIN calendar_rollup c
            ON s.service_id = c.service_id
        LEFT JOIN exception_rollup e
            ON s.service_id = e.service_id
        ORDER BY s.service_id
        """
    )
    return connection.execute("SELECT COUNT(*) FROM dim_service_gtfs").fetchone()[0]


def run() -> dict[str, Any]:
    """Build GTFS dimension parquet files and return row metrics."""
    paths = resolve_project_paths()
    dimensions_root = paths.project_root / "dimensions"
    connection = ensure_duckdb_connection(paths.db_path)
    _ensure_required_tables(connection, REQUIRED_GTFS_TABLES)

    route_rows = _build_dim_route_gtfs(connection)
    stop_rows = _build_dim_stop_gtfs(connection)
    service_rows = _build_dim_service_gtfs(connection)

    route_path = dimensions_root / "dim_route_gtfs.parquet"
    stop_path = dimensions_root / "dim_stop_gtfs.parquet"
    service_path = dimensions_root / "dim_service_gtfs.parquet"

    route_rows = _copy_table_to_parquet(connection, "dim_route_gtfs", route_path)
    stop_rows = _copy_table_to_parquet(connection, "dim_stop_gtfs", stop_path)
    service_rows = _copy_table_to_parquet(connection, "dim_service_gtfs", service_path)

    connection.close()
    return {
        "duckdb_path": paths.db_path.resolve().as_posix(),
        "outputs": {
            "dim_route_gtfs": route_path.resolve().as_posix(),
            "dim_stop_gtfs": stop_path.resolve().as_posix(),
            "dim_service_gtfs": service_path.resolve().as_posix(),
        },
        "row_counts": {
            "dim_route_gtfs": route_rows,
            "dim_stop_gtfs": stop_rows,
            "dim_service_gtfs": service_rows,
        },
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
