"""Build GTFS route-direction-stop bridge for Step 2 linkage."""

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


def _build_bridge(connection: Any) -> int:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE bridge_route_direction_stop AS
        WITH trip_base AS (
            SELECT
                trim(trip_id) AS trip_id,
                trim(route_id) AS route_id,
                trim(service_id) AS service_id,
                COALESCE(NULLIF(trim(direction_id), ''), 'unknown') AS direction_id
            FROM bronze_gtfs_trips
            WHERE trip_id IS NOT NULL
              AND trim(trip_id) <> ''
              AND route_id IS NOT NULL
              AND trim(route_id) <> ''
        ),
        stop_time_base AS (
            SELECT
                trim(trip_id) AS trip_id,
                trim(stop_id) AS stop_id,
                TRY_CAST(NULLIF(trim(stop_sequence), '') AS INTEGER) AS stop_sequence
            FROM bronze_gtfs_stop_times
            WHERE trip_id IS NOT NULL
              AND trim(trip_id) <> ''
              AND stop_id IS NOT NULL
              AND trim(stop_id) <> ''
        ),
        aggregate_bridge AS (
            SELECT
                t.route_id,
                t.direction_id,
                st.stop_id,
                COUNT(*) AS stop_time_rows,
                COUNT(DISTINCT t.trip_id) AS trip_count,
                COUNT(DISTINCT t.service_id) AS service_count,
                MIN(st.stop_sequence) AS first_stop_sequence,
                MAX(st.stop_sequence) AS last_stop_sequence
            FROM trip_base t
            JOIN stop_time_base st
                ON t.trip_id = st.trip_id
            GROUP BY 1, 2, 3
        )
        SELECT
            row_number() OVER (ORDER BY b.route_id, b.direction_id, b.stop_id) AS bridge_sk,
            b.route_id,
            NULLIF(trim(r.route_short_name), '') AS route_short_name,
            NULLIF(trim(r.route_long_name), '') AS route_long_name,
            TRY_CAST(NULLIF(trim(r.route_type), '') AS INTEGER) AS route_type,
            b.direction_id,
            b.stop_id,
            NULLIF(trim(s.stop_name), '') AS stop_name,
            TRY_CAST(NULLIF(trim(s.stop_lat), '') AS DOUBLE) AS stop_lat,
            TRY_CAST(NULLIF(trim(s.stop_lon), '') AS DOUBLE) AS stop_lon,
            b.trip_count,
            b.service_count,
            b.stop_time_rows,
            b.first_stop_sequence,
            b.last_stop_sequence
        FROM aggregate_bridge b
        LEFT JOIN bronze_gtfs_routes r
            ON b.route_id = trim(r.route_id)
        LEFT JOIN bronze_gtfs_stops s
            ON b.stop_id = trim(s.stop_id)
        ORDER BY b.route_id, b.direction_id, b.stop_id
        """
    )
    return connection.execute("SELECT COUNT(*) FROM bridge_route_direction_stop").fetchone()[0]


def run() -> dict[str, Any]:
    """Build route-direction-stop bridge parquet and return row metrics."""
    paths = resolve_project_paths()
    bridge_root = paths.project_root / "bridge"
    output_path = bridge_root / "bridge_route_direction_stop.parquet"

    connection = ensure_duckdb_connection(paths.db_path)
    _ensure_required_tables(connection, REQUIRED_GTFS_TABLES)
    bridge_rows = _build_bridge(connection)
    bridge_rows = _copy_table_to_parquet(connection, "bridge_route_direction_stop", output_path)
    connection.close()

    return {
        "duckdb_path": paths.db_path.resolve().as_posix(),
        "outputs": {
            "bridge_route_direction_stop": output_path.resolve().as_posix(),
        },
        "row_counts": {
            "bridge_route_direction_stop": bridge_rows,
        },
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
