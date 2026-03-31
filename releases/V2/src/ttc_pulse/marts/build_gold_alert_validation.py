"""Build the gold_alert_validation mart."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.marts._gold_utils import (
    create_alert_events_view,
    ensure_table_from_parquet,
    get_table_columns,
    materialize_query_to_gold,
)
from ttc_pulse.utils.project_setup import ensure_duckdb_connection, resolve_project_paths

TABLE_NAME = "gold_alert_validation"
OUTPUT_FILENAME = "gold_alert_validation.parquet"


def _build_query() -> str:
    return """
    WITH route_dim AS (
        SELECT route_id
        FROM dim_route_gtfs
        WHERE route_id IS NOT NULL
    ),
    stop_dim AS (
        SELECT stop_id
        FROM dim_stop_gtfs
        WHERE stop_id IS NOT NULL
    )
    SELECT
        a.snapshot_ts,
        a.alert_id,
        a.route_id_gtfs,
        a.stop_id_gtfs,
        a.selector_scope,
        CASE
            WHEN a.match_status IS NOT NULL AND a.match_status <> 'unknown' THEN a.match_status
            WHEN a.route_id_gtfs IS NOT NULL AND r.route_id IS NULL THEN 'invalid_route'
            WHEN a.stop_id_gtfs IS NOT NULL AND s.stop_id IS NULL THEN 'invalid_stop'
            WHEN a.route_id_gtfs IS NULL AND a.stop_id_gtfs IS NULL THEN 'missing_selector'
            ELSE 'matched'
        END AS match_status,
        a.header_text,
        a.description_text,
        CASE WHEN a.route_id_gtfs IS NULL THEN NULL ELSE r.route_id IS NOT NULL END AS route_id_valid,
        CASE WHEN a.stop_id_gtfs IS NULL THEN NULL ELSE s.stop_id IS NOT NULL END AS stop_id_valid,
        CASE
            WHEN a.route_id_gtfs IS NULL AND a.stop_id_gtfs IS NULL THEN FALSE
            WHEN (a.route_id_gtfs IS NOT NULL AND r.route_id IS NULL)
                OR (a.stop_id_gtfs IS NOT NULL AND s.stop_id IS NULL)
                THEN FALSE
            ELSE TRUE
        END AS selector_valid
    FROM alert_events_src AS a
    LEFT JOIN route_dim AS r
        ON a.route_id_gtfs = r.route_id
    LEFT JOIN stop_dim AS s
        ON a.stop_id_gtfs = s.stop_id
    """


def run_build_gold_alert_validation(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build GTFS-RT alert selector validation mart."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output_path = (output_path or (paths.project_root / "gold" / OUTPUT_FILENAME)).resolve()
    caveats: list[str] = []

    connection = ensure_duckdb_connection(resolved_db_path)
    try:
        source_info = create_alert_events_view(connection, paths.project_root, caveats)

        ensure_table_from_parquet(connection, "dim_route_gtfs", paths.project_root, caveats)
        ensure_table_from_parquet(connection, "dim_stop_gtfs", paths.project_root, caveats)

        route_columns = get_table_columns(connection, "dim_route_gtfs")
        stop_columns = get_table_columns(connection, "dim_stop_gtfs")
        if "route_id" not in route_columns:
            caveats.append("dim_route_gtfs: missing route_id; route selector validation may be incomplete.")
        if "stop_id" not in stop_columns:
            caveats.append("dim_stop_gtfs: missing stop_id; stop selector validation may be incomplete.")

        row_count = materialize_query_to_gold(
            connection=connection,
            query_sql=_build_query(),
            table_name=TABLE_NAME,
            parquet_path=resolved_output_path,
        )
    finally:
        connection.close()

    return {
        "table_name": TABLE_NAME,
        "output_path": resolved_output_path.as_posix(),
        "duckdb_path": resolved_db_path.as_posix(),
        "row_count": row_count,
        "status": "built_with_caveats" if caveats else "built",
        "caveats": caveats,
        "source": source_info,
    }


def main() -> None:
    result = run_build_gold_alert_validation()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
