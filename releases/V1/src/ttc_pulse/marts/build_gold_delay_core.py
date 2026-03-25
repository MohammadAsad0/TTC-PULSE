"""Build the gold_delay_events_core mart."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.marts._gold_utils import create_delay_events_view, materialize_query_to_gold
from ttc_pulse.utils.project_setup import ensure_duckdb_connection, resolve_project_paths

TABLE_NAME = "gold_delay_events_core"
OUTPUT_FILENAME = "gold_delay_events_core.parquet"


def _build_query() -> str:
    return """
    SELECT
        mode,
        service_date,
        hour_bin,
        route_id_gtfs,
        station_canonical,
        incident_category,
        COUNT(*)::BIGINT AS event_count,
        quantile_cont(min_delay, 0.5) FILTER (WHERE min_delay IS NOT NULL) AS min_delay_p50,
        quantile_cont(min_delay, 0.9) FILTER (WHERE min_delay IS NOT NULL) AS min_delay_p90,
        quantile_cont(min_gap, 0.9) FILTER (WHERE min_gap IS NOT NULL) AS min_gap_p90
    FROM delay_events_src
    GROUP BY 1, 2, 3, 4, 5, 6
    """


def run_build_gold_delay_core(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build gold_delay_events_core from canonical delay events."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output_path = (output_path or (paths.project_root / "gold" / OUTPUT_FILENAME)).resolve()
    caveats: list[str] = []

    connection = ensure_duckdb_connection(resolved_db_path)
    try:
        source_info = create_delay_events_view(connection, paths.project_root, caveats)
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
    result = run_build_gold_delay_core()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
