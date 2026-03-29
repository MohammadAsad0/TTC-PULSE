"""Register Step 2 parquet outputs into DuckDB and log registration results."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import (
    ensure_duckdb_connection,
    quote_identifier,
    resolve_project_paths,
    sql_literal,
    utc_now_iso,
)


@dataclass(frozen=True)
class Step2TableSpec:
    """Mapping between required Step 2 table name and parquet path."""

    table_name: str
    relative_parquet_path: str


STEP2_TABLE_SPECS = [
    Step2TableSpec("silver_bus_events", "silver/silver_bus_events.parquet"),
    Step2TableSpec("silver_streetcar_events", "silver/silver_streetcar_events.parquet"),
    Step2TableSpec("silver_subway_events", "silver/silver_subway_events.parquet"),
    Step2TableSpec("silver_gtfsrt_alert_entities", "silver/silver_gtfsrt_alert_entities.parquet"),
    Step2TableSpec("fact_delay_events_norm", "silver/fact_delay_events_norm.parquet"),
    Step2TableSpec("fact_gtfsrt_alerts_norm", "silver/fact_gtfsrt_alerts_norm.parquet"),
    Step2TableSpec("dim_route_gtfs", "dimensions/dim_route_gtfs.parquet"),
    Step2TableSpec("dim_stop_gtfs", "dimensions/dim_stop_gtfs.parquet"),
    Step2TableSpec("dim_service_gtfs", "dimensions/dim_service_gtfs.parquet"),
    Step2TableSpec("dim_route_alias", "dimensions/dim_route_alias.parquet"),
    Step2TableSpec("dim_station_alias", "dimensions/dim_station_alias.parquet"),
    Step2TableSpec("dim_incident_code", "dimensions/dim_incident_code.parquet"),
    Step2TableSpec("bridge_route_direction_stop", "bridge/bridge_route_direction_stop.parquet"),
    Step2TableSpec("route_alias_review", "reviews/route_alias_review.parquet"),
    Step2TableSpec("station_alias_review", "reviews/station_alias_review.parquet"),
    Step2TableSpec("incident_code_review", "reviews/incident_code_review.parquet"),
]

LOG_FIELDNAMES = ["table_name", "row_count", "parquet_path", "registered_at", "status"]


def _register_from_parquet(connection: Any, table_name: str, parquet_path: Path) -> tuple[int, str]:
    if not parquet_path.exists():
        return 0, "missing_parquet"

    try:
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE {quote_identifier(table_name)} AS
            SELECT * FROM read_parquet({sql_literal(parquet_path.as_posix())})
            """
        )
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(table_name)}"
        ).fetchone()[0]
        return int(row_count), "registered"
    except Exception as exc:  # pragma: no cover
        return 0, f"error:{type(exc).__name__}"


def _write_registration_log(log_path: Path, rows: list[dict[str, Any]]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LOG_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def run(db_path: Path | None = None, log_path: Path | None = None) -> dict[str, Any]:
    """Register Step 2 parquet outputs into DuckDB with table-level logging."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_log_path = (log_path or (paths.logs_root / "step2_registration_log.csv")).resolve()

    connection = ensure_duckdb_connection(resolved_db_path)
    log_rows: list[dict[str, Any]] = []
    row_counts: dict[str, int] = {}
    parquet_files: dict[str, str] = {}

    try:
        for table_spec in STEP2_TABLE_SPECS:
            parquet_path = (paths.project_root / table_spec.relative_parquet_path).resolve()
            row_count, status = _register_from_parquet(connection, table_spec.table_name, parquet_path)
            registered_at = utc_now_iso()
            row_counts[table_spec.table_name] = row_count
            parquet_files[table_spec.table_name] = parquet_path.as_posix()
            log_rows.append(
                {
                    "table_name": table_spec.table_name,
                    "row_count": row_count,
                    "parquet_path": parquet_path.as_posix(),
                    "registered_at": registered_at,
                    "status": status,
                }
            )
    finally:
        connection.close()

    _write_registration_log(resolved_log_path, log_rows)

    return {
        "duckdb_path": resolved_db_path.as_posix(),
        "registration_log_path": resolved_log_path.as_posix(),
        "row_counts": row_counts,
        "parquet_files": parquet_files,
        "statuses": {row["table_name"]: row["status"] for row in log_rows},
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
