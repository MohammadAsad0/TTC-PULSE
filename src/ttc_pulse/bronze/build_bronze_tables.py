"""Build Step 1 raw registries and bronze tables in DuckDB."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.alerts.load_parsed_into_bronze import load_parsed_alerts_into_bronze
from ttc_pulse.ingestion.ingest_bus import ingest_bus_registry
from ttc_pulse.ingestion.ingest_gtfs import ingest_gtfs_registry
from ttc_pulse.ingestion.ingest_subway import ingest_subway_registry
from ttc_pulse.ingestion.register_gtfsrt_snapshots import register_gtfsrt_snapshots
from ttc_pulse.utils.project_setup import (
    ensure_duckdb_connection,
    ensure_project_layout,
    quote_identifier,
    resolve_project_paths,
    sql_file_array,
    sql_literal,
    utc_now_iso,
    write_log_rows,
)

GTFS_REQUIRED_BRONZE_TABLES = [
    "routes",
    "trips",
    "stop_times",
    "stops",
    "calendar",
    "calendar_dates",
]
GTFS_OPTIONAL_BRONZE_TABLES = ["shapes"]


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


def _table_count(connection: Any, table_name: str) -> int:
    if not _table_exists(connection, table_name):
        return 0
    return connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]


def _row_hash_expression(data_columns: list[str]) -> str:
    if not data_columns:
        return "md5('')"
    packed = ", ".join(
        f"{quote_identifier(column)} := {quote_identifier(column)}" for column in data_columns
    )
    return f"md5(to_json(struct_pack({packed})))"


def _create_empty_bronze_table(connection: Any, table_name: str) -> None:
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {quote_identifier(table_name)} (
            source_file VARCHAR,
            source_sheet VARCHAR,
            source_row_id BIGINT,
            ingested_at TIMESTAMP,
            row_hash VARCHAR
        )
        """
    )


def _build_bronze_from_csv_files(
    connection: Any,
    table_name: str,
    file_paths: list[Path],
    ingested_at: str,
) -> int:
    if not file_paths:
        _create_empty_bronze_table(connection, table_name)
        return 0

    stage_name = f"tmp_stage_{table_name}"
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {quote_identifier(stage_name)} AS
        SELECT *
        FROM read_csv_auto(
            {sql_file_array(file_paths)},
            all_varchar = TRUE,
            union_by_name = TRUE,
            filename = TRUE,
            ignore_errors = TRUE
        )
        """
    )

    stage_info = connection.execute(f"PRAGMA table_info('{stage_name}')").fetchall()
    stage_columns = [row[1] for row in stage_info]
    data_columns = [column for column in stage_columns if column != "filename"]
    if not data_columns:
        _create_empty_bronze_table(connection, table_name)
        return 0

    data_column_sql = ",\n            ".join(quote_identifier(column) for column in data_columns)
    row_hash_sql = _row_hash_expression(data_columns)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {quote_identifier(table_name)} AS
        SELECT
            {data_column_sql},
            filename AS source_file,
            NULL::VARCHAR AS source_sheet,
            row_number() OVER (PARTITION BY filename) AS source_row_id,
            CAST({sql_literal(ingested_at)} AS TIMESTAMP) AS ingested_at,
            {row_hash_sql} AS row_hash
        FROM {quote_identifier(stage_name)}
        """
    )
    return _table_count(connection, table_name)


def _build_bronze_from_single_csv(
    connection: Any,
    table_name: str,
    source_path: Path,
    ingested_at: str,
) -> int:
    if not source_path.exists():
        _create_empty_bronze_table(connection, table_name)
        return 0

    stage_name = f"tmp_stage_{table_name}"
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {quote_identifier(stage_name)} AS
        SELECT *
        FROM read_csv_auto(
            {sql_literal(source_path.resolve().as_posix())},
            all_varchar = TRUE,
            union_by_name = TRUE,
            ignore_errors = TRUE
        )
        """
    )

    stage_info = connection.execute(f"PRAGMA table_info('{stage_name}')").fetchall()
    data_columns = [row[1] for row in stage_info]
    if not data_columns:
        _create_empty_bronze_table(connection, table_name)
        return 0

    data_column_sql = ",\n            ".join(quote_identifier(column) for column in data_columns)
    row_hash_sql = _row_hash_expression(data_columns)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {quote_identifier(table_name)} AS
        SELECT
            {data_column_sql},
            {sql_literal(source_path.resolve().as_posix())} AS source_file,
            NULL::VARCHAR AS source_sheet,
            row_number() OVER () AS source_row_id,
            CAST({sql_literal(ingested_at)} AS TIMESTAMP) AS ingested_at,
            {row_hash_sql} AS row_hash
        FROM {quote_identifier(stage_name)}
        """
    )
    return _table_count(connection, table_name)


def _write_source_inventory(
    source_inventory_path: Path,
    run_id: str,
    ingested_at: str,
    bus_result: dict[str, Any],
    subway_result: dict[str, Any],
    gtfs_result: dict[str, Any],
    gtfsrt_result: dict[str, Any],
) -> None:
    lines: list[str] = []
    lines.append("# Source Inventory (Step 1 Ingestion/Foundation)")
    lines.append("")
    lines.append(f"- Run ID: `{run_id}`")
    lines.append(f"- Ingested at (UTC): `{ingested_at}`")
    lines.append("")
    lines.append("## Source Roots Used")
    lines.append("")
    lines.append(f"- Bus source root: `{bus_result['source_root']}`")
    lines.append(f"- Subway source root: `{subway_result['source_root']}`")
    lines.append(f"- GTFS source root: `{gtfs_result['source_root']}`")
    lines.append("- GTFS-RT candidate roots:")
    for root in gtfsrt_result["candidate_roots"]:
        lines.append(f"  - `{root}`")
    lines.append("")
    lines.append("## Discovery Metrics")
    lines.append("")
    lines.append(f"- Bus files discovered: **{bus_result['discovered_files']}**")
    lines.append(f"- Subway files discovered: **{subway_result['discovered_files']}**")
    lines.append(f"- GTFS files discovered: **{gtfs_result['discovered_files']}**")
    lines.append(f"- GTFS-RT snapshot files discovered: **{gtfsrt_result['discovered_files']}**")
    lines.append("")
    lines.append("## Raw Registry Outputs")
    lines.append("")
    lines.append(
        f"- Bus registry CSV: `{bus_result['registry_csv_path']}` (table `{bus_result['registry_table']}`)"
    )
    lines.append(
        f"- Subway registry CSV: `{subway_result['registry_csv_path']}` (table `{subway_result['registry_table']}`)"
    )
    lines.append(
        f"- GTFS registry CSV: `{gtfs_result['registry_csv_path']}` (table `{gtfs_result['registry_table']}`)"
    )
    lines.append(
        f"- GTFS-RT registry CSV: `{gtfsrt_result['registry_csv_path']}` (table `{gtfsrt_result['registry_table']}`)"
    )
    lines.append("")
    lines.append("## GTFS File Map")
    lines.append("")
    for table_name in GTFS_REQUIRED_BRONZE_TABLES + GTFS_OPTIONAL_BRONZE_TABLES:
        source_path = gtfs_result["table_files"].get(table_name)
        if source_path:
            lines.append(f"- `{table_name}` -> `{source_path}`")
        else:
            lines.append(f"- `{table_name}` -> _not found_")
    lines.append("")
    lines.append("## GTFS-RT Snapshot Paths")
    lines.append("")
    if gtfsrt_result["files"]:
        for source_path in gtfsrt_result["files"]:
            lines.append(f"- `{source_path}`")
    else:
        lines.append("- _No GTFS-RT snapshot files discovered._")

    source_inventory_path.parent.mkdir(parents=True, exist_ok=True)
    source_inventory_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_step1_summary(
    summary_path: Path,
    run_id: str,
    ingested_at: str,
    db_path: Path,
    row_counts: dict[str, int],
    assumptions: list[str],
) -> None:
    lines: list[str] = []
    lines.append("# Step 1 Summary (Ingestion/Foundation)")
    lines.append("")
    lines.append(f"- Run ID: `{run_id}`")
    lines.append(f"- Ingested at (UTC): `{ingested_at}`")
    lines.append(f"- DuckDB path: `{db_path.resolve().as_posix()}`")
    lines.append("")
    lines.append("## Row Counts")
    lines.append("")
    for table_name in sorted(row_counts):
        lines.append(f"- `{table_name}`: **{row_counts[table_name]}**")
    lines.append("")
    lines.append("## Assumptions / Notes")
    lines.append("")
    for assumption in assumptions:
        lines.append(f"- {assumption}")

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_step1() -> dict[str, Any]:
    """Execute Step 1 ingestion/foundation build and return metrics."""
    paths = resolve_project_paths()
    ensure_project_layout(paths)
    run_id = utc_now_iso().replace("-", "").replace(":", "")
    ingested_at = utc_now_iso()

    log_rows: list[dict[str, Any]] = []
    assumptions: list[str] = []

    connection = ensure_duckdb_connection(paths.db_path)
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "ensure_duckdb",
            "status": "ok",
            "row_count": 1,
            "details": f"duckdb={paths.db_path.resolve().as_posix()}",
        }
    )

    bus_result = ingest_bus_registry(connection, run_id=run_id, ingested_at=ingested_at)
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "ingest_bus_registry",
            "status": "ok",
            "row_count": bus_result["appended_registry_rows"],
            "details": f"files={bus_result['discovered_files']}",
        }
    )

    subway_result = ingest_subway_registry(connection, run_id=run_id, ingested_at=ingested_at)
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "ingest_subway_registry",
            "status": "ok",
            "row_count": subway_result["appended_registry_rows"],
            "details": f"files={subway_result['discovered_files']}",
        }
    )

    gtfs_result = ingest_gtfs_registry(connection, run_id=run_id, ingested_at=ingested_at)
    gtfs_missing = gtfs_result["missing_required_tables"]
    gtfs_status = "ok" if not gtfs_missing else "warning"
    if gtfs_missing:
        assumptions.append(
            "Missing required GTFS files for tables: " + ", ".join(sorted(gtfs_missing))
        )
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "ingest_gtfs_registry",
            "status": gtfs_status,
            "row_count": gtfs_result["appended_registry_rows"],
            "details": f"files={gtfs_result['discovered_files']}",
        }
    )

    gtfsrt_result = register_gtfsrt_snapshots(connection, run_id=run_id, ingested_at=ingested_at)
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "register_gtfsrt_snapshots",
            "status": "ok",
            "row_count": gtfsrt_result["appended_registry_rows"],
            "details": f"files={gtfsrt_result['discovered_files']}",
        }
    )

    bus_bronze_count = _build_bronze_from_csv_files(
        connection,
        table_name="bronze_bus",
        file_paths=[Path(path) for path in bus_result["files"]],
        ingested_at=ingested_at,
    )
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "build_bronze_bus",
            "status": "ok",
            "row_count": bus_bronze_count,
            "details": "row-preserving bus bronze with lineage",
        }
    )

    subway_bronze_count = _build_bronze_from_csv_files(
        connection,
        table_name="bronze_subway",
        file_paths=[Path(path) for path in subway_result["files"]],
        ingested_at=ingested_at,
    )
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "build_bronze_subway",
            "status": "ok",
            "row_count": subway_bronze_count,
            "details": "row-preserving subway bronze with lineage",
        }
    )

    gtfs_bronze_counts: dict[str, int] = {}
    for table_name in GTFS_REQUIRED_BRONZE_TABLES + GTFS_OPTIONAL_BRONZE_TABLES:
        bronze_table = f"bronze_gtfs_{table_name}"
        source_path = gtfs_result["table_files"].get(table_name)
        if source_path:
            row_count = _build_bronze_from_single_csv(
                connection,
                table_name=bronze_table,
                source_path=Path(source_path),
                ingested_at=ingested_at,
            )
            status = "ok"
            detail = f"source={source_path}"
        else:
            _create_empty_bronze_table(connection, bronze_table)
            row_count = 0
            status = "warning" if table_name in GTFS_REQUIRED_BRONZE_TABLES else "ok"
            detail = "source file not found; created empty shell table"
        gtfs_bronze_counts[bronze_table] = row_count
        log_rows.append(
            {
                "run_id": run_id,
                "logged_at": utc_now_iso(),
                "step": f"build_{bronze_table}",
                "status": status,
                "row_count": row_count,
                "details": detail,
            }
        )

    parsed_entities_path = (paths.project_root / "alerts" / "parsed" / "service_alert_entities.csv").resolve()
    gtfsrt_load_result = load_parsed_alerts_into_bronze(
        connection=connection,
        parsed_csv_path=parsed_entities_path,
        ingested_at=ingested_at,
    )
    gtfsrt_status = "ok" if gtfsrt_load_result["status"] == "loaded" else "warning"
    if gtfsrt_load_result["status"] == "missing_parsed_csv":
        assumptions.append(
            "Parsed GTFS-RT alerts CSV was not found; bronze_gtfsrt tables were created as empty shells."
        )
    elif gtfsrt_load_result["status"] == "parsed_csv_empty":
        assumptions.append(
            "Parsed GTFS-RT alerts CSV exists but has zero rows; bronze_gtfsrt tables remain empty."
        )
    elif gtfsrt_load_result["status"] == "loaded":
        assumptions.append(
            "GTFS-RT bronze tables were populated from alerts/parsed/service_alert_entities.csv."
        )
    if gtfsrt_result["discovered_files"] > 0 and gtfsrt_load_result["status"] != "loaded":
        assumptions.append(
            "GTFS-RT snapshots were discovered but parsed CSV did not provide rows for bronze_gtfsrt loading."
        )
    gtfsrt_details = (
        f"status={gtfsrt_load_result['status']}; "
        f"parsed_rows={gtfsrt_load_result['input_rows']}; "
        f"source={gtfsrt_load_result['parsed_csv_path']}"
    )
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "load_gtfsrt_parsed_into_bronze",
            "status": gtfsrt_status,
            "row_count": gtfsrt_load_result["bronze_entities_row_count"],
            "details": gtfsrt_details,
        }
    )

    raw_registry_tables = [
        bus_result["registry_table"],
        subway_result["registry_table"],
        gtfs_result["registry_table"],
        gtfsrt_result["registry_table"],
    ]
    bronze_tables = [
        "bronze_bus",
        "bronze_subway",
        *sorted(gtfs_bronze_counts.keys()),
        "bronze_gtfsrt_alerts",
        "bronze_gtfsrt_entities",
    ]

    row_counts = {
        table_name: _table_count(connection, table_name)
        for table_name in sorted(set(raw_registry_tables + bronze_tables))
    }

    log_path = paths.logs_root / "ingestion_log.csv"
    write_log_rows(log_path, log_rows)

    source_inventory_path = paths.docs_root / "source_inventory.md"
    _write_source_inventory(
        source_inventory_path=source_inventory_path,
        run_id=run_id,
        ingested_at=ingested_at,
        bus_result=bus_result,
        subway_result=subway_result,
        gtfs_result=gtfs_result,
        gtfsrt_result=gtfsrt_result,
    )

    summary_path = paths.docs_root / "step1_summary.md"
    _write_step1_summary(
        summary_path=summary_path,
        run_id=run_id,
        ingested_at=ingested_at,
        db_path=paths.db_path,
        row_counts=row_counts,
        assumptions=assumptions,
    )
    log_rows.append(
        {
            "run_id": run_id,
            "logged_at": utc_now_iso(),
            "step": "write_docs",
            "status": "ok",
            "row_count": 2,
            "details": "source_inventory.md and step1_summary.md updated",
        }
    )
    write_log_rows(log_path, [log_rows[-1]])

    connection.close()
    return {
        "run_id": run_id,
        "ingested_at": ingested_at,
        "duckdb_path": paths.db_path.resolve().as_posix(),
        "source_results": {
            "bus": bus_result,
            "subway": subway_result,
            "gtfs": gtfs_result,
            "gtfsrt": gtfsrt_result,
        },
        "row_counts": row_counts,
        "assumptions": assumptions,
        "artifacts": {
            "ingestion_log_csv": log_path.resolve().as_posix(),
            "source_inventory_md": source_inventory_path.resolve().as_posix(),
            "step1_summary_md": summary_path.resolve().as_posix(),
        },
    }


def main() -> None:
    result = run_step1()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
