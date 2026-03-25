"""Build QA summary for bus quality-gate hardening evidence."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import duckdb

from ttc_pulse.utils.project_setup import resolve_project_paths, utc_now_iso

OUTPUT_FILENAME = "bus_quality_gate_summary.csv"


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["metric", "value", "notes"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _safe_count(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def run_build_bus_quality_gate_summary(output_path: Path | None = None) -> dict[str, Any]:
    """Generate a QA CSV proving unmatched-route and sentinel handling impacts."""
    paths = resolve_project_paths()
    fact_path = (paths.project_root / "silver" / "fact_delay_events_norm.parquet").resolve()
    dim_route_path = (paths.project_root / "dimensions" / "dim_route_gtfs.parquet").resolve()
    resolved_output_path = (
        output_path or (paths.project_root / "outputs" / "qa" / OUTPUT_FILENAME)
    ).resolve()

    if not fact_path.exists() or not dim_route_path.exists():
        rows = [
            {
                "metric": "status",
                "value": "missing_inputs",
                "notes": f"Required parquet missing: fact={fact_path.exists()}, dim_route={dim_route_path.exists()}",
            }
        ]
        _write_rows(resolved_output_path, rows)
        return {
            "output_path": resolved_output_path.as_posix(),
            "status": "missing_inputs",
            "generated_at_utc": utc_now_iso(),
        }

    connection = duckdb.connect()
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW fact_bus AS
        SELECT
            mode,
            NULLIF(TRIM(CAST(route_id_gtfs AS VARCHAR)), '') AS route_id_gtfs_raw,
            NULLIF(TRIM(CAST(route_short_name_norm AS VARCHAR)), '') AS route_short_name_norm,
            NULLIF(TRIM(CAST(line_code_norm AS VARCHAR)), '') AS line_code_norm,
            LOWER(COALESCE(link_status, 'unknown')) AS link_status_norm,
            CAST(min_delay AS DOUBLE) AS min_delay,
            CAST(min_gap AS DOUBLE) AS min_gap,
            COALESCE(
                NULLIF(TRIM(CAST(route_id_gtfs AS VARCHAR)), ''),
                NULLIF(TRIM(CAST(route_short_name_norm AS VARCHAR)), ''),
                NULLIF(TRIM(CAST(line_code_norm AS VARCHAR)), '')
            ) AS legacy_route_key
        FROM read_parquet('{fact_path.as_posix()}')
        WHERE LOWER(COALESCE(mode, '')) = 'bus'
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW bus_gtfs_reference AS
        SELECT DISTINCT route_id
        FROM read_parquet('{dim_route_path.as_posix()}')
        WHERE route_mode = 'bus'
            AND route_id IS NOT NULL
            AND TRIM(route_id) <> ''
        """
    )

    total_bus_rows = _safe_count(connection, "SELECT COUNT(*) FROM fact_bus")
    excluded_unmatched_rows = _safe_count(
        connection,
        """
        SELECT COUNT(*)
        FROM fact_bus
        WHERE legacy_route_key IS NOT NULL
            AND link_status_norm <> 'matched'
        """,
    )
    sentinel_delay_rows = _safe_count(
        connection,
        "SELECT COUNT(*) FROM fact_bus WHERE min_delay IS NOT NULL AND min_delay >= 999",
    )
    sentinel_gap_rows = _safe_count(
        connection,
        "SELECT COUNT(*) FROM fact_bus WHERE min_gap IS NOT NULL AND min_gap >= 999",
    )
    pre_route_entity_count = _safe_count(
        connection,
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT legacy_route_key
            FROM fact_bus
            WHERE legacy_route_key IS NOT NULL
        ) AS x
        """,
    )
    post_route_entity_count = _safe_count(
        connection,
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT f.route_id_gtfs_raw
            FROM fact_bus f
            INNER JOIN bus_gtfs_reference b
                ON f.route_id_gtfs_raw = b.route_id
            WHERE f.link_status_norm = 'matched'
                AND f.route_id_gtfs_raw IS NOT NULL
        ) AS x
        """,
    )
    pre_non_gtfs_entities = _safe_count(
        connection,
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT legacy_route_key
            FROM fact_bus
            WHERE legacy_route_key IS NOT NULL
        ) p
        LEFT JOIN bus_gtfs_reference b
            ON p.legacy_route_key = b.route_id
        WHERE b.route_id IS NULL
        """,
    )
    pre_contains_714 = _safe_count(
        connection,
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT legacy_route_key
            FROM fact_bus
            WHERE legacy_route_key = '714'
        ) AS x
        """,
    )
    post_contains_714 = _safe_count(
        connection,
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT f.route_id_gtfs_raw
            FROM fact_bus f
            INNER JOIN bus_gtfs_reference b
                ON f.route_id_gtfs_raw = b.route_id
            WHERE f.link_status_norm = 'matched'
                AND f.route_id_gtfs_raw = '714'
        ) AS x
        """,
    )
    connection.close()

    generated_at = utc_now_iso()
    rows = [
        {"metric": "generated_at_utc", "value": generated_at, "notes": "Audit timestamp"},
        {"metric": "total_bus_rows_fact", "value": str(total_bus_rows), "notes": "Bus rows in canonical fact"},
        {
            "metric": "excluded_unmatched_bus_rows_from_stakeholder_route_marts",
            "value": str(excluded_unmatched_rows),
            "notes": "Rows legacy logic could include via fallback route key but strict gate excludes",
        },
        {
            "metric": "sentinel_delay_rows_ge_999",
            "value": str(sentinel_delay_rows),
            "notes": "Bus min_delay rows nullified for severity metrics",
        },
        {
            "metric": "sentinel_gap_rows_ge_999",
            "value": str(sentinel_gap_rows),
            "notes": "Bus min_gap rows nullified for regularity metrics",
        },
        {
            "metric": "pre_route_entity_count_legacy",
            "value": str(pre_route_entity_count),
            "notes": "Distinct legacy route keys using fallback coalesce(route_id, short_name, line_code)",
        },
        {
            "metric": "post_route_entity_count_gated",
            "value": str(post_route_entity_count),
            "notes": "Distinct GTFS-backed bus route_ids after strict stakeholder gate",
        },
        {
            "metric": "pre_route_entities_without_gtfs_bus_match",
            "value": str(pre_non_gtfs_entities),
            "notes": "Legacy route entities not found in GTFS bus route dictionary",
        },
        {
            "metric": "pre_contains_route_714",
            "value": str(pre_contains_714),
            "notes": "Legacy route-key presence check for 714",
        },
        {
            "metric": "post_contains_route_714",
            "value": str(post_contains_714),
            "notes": "Strict gated stakeholder bus routes should be zero",
        },
    ]
    _write_rows(resolved_output_path, rows)

    return {
        "output_path": resolved_output_path.as_posix(),
        "status": "built",
        "generated_at_utc": generated_at,
        "metrics_written": len(rows),
    }


def main() -> None:
    result = run_build_bus_quality_gate_summary()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

