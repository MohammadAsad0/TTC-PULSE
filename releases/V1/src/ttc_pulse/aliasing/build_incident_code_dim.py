"""Build incident code dimension from bronze bus/subway feeds."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import ensure_duckdb_connection, quote_identifier, resolve_project_paths

REQUIRED_TABLES = ["bronze_bus", "bronze_subway"]


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
        raise RuntimeError(f"Missing required tables for incident code dimension build: {joined}")


def _copy_table_to_parquet(connection: Any, table_name: str, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    escaped_path = output_path.resolve().as_posix().replace("'", "''")
    connection.execute(
        f"COPY {quote_identifier(table_name)} TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]


def _build_incident_code_dimension(connection: Any) -> int:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE dim_incident_code AS
        WITH bus_codes AS (
            SELECT
                upper(trim(Code)) AS incident_code,
                COUNT(*) AS bus_rows
            FROM bronze_bus
            WHERE Code IS NOT NULL
              AND trim(Code) <> ''
              AND upper(trim(Code)) NOT IN ('NONE', 'NULL', 'N/A')
            GROUP BY 1
        ),
        subway_codes AS (
            SELECT
                upper(trim(Code)) AS incident_code,
                COUNT(*) AS subway_rows
            FROM bronze_subway
            WHERE Code IS NOT NULL
              AND trim(Code) <> ''
              AND upper(trim(Code)) NOT IN ('NONE', 'NULL', 'N/A')
            GROUP BY 1
        ),
        unified_codes AS (
            SELECT
                COALESCE(b.incident_code, s.incident_code) AS incident_code,
                COALESCE(b.bus_rows, 0) AS bus_rows,
                COALESCE(s.subway_rows, 0) AS subway_rows
            FROM bus_codes b
            FULL OUTER JOIN subway_codes s
                ON b.incident_code = s.incident_code
        )
        SELECT
            row_number() OVER (ORDER BY incident_code) AS incident_code_sk,
            incident_code,
            bus_rows,
            subway_rows,
            (bus_rows + subway_rows) AS observed_rows,
            CASE
                WHEN bus_rows > 0 AND subway_rows > 0 THEN 'both'
                WHEN bus_rows > 0 THEN 'bus'
                WHEN subway_rows > 0 THEN 'subway'
                ELSE 'unknown'
            END AS source_mode,
            LEFT(incident_code, 1) AS code_family,
            CASE LEFT(incident_code, 1)
                WHEN 'M' THEN 'M-family'
                WHEN 'S' THEN 'S-family'
                WHEN 'T' THEN 'T-family'
                WHEN 'E' THEN 'E-family'
                WHEN 'P' THEN 'P-family'
                ELSE 'other-family'
            END AS code_family_label,
            NULL::VARCHAR AS incident_description,
            'bronze_code_only' AS description_source,
            'unresolved' AS mapping_status,
            'description_not_available_in_bronze_scope' AS mapping_rule,
            TRUE AS reference_gtfs_bridge_family,
            CURRENT_TIMESTAMP AS generated_at
        FROM unified_codes
        ORDER BY incident_code
        """
    )
    return connection.execute("SELECT COUNT(*) FROM dim_incident_code").fetchone()[0]


def run() -> dict[str, Any]:
    """Build incident code dimension parquet and return row metrics."""
    paths = resolve_project_paths()
    dimensions_root = paths.project_root / "dimensions"
    output_path = dimensions_root / "dim_incident_code.parquet"

    connection = ensure_duckdb_connection(paths.db_path)
    _ensure_required_tables(connection, REQUIRED_TABLES)
    dim_rows = _build_incident_code_dimension(connection)
    dim_rows = _copy_table_to_parquet(connection, "dim_incident_code", output_path)
    unresolved_count = connection.execute(
        "SELECT COUNT(*) FROM dim_incident_code WHERE mapping_status = 'unresolved'"
    ).fetchone()[0]
    connection.close()

    return {
        "duckdb_path": paths.db_path.resolve().as_posix(),
        "outputs": {
            "dim_incident_code": output_path.resolve().as_posix(),
        },
        "row_counts": {
            "dim_incident_code": dim_rows,
        },
        "unresolved_counts": {
            "incident_code_unresolved": unresolved_count,
        },
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
