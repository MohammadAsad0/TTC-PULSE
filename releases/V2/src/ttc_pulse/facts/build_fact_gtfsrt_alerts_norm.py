"""Build Step 2 normalized GTFS-RT alert fact parquet from silver entities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from ttc_pulse.utils.project_setup import resolve_project_paths, sql_literal

OUTPUT_FILENAME = "fact_gtfsrt_alerts_norm.parquet"


def _create_empty_entities_view(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE OR REPLACE TEMP VIEW gtfsrt_entities AS
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


def _build_fact_sql() -> str:
    return """
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                COALESCE(feed_ts, snapshot_ts),
                alert_id,
                source_file,
                source_row_id
        ) AS alert_event_id,
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
        row_hash,
        ingested_at
    FROM gtfsrt_entities
    """


def run_build_fact_gtfsrt_alerts_norm(
    output_path: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Build fact_gtfsrt_alerts_norm parquet from silver GTFS-RT alert entities."""
    paths = resolve_project_paths()
    silver_root = paths.project_root / "silver"
    entities_path = (silver_root / "silver_gtfsrt_alert_entities.parquet").resolve()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output = (output_path or (silver_root / OUTPUT_FILENAME)).resolve()
    resolved_output.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(resolved_db_path))
    connection.execute("PRAGMA disable_progress_bar")

    if entities_path.exists():
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW gtfsrt_entities AS
            SELECT * FROM read_parquet({sql_literal(entities_path.as_posix())})
            """
        )
    else:
        _create_empty_entities_view(connection)

    connection.execute(f"CREATE OR REPLACE TEMP TABLE tmp_fact_gtfsrt_alerts_norm AS {_build_fact_sql()}")
    row_count = connection.execute("SELECT COUNT(*) FROM tmp_fact_gtfsrt_alerts_norm").fetchone()[0]
    connection.execute(
        """
        CREATE OR REPLACE TABLE fact_gtfsrt_alerts_norm AS
        SELECT * FROM tmp_fact_gtfsrt_alerts_norm
        """
    )
    table_row_count = connection.execute("SELECT COUNT(*) FROM fact_gtfsrt_alerts_norm").fetchone()[0]

    connection.execute(
        f"""
        COPY tmp_fact_gtfsrt_alerts_norm
        TO {sql_literal(resolved_output.as_posix())}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    connection.close()

    return {
        "duckdb_path": resolved_db_path.as_posix(),
        "output_path": resolved_output.as_posix(),
        "row_count": row_count,
        "table_name": "fact_gtfsrt_alerts_norm",
        "table_row_count": table_row_count,
        "inputs": {
            "silver_gtfsrt_alert_entities_parquet": entities_path.as_posix(),
            "entities_exists": entities_path.exists(),
        },
    }


def main() -> None:
    result = run_build_fact_gtfsrt_alerts_norm()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
