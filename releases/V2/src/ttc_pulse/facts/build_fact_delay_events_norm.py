"""Build Step 2 normalized delay fact parquet from silver bus/subway events."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from ttc_pulse.utils.project_setup import resolve_project_paths, sql_literal

OUTPUT_FILENAME = "fact_delay_events_norm.parquet"


def _create_empty_bus_view(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE OR REPLACE TEMP VIEW bus_events AS
        SELECT
            CAST(NULL AS BIGINT) AS event_id,
            CAST(NULL AS DATE) AS service_date,
            CAST(NULL AS TIMESTAMP) AS event_ts,
            CAST(NULL AS VARCHAR) AS day_name,
            CAST(NULL AS INTEGER) AS hour_bin,
            CAST(NULL AS DATE) AS month_bin,
            CAST(NULL AS VARCHAR) AS route_label_raw,
            CAST(NULL AS VARCHAR) AS route_short_name_norm,
            CAST(NULL AS VARCHAR) AS route_id_gtfs,
            CAST(NULL AS VARCHAR) AS location_text_raw,
            CAST(NULL AS VARCHAR) AS station_text_raw,
            CAST(NULL AS VARCHAR) AS incident_text_raw,
            CAST(NULL AS VARCHAR) AS incident_code_raw,
            CAST(NULL AS VARCHAR) AS incident_category,
            CAST(NULL AS DOUBLE) AS min_delay,
            CAST(NULL AS DOUBLE) AS min_gap,
            CAST(NULL AS VARCHAR) AS direction_raw,
            CAST(NULL AS VARCHAR) AS direction_norm,
            CAST(NULL AS VARCHAR) AS vehicle_id_raw,
            CAST(NULL AS VARCHAR) AS match_method,
            CAST(NULL AS DOUBLE) AS match_confidence,
            CAST(NULL AS VARCHAR) AS link_status,
            CAST(NULL AS VARCHAR) AS source_file,
            CAST(NULL AS VARCHAR) AS source_sheet,
            CAST(NULL AS BIGINT) AS source_row_id,
            CAST(NULL AS TIMESTAMP) AS ingested_at,
            CAST(NULL AS VARCHAR) AS row_hash
        WHERE FALSE
        """
    )


def _create_empty_subway_view(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE OR REPLACE TEMP VIEW subway_events AS
        SELECT
            CAST(NULL AS BIGINT) AS event_id,
            CAST(NULL AS DATE) AS service_date,
            CAST(NULL AS TIMESTAMP) AS event_ts,
            CAST(NULL AS VARCHAR) AS day_name,
            CAST(NULL AS INTEGER) AS hour_bin,
            CAST(NULL AS DATE) AS month_bin,
            CAST(NULL AS VARCHAR) AS line_code_raw,
            CAST(NULL AS VARCHAR) AS line_code_norm,
            CAST(NULL AS VARCHAR) AS route_id_gtfs,
            CAST(NULL AS VARCHAR) AS station_text_raw,
            CAST(NULL AS VARCHAR) AS station_canonical,
            CAST(NULL AS VARCHAR) AS location_text_raw,
            CAST(NULL AS VARCHAR) AS incident_text_raw,
            CAST(NULL AS VARCHAR) AS incident_code_raw,
            CAST(NULL AS VARCHAR) AS incident_category,
            CAST(NULL AS DOUBLE) AS min_delay,
            CAST(NULL AS DOUBLE) AS min_gap,
            CAST(NULL AS VARCHAR) AS direction_raw,
            CAST(NULL AS VARCHAR) AS direction_norm,
            CAST(NULL AS VARCHAR) AS vehicle_id_raw,
            CAST(NULL AS VARCHAR) AS match_method,
            CAST(NULL AS DOUBLE) AS match_confidence,
            CAST(NULL AS VARCHAR) AS link_status,
            CAST(NULL AS VARCHAR) AS source_file,
            CAST(NULL AS VARCHAR) AS source_sheet,
            CAST(NULL AS BIGINT) AS source_row_id,
            CAST(NULL AS TIMESTAMP) AS ingested_at,
            CAST(NULL AS VARCHAR) AS row_hash
        WHERE FALSE
        """
    )


def _build_fact_sql() -> str:
    return """
    WITH bus_rows AS (
        SELECT
            'bus' AS mode,
            service_date,
            event_ts,
            day_name,
            hour_bin,
            month_bin,
            route_label_raw,
            route_short_name_norm,
            route_id_gtfs,
            NULL::VARCHAR AS line_code_raw,
            NULL::VARCHAR AS line_code_norm,
            location_text_raw,
            station_text_raw,
            NULL::VARCHAR AS station_canonical,
            incident_text_raw,
            incident_code_raw,
            incident_category,
            min_delay,
            min_gap,
            direction_raw,
            direction_norm,
            vehicle_id_raw,
            match_method,
            match_confidence,
            link_status,
            'bus' AS source_mode,
            source_file,
            source_sheet,
            source_row_id,
            row_hash,
            ingested_at
        FROM bus_events
    ),
    subway_rows AS (
        SELECT
            'subway' AS mode,
            service_date,
            event_ts,
            day_name,
            hour_bin,
            month_bin,
            NULL::VARCHAR AS route_label_raw,
            NULL::VARCHAR AS route_short_name_norm,
            route_id_gtfs,
            line_code_raw,
            line_code_norm,
            location_text_raw,
            station_text_raw,
            station_canonical,
            incident_text_raw,
            incident_code_raw,
            incident_category,
            min_delay,
            min_gap,
            direction_raw,
            direction_norm,
            vehicle_id_raw,
            match_method,
            match_confidence,
            link_status,
            'subway' AS source_mode,
            source_file,
            source_sheet,
            source_row_id,
            row_hash,
            ingested_at
        FROM subway_events
    ),
    unioned AS (
        SELECT * FROM bus_rows
        UNION ALL
        SELECT * FROM subway_rows
    )
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                COALESCE(event_ts, CAST(service_date AS TIMESTAMP)),
                mode,
                source_file,
                source_row_id
        ) AS event_id,
        mode,
        service_date,
        event_ts,
        day_name,
        hour_bin,
        month_bin,
        route_label_raw,
        route_short_name_norm,
        route_id_gtfs,
        line_code_raw,
        line_code_norm,
        location_text_raw,
        station_text_raw,
        station_canonical,
        incident_text_raw,
        incident_code_raw,
        incident_category,
        min_delay,
        min_gap,
        direction_raw,
        direction_norm,
        vehicle_id_raw,
        match_method,
        match_confidence,
        link_status,
        source_mode,
        source_file,
        source_sheet,
        source_row_id,
        row_hash,
        ingested_at
    FROM unioned
    """


def run_build_fact_delay_events_norm(output_path: Path | None = None) -> dict[str, Any]:
    """Build fact_delay_events_norm parquet from existing silver bus/subway parquet files."""
    paths = resolve_project_paths()
    silver_root = paths.project_root / "silver"
    bus_path = (silver_root / "silver_bus_events.parquet").resolve()
    subway_path = (silver_root / "silver_subway_events.parquet").resolve()
    resolved_output = (output_path or (silver_root / OUTPUT_FILENAME)).resolve()
    resolved_output.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    connection.execute("PRAGMA disable_progress_bar")

    if bus_path.exists():
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW bus_events AS
            SELECT * FROM read_parquet({sql_literal(bus_path.as_posix())})
            """
        )
    else:
        _create_empty_bus_view(connection)

    if subway_path.exists():
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW subway_events AS
            SELECT * FROM read_parquet({sql_literal(subway_path.as_posix())})
            """
        )
    else:
        _create_empty_subway_view(connection)

    connection.execute(f"CREATE OR REPLACE TEMP TABLE tmp_fact_delay_events_norm AS {_build_fact_sql()}")
    row_count = connection.execute("SELECT COUNT(*) FROM tmp_fact_delay_events_norm").fetchone()[0]

    connection.execute(
        f"""
        COPY tmp_fact_delay_events_norm
        TO {sql_literal(resolved_output.as_posix())}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    connection.close()

    return {
        "output_path": resolved_output.as_posix(),
        "row_count": row_count,
        "inputs": {
            "silver_bus_events_parquet": bus_path.as_posix(),
            "silver_subway_events_parquet": subway_path.as_posix(),
            "bus_exists": bus_path.exists(),
            "subway_exists": subway_path.exists(),
        },
    }


def main() -> None:
    result = run_build_fact_delay_events_norm()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
