"""Step 2 bus normalization (route-first) into Silver parquet."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from ttc_pulse.utils.project_setup import resolve_project_paths, sql_literal

OUTPUT_FILENAME = "silver_bus_events.parquet"


def _normalize_bus_sql() -> str:
    return """
    WITH gtfs_routes AS (
        SELECT route_id, route_short_name
        FROM bronze_gtfs_routes
        WHERE route_short_name IS NOT NULL
    ),
    base AS (
        SELECT
            source_file,
            source_sheet,
            source_row_id,
            ingested_at,
            row_hash,
            NULLIF(TRIM("Date"), '') AS date_raw,
            NULLIF(TRIM("Report Date"), '') AS report_date_raw,
            NULLIF(TRIM("Time"), '') AS time_raw,
            NULLIF(TRIM("Day"), '') AS day_raw,
            NULLIF(TRIM("Route"), '') AS route_raw,
            NULLIF(TRIM("Line"), '') AS line_raw,
            NULLIF(TRIM("Location"), '') AS location_raw,
            NULLIF(TRIM("Station"), '') AS station_raw,
            NULLIF(TRIM("Incident"), '') AS incident_raw,
            NULLIF(TRIM("Code"), '') AS code_raw,
            NULLIF(TRIM("Direction"), '') AS direction_raw,
            NULLIF(TRIM("Bound"), '') AS bound_raw,
            NULLIF(TRIM("Vehicle"), '') AS vehicle_raw,
            TRY_CAST(REPLACE(NULLIF(TRIM("Min Delay"), ''), ',', '') AS DOUBLE) AS min_delay,
            TRY_CAST(REPLACE(NULLIF(TRIM("Min Gap"), ''), ',', '') AS DOUBLE) AS min_gap
        FROM bronze_bus
    ),
    parsed_dates AS (
        SELECT
            *,
            COALESCE(
                TRY_CAST(date_raw AS DATE),
                TRY_CAST(SPLIT_PART(date_raw, 'T', 1) AS DATE),
                CAST(TRY_STRPTIME(date_raw, '%m/%d/%Y') AS DATE),
                TRY_CAST(report_date_raw AS DATE),
                CAST(TRY_STRPTIME(report_date_raw, '%m/%d/%Y') AS DATE)
            ) AS service_date
        FROM base
    ),
    route_tokens AS (
        SELECT
            *,
            NULLIF(
                REGEXP_EXTRACT(COALESCE(route_raw, line_raw), '^([0-9]{1,4})(?:\\.0+)?$', 1),
                ''
            ) AS route_exact_token,
            NULLIF(
                REGEXP_EXTRACT(COALESCE(route_raw, line_raw), '^([0-9]{1,4})(?:\\.0+)?\\b', 1),
                ''
            ) AS route_prefix_token,
            NULLIF(REGEXP_EXTRACT(line_raw, '^([0-9]{1,4})(?:\\.0+)?\\b', 1), '') AS line_prefix_token,
            NULLIF(
                REGEXP_EXTRACT(UPPER(COALESCE(route_raw, line_raw)), 'LINE\\s*([0-9]{1,2})', 1),
                ''
            ) AS route_line_token,
            NULLIF(REGEXP_EXTRACT(UPPER(line_raw), 'LINE\\s*([0-9]{1,2})', 1), '') AS line_line_token,
            COALESCE(route_raw, line_raw) AS route_label_raw
        FROM parsed_dates
    ),
    route_resolved AS (
        SELECT
            *,
            COALESCE(
                route_exact_token,
                route_prefix_token,
                line_prefix_token,
                route_line_token,
                line_line_token
            ) AS route_short_name_norm,
            CASE
                WHEN route_exact_token IS NOT NULL THEN 'exact'
                WHEN route_prefix_token IS NOT NULL OR line_prefix_token IS NOT NULL THEN 'token'
                WHEN route_line_token IS NOT NULL OR line_line_token IS NOT NULL THEN 'token'
                ELSE 'fallback'
            END AS pre_match_method,
            CASE
                WHEN route_exact_token IS NOT NULL THEN 1.0
                WHEN route_prefix_token IS NOT NULL THEN 0.95
                WHEN line_prefix_token IS NOT NULL THEN 0.90
                WHEN route_line_token IS NOT NULL OR line_line_token IS NOT NULL THEN 0.70
                ELSE 0.0
            END AS pre_match_confidence
        FROM route_tokens
    ),
    joined AS (
        SELECT
            rr.*,
            r.route_id AS route_id_gtfs,
            COALESCE(
                TRY_CAST(CAST(service_date AS VARCHAR) || ' ' || time_raw AS TIMESTAMP),
                CAST(service_date AS TIMESTAMP)
            ) AS event_ts
        FROM route_resolved rr
        LEFT JOIN gtfs_routes r
            ON r.route_short_name = rr.route_short_name_norm
    ),
    direction_ready AS (
        SELECT
            *,
            UPPER(REGEXP_REPLACE(COALESCE(direction_raw, bound_raw, ''), '[^A-Za-z/]', '', 'g')) AS dir_clean
        FROM joined
    ),
    final AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY source_file, source_row_id) AS event_id,
            service_date,
            event_ts,
            COALESCE(
                day_raw,
                CASE
                    WHEN service_date IS NOT NULL THEN STRFTIME(service_date, '%A')
                    ELSE NULL
                END
            ) AS day_name,
            CAST(EXTRACT(HOUR FROM event_ts) AS INTEGER) AS hour_bin,
            CAST(DATE_TRUNC('month', service_date) AS DATE) AS month_bin,
            route_label_raw,
            route_short_name_norm,
            route_id_gtfs,
            COALESCE(location_raw, station_raw) AS location_text_raw,
            station_raw AS station_text_raw,
            incident_raw AS incident_text_raw,
            code_raw AS incident_code_raw,
            COALESCE(incident_raw, code_raw) AS incident_category,
            min_delay,
            min_gap,
            COALESCE(direction_raw, bound_raw) AS direction_raw,
            CASE
                WHEN dir_clean IN ('N', 'NB', 'N/B', 'NORTH', 'NORTHBOUND') THEN 'N'
                WHEN dir_clean IN ('S', 'SB', 'S/B', 'SOUTH', 'SOUTHBOUND') THEN 'S'
                WHEN dir_clean IN ('E', 'EB', 'E/B', 'EAST', 'EASTBOUND') THEN 'E'
                WHEN dir_clean IN ('W', 'WB', 'W/B', 'WEST', 'WESTBOUND') THEN 'W'
                WHEN dir_clean IN ('B', 'BW', 'B/W', 'BOTH') THEN 'B'
                WHEN NULLIF(dir_clean, '') IS NOT NULL THEN LEFT(dir_clean, 1)
                ELSE NULL
            END AS direction_norm,
            vehicle_raw AS vehicle_id_raw,
            CASE
                WHEN route_id_gtfs IS NOT NULL THEN pre_match_method
                WHEN route_short_name_norm IS NOT NULL THEN 'token'
                ELSE 'fallback'
            END AS match_method,
            CASE
                WHEN route_id_gtfs IS NOT NULL THEN pre_match_confidence
                WHEN route_short_name_norm IS NOT NULL THEN LEAST(pre_match_confidence, 0.35)
                ELSE 0.0
            END AS match_confidence,
            CASE
                WHEN route_id_gtfs IS NOT NULL THEN 'matched'
                ELSE 'unmatched_review'
            END AS link_status,
            source_file,
            source_sheet,
            source_row_id,
            ingested_at,
            row_hash
        FROM direction_ready
    )
    SELECT
        event_id,
        service_date,
        event_ts,
        day_name,
        hour_bin,
        month_bin,
        route_label_raw,
        route_short_name_norm,
        route_id_gtfs,
        location_text_raw,
        station_text_raw,
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
        source_file,
        source_sheet,
        source_row_id,
        ingested_at,
        row_hash
    FROM final
    """


def run_normalize_bus(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build silver bus events parquet from bronze_bus using route-first matching."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output = (output_path or (paths.project_root / "silver" / OUTPUT_FILENAME)).resolve()
    resolved_output.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(resolved_db_path), read_only=True)
    connection.execute("PRAGMA disable_progress_bar")
    connection.execute(f"CREATE OR REPLACE TEMP TABLE tmp_silver_bus_events AS {_normalize_bus_sql()}")

    row_count = connection.execute("SELECT COUNT(*) FROM tmp_silver_bus_events").fetchone()[0]
    connection.execute(
        f"""
        COPY tmp_silver_bus_events
        TO {sql_literal(resolved_output.as_posix())}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    connection.close()

    return {
        "mode": "bus",
        "duckdb_path": resolved_db_path.as_posix(),
        "output_path": resolved_output.as_posix(),
        "row_count": row_count,
    }


def main() -> None:
    result = run_normalize_bus()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
