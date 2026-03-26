"""Step 2 subway normalization (station-first) into Silver parquet."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from ttc_pulse.utils.project_setup import resolve_project_paths, sql_literal

OUTPUT_FILENAME = "silver_subway_events.parquet"


def _normalize_subway_sql(code_reference_path: Path | None = None) -> str:
    code_reference_cte = """
    subway_code_reference AS (
        SELECT DISTINCT
            UPPER(TRIM(code_raw)) AS incident_code_raw,
            NULLIF(TRIM(code_description), '') AS incident_description
        FROM (
            SELECT column2 AS code_raw, column3 AS code_description
            FROM read_csv_auto(
                {code_reference_path},
                all_varchar = TRUE,
                header = FALSE,
                ignore_errors = TRUE
            )
            UNION ALL
            SELECT column6 AS code_raw, column7 AS code_description
            FROM read_csv_auto(
                {code_reference_path},
                all_varchar = TRUE,
                header = FALSE,
                ignore_errors = TRUE
            )
        ) raw_codes
        WHERE code_raw IS NOT NULL
            AND TRIM(code_raw) <> ''
            AND UPPER(TRIM(code_raw)) NOT IN ('SUB RMENU CODE', 'SRT RMENU CODE')
    ),
    """.format(
        code_reference_path=sql_literal(code_reference_path.as_posix())
    )
    if code_reference_path is None or not code_reference_path.exists():
        code_reference_cte = """
    subway_code_reference AS (
        SELECT
            CAST(NULL AS VARCHAR) AS incident_code_raw,
            CAST(NULL AS VARCHAR) AS incident_description
        WHERE FALSE
    ),
    """

    return f"""
    WITH gtfs_line_routes AS (
        SELECT route_short_name, route_id
        FROM bronze_gtfs_routes
        WHERE route_type = '1'
    ),
    subway_stop_names AS (
        SELECT DISTINCT s.stop_name
        FROM bronze_gtfs_stops s
        INNER JOIN bronze_gtfs_stop_times st
            ON st.stop_id = s.stop_id
        INNER JOIN bronze_gtfs_trips t
            ON t.trip_id = st.trip_id
        INNER JOIN bronze_gtfs_routes r
            ON r.route_id = t.route_id
        WHERE r.route_type = '1'
    ),
    {code_reference_cte}
    gtfs_station_tokens AS (
        SELECT DISTINCT
            NULLIF(
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    UPPER(COALESCE(stop_name, '')),
                                    '[^A-Z0-9 ]',
                                    ' ',
                                    'g'
                                ),
                                '\\b(NORTHBOUND|SOUTHBOUND|EASTBOUND|WESTBOUND|PLATFORM|PLATFORMS|TOWARDS|TO)\\b',
                                ' ',
                                'g'
                            ),
                            '\\b(STATION|STN|SUBWAY|LINE|LINES|YUS|YU|BD|SRT|SHP|SHEP|MC|CTR|CENTRE|CENTER)\\b',
                            ' ',
                            'g'
                        ),
                        '\\s+',
                        ' ',
                        'g'
                    )
                ),
                ''
            ) AS station_canonical_gtfs
        FROM subway_stop_names
    ),
    base AS (
        SELECT
            source_file,
            source_sheet,
            source_row_id,
            ingested_at,
            row_hash,
            NULLIF(TRIM("Date"), '') AS date_raw,
            NULLIF(TRIM("Time"), '') AS time_raw,
            NULLIF(TRIM("Day"), '') AS day_raw,
            NULLIF(TRIM("Line"), '') AS line_raw,
            NULLIF(TRIM("Station"), '') AS station_raw,
            NULLIF(TRIM("Code"), '') AS code_raw,
            NULLIF(TRIM("Bound"), '') AS bound_raw,
            NULLIF(TRIM("Vehicle"), '') AS vehicle_raw,
            TRY_CAST(REPLACE(NULLIF(TRIM("Min Delay"), ''), ',', '') AS DOUBLE) AS min_delay,
            TRY_CAST(REPLACE(NULLIF(TRIM("Min Gap"), ''), ',', '') AS DOUBLE) AS min_gap
        FROM bronze_subway
    ),
    parsed AS (
        SELECT
            *,
            COALESCE(
                TRY_CAST(date_raw AS DATE),
                TRY_CAST(SPLIT_PART(date_raw, 'T', 1) AS DATE),
                CAST(TRY_STRPTIME(date_raw, '%m/%d/%Y') AS DATE)
            ) AS service_date,
            UPPER(line_raw) AS line_upper
        FROM base
    ),
    line_norm AS (
        SELECT
            *,
            CASE
                WHEN line_upper IS NULL THEN NULL
                WHEN line_upper LIKE '%SHEP%' OR REGEXP_MATCHES(line_upper, '(^|\\W)4(\\W|$)') THEN '4'
                WHEN line_upper LIKE '%SRT%'
                    OR line_upper LIKE '%SCARB%'
                    OR line_upper LIKE '%LINE 3%'
                    OR line_upper LIKE '%LINE3%'
                    OR REGEXP_MATCHES(line_upper, '(^|\\W)3(\\W|$)') THEN '3'
                WHEN line_upper LIKE '%BD%'
                    OR line_upper LIKE '%B/D%'
                    OR line_upper LIKE '%BLOOR%'
                    OR line_upper LIKE '%DANFORTH%'
                    OR line_upper LIKE '%LINE 2%'
                    OR line_upper LIKE '%LINE2%'
                    OR REGEXP_MATCHES(line_upper, '(^|\\W)2(\\W|$)') THEN '2'
                WHEN line_upper LIKE '%YU%'
                    OR line_upper LIKE '%YUS%'
                    OR line_upper LIKE '%YONGE%'
                    OR line_upper LIKE '%UNIVERSITY%'
                    OR line_upper LIKE '%LINE 1%'
                    OR line_upper LIKE '%LINE1%'
                    OR REGEXP_MATCHES(line_upper, '(^|\\W)1(\\W|$)') THEN '1'
                ELSE NULL
            END AS line_code_norm
        FROM parsed
    ),
    station_norm AS (
        SELECT
            *,
            NULLIF(
                TRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    UPPER(COALESCE(station_raw, '')),
                                    '[^A-Z0-9 ]',
                                    ' ',
                                    'g'
                                ),
                                '\\b(NORTHBOUND|SOUTHBOUND|EASTBOUND|WESTBOUND|PLATFORM|PLATFORMS|TOWARDS|TO)\\b',
                                ' ',
                                'g'
                            ),
                            '\\b(STATION|STN|SUBWAY|LINE|LINES|YUS|YU|BD|SRT|SHP|SHEP|MC|CTR|CENTRE|CENTER)\\b',
                            ' ',
                            'g'
                        ),
                        '\\s+',
                        ' ',
                        'g'
                    )
                ),
                ''
            ) AS station_canonical,
            COALESCE(
                TRY_CAST(CAST(service_date AS VARCHAR) || ' ' || time_raw AS TIMESTAMP),
                CAST(service_date AS TIMESTAMP)
            ) AS event_ts
        FROM line_norm
    ),
    direction_ready AS (
        SELECT
            *,
            UPPER(REGEXP_REPLACE(COALESCE(bound_raw, ''), '[^A-Za-z/]', '', 'g')) AS dir_clean
        FROM station_norm
    ),
    linked AS (
        SELECT
            d.*,
            r.route_id AS route_id_gtfs,
            gs.station_canonical_gtfs,
            scr.incident_description
        FROM direction_ready d
        LEFT JOIN gtfs_line_routes r
            ON r.route_short_name = d.line_code_norm
        LEFT JOIN gtfs_station_tokens gs
            ON gs.station_canonical_gtfs = d.station_canonical
        LEFT JOIN subway_code_reference scr
            ON UPPER(COALESCE(d.code_raw, '')) = scr.incident_code_raw
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
            line_raw AS line_code_raw,
            line_code_norm,
            route_id_gtfs,
            station_raw AS station_text_raw,
            station_canonical,
            station_raw AS location_text_raw,
            incident_description AS incident_text_raw,
            code_raw AS incident_code_raw,
            COALESCE(incident_description, code_raw) AS incident_category,
            min_delay,
            min_gap,
            bound_raw AS direction_raw,
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
                WHEN station_canonical_gtfs IS NOT NULL AND route_id_gtfs IS NOT NULL THEN 'exact'
                WHEN station_canonical_gtfs IS NOT NULL THEN 'token'
                WHEN route_id_gtfs IS NOT NULL THEN 'fallback'
                ELSE 'fallback'
            END AS match_method,
            CASE
                WHEN station_canonical_gtfs IS NOT NULL AND route_id_gtfs IS NOT NULL THEN 0.95
                WHEN station_canonical_gtfs IS NOT NULL THEN 0.80
                WHEN route_id_gtfs IS NOT NULL THEN 0.55
                ELSE 0.0
            END AS match_confidence,
            CASE
                WHEN station_canonical_gtfs IS NOT NULL THEN 'matched'
                WHEN route_id_gtfs IS NOT NULL THEN 'ambiguous_review'
                ELSE 'unmatched_review'
            END AS link_status,
            source_file,
            source_sheet,
            source_row_id,
            ingested_at,
            row_hash
        FROM linked
    )
    SELECT
        event_id,
        service_date,
        event_ts,
        day_name,
        hour_bin,
        month_bin,
        line_code_raw,
        line_code_norm,
        route_id_gtfs,
        station_text_raw,
        station_canonical,
        location_text_raw,
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


def run_normalize_subway(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build silver subway events parquet from bronze_subway using station-first matching."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output = (output_path or (paths.project_root / "silver" / OUTPUT_FILENAME)).resolve()
    code_reference_path = (
        paths.project_root / "data" / "subway" / "ttc-subway-delay-codes__01_Sheet_1.csv"
    ).resolve()
    resolved_output.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(resolved_db_path), read_only=True)
    connection.execute("PRAGMA disable_progress_bar")
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE tmp_silver_subway_events AS "
        f"{_normalize_subway_sql(code_reference_path=code_reference_path)}"
    )

    row_count = connection.execute("SELECT COUNT(*) FROM tmp_silver_subway_events").fetchone()[0]
    connection.execute(
        f"""
        COPY tmp_silver_subway_events
        TO {sql_literal(resolved_output.as_posix())}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    connection.close()

    return {
        "mode": "subway",
        "duckdb_path": resolved_db_path.as_posix(),
        "output_path": resolved_output.as_posix(),
        "row_count": row_count,
    }


def main() -> None:
    result = run_normalize_subway()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
