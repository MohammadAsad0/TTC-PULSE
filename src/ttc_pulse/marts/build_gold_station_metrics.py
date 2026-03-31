"""Build station-level Gold marts: gold_station_time_metrics and gold_spatial_hotspot."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.marts._gold_utils import (
    create_delay_events_view,
    ensure_table_from_parquet,
    get_table_columns,
    materialize_query_to_gold,
)
from ttc_pulse.marts.scoring import DEFAULT_SCORE_WEIGHTS, composite_score_sql, validate_weights
from ttc_pulse.utils.project_setup import ensure_duckdb_connection, quote_identifier, resolve_project_paths

TABLE_NAME = "gold_station_time_metrics"
OUTPUT_FILENAME = "gold_station_time_metrics.parquet"

SPATIAL_TABLE_NAME = "gold_spatial_hotspot"
SPATIAL_OUTPUT_FILENAME = "gold_spatial_hotspot.parquet"

STATION_LINKAGE_THRESHOLD = 0.80
AMBIGUOUS_SHARE_THRESHOLD = 0.15
MIN_HIGH_CONF_SPATIAL_ROWS = 1000


def _station_key_expr(column_name: str) -> str:
    return f"""
    NULLIF(
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            UPPER(COALESCE({column_name}, '')),
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
    )
    """


def _build_station_query() -> str:
    composite_expr = composite_score_sql(
        frequency_expr="frequency",
        severity_p90_expr="severity_p90",
        regularity_p90_expr="regularity_p90",
        cause_mix_expr="cause_mix_score",
        partition_columns=["service_date", "hour_bin"],
        weights=DEFAULT_SCORE_WEIGHTS,
    )
    return f"""
    WITH base AS (
        SELECT
            line_code_norm,
            station_canonical,
            service_date,
            hour_bin,
            incident_category,
            min_delay,
            min_gap
        FROM delay_events_src
        WHERE mode = 'subway'
            AND service_date IS NOT NULL
            AND hour_bin IS NOT NULL
            AND station_canonical IS NOT NULL
            AND link_status = 'matched'
            AND confidence_tier IN ('exact_gtfs_match', 'token_gtfs_match', 'alias_match')
    ),
    metrics AS (
        SELECT
            line_code_norm,
            station_canonical,
            service_date,
            hour_bin,
            COUNT(*)::BIGINT AS frequency,
            quantile_cont(min_delay, 0.5) FILTER (WHERE min_delay IS NOT NULL) AS severity_median,
            quantile_cont(min_delay, 0.9) FILTER (WHERE min_delay IS NOT NULL) AS severity_p90,
            quantile_cont(min_gap, 0.9) FILTER (WHERE min_gap IS NOT NULL) AS regularity_p90
        FROM base
        GROUP BY 1, 2, 3, 4
    ),
    category_counts AS (
        SELECT
            line_code_norm,
            station_canonical,
            service_date,
            hour_bin,
            incident_category,
            COUNT(*)::DOUBLE AS category_count
        FROM base
        GROUP BY 1, 2, 3, 4, 5
    ),
    cause_mix AS (
        SELECT
            line_code_norm,
            station_canonical,
            service_date,
            hour_bin,
            1.0 - SUM(POWER(category_count / NULLIF(total_count, 0.0), 2)) AS cause_mix_score
        FROM (
            SELECT
                line_code_norm,
                station_canonical,
                service_date,
                hour_bin,
                category_count,
                SUM(category_count) OVER (
                    PARTITION BY line_code_norm, station_canonical, service_date, hour_bin
                ) AS total_count
            FROM category_counts
        ) AS x
        GROUP BY 1, 2, 3, 4
    ),
    scored AS (
        SELECT
            m.line_code_norm,
            m.station_canonical,
            m.service_date,
            m.hour_bin,
            m.frequency,
            m.severity_median,
            m.severity_p90,
            m.regularity_p90,
            COALESCE(c.cause_mix_score, 0.0) AS cause_mix_score
        FROM metrics AS m
        LEFT JOIN cause_mix AS c
            ON m.line_code_norm = c.line_code_norm
            AND m.station_canonical = c.station_canonical
            AND m.service_date = c.service_date
            AND m.hour_bin = c.hour_bin
    )
    SELECT
        line_code_norm,
        station_canonical,
        service_date,
        hour_bin,
        frequency,
        severity_median,
        severity_p90,
        regularity_p90,
        cause_mix_score,
        {composite_expr} AS composite_score
    FROM scored
    """


def _build_spatial_query() -> str:
    composite_expr = composite_score_sql(
        frequency_expr="frequency",
        severity_p90_expr="severity_p90",
        regularity_p90_expr="regularity_p90",
        cause_mix_expr="cause_mix_score",
        partition_columns=["mode"],
        weights=DEFAULT_SCORE_WEIGHTS,
    )
    stop_station_key_expr = _station_key_expr("stop_name")
    return f"""
    WITH base AS (
        SELECT
            'subway' AS mode,
            station_canonical AS spatial_unit_id,
            incident_category,
            min_delay,
            min_gap,
            match_confidence
        FROM delay_events_src
        WHERE mode = 'subway'
            AND station_canonical IS NOT NULL
            AND link_status = 'matched'
            AND confidence_tier IN ('exact_gtfs_match', 'token_gtfs_match')
    ),
    metrics AS (
        SELECT
            mode,
            spatial_unit_id,
            COUNT(*)::BIGINT AS frequency,
            quantile_cont(min_delay, 0.9) FILTER (WHERE min_delay IS NOT NULL) AS severity_p90,
            quantile_cont(min_gap, 0.9) FILTER (WHERE min_gap IS NOT NULL) AS regularity_p90,
            AVG(match_confidence) FILTER (WHERE match_confidence IS NOT NULL) AS confidence_score
        FROM base
        GROUP BY 1, 2
    ),
    category_counts AS (
        SELECT
            mode,
            spatial_unit_id,
            incident_category,
            COUNT(*)::DOUBLE AS category_count
        FROM base
        GROUP BY 1, 2, 3
    ),
    cause_mix AS (
        SELECT
            mode,
            spatial_unit_id,
            1.0 - SUM(POWER(category_count / NULLIF(total_count, 0.0), 2)) AS cause_mix_score
        FROM (
            SELECT
                mode,
                spatial_unit_id,
                category_count,
                SUM(category_count) OVER (PARTITION BY mode, spatial_unit_id) AS total_count
            FROM category_counts
        ) AS x
        GROUP BY 1, 2
    ),
    stop_lookup_norm AS (
        SELECT
            {stop_station_key_expr} AS station_key,
            AVG(stop_lat) AS centroid_lat,
            AVG(stop_lon) AS centroid_lon
        FROM dim_stop_gtfs
        WHERE stop_name IS NOT NULL
            AND serves_subway IS TRUE
            AND {stop_station_key_expr} IS NOT NULL
        GROUP BY 1
    ),
    scored AS (
        SELECT
            m.mode,
            'station' AS spatial_unit_type,
            m.spatial_unit_id,
            l.centroid_lat,
            l.centroid_lon,
            m.frequency,
            m.severity_p90,
            m.regularity_p90,
            COALESCE(c.cause_mix_score, 0.0) AS cause_mix_score,
            COALESCE(m.confidence_score, 0.0) AS confidence_score
        FROM metrics AS m
        LEFT JOIN cause_mix AS c
            ON m.mode = c.mode AND m.spatial_unit_id = c.spatial_unit_id
        LEFT JOIN stop_lookup_norm AS l
            ON m.spatial_unit_id = l.station_key
    )
    SELECT
        mode,
        spatial_unit_type,
        spatial_unit_id,
        centroid_lat,
        centroid_lon,
        frequency,
        severity_p90,
        regularity_p90,
        {composite_expr} AS composite_score,
        confidence_score
    FROM scored
    """


def _build_empty_spatial_query() -> str:
    return """
    SELECT
        CAST(NULL AS VARCHAR) AS mode,
        CAST(NULL AS VARCHAR) AS spatial_unit_type,
        CAST(NULL AS VARCHAR) AS spatial_unit_id,
        CAST(NULL AS DOUBLE) AS centroid_lat,
        CAST(NULL AS DOUBLE) AS centroid_lon,
        CAST(NULL AS BIGINT) AS frequency,
        CAST(NULL AS DOUBLE) AS severity_p90,
        CAST(NULL AS DOUBLE) AS regularity_p90,
        CAST(NULL AS DOUBLE) AS composite_score,
        CAST(NULL AS DOUBLE) AS confidence_score
    WHERE FALSE
    """


def _prepare_dim_stop(connection: Any, project_root: Path, caveats: list[str]) -> bool:
    ensure_table_from_parquet(
        connection=connection,
        table_name="dim_stop_gtfs",
        project_root=project_root,
        caveats=caveats,
    )
    stop_columns = get_table_columns(connection, "dim_stop_gtfs")
    required = {"stop_name", "stop_lat", "stop_lon"}
    if not required.issubset(stop_columns):
        missing = sorted(required - stop_columns)
        caveats.append(
            f"dim_stop_gtfs: missing columns {', '.join(missing)}; centroid columns may be NULL in hotspot output."
        )
        return False
    return True


def run_build_gold_station_metrics(
    db_path: Path | None = None,
    output_path: Path | None = None,
    spatial_output_path: Path | None = None,
) -> dict[str, Any]:
    """Build station-level metrics and confidence-gated spatial hotspot output."""
    validate_weights(DEFAULT_SCORE_WEIGHTS)
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output_path = (output_path or (paths.project_root / "gold" / OUTPUT_FILENAME)).resolve()
    resolved_spatial_output_path = (
        spatial_output_path or (paths.project_root / "gold" / SPATIAL_OUTPUT_FILENAME)
    ).resolve()

    caveats: list[str] = []
    spatial_caveats: list[str] = []
    connection = ensure_duckdb_connection(resolved_db_path)

    try:
        source_info = create_delay_events_view(connection, paths.project_root, caveats)

        filtered_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM delay_events_src
            WHERE mode = 'subway'
                AND service_date IS NOT NULL
                AND hour_bin IS NOT NULL
                AND station_canonical IS NOT NULL
                AND link_status = 'matched'
                AND confidence_tier IN ('exact_gtfs_match', 'token_gtfs_match', 'alias_match')
            """
        ).fetchone()[0]
        if int(filtered_rows) == 0:
            caveats.append(
                "No high-confidence subway station rows were available for station metrics."
            )

        row_count = materialize_query_to_gold(
            connection=connection,
            query_sql=_build_station_query(),
            table_name=TABLE_NAME,
            parquet_path=resolved_output_path,
        )

        gating_row = connection.execute(
            """
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN station_canonical IS NOT NULL
                                AND link_status = 'matched'
                                AND confidence_tier IN ('exact_gtfs_match', 'token_gtfs_match')
                            THEN 1 ELSE 0
                        END
                    )::DOUBLE / NULLIF(COUNT(*)::DOUBLE, 0.0),
                    0.0
                ) AS station_linkage_coverage,
                COALESCE(
                    SUM(CASE WHEN link_status = 'ambiguous_review' THEN 1 ELSE 0 END)::DOUBLE
                    / NULLIF(COUNT(*)::DOUBLE, 0.0),
                    0.0
                ) AS ambiguous_share,
                SUM(
                    CASE
                        WHEN station_canonical IS NOT NULL
                            AND link_status = 'matched'
                            AND confidence_tier IN ('exact_gtfs_match', 'token_gtfs_match')
                        THEN 1 ELSE 0
                    END
                )::BIGINT AS high_confidence_rows,
                COUNT(*)::BIGINT AS eligible_rows
            FROM delay_events_src
            WHERE mode = 'subway'
            """
        ).fetchone()

        station_linkage_coverage = float(gating_row[0]) if gating_row else 0.0
        ambiguous_share = float(gating_row[1]) if gating_row else 0.0
        high_confidence_rows = int(gating_row[2]) if gating_row else 0
        eligible_rows = int(gating_row[3]) if gating_row else 0

        gating_passed = (
            eligible_rows > 0
            and station_linkage_coverage >= STATION_LINKAGE_THRESHOLD
            and ambiguous_share < AMBIGUOUS_SHARE_THRESHOLD
            and high_confidence_rows >= MIN_HIGH_CONF_SPATIAL_ROWS
        )

        if not gating_passed:
            spatial_caveats.append(
                "Spatial hotspot confidence gate not met; emitted schema-only zero-row scaffold."
            )
            spatial_caveats.append(
                "Gate metrics: "
                f"station_linkage_coverage={station_linkage_coverage:.4f} "
                f"(threshold {STATION_LINKAGE_THRESHOLD:.2f}), "
                f"ambiguous_share={ambiguous_share:.4f} "
                f"(threshold < {AMBIGUOUS_SHARE_THRESHOLD:.2f}), "
                f"high_confidence_rows={high_confidence_rows} "
                f"(threshold >= {MIN_HIGH_CONF_SPATIAL_ROWS}), "
                f"eligible_rows={eligible_rows}"
            )
            spatial_row_count = materialize_query_to_gold(
                connection=connection,
                query_sql=_build_empty_spatial_query(),
                table_name=SPATIAL_TABLE_NAME,
                parquet_path=resolved_spatial_output_path,
            )
        else:
            _prepare_dim_stop(connection, paths.project_root, spatial_caveats)
            spatial_row_count = materialize_query_to_gold(
                connection=connection,
                query_sql=_build_spatial_query(),
                table_name=SPATIAL_TABLE_NAME,
                parquet_path=resolved_spatial_output_path,
            )
    finally:
        connection.close()

    spatial_result = {
        "table_name": SPATIAL_TABLE_NAME,
        "output_path": resolved_spatial_output_path.as_posix(),
        "duckdb_path": resolved_db_path.as_posix(),
        "row_count": spatial_row_count,
        "status": "built_with_caveats" if spatial_caveats else "built",
        "caveats": spatial_caveats,
        "gating": {
            "station_linkage_coverage": station_linkage_coverage,
            "ambiguous_share": ambiguous_share,
            "high_confidence_rows": high_confidence_rows,
            "eligible_rows": eligible_rows,
            "thresholds": {
                "station_linkage_coverage": STATION_LINKAGE_THRESHOLD,
                "ambiguous_share_max": AMBIGUOUS_SHARE_THRESHOLD,
                "high_confidence_rows_min": MIN_HIGH_CONF_SPATIAL_ROWS,
            },
            "passed": gating_passed,
        },
    }

    return {
        "table_name": TABLE_NAME,
        "output_path": resolved_output_path.as_posix(),
        "duckdb_path": resolved_db_path.as_posix(),
        "row_count": row_count,
        "status": "built_with_caveats" if caveats else "built",
        "caveats": caveats,
        "source": source_info,
        "weights": DEFAULT_SCORE_WEIGHTS.as_dict(),
        "spatial_hotspot": spatial_result,
    }


def main() -> None:
    result = run_build_gold_station_metrics()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
