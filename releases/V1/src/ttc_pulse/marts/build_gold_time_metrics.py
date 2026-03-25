"""Build the gold_time_reliability mart."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.marts._gold_utils import create_delay_events_view, materialize_query_to_gold
from ttc_pulse.marts.scoring import DEFAULT_SCORE_WEIGHTS, composite_score_sql, validate_weights
from ttc_pulse.utils.project_setup import ensure_duckdb_connection, resolve_project_paths

TABLE_NAME = "gold_time_reliability"
OUTPUT_FILENAME = "gold_time_reliability.parquet"


def _build_query() -> str:
    composite_expr = composite_score_sql(
        frequency_expr="frequency",
        severity_p90_expr="severity_p90",
        regularity_p90_expr="regularity_p90",
        cause_mix_expr="cause_mix_score",
        partition_columns=["mode"],
        weights=DEFAULT_SCORE_WEIGHTS,
    )
    return f"""
    WITH base AS (
        SELECT
            mode,
            day_name,
            hour_bin,
            incident_category,
            min_delay,
            min_gap
        FROM delay_events_src
        WHERE day_name IS NOT NULL
            AND hour_bin IS NOT NULL
    ),
    metrics AS (
        SELECT
            mode,
            day_name,
            hour_bin,
            COUNT(*)::BIGINT AS frequency,
            quantile_cont(min_delay, 0.9) FILTER (WHERE min_delay IS NOT NULL) AS severity_p90,
            quantile_cont(min_gap, 0.9) FILTER (WHERE min_gap IS NOT NULL) AS regularity_p90
        FROM base
        GROUP BY 1, 2, 3
    ),
    category_counts AS (
        SELECT
            mode,
            day_name,
            hour_bin,
            incident_category,
            COUNT(*)::DOUBLE AS category_count
        FROM base
        GROUP BY 1, 2, 3, 4
    ),
    cause_mix AS (
        SELECT
            mode,
            day_name,
            hour_bin,
            1.0 - SUM(POWER(category_count / NULLIF(total_count, 0.0), 2)) AS cause_mix_score
        FROM (
            SELECT
                mode,
                day_name,
                hour_bin,
                category_count,
                SUM(category_count) OVER (PARTITION BY mode, day_name, hour_bin) AS total_count
            FROM category_counts
        ) AS x
        GROUP BY 1, 2, 3
    ),
    scored AS (
        SELECT
            m.mode,
            m.day_name,
            m.hour_bin,
            m.frequency,
            m.severity_p90,
            m.regularity_p90,
            COALESCE(c.cause_mix_score, 0.0) AS cause_mix_score
        FROM metrics AS m
        LEFT JOIN cause_mix AS c
            ON m.mode = c.mode
            AND m.day_name = c.day_name
            AND m.hour_bin = c.hour_bin
    )
    SELECT
        mode,
        day_name,
        hour_bin,
        frequency,
        severity_p90,
        regularity_p90,
        cause_mix_score,
        {composite_expr} AS composite_score
    FROM scored
    """


def run_build_gold_time_metrics(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build day/hour reliability metrics with composite scoring."""
    validate_weights(DEFAULT_SCORE_WEIGHTS)
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output_path = (output_path or (paths.project_root / "gold" / OUTPUT_FILENAME)).resolve()
    caveats: list[str] = []

    connection = ensure_duckdb_connection(resolved_db_path)
    try:
        source_info = create_delay_events_view(connection, paths.project_root, caveats)
        filtered_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM delay_events_src
            WHERE day_name IS NOT NULL
                AND hour_bin IS NOT NULL
            """
        ).fetchone()[0]
        if int(filtered_rows) == 0:
            caveats.append("No rows with day_name/hour_bin were available for time reliability metrics.")

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
        "weights": DEFAULT_SCORE_WEIGHTS.as_dict(),
    }


def main() -> None:
    result = run_build_gold_time_metrics()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
