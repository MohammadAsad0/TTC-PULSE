"""Build the gold_linkage_quality mart."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.marts._gold_utils import create_delay_events_view, materialize_query_to_gold
from ttc_pulse.utils.project_setup import ensure_duckdb_connection, resolve_project_paths

TABLE_NAME = "gold_linkage_quality"
OUTPUT_FILENAME = "gold_linkage_quality.parquet"


def _build_query() -> str:
    return """
    WITH base AS (
        SELECT
            mode,
            service_date,
            match_method,
            link_status,
            confidence_tier
        FROM delay_events_src
    ),
    periodized AS (
        SELECT
            mode,
            CAST(DATE_TRUNC('month', service_date) AS DATE) AS period_start,
            CAST(
                DATE_TRUNC('month', service_date) + INTERVAL '1 month' - INTERVAL '1 day'
                AS DATE
            ) AS period_end,
            match_method,
            link_status,
            confidence_tier
        FROM base
        WHERE service_date IS NOT NULL
    ),
    agg AS (
        SELECT
            mode,
            period_start,
            period_end,
            match_method,
            link_status,
            confidence_tier,
            COUNT(*)::BIGINT AS row_count
        FROM periodized
        GROUP BY 1, 2, 3, 4, 5, 6
    ),
    mode_totals AS (
        SELECT
            mode,
            period_start,
            SUM(row_count)::DOUBLE AS mode_rows
        FROM agg
        GROUP BY 1, 2
    )
    SELECT
        a.mode,
        a.period_start,
        a.period_end,
        a.match_method,
        a.link_status,
        a.confidence_tier,
        a.row_count,
        COALESCE(a.row_count::DOUBLE / NULLIF(t.mode_rows, 0), 0.0) AS pct_of_mode_rows
    FROM agg AS a
    LEFT JOIN mode_totals AS t
        ON a.mode = t.mode AND a.period_start = t.period_start
    """


def run_build_gold_linkage_quality(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build gold_linkage_quality from canonical delay events."""
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output_path = (output_path or (paths.project_root / "gold" / OUTPUT_FILENAME)).resolve()
    caveats: list[str] = []

    connection = ensure_duckdb_connection(resolved_db_path)
    try:
        source_info = create_delay_events_view(connection, paths.project_root, caveats)
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
    }


def main() -> None:
    result = run_build_gold_linkage_quality()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
