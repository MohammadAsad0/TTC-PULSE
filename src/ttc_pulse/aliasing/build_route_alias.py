"""Build initial route alias dimension using bronze bus/subway tokens and GTFS routes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import ensure_duckdb_connection, quote_identifier, resolve_project_paths

REQUIRED_TABLES = ["bronze_bus", "bronze_subway", "bronze_gtfs_routes"]


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
        raise RuntimeError(f"Missing required tables for route alias build: {joined}")


def _copy_table_to_parquet(connection: Any, table_name: str, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    escaped_path = output_path.resolve().as_posix().replace("'", "''")
    connection.execute(
        f"COPY {quote_identifier(table_name)} TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]


def _build_route_alias_table(connection: Any) -> int:
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE route_reference AS
        SELECT DISTINCT
            trim(route_id) AS route_id,
            trim(route_short_name) AS route_short_name,
            NULLIF(trim(route_long_name), '') AS route_long_name,
            TRY_CAST(NULLIF(trim(route_type), '') AS INTEGER) AS route_type
        FROM bronze_gtfs_routes
        WHERE route_id IS NOT NULL
          AND trim(route_id) <> ''
          AND route_short_name IS NOT NULL
          AND trim(route_short_name) <> ''
        """
    )

    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE dim_route_alias AS
        WITH raw_tokens AS (
            SELECT
                trim(Route) AS alias_token,
                'bus' AS source_mode,
                'Route' AS source_field,
                COUNT(*) AS observed_rows
            FROM bronze_bus
            WHERE Route IS NOT NULL
              AND trim(Route) <> ''
              AND upper(trim(Route)) NOT IN ('NONE', 'NULL', 'N/A')
            GROUP BY 1
            UNION ALL
            SELECT
                trim(Line) AS alias_token,
                'subway' AS source_mode,
                'Line' AS source_field,
                COUNT(*) AS observed_rows
            FROM bronze_subway
            WHERE Line IS NOT NULL
              AND trim(Line) <> ''
              AND upper(trim(Line)) NOT IN ('NONE', 'NULL', 'N/A')
            GROUP BY 1
        ),
        normalized AS (
            SELECT
                alias_token,
                source_mode,
                source_field,
                observed_rows,
                upper(trim(alias_token)) AS alias_upper,
                REGEXP_REPLACE(upper(trim(alias_token)), '\\s+', ' ', 'g') AS alias_token_norm,
                NULLIF(
                    REGEXP_EXTRACT(upper(trim(alias_token)), '^([0-9]{1,4})(?:\\.0+)?$', 1),
                    ''
                ) AS exact_numeric_token,
                NULLIF(
                    REGEXP_EXTRACT(
                        upper(trim(alias_token)),
                        '^([0-9]{1,4})(?:\\.0+)?(?:[^0-9].*)?$',
                        1
                    ),
                    ''
                ) AS leading_numeric_token
            FROM raw_tokens
        ),
        line_flags AS (
            SELECT
                n.*,
                CASE
                    WHEN regexp_matches(
                        n.alias_upper,
                        '(^|[^A-Z0-9])(YU|YUS|LINE[ ]*1|YONGE[ ]*[-/]?[ ]*UNIVERSITY)([^A-Z0-9]|$)'
                    ) THEN 1 ELSE 0
                END AS has_line1,
                CASE
                    WHEN regexp_matches(
                        n.alias_upper,
                        '(^|[^A-Z0-9])(BD|B[ ]*[-/][ ]*D|LINE[ ]*2|BLOOR[ ]*[- ]*DANFORTH)([^A-Z0-9]|$)'
                    ) THEN 1 ELSE 0
                END AS has_line2,
                CASE
                    WHEN regexp_matches(
                        n.alias_upper,
                        '(^|[^A-Z0-9])(SHP|SHEP|SHEPPARD|LINE[ ]*4)([^A-Z0-9]|$)'
                    ) THEN 1 ELSE 0
                END AS has_line4,
                CASE
                    WHEN regexp_matches(
                        n.alias_upper,
                        '(^|[^A-Z0-9])(SRT|RT)([^A-Z0-9]|$)'
                    ) THEN 1 ELSE 0
                END AS has_srt
            FROM normalized n
        ),
        mapped AS (
            SELECT
                lf.*,
                CASE
                    WHEN lf.source_mode = 'subway'
                        AND (lf.has_line1 + lf.has_line2 + lf.has_line4) > 1 THEN NULL
                    WHEN lf.source_mode = 'subway'
                        AND lf.has_line1 = 1
                        AND lf.has_line2 = 0
                        AND lf.has_line4 = 0 THEN '1'
                    WHEN lf.source_mode = 'subway'
                        AND lf.has_line2 = 1
                        AND lf.has_line1 = 0
                        AND lf.has_line4 = 0 THEN '2'
                    WHEN lf.source_mode = 'subway'
                        AND lf.has_line4 = 1
                        AND lf.has_line1 = 0
                        AND lf.has_line2 = 0 THEN '4'
                    WHEN lf.source_mode = 'subway'
                        AND lf.has_srt = 1
                        AND (lf.has_line1 + lf.has_line2 + lf.has_line4) = 0 THEN 'legacy_unmapped'
                    WHEN EXISTS (
                        SELECT 1
                        FROM route_reference rr
                        WHERE rr.route_short_name = lf.alias_token
                    ) THEN lf.alias_token
                    WHEN lf.exact_numeric_token IS NOT NULL
                        AND EXISTS (
                            SELECT 1
                            FROM route_reference rr
                            WHERE rr.route_short_name = lf.exact_numeric_token
                        ) THEN lf.exact_numeric_token
                    WHEN lf.leading_numeric_token IS NOT NULL
                        AND EXISTS (
                            SELECT 1
                            FROM route_reference rr
                            WHERE rr.route_short_name = lf.leading_numeric_token
                        ) THEN lf.leading_numeric_token
                    ELSE NULL
                END AS mapped_route_short_name,
                CASE
                    WHEN lf.source_mode = 'subway'
                        AND (lf.has_line1 + lf.has_line2 + lf.has_line4) > 1 THEN
                        concat_ws(
                            '|',
                            CASE WHEN lf.has_line1 = 1 THEN '1' END,
                            CASE WHEN lf.has_line2 = 1 THEN '2' END,
                            CASE WHEN lf.has_line4 = 1 THEN '4' END
                        )
                    ELSE NULL
                END AS ambiguous_candidate_short_names
            FROM line_flags lf
        )
        SELECT
            row_number() OVER (ORDER BY source_mode, observed_rows DESC, alias_token) AS route_alias_sk,
            alias_token,
            alias_token_norm,
            source_mode,
            source_field,
            observed_rows,
            CASE
                WHEN ambiguous_candidate_short_names IS NOT NULL THEN 'ambiguous'
                WHEN mapped_route_short_name = 'legacy_unmapped' THEN 'legacy_unmapped'
                WHEN mapped_route_short_name IS NOT NULL THEN 'resolved'
                ELSE 'unresolved'
            END AS mapping_status,
            CASE
                WHEN ambiguous_candidate_short_names IS NOT NULL THEN 'subway_multi_line_token'
                WHEN mapped_route_short_name = 'legacy_unmapped' THEN 'subway_srt_legacy'
                WHEN source_mode = 'subway' AND mapped_route_short_name IN ('1', '2', '4')
                    THEN 'subway_known_token_rule'
                WHEN mapped_route_short_name IS NOT NULL THEN 'direct_or_numeric_route_short_name'
                ELSE 'no_reference_match'
            END AS mapping_rule,
            CASE
                WHEN ambiguous_candidate_short_names IS NOT NULL THEN NULL
                ELSE mapped_route_short_name
            END AS mapped_route_short_name,
            rr.route_id AS mapped_route_id,
            rr.route_long_name AS mapped_route_long_name,
            rr.route_type AS mapped_route_type,
            CASE
                WHEN ambiguous_candidate_short_names IS NOT NULL THEN ambiguous_candidate_short_names
                WHEN mapped_route_short_name IS NOT NULL THEN mapped_route_short_name
                ELSE NULL
            END AS candidate_route_short_names,
            CASE
                WHEN ambiguous_candidate_short_names IS NOT NULL THEN
                    (has_line1 + has_line2 + has_line4)
                WHEN mapped_route_short_name IS NOT NULL THEN 1
                ELSE 0
            END AS candidate_count,
            TRUE AS reference_gtfs_bridge_family,
            CURRENT_TIMESTAMP AS generated_at
        FROM mapped
        LEFT JOIN route_reference rr
            ON mapped.mapped_route_short_name = rr.route_short_name
        ORDER BY source_mode, observed_rows DESC, alias_token
        """
    )
    return connection.execute("SELECT COUNT(*) FROM dim_route_alias").fetchone()[0]


def run() -> dict[str, Any]:
    """Build route alias dimension parquet and return row metrics."""
    paths = resolve_project_paths()
    dimensions_root = paths.project_root / "dimensions"
    output_path = dimensions_root / "dim_route_alias.parquet"

    connection = ensure_duckdb_connection(paths.db_path)
    _ensure_required_tables(connection, REQUIRED_TABLES)
    alias_rows = _build_route_alias_table(connection)
    alias_rows = _copy_table_to_parquet(connection, "dim_route_alias", output_path)

    unresolved_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM dim_route_alias
        WHERE mapping_status IN ('unresolved', 'ambiguous', 'legacy_unmapped')
        """
    ).fetchone()[0]
    ambiguous_count = connection.execute(
        "SELECT COUNT(*) FROM dim_route_alias WHERE mapping_status = 'ambiguous'"
    ).fetchone()[0]

    connection.close()
    return {
        "duckdb_path": paths.db_path.resolve().as_posix(),
        "outputs": {
            "dim_route_alias": output_path.resolve().as_posix(),
        },
        "row_counts": {
            "dim_route_alias": alias_rows,
        },
        "unresolved_counts": {
            "route_alias_unresolved_like": unresolved_count,
            "route_alias_ambiguous": ambiguous_count,
        },
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
