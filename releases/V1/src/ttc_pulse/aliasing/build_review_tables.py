"""Build alias review parquet tables for unresolved and ambiguous mappings."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.aliasing.build_incident_code_dim import run as run_incident_code_dim
from ttc_pulse.aliasing.build_route_alias import run as run_route_alias
from ttc_pulse.aliasing.build_station_alias import run as run_station_alias
from ttc_pulse.utils.project_setup import ensure_duckdb_connection, quote_identifier, resolve_project_paths


def _copy_table_to_parquet(connection: Any, table_name: str, output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    escaped_path = output_path.resolve().as_posix().replace("'", "''")
    connection.execute(
        f"COPY {quote_identifier(table_name)} TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]


def _ensure_alias_dimensions(paths: Any) -> dict[str, str]:
    dim_route = paths.project_root / "dimensions" / "dim_route_alias.parquet"
    dim_station = paths.project_root / "dimensions" / "dim_station_alias.parquet"
    dim_incident = paths.project_root / "dimensions" / "dim_incident_code.parquet"

    if not dim_route.exists():
        run_route_alias()
    if not dim_station.exists():
        run_station_alias()
    if not dim_incident.exists():
        run_incident_code_dim()

    return {
        "dim_route_alias": dim_route.resolve().as_posix(),
        "dim_station_alias": dim_station.resolve().as_posix(),
        "dim_incident_code": dim_incident.resolve().as_posix(),
    }


def _build_route_alias_review(connection: Any, dim_path: str) -> int:
    escaped = dim_path.replace("'", "''")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE route_alias_review AS
        SELECT
            row_number() OVER (
                ORDER BY
                    CASE mapping_status
                        WHEN 'unresolved' THEN 1
                        WHEN 'ambiguous' THEN 2
                        WHEN 'legacy_unmapped' THEN 3
                        ELSE 4
                    END,
                    observed_rows DESC,
                    alias_token
            ) AS review_sk,
            alias_token,
            alias_token_norm,
            source_mode,
            source_field,
            observed_rows,
            mapping_status,
            mapping_rule,
            candidate_route_short_names,
            candidate_count,
            mapped_route_short_name,
            mapped_route_id,
            mapped_route_long_name,
            CASE
                WHEN mapping_status = 'unresolved' THEN 'add_token_rule_or_manual_mapping'
                WHEN mapping_status = 'ambiguous' THEN 'choose_single_line_or_split_token'
                WHEN mapping_status = 'legacy_unmapped' THEN 'legacy_srt_expected_keep_unmapped'
                ELSE 'none'
            END AS review_action,
            CURRENT_TIMESTAMP AS review_generated_at
        FROM read_parquet('{escaped}')
        WHERE mapping_status IN ('unresolved', 'ambiguous', 'legacy_unmapped')
        ORDER BY observed_rows DESC, alias_token
        """
    )
    return connection.execute("SELECT COUNT(*) FROM route_alias_review").fetchone()[0]


def _build_station_alias_review(connection: Any, dim_path: str) -> int:
    escaped = dim_path.replace("'", "''")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE station_alias_review AS
        SELECT
            row_number() OVER (
                ORDER BY
                    CASE mapping_status
                        WHEN 'unresolved' THEN 1
                        WHEN 'ambiguous' THEN 2
                        ELSE 3
                    END,
                    observed_rows DESC,
                    alias_token
            ) AS review_sk,
            alias_token,
            alias_station_key,
            source_mode,
            source_field,
            observed_rows,
            mapping_status,
            mapping_rule,
            mapped_station_key,
            mapped_stop_name,
            mapped_stop_id,
            candidate_stop_count,
            candidate_stop_ids,
            mapped_serves_subway,
            route_types_served,
            CASE
                WHEN mapping_status = 'unresolved' THEN 'add_station_token_rule_or_manual_map'
                WHEN mapping_status = 'ambiguous' THEN 'select_canonical_stop_for_station_key'
                ELSE 'none'
            END AS review_action,
            CURRENT_TIMESTAMP AS review_generated_at
        FROM read_parquet('{escaped}')
        WHERE mapping_status IN ('unresolved', 'ambiguous')
        ORDER BY observed_rows DESC, alias_token
        """
    )
    return connection.execute("SELECT COUNT(*) FROM station_alias_review").fetchone()[0]


def _build_incident_code_review(connection: Any, dim_path: str) -> int:
    escaped = dim_path.replace("'", "''")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE incident_code_review AS
        SELECT
            row_number() OVER (
                ORDER BY observed_rows DESC, incident_code
            ) AS review_sk,
            incident_code,
            source_mode,
            bus_rows,
            subway_rows,
            observed_rows,
            code_family,
            code_family_label,
            incident_description,
            mapping_status,
            mapping_rule,
            CASE
                WHEN mapping_status = 'unresolved' THEN 'attach_official_code_description'
                WHEN mapping_status = 'ambiguous' THEN 'disambiguate_code_description'
                ELSE 'none'
            END AS review_action,
            CURRENT_TIMESTAMP AS review_generated_at
        FROM read_parquet('{escaped}')
        WHERE mapping_status IN ('unresolved', 'ambiguous')
        ORDER BY observed_rows DESC, incident_code
        """
    )
    return connection.execute("SELECT COUNT(*) FROM incident_code_review").fetchone()[0]


def run() -> dict[str, Any]:
    """Build review parquet outputs for route/station/incident alias QA."""
    paths = resolve_project_paths()
    dim_paths = _ensure_alias_dimensions(paths)
    reviews_root = paths.project_root / "reviews"

    route_review_path = reviews_root / "route_alias_review.parquet"
    station_review_path = reviews_root / "station_alias_review.parquet"
    incident_review_path = reviews_root / "incident_code_review.parquet"

    connection = ensure_duckdb_connection(paths.db_path)
    route_review_rows = _build_route_alias_review(connection, dim_paths["dim_route_alias"])
    station_review_rows = _build_station_alias_review(connection, dim_paths["dim_station_alias"])
    incident_review_rows = _build_incident_code_review(connection, dim_paths["dim_incident_code"])

    route_review_rows = _copy_table_to_parquet(connection, "route_alias_review", route_review_path)
    station_review_rows = _copy_table_to_parquet(connection, "station_alias_review", station_review_path)
    incident_review_rows = _copy_table_to_parquet(connection, "incident_code_review", incident_review_path)
    connection.close()

    return {
        "outputs": {
            "route_alias_review": route_review_path.resolve().as_posix(),
            "station_alias_review": station_review_path.resolve().as_posix(),
            "incident_code_review": incident_review_path.resolve().as_posix(),
        },
        "row_counts": {
            "route_alias_review": route_review_rows,
            "station_alias_review": station_review_rows,
            "incident_code_review": incident_review_rows,
        },
        "unresolved_counts": {
            "route_alias_review_unresolved_like": route_review_rows,
            "station_alias_review_unresolved_like": station_review_rows,
            "incident_code_review_unresolved_like": incident_review_rows,
        },
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
