"""Build top-offender rankings and orchestrate the full Step 3 Gold build."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from ttc_pulse.marts._gold_utils import ensure_table_from_parquet, materialize_query_to_gold, write_csv
from ttc_pulse.marts.build_gold_alert_validation import run_build_gold_alert_validation
from ttc_pulse.marts.build_gold_delay_core import run_build_gold_delay_core
from ttc_pulse.marts.build_gold_linkage_quality import run_build_gold_linkage_quality
from ttc_pulse.marts.build_gold_route_metrics import run_build_gold_route_metrics
from ttc_pulse.marts.build_gold_station_metrics import run_build_gold_station_metrics
from ttc_pulse.marts.build_gold_time_metrics import run_build_gold_time_metrics
from ttc_pulse.marts.scoring import DEFAULT_SCORE_WEIGHTS, composite_score_sql, validate_weights
from ttc_pulse.utils.project_setup import ensure_duckdb_connection, resolve_project_paths, utc_now_iso

TABLE_NAME = "gold_top_offender_ranking"
OUTPUT_FILENAME = "gold_top_offender_ranking.parquet"

STEP3_LOG_FILENAME = "step3_gold_build_log.csv"
FINAL_SUMMARY_FILENAME = "final_metrics_summary.md"


def _build_query() -> str:
    composite_expr = composite_score_sql(
        frequency_expr="frequency",
        severity_p90_expr="severity_p90",
        regularity_p90_expr="regularity_p90",
        cause_mix_expr="cause_mix_score",
        partition_columns=["ranking_date", "mode", "entity_type"],
        weights=DEFAULT_SCORE_WEIGHTS,
    )
    return f"""
    WITH route_metrics AS (
        SELECT
            mode,
            route_id_gtfs,
            service_date,
            frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score
        FROM gold_route_time_metrics
    ),
    station_metrics AS (
        SELECT
            station_canonical,
            service_date,
            frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score
        FROM gold_station_time_metrics
    ),
    latest AS (
        SELECT
            COALESCE(MAX(service_date), CURRENT_DATE) AS ranking_date
        FROM (
            SELECT service_date FROM route_metrics
            UNION ALL
            SELECT service_date FROM station_metrics
        ) AS all_dates
    ),
    route_window AS (
        SELECT
            l.ranking_date,
            r.mode,
            r.route_id_gtfs,
            r.frequency,
            r.severity_p90,
            r.regularity_p90,
            r.cause_mix_score
        FROM route_metrics AS r
        CROSS JOIN latest AS l
        WHERE r.service_date >= l.ranking_date - INTERVAL '89 day'
    ),
    station_window AS (
        SELECT
            l.ranking_date,
            'subway' AS mode,
            s.station_canonical,
            s.frequency,
            s.severity_p90,
            s.regularity_p90,
            s.cause_mix_score
        FROM station_metrics AS s
        CROSS JOIN latest AS l
        WHERE s.service_date >= l.ranking_date - INTERVAL '89 day'
    ),
    route_entities AS (
        SELECT
            ranking_date,
            mode,
            'route' AS entity_type,
            route_id_gtfs AS entity_id,
            SUM(frequency)::BIGINT AS frequency,
            quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
            quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
            AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
        FROM route_window
        WHERE route_id_gtfs IS NOT NULL
        GROUP BY 1, 2, 3, 4
    ),
    station_entities AS (
        SELECT
            ranking_date,
            mode,
            'station' AS entity_type,
            station_canonical AS entity_id,
            SUM(frequency)::BIGINT AS frequency,
            quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
            quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
            AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
        FROM station_window
        WHERE station_canonical IS NOT NULL
        GROUP BY 1, 2, 3, 4
    ),
    unioned AS (
        SELECT * FROM route_entities
        UNION ALL
        SELECT * FROM station_entities
    ),
    scored AS (
        SELECT
            ranking_date,
            mode,
            entity_type,
            entity_id,
            frequency,
            severity_p90,
            regularity_p90,
            COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
            {composite_expr} AS composite_score
        FROM unioned
    ),
    ranked AS (
        SELECT
            ranking_date,
            mode,
            entity_type,
            entity_id,
            frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score,
            RANK() OVER (
                PARTITION BY ranking_date, mode, entity_type
                ORDER BY composite_score DESC NULLS LAST, frequency DESC
            ) AS rank_position
        FROM scored
    )
    SELECT
        ranking_date,
        mode,
        entity_type,
        entity_id,
        frequency,
        severity_p90,
        regularity_p90,
        cause_mix_score,
        composite_score,
        rank_position
    FROM ranked
    """


def run_build_gold_rankings(
    db_path: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Build top-offender ranking mart from route and station Gold metrics."""
    validate_weights(DEFAULT_SCORE_WEIGHTS)
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_output_path = (output_path or (paths.project_root / "gold" / OUTPUT_FILENAME)).resolve()
    caveats: list[str] = []

    connection = ensure_duckdb_connection(resolved_db_path)
    try:
        ensure_table_from_parquet(connection, "gold_route_time_metrics", paths.project_root, caveats)
        ensure_table_from_parquet(connection, "gold_station_time_metrics", paths.project_root, caveats)

        route_rows = 0
        station_rows = 0
        if connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='gold_route_time_metrics'"
        ).fetchone()[0]:
            route_rows = int(connection.execute("SELECT COUNT(*) FROM gold_route_time_metrics").fetchone()[0])
        if connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='gold_station_time_metrics'"
        ).fetchone()[0]:
            station_rows = int(connection.execute("SELECT COUNT(*) FROM gold_station_time_metrics").fetchone()[0])

        if route_rows == 0 and station_rows == 0:
            caveats.append("Route and station Gold metrics are empty; ranking output is expected to be empty.")

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
        "weights": DEFAULT_SCORE_WEIGHTS.as_dict(),
    }


def _run_safe(
    builder: Callable[..., dict[str, Any]],
    table_name: str,
    db_path: Path,
) -> dict[str, Any]:
    try:
        return builder(db_path=db_path)
    except Exception as exc:  # pragma: no cover
        return {
            "table_name": table_name,
            "output_path": "",
            "duckdb_path": db_path.as_posix(),
            "row_count": 0,
            "status": f"error:{type(exc).__name__}",
            "caveats": [str(exc)],
        }


def _write_final_summary(summary_path: Path, results: list[dict[str, Any]]) -> None:
    now_ts = utc_now_iso()
    lines = [
        "# Final Metrics Summary",
        "",
        f"Generated at (UTC): {now_ts}",
        "",
        "## Gold Row Counts",
        "",
        "| Table | Row count | Status |",
        "|---|---:|---|",
    ]
    for result in results:
        lines.append(
            f"| {result.get('table_name', 'unknown')} | {int(result.get('row_count', 0))} | {result.get('status', 'unknown')} |"
        )

    lines.extend(
        [
            "",
            "## Composite Scoring Policy",
            "",
            (
                "S = 0.35*z(freq) + 0.30*z(sev90) + 0.20*z(reg90) + 0.15*cause_mix "
                "(weights version: step3-default-v1)"
            ),
            "",
            "## Metric Caveats",
            "",
        ]
    )

    all_caveats: list[str] = []
    for result in results:
        for caveat in result.get("caveats", []):
            if caveat not in all_caveats:
                all_caveats.append(caveat)

    if all_caveats:
        lines.extend(f"- {caveat}" for caveat in all_caveats)
    else:
        lines.append("- No caveats were emitted during the Step 3 Gold build.")

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_build_all_gold_marts(
    db_path: Path | None = None,
    log_path: Path | None = None,
    summary_path: Path | None = None,
) -> dict[str, Any]:
    """Run all Step 3 Gold builders, register outputs, and emit log/summary artifacts."""
    validate_weights(DEFAULT_SCORE_WEIGHTS)
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()
    resolved_log_path = (log_path or (paths.project_root / "logs" / STEP3_LOG_FILENAME)).resolve()
    resolved_summary_path = (
        summary_path or (paths.project_root / "outputs" / FINAL_SUMMARY_FILENAME)
    ).resolve()

    delay_result = _run_safe(run_build_gold_delay_core, "gold_delay_events_core", resolved_db_path)
    linkage_result = _run_safe(run_build_gold_linkage_quality, "gold_linkage_quality", resolved_db_path)
    route_result = _run_safe(run_build_gold_route_metrics, "gold_route_time_metrics", resolved_db_path)
    station_result = _run_safe(run_build_gold_station_metrics, "gold_station_time_metrics", resolved_db_path)
    time_result = _run_safe(run_build_gold_time_metrics, "gold_time_reliability", resolved_db_path)
    ranking_result = _run_safe(run_build_gold_rankings, "gold_top_offender_ranking", resolved_db_path)
    alert_result = _run_safe(run_build_gold_alert_validation, "gold_alert_validation", resolved_db_path)

    spatial_result = station_result.get("spatial_hotspot")
    if not isinstance(spatial_result, dict):
        spatial_result = {
            "table_name": "gold_spatial_hotspot",
            "output_path": "",
            "duckdb_path": resolved_db_path.as_posix(),
            "row_count": 0,
            "status": "error:MissingSpatialResult",
            "caveats": ["Station mart builder did not return a spatial_hotspot result."],
        }

    results = [
        delay_result,
        linkage_result,
        route_result,
        station_result,
        time_result,
        ranking_result,
        alert_result,
        spatial_result,
    ]

    logged_at = utc_now_iso()
    log_rows = [
        {
            "table": result.get("table_name", "unknown"),
            "row_count": int(result.get("row_count", 0)),
            "status": result.get("status", "unknown"),
            "timestamp": logged_at,
        }
        for result in results
    ]
    write_csv(
        path=resolved_log_path,
        fieldnames=["table", "row_count", "status", "timestamp"],
        rows=log_rows,
    )
    _write_final_summary(resolved_summary_path, results)

    return {
        "duckdb_path": resolved_db_path.as_posix(),
        "step3_log_path": resolved_log_path.as_posix(),
        "final_summary_path": resolved_summary_path.as_posix(),
        "table_results": results,
        "weights": DEFAULT_SCORE_WEIGHTS.as_dict(),
    }


def main() -> None:
    result = run_build_all_gold_marts()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
