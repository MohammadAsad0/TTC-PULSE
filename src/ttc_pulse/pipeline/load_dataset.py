"""Run a full raw-to-gold dataset load for local TTC Pulse artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ttc_pulse.aliasing.build_incident_code_dim import run as run_build_incident_code_dim
from ttc_pulse.aliasing.build_review_tables import run as run_build_review_tables
from ttc_pulse.aliasing.build_route_alias import run as run_build_route_alias
from ttc_pulse.aliasing.build_station_alias import run as run_build_station_alias
from ttc_pulse.bronze.build_bronze_tables import run_step1
from ttc_pulse.facts.build_fact_delay_events_norm import run_build_fact_delay_events_norm
from ttc_pulse.facts.build_fact_gtfsrt_alerts_norm import run_build_fact_gtfsrt_alerts_norm
from ttc_pulse.gtfs.build_bridge import run as run_build_bridge
from ttc_pulse.gtfs.build_dimensions import run as run_build_dimensions
from ttc_pulse.marts.build_gold_rankings import run_build_all_gold_marts
from ttc_pulse.normalization.normalize_bus import run_normalize_bus
from ttc_pulse.normalization.normalize_gtfsrt_entities import run_normalize_gtfsrt_entities
from ttc_pulse.normalization.normalize_streetcar import run_normalize_streetcar
from ttc_pulse.normalization.normalize_subway import run_normalize_subway
from ttc_pulse.normalization.register_step2_tables import run as run_register_step2_tables
from ttc_pulse.utils.project_setup import resolve_project_paths, utc_now_iso


def _safe_row_count(result: dict[str, Any], fallback_keys: list[str] | None = None) -> int:
    if "row_count" in result:
        try:
            return int(result["row_count"])
        except (TypeError, ValueError):
            return 0
    for key in fallback_keys or []:
        value = result.get("row_counts", {}).get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
    return 0


def run_load_dataset(db_path: Path | None = None) -> dict[str, Any]:
    """Execute the full raw CSV -> parquet + DuckDB build sequence."""
    started_at = utc_now_iso()
    paths = resolve_project_paths()
    resolved_db_path = (db_path or paths.db_path).resolve()

    step1 = run_step1()
    dim = run_build_dimensions()
    bridge = run_build_bridge()
    route_alias = run_build_route_alias()
    station_alias = run_build_station_alias()
    incident_code = run_build_incident_code_dim()
    reviews = run_build_review_tables()
    bus = run_normalize_bus(db_path=resolved_db_path)
    streetcar = run_normalize_streetcar(db_path=resolved_db_path)
    subway = run_normalize_subway(db_path=resolved_db_path)
    gtfsrt_entities = run_normalize_gtfsrt_entities(db_path=resolved_db_path)
    fact_delay = run_build_fact_delay_events_norm()
    fact_alerts = run_build_fact_gtfsrt_alerts_norm(db_path=resolved_db_path)
    step2_registration = run_register_step2_tables(db_path=resolved_db_path)
    gold = run_build_all_gold_marts(db_path=resolved_db_path)

    return {
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "duckdb_path": resolved_db_path.as_posix(),
        "highlights": {
            "bronze_bus_rows": _safe_row_count(step1, ["bronze_bus"]),
            "bronze_streetcar_rows": _safe_row_count(step1, ["bronze_streetcar"]),
            "bronze_subway_rows": _safe_row_count(step1, ["bronze_subway"]),
            "silver_bus_rows": _safe_row_count(bus),
            "silver_streetcar_rows": _safe_row_count(streetcar),
            "silver_subway_rows": _safe_row_count(subway),
            "silver_gtfsrt_alert_entities_rows": _safe_row_count(gtfsrt_entities),
            "fact_delay_events_norm_rows": _safe_row_count(fact_delay),
            "fact_gtfsrt_alerts_norm_rows": _safe_row_count(fact_alerts),
            "gold_table_count": len(gold.get("table_results", [])),
        },
        "artifacts": {
            "step1_summary_md": step1.get("artifacts", {}).get("step1_summary_md", ""),
            "step2_registration_log_csv": step2_registration.get("registration_log_path", ""),
            "step3_log_csv": gold.get("step3_log_path", ""),
            "final_summary_md": gold.get("final_summary_path", ""),
        },
        "steps": {
            "step1": step1,
            "build_dimensions": dim,
            "build_bridge": bridge,
            "build_route_alias": route_alias,
            "build_station_alias": station_alias,
            "build_incident_code_dim": incident_code,
            "build_review_tables": reviews,
            "normalize_bus": bus,
            "normalize_streetcar": streetcar,
            "normalize_subway": subway,
            "normalize_gtfsrt_entities": gtfsrt_entities,
            "build_fact_delay_events_norm": fact_delay,
            "build_fact_gtfsrt_alerts_norm": fact_alerts,
            "register_step2_tables": step2_registration,
            "build_all_gold_marts": gold,
        },
    }


def main() -> None:
    print(json.dumps(run_load_dataset(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()



