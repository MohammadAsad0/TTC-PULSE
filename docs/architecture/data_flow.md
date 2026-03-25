# Data Flow and Lineage (Step 4 Runtime)

## Runtime and Scope Lock
- Runtime lock: Streamlit UI, DuckDB + Parquet data engine.
- Scope lock: bus delay, subway delay, static GTFS, GTFS-RT Service Alerts.
- Spark is excluded.

## End-to-End Batch Flow
1. Step 1 ingestion and Bronze materialization
- Entry point: `src/ttc_pulse/bronze/build_bronze_tables.py` (`run_step1`).
- Writes raw registry CSV artifacts and DuckDB raw/bronze tables.
- Emits `logs/ingestion_log.csv`, `docs/source_inventory.md`, `docs/step1_summary.md`.

2. Step 2 canonical Silver assets
- Dimensions and bridge: `src/ttc_pulse/gtfs/build_dimensions.py`, `src/ttc_pulse/gtfs/build_bridge.py`.
- Alias and review outputs: `src/ttc_pulse/aliasing/build_route_alias.py`, `src/ttc_pulse/aliasing/build_station_alias.py`, `src/ttc_pulse/aliasing/build_incident_code_dim.py`, `src/ttc_pulse/aliasing/build_review_tables.py`.
- Normalized events and facts: `src/ttc_pulse/normalization/normalize_bus.py`, `src/ttc_pulse/normalization/normalize_subway.py`, `src/ttc_pulse/normalization/normalize_gtfsrt_entities.py`, `src/ttc_pulse/facts/build_fact_delay_events_norm.py`, `src/ttc_pulse/facts/build_fact_gtfsrt_alerts_norm.py`.
- Registration entry point: `src/ttc_pulse/normalization/register_step2_tables.py`.
- Primary artifacts: Parquet under `silver/`, `dimensions/`, `bridge/`, `reviews/`; registration log at `logs/step2_registration_log.csv`.

3. Step 3 Gold marts
- Entry point: `src/ttc_pulse/marts/build_gold_rankings.py` (`run_build_all_gold_marts`).
- Materializes all Gold marts under `gold/`.
- Emits `logs/step3_gold_build_log.csv` and `outputs/final_metrics_summary.md`.

4. Dashboard consumption
- Streamlit reads Gold outputs through DuckDB/Parquet-backed contracts.
- Current app code is scaffold-level; contracts are data-ready.

## GTFS-RT Service Alerts Side-Car Flow
1. Poll snapshot (`poll_service_alerts`) with offline-safe defaults.
2. Register raw snapshot manifest row (`alerts/raw_snapshots/manifest.csv`).
3. Parse snapshots to structured CSV (`alerts/parsed/service_alert_entities.csv`) in append/dedupe mode.
4. Normalize into Silver alert entities and alert fact.
5. Refresh `gold_alert_validation`.

Scheduler assets:
- macOS active scheduler: `scripts/alerts/install_launchd_scheduler.sh` with runner `scripts/alerts/run_sidecar_cycle.sh`.
- Windows equivalent scheduler: `scripts/alerts/install_windows_scheduler.ps1` with runner `scripts/alerts/run_sidecar_cycle.ps1`.
- Shared cycle entry point: `src/ttc_pulse/alerts/run_sidecar_cycle.py` (lock-safe, poll + parse).
- Airflow DAG files under `airflow/dags/` are retained as legacy reference only for this local-first architecture.

## Lineage and Traceability Keys
- Required lineage keys across normalized event data: `source_file`, `source_sheet`, `source_row_id`, `ingested_at`.
- Required linkage trust fields for reliability outputs: `match_method`, `match_confidence`, `link_status`.

## Mandatory Caveats
- Alert validation can be empty if `fact_gtfsrt_alerts_norm` has zero rows for the run window.
- Spatial hotspot remains deferred when confidence gate fails; in that case the builder writes a schema-only zero-row artifact.
- If GTFS-RT protobuf decoder bindings are unavailable, parser outputs fallback metadata rows with explicit caveats.
