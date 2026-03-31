# Agent Pipeline Workflow (Step 4)

## Purpose
Define the operational build sequence for TTC Pulse from ingestion through Gold marts, plus the documentation control loop used to keep contracts and caveats synchronized.

## Runtime Lock
- Dashboard: Streamlit.
- Data engine: DuckDB + Parquet.
- Spark: excluded.

## Build Sequence (Implementation Order)
1. Foundation wave (Step 1)
- Run ingestion and Bronze bootstrap (`bronze/build_bronze_tables.py`).
- Confirm `logs/ingestion_log.csv` and Step 1 docs were emitted.

2. Canonicalization wave (Step 2)
- Build GTFS dimensions + bridge.
- Build alias dimensions and review queues.
- Build normalized Silver entities/facts and register parquets in DuckDB.
- Confirm `logs/step2_registration_log.csv`.

3. Gold mart wave (Step 3 runtime)
- Run full Gold materialization (`marts/build_gold_rankings.py`, `run_build_all_gold_marts`).
- Confirm `logs/step3_gold_build_log.csv` and `outputs/final_metrics_summary.md`.

4. Alerts side-car wave
- Poll/parse Service Alerts (CLI and/or Airflow side-car DAG).
- Re-run alert normalization/fact + Gold alert validation as needed.
- Track side-car health in `logs/step3_alerts_sidecar_log.csv`.

5. Documentation wave (Step 4)
- Refresh architecture, dashboard, pipeline, runbook, data dictionary, changelog, and final report artifacts.
- Ensure caveats in runtime code and caveats in docs are consistent.

## Quality Gates
- Scope remains bus + subway + static GTFS + GTFS-RT Service Alerts.
- Every ranking output with `composite_score` includes component metrics.
- Alert-validation caveat is documented when alert fact rows are zero.
- Spatial hotspot remains deferred whenever confidence gate fails.
- All docs continue to state DuckDB/Parquet + Streamlit and Spark exclusion.

## Escalation Rules
- If Silver alert entities are empty, do not treat Gold alert validation emptiness as a failure by default; treat it as a known data-availability caveat.
- If confidence gate fails, block hotspot exposure and log gate metrics instead of forcing map output.
