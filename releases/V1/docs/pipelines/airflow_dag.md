# Airflow DAG (GTFS-RT Alerts Side-Car, Step 3)

## Purpose
Define the approved Airflow side-car that handles cadence-sensitive GTFS-RT Service Alerts ingestion without taking ownership of the full historical batch pipeline.

## Scope Lock
- Exactly one DAG: `ttc_gtfsrt_alerts_pipeline`.
- Schedule: every 30 minutes (`*/30 * * * *`).
- Feed scope: GTFS-RT Service Alerts only.
- Explicitly out of scope: Vehicle Positions, Trip Updates, and historical Silver/Gold batch orchestration.
- `catchup=False` to avoid backfill replay unless intentionally implemented later.

## Current Repository Status
- DAG scaffold exists at `airflow/dags/ttc_gtfsrt_alerts_pipeline.py`.
- Current DAG contains placeholder tasks (`poll_alerts`, `process_alerts`) and tags.
- Start date and cadence are configured; production dataflow logic is pending.

## Side-Car Task Contract (Target)
1. `poll_alerts`
- Fetch TTC GTFS-RT Service Alerts snapshot with timeout/retry policy.

2. `persist_raw_snapshot`
- Persist raw protobuf payload with deterministic `snapshot_ts`.

3. `parse_informed_entities`
- Flatten alert selectors and entity payload into structured records.

4. `validate_selectors_vs_gtfs`
- Validate route/stop selectors against static GTFS dimensions.

5. `upsert_alert_facts_and_marts`
- Upsert `silver.fact_gtfsrt_alerts_norm` and `gold.gold_alert_validation`.

6. `log_run_metrics`
- Emit run metrics (row counts, parse/validation failures, selector coverage, runtime).

## Operational Contracts
- Idempotent writes keyed by snapshot timestamp and alert identifiers.
- Structured run logs for failure visibility and selector-quality trend monitoring.
- Raw snapshots are append-only; no destructive rewrite of prior snapshots.
- DAG is a side-car, not a controller for the broader Bronze/Silver/Gold dependency graph.

## Step 3 Caveats and Step 4 Handoff
- Step 3 keeps the side-car contract and cadence fixed while implementation remains scaffold-level.
- Step 4 will replace placeholders with production operators and connect outputs to downstream quality and dashboard validation workflows.
