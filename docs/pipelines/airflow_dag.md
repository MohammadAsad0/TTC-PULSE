# Airflow DAG (GTFS-RT Alerts Side-Car, Legacy Reference)

## Purpose
Document the retained Airflow DAG contract as a legacy reference. Local runtime scheduling now uses OS-native schedulers (`launchd` on macOS and Windows Task Scheduler on Windows).

## Legacy DAG Contract
- Primary DAG: `poll_gtfsrt_alerts` (`airflow/dags/poll_gtfsrt_alerts.py`).
- Schedule: every 30 minutes (`*/30 * * * *`).
- Start date: `2026-03-17`.
- Catchup: `False` (no historical backfill replay).
- Feed scope: GTFS-RT Service Alerts only.
- Explicitly out of scope: Vehicle Positions, Trip Updates, and full Bronze/Silver/Gold orchestration.

## Task Flow
1. `poll_service_alerts`
- Calls `ttc_pulse.alerts.poll_service_alerts.run_poll_service_alerts`.
- Default behavior in DAG is live polling (`allow_network=True`) unless overridden by environment variables.
- Registers raw snapshot rows into `alerts/raw_snapshots/manifest.csv`.
- Writes operational rows into `logs/step3_alerts_sidecar_log.csv`.

2. `parse_entities`
- Parses only the produced snapshot path from the poll task (no EDA backfill scanning in DAG mode).
- Uses append/dedupe output mode so repeated runs do not overwrite prior parsed history.
- Writes/updates:
  - `alerts/parsed/service_alert_entities.csv`
  - `alerts/parsed/parse_manifest.csv`
  - `alerts/parsed/parse_summary.json`
  - side-car log rows in `logs/step3_alerts_sidecar_log.csv`

3. `hook_fact_normalization` (placeholder hook)
4. `hook_gold_alert_validation_refresh` (placeholder hook)

## Environment Controls
- `TTC_PULSE_ALERTS_ALLOW_NETWORK` (default `true` in DAG)
- `TTC_PULSE_ALERTS_DRY_RUN` (default `false`)
- `TTC_PULSE_ALERTS_TEST_MODE` (default `false`)

## Operational Guarantees
- Raw snapshot manifest is append-only.
- Parsed outputs are append-oriented with snapshot-level dedupe to avoid duplicate re-parsing.
- Side-car log records poll, registration, and parse outcomes with timestamped status rows.

## Backfill Policy
- Historical GTFS-RT backfill before `2026-03-17` is not attempted by default.
- Forward capture starts now and continues on the 30-minute schedule.

## Legacy Scaffold
- `airflow/dags/ttc_gtfsrt_alerts_pipeline.py` remains as a minimal scaffold artifact, is unscheduled (`schedule_interval=None`), and is not the active side-car flow.
