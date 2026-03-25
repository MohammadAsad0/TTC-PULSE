# GTFS-RT Alerts 30-Minute Ingestion Run

## Run Metadata
- Run date (UTC): 2026-03-23T00:00:00Z
- Project: `ttc_pulse`
- Objective: capture GTFS-RT Service Alerts forward-only every 30 minutes from the project start window, with durable raw/parsed outputs and run logging.

## Operational Changes
- Poll sidecar now appends operational rows into `logs/step3_alerts_sidecar_log.csv`.
- Raw snapshot registration writes a manifest row and a matching log row.
- Parse sidecar now logs parse outcome rows into the same side-car log.
- Parser output mode is append/dedupe by default to avoid overwriting prior parsed history.
- Airflow polling DAG uses a 30-minute schedule, live polling by default, and registers raw snapshots by default.
- Start date is aligned to the project start window (`2026-03-17`) with `catchup=False`, so no historical backfill is attempted.

## Start Now (Current Runtime)
```bash
./scripts/alerts/install_launchd_scheduler.sh
```

Windows equivalent:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\alerts\install_windows_scheduler.ps1
```

## Verification Notes
- Confirm `logs/step3_alerts_sidecar_log.csv` receives new rows for:
  - `poll_service_alerts_*`
  - `register_raw_snapshot_manifest`
  - `parse_service_alert_snapshots`
- Confirm `alerts/raw_snapshots/manifest.csv` and `alerts/parsed/parse_manifest.csv` advance when snapshots are available.

## Caveat
- Historical alert backfill before the start window is not required and is not attempted by this change.
