# Scheduler Migration Run (Launchd + Windows Task Scheduler)

## Run Metadata
- Run date (UTC): 2026-03-23
- Objective: move active alert side-car scheduling from Airflow runtime dependency to local OS-native schedulers while preserving 30-minute forward-only ingestion behavior.

## Decisions Applied
- Active scheduler on macOS: `launchd`.
- Windows compatibility: Task Scheduler scripts provided with same side-car cycle contract.
- Airflow DAGs retained as legacy documentation/reference only; not the active local runtime requirement.

## Implementation
- Added lock-safe side-car cycle module:
  - `src/ttc_pulse/alerts/run_sidecar_cycle.py`
- Added no-change write behavior in poller:
  - when latest payload hash matches latest manifest hash, skip raw snapshot write and downstream parse write.
- Added macOS scheduler scripts:
  - `scripts/alerts/run_sidecar_cycle.sh`
  - `scripts/alerts/install_launchd_scheduler.sh`
  - `scripts/alerts/uninstall_launchd_scheduler.sh`
- Added Windows scheduler scripts:
  - `scripts/alerts/run_sidecar_cycle.ps1`
  - `scripts/alerts/install_windows_scheduler.ps1`
  - `scripts/alerts/uninstall_windows_scheduler.ps1`

## Verification
- One live network poll succeeded (HTTP 200) and appended a new raw snapshot.
- Parse step appended new rows for changed payload.
- Re-run on unchanged payload reports `no_change`/`skipped_existing` behavior and avoids duplicate writes.

## Documentation Updated
- `docs/architecture/overview.md`
- `docs/architecture/data_flow.md`
- `docs/decisions/design_decisions.md`
- `docs/runbook.md`
- `docs/README.md`
- `docs/pipelines/airflow_dag.md`
- `docs/pipelines/scheduler_ops.md`
