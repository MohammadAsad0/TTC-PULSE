# Scheduler Operations (Active Local Runtime)

## Active Strategy
- Scheduler model: local OS-native recurring job.
- Cadence: every 30 minutes.
- Start window policy: forward-only from `2026-03-17`.
- Write policy: raw/parsed files are only updated when payload changes.

## Shared Side-Car Cycle
- Entry point: `src/ttc_pulse/alerts/run_sidecar_cycle.py`
- Behavior:
  - lock-safe cycle (skips overlapping runs),
  - live poll,
  - raw manifest registration when snapshot changes,
  - parse only the produced snapshot in append/dedupe mode.

## macOS (launchd)
- Install/start:
  - `./scripts/alerts/install_launchd_scheduler.sh`
- Uninstall/stop:
  - `./scripts/alerts/uninstall_launchd_scheduler.sh`
- Runner:
  - `scripts/alerts/run_sidecar_cycle.sh`
- Logs:
  - `logs/launchd_alerts_sidecar.out.log`
  - `logs/launchd_alerts_sidecar.err.log`

## Windows (Task Scheduler)
- Install/start:
  - `powershell -ExecutionPolicy Bypass -File .\scripts\alerts\install_windows_scheduler.ps1`
- Uninstall/stop:
  - `powershell -ExecutionPolicy Bypass -File .\scripts\alerts\uninstall_windows_scheduler.ps1`
- Runner:
  - `scripts/alerts/run_sidecar_cycle.ps1`

## Monitoring
- Operational status CSV:
  - `logs/step3_alerts_sidecar_log.csv`
- Raw snapshot registry:
  - `alerts/raw_snapshots/manifest.csv`
- Parsed snapshot registry:
  - `alerts/parsed/parse_manifest.csv`
