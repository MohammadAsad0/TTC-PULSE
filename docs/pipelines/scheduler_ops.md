# Scheduler Operations (Current Runtime)

## Active Strategy
- Scheduler model: in-app APScheduler (`BackgroundScheduler`) started by the Streamlit **Live Alert Alignment** page.
- Cadence: every 30 seconds.
- Scope: GTFS-RT Service Alerts polling/parsing for subway, bus, and streetcar feeds.
- Runtime behavior: OS-agnostic (Windows, macOS, Linux).

## Polling Behavior
- First scheduler run seeds current alerts and marks seen IDs.
- Subsequent runs process only newly observed distinct alert IDs.
- No new alerts: current alerts table remains unchanged.
- New alerts found: current alerts table and scheduler timeline are updated.

## Manual Refresh
- UI action: **Refresh Alert Data**.
- Behavior: immediate on-demand poll/parse cycle in the same runtime path as scheduler polling.

## Persistence and Monitoring
- Poll timeline history: `logs/live_alert_poll_timeline.csv`
- Side-car operational log: `logs/step3_alerts_sidecar_log.csv`
- Raw snapshot manifest: `alerts/raw_snapshots/manifest.csv`
- Parsed alerts history: `alerts/parsed/service_alert_entities.csv`
- Parsed manifest: `alerts/parsed/parse_manifest.csv`

## Legacy Notes
- External OS scheduler scripts are no longer the active polling mechanism.
- Airflow scheduler artifacts are retained only as historical reference.
