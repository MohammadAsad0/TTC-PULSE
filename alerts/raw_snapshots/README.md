# GTFS-RT Service Alerts Raw Snapshots

This folder stores raw TTC GTFS-RT **Service Alerts** snapshots (`.pb` / `.bin`) captured by the Step 3 side-car poller.

## Files
- `manifest.csv`: append-only registry of side-car poll runs and snapshot capture status.
- `alerts_*.pb`: raw feed snapshots when polling or test-mode copy succeeds.

## Safety Notes
- Polling is offline-safe by default (`allow_network=False`).
- `test_mode` can copy from available local snapshots without outbound network calls.
- Vehicle positions and trip updates are intentionally out of scope.
