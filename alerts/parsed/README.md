# GTFS-RT Service Alerts Parsed Outputs

This folder stores parser outputs produced from local Service Alerts snapshots.

## Files
- `service_alert_entities.csv`: structured rows (protobuf decode rows when available; fallback metadata rows otherwise).
- `parse_manifest.csv`: per-snapshot parse status and row counts.
- `parse_summary.json`: run-level summary, caveats, and output paths.

## Write Policy
- Parser default mode is append with snapshot-level dedupe.
- Use `--overwrite-outputs` only when intentionally rebuilding outputs from scratch.

## Decoder Caveat
If GTFS-RT protobuf bindings are unavailable, the parser emits fallback rows with explicit caveats and no decoded alert/entity fields.
