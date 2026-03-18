# GTFS-RT Alerts Gap Fix Run Log

## Run Metadata
- Run date (UTC): 2026-03-17T21:27:47Z
- Project: `ttc_pulse`
- Objective: load parsed GTFS-RT alert entities into bronze and execute downstream GTFS-RT normalization/fact/gold pipeline.

## Code Changes Implemented
- Added new loader module:
  - `src/ttc_pulse/alerts/load_parsed_into_bronze.py`
- Wired existing Step 1 bronze flow to call parsed GTFS-RT loader:
  - `src/ttc_pulse/bronze/build_bronze_tables.py`
- Exported new alerts module path:
  - `src/ttc_pulse/alerts/__init__.py`
- Ensured GTFS-RT normalize/fact builders also refresh DuckDB tables (not parquet-only):
  - `src/ttc_pulse/normalization/normalize_gtfsrt_entities.py`
  - `src/ttc_pulse/facts/build_fact_gtfsrt_alerts_norm.py`
- Added runtime dependencies:
  - `requirements.txt` (`gtfs-realtime-bindings`, `protobuf`, `pyyaml`)

## Pipeline Execution Order
1. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.alerts.load_parsed_into_bronze`
2. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.normalization.normalize_gtfsrt_entities`
3. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.facts.build_fact_gtfsrt_alerts_norm`
4. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.marts.build_gold_alert_validation`

## Before / After Row Counts
| Object | Before | After |
|---|---:|---:|
| `bronze_gtfsrt_entities` | 0 | 3 |
| `silver_gtfsrt_alert_entities` | 0 | 3 |
| `fact_gtfsrt_alerts_norm` | 0 | 3 |
| `gold_alert_validation` | 0 | 3 |

## Caveats
- The currently committed parsed source (`alerts/parsed/service_alert_entities.csv`) still contains only `fallback_binary_metadata` rows (3 rows), so selector-level GTFS fields remain sparse.
- Local dependency check in this environment: `google.transit.gtfs_realtime_pb2` import is available.
- Full protobuf-decoded alert rows still require re-running the parse step to regenerate `alerts/parsed/service_alert_entities.csv` from snapshots; without that rerun, downstream tables will continue to reflect fallback metadata content.

## Artifacts
- `logs/alerts_fix_log.csv`
- `docs/changelog/agent_run_logs/alerts_fix_run.md`
