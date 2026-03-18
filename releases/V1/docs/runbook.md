# TTC Pulse Runbook (Deployment and Execution)

## Execution Root
`/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse`

## Prerequisites
- Python 3.14+ available as `python3`.
- Virtual environment: `../.venv-ttc`.
- Set module path before running project modules:

```bash
cd /Users/om-college/Work/2\ Canada/York/Winter26/DataViz/Project/ttc_pulse
export PYTHONPATH=src
```

## Environment Bootstrap
Install runtime dependencies in venv as needed:

```bash
../.venv-ttc/bin/python -m pip install duckdb pyyaml streamlit gtfs-realtime-bindings
```

Notes:
- `duckdb` and `pyyaml` are already present in the current venv.
- `streamlit` and protobuf bindings may need installation in new environments.

## Step 1 - Ingestion and Bronze
```bash
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.bronze.build_bronze_tables
```

Expected artifacts:
- `logs/ingestion_log.csv`
- `docs/source_inventory.md`
- `docs/step1_summary.md`

## Step 2 - Silver Canonical Build
Run in this order:

```bash
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.gtfs.build_dimensions
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.gtfs.build_bridge
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.aliasing.build_route_alias
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.aliasing.build_station_alias
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.aliasing.build_incident_code_dim
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.aliasing.build_review_tables
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.normalization.normalize_bus
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.normalization.normalize_subway
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.normalization.normalize_gtfsrt_entities
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.facts.build_fact_delay_events_norm
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.facts.build_fact_gtfsrt_alerts_norm
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.normalization.register_step2_tables
```

Expected artifact:
- `logs/step2_registration_log.csv`

## Step 3 - Gold Mart Build
```bash
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.marts.build_gold_rankings
```

Expected artifacts:
- `logs/step3_gold_build_log.csv`
- `outputs/final_metrics_summary.md`
- Parquet marts under `gold/`

## GTFS-RT Alerts Side-Car Operations
One-shot local test-mode poll + parse:

```bash
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.alerts.poll_service_alerts --test-mode --register-manifest
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.alerts.parse_service_alerts
```

Live poll (network enabled):

```bash
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.alerts.poll_service_alerts --allow-network --register-manifest
```

Airflow side-car trigger:

```bash
airflow dags trigger poll_gtfsrt_alerts
```

## Dashboard Launch
```bash
PYTHONPATH=src ../.venv-ttc/bin/python -m streamlit run app/streamlit_app.py
```

## Verification Checklist
- Gold row counts: `outputs/final_metrics_summary.md`
- Step logs: `logs/ingestion_log.csv`, `logs/step2_registration_log.csv`, `logs/step3_gold_build_log.csv`
- Alerts side-car status: `logs/step3_alerts_sidecar_log.csv`

## Known Runtime Caveats
- `gold_alert_validation` can be empty when `fact_gtfsrt_alerts_norm` has no rows.
- `gold_spatial_hotspot` is intentionally deferred when confidence gate fails.
- If protobuf decoder dependency is missing, parsed alert outputs are fallback metadata rows only.
