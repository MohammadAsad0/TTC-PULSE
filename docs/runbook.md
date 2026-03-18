# TTC Pulse Runbook

This runbook is the execution reference for rebuilding TTC Pulse from raw datasets to the Streamlit dashboard.

## Execution Root

Run all commands from the repository execution folder:

```bash
cd ttc_pulse
```

## Environment

Recommended:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
export PYTHONPATH=src
```

## Required Workspace Layout

The code expects `datasets/` to exist next to `ttc_pulse/`.

```text
TTC-PULSE/
├── datasets/
│   ├── 01_gtfs_merged/
│   ├── 02_bus_delay/
│   │   └── csv/
│   └── 03_subway_delay/
│       └── csv/
└── ttc_pulse/
```

## Step 1: Raw and Bronze

```bash
python -m ttc_pulse.bronze.build_bronze_tables
```

Main outputs:
- `raw/bus/bus_file_registry.csv`
- `raw/subway/subway_file_registry.csv`
- `raw/gtfs/gtfs_file_registry.csv`
- `raw/gtfsrt/gtfsrt_snapshot_registry.csv`
- `bronze/`
- `data/ttc_pulse.duckdb`
- `logs/ingestion_log.csv`

## Step 2: Silver, Dimensions, Bridge, Reviews

Run in order:

```bash
python -m ttc_pulse.gtfs.build_dimensions
python -m ttc_pulse.gtfs.build_bridge
python -m ttc_pulse.aliasing.build_route_alias
python -m ttc_pulse.aliasing.build_station_alias
python -m ttc_pulse.aliasing.build_incident_code_dim
python -m ttc_pulse.aliasing.build_review_tables
python -m ttc_pulse.normalization.normalize_bus
python -m ttc_pulse.normalization.normalize_subway
python -m ttc_pulse.normalization.normalize_gtfsrt_entities
python -m ttc_pulse.facts.build_fact_delay_events_norm
python -m ttc_pulse.facts.build_fact_gtfsrt_alerts_norm
python -m ttc_pulse.normalization.register_step2_tables
```

Main outputs:
- `silver/`
- `dimensions/`
- `bridge/`
- `reviews/`
- `logs/step2_registration_log.csv`

## Step 3: Gold Marts

```bash
python -m ttc_pulse.marts.build_gold_rankings
```

This builds the Gold analytical layer, including:
- `gold_delay_events_core`
- `gold_linkage_quality`
- `gold_route_time_metrics`
- `gold_station_time_metrics`
- `gold_time_reliability`
- `gold_top_offender_ranking`
- `gold_alert_validation`
- `gold_spatial_hotspot`

Main outputs:
- `gold/*.parquet`
- `logs/step3_gold_build_log.csv`
- `outputs/final_metrics_summary.md`

## Step 4: Launch Dashboard

```bash
streamlit run app/streamlit_app.py
```

Default local URL:
- `http://localhost:8501`

## GTFS-RT Service Alerts

One-shot live poll:

```bash
python -m ttc_pulse.alerts.poll_service_alerts --allow-network --register-manifest
```

Parse available snapshots:

```bash
python -m ttc_pulse.alerts.parse_service_alerts
```

Offline/test-mode collection:

```bash
python -m ttc_pulse.alerts.poll_service_alerts --test-mode --register-manifest
python -m ttc_pulse.alerts.parse_service_alerts
```

Outputs:
- `alerts/raw_snapshots/`
- `alerts/parsed/`

## Validation Checklist

Check these after a full run:
- `logs/ingestion_log.csv`
- `logs/step2_registration_log.csv`
- `logs/step3_gold_build_log.csv`
- `outputs/final_metrics_summary.md`
- `reports/dashboard_revision_regression.md`

## Known Caveats

- If the dashboard cannot find DuckDB tables, it falls back to Gold parquet files where available.
- `gold_alert_validation` may be empty if GTFS-RT alerts were not collected and normalized.
- `gold_spatial_hotspot` is confidence-gated.
- Bus hotspot points are provisional route-centroid estimates in the current MVP.
