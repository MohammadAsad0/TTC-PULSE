# TTC Pulse

TTC Pulse is a DuckDB + Parquet + Streamlit analytics project for studying TTC reliability using:
- historical TTC bus delay logs
- historical TTC subway delay logs
- static GTFS
- GTFS-Realtime Service Alerts

The project builds a layered pipeline:
- `Raw` = immutable source registries
- `Bronze` = row-preserving extracts with lineage
- `Silver` = normalized facts, dimensions, alias tables, and GTFS bridge assets
- `Gold` = stakeholder-facing marts for rankings, trends, heatmaps, alert validation, and hotspot analysis

The MVP runtime is:
- Python
- DuckDB
- Parquet
- Streamlit

Spark is intentionally excluded from the MVP.

## Repository Layout

Key folders:
- `app/` Streamlit dashboard
- `src/ttc_pulse/` pipeline modules
- `raw/`, `bronze/`, `silver/`, `dimensions/`, `bridge/`, `reviews/`, `gold/` data artifacts
- `alerts/` GTFS-RT raw snapshots and parsed outputs
- `data/ttc_pulse.duckdb` local analytical database
- `docs/` technical documentation and run logs
- `reports/` QA, freeze, and regression outputs

## Prerequisites

- Python `3.11+`
- `git`
- internet access only if you want to poll live GTFS-RT alerts

## Clone and Environment Setup

```bash
git clone https://github.com/MohammadAsad0/TTC-PULSE.git
cd TTC-PULSE/ttc_pulse

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

All commands below assume:
- you are inside the `ttc_pulse/` folder
- your virtual environment is activated

## Required Data Layout

The ingestion code resolves datasets from a sibling folder named `datasets/`.

Expected workspace layout:

```text
TTC-PULSE/
├── datasets/
│   ├── 01_gtfs_merged/
│   │   ├── routes.txt
│   │   ├── trips.txt
│   │   ├── stop_times.txt
│   │   ├── stops.txt
│   │   ├── calendar.txt
│   │   ├── calendar_dates.txt
│   │   └── shapes.txt
│   ├── 02_bus_delay/
│   │   └── csv/
│   └── 03_subway_delay/
│       └── csv/
└── ttc_pulse/
```

Notes:
- `datasets/` must be next to `ttc_pulse/`, not inside it.
- Bus ingestion expects files under `datasets/02_bus_delay/csv`.
- Subway ingestion expects files under `datasets/03_subway_delay/csv`.
- GTFS ingestion expects files under `datasets/01_gtfs_merged`.
- GTFS-RT snapshot polling writes into `ttc_pulse/alerts/raw_snapshots/`.

## Quick Start

If you want the full local build from raw data to dashboard:

```bash
export PYTHONPATH=src
python -m ttc_pulse.bronze.build_bronze_tables
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
python -m ttc_pulse.marts.build_gold_rankings
streamlit run app/streamlit_app.py
```

If the Gold parquet outputs are already present in the repo or your local copy, you can often skip directly to:

```bash
streamlit run app/streamlit_app.py
```

## Step-by-Step Execution

### 1. Build Raw and Bronze

```bash
export PYTHONPATH=src
python -m ttc_pulse.bronze.build_bronze_tables
```

This creates:
- raw registries in `raw/`
- Bronze parquet outputs in `bronze/`
- source inventory and step summary docs
- `data/ttc_pulse.duckdb`

Important outputs:
- `logs/ingestion_log.csv`
- `docs/source_inventory.md`
- `docs/step1_summary.md`

### 2. Build Silver, Dimensions, Bridge, and Reviews

Run in this order:

```bash
export PYTHONPATH=src
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

Important outputs:
- `silver/*.parquet`
- `dimensions/*.parquet`
- `bridge/*.parquet`
- `reviews/*.parquet`
- `logs/step2_registration_log.csv`

### 3. Build Gold Marts

```bash
export PYTHONPATH=src
python -m ttc_pulse.marts.build_gold_rankings
```

This orchestrates the Gold build and writes:
- `gold/gold_delay_events_core.parquet`
- `gold/gold_linkage_quality.parquet`
- `gold/gold_route_time_metrics.parquet`
- `gold/gold_station_time_metrics.parquet`
- `gold/gold_time_reliability.parquet`
- `gold/gold_top_offender_ranking.parquet`
- `gold/gold_alert_validation.parquet`
- `gold/gold_spatial_hotspot.parquet`

Important outputs:
- `logs/step3_gold_build_log.csv`
- `outputs/final_metrics_summary.md`

### 4. Launch the Dashboard

```bash
export PYTHONPATH=src
streamlit run app/streamlit_app.py
```

Default local URL:
- [http://localhost:8501](http://localhost:8501)

## GTFS-RT Alerts

### Poll live Service Alerts once

```bash
export PYTHONPATH=src
python -m ttc_pulse.alerts.poll_service_alerts --allow-network --register-manifest
```

### Parse local GTFS-RT snapshots

```bash
export PYTHONPATH=src
python -m ttc_pulse.alerts.parse_service_alerts
```

### Test mode without relying on live collection

```bash
export PYTHONPATH=src
python -m ttc_pulse.alerts.poll_service_alerts --test-mode --register-manifest
python -m ttc_pulse.alerts.parse_service_alerts
```

Outputs land in:
- `alerts/raw_snapshots/`
- `alerts/parsed/`

## Verification

Useful files to inspect after a run:
- `logs/ingestion_log.csv`
- `logs/step2_registration_log.csv`
- `logs/step3_gold_build_log.csv`
- `outputs/final_metrics_summary.md`
- `docs/step1_summary.md`
- `docs/step2_summary.md`
- `docs/step3_summary.md`

## Known Runtime Notes

- The dashboard reads from DuckDB first and falls back to Gold parquet files when needed.
- `gold_alert_validation` can be empty if no normalized GTFS-RT alert facts exist yet.
- `gold_spatial_hotspot` is confidence-gated; if the gate fails, the hotspot page may have limited or deferred outputs.
- Bus spatial hotspots in the current MVP are provisional route-centroid views, not precise incident geocodes.

## Documentation

For more detail:
- [Runbook](docs/runbook.md)
- [Architecture](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)

## Current Dashboard Scope

The current dashboard includes:
- Reliability Overview
- Bus Route Ranking
- Subway Station Ranking
- Weekday Hour Heatmap
- Monthly Trends
- Cause Category Mix
- Live Alert Validation
- Spatial Hotspot Map
- Bus Reliability Drill-Down
- Subway Reliability Drill-Down
