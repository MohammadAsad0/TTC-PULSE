# TTC Pulse

TTC Pulse is a DuckDB + Parquet + Streamlit reliability analytics project for TTC operational data.

Core scope:
- historical bus delay logs
- historical subway delay logs
- static GTFS reference data
- GTFS-Realtime service alerts

Current branch extensions:
- streetcar ingestion and some mart coverage
- AI Explain for major charts
- AI Chat Bot

## Canonical Documentation

Active project docs live in `docs/`:
- `docs/ARCHITECTURE.md`
- `docs/TECHDOC.md`
- `docs/PROJECT_SCOPE.md`
- `docs/STORYLINE.md`

Archived historical material (older docs, reports, releases) is stored outside the repo under `../Project Docs/TTC Pulse Archive/`.

## Repository Layout
- `app/` Streamlit dashboard pages
- `src/ttc_pulse/` pipeline, marts, dashboard helpers, alerts sidecar
- `raw/`, `bronze/`, `silver/`, `dimensions/`, `bridge/`, `reviews/`, `gold/` data artifacts
- `alerts/` GTFS-RT raw snapshots and parsed outputs
- `logs/` execution logs
- `outputs/` final metric summaries
- `data/ttc_pulse.duckdb` local analytical database
- `docs/` active project docs (this repo)

## Environment

Recommended stable interpreter in this workspace:
```bash
cd ttc_pulse
PYTHONPATH=src ../.venv-ttc/bin/python -m streamlit run app/streamlit_app.py
```

Fresh local env (if needed):
```bash
cd ttc_pulse
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Data Layout

The pipeline expects datasets in a sibling `datasets/` folder:
```
Project/
|-- datasets/
|   |-- 01_gtfs_merged/
|   |-- 02_bus_delay/
|   `-- 03_subway_delay/
`-- ttc_pulse/
```

GTFS-RT snapshots land in `alerts/raw_snapshots/`.

## Quick Start

If Gold outputs already exist:
```bash
cd ttc_pulse
PYTHONPATH=src ../.venv-ttc/bin/python -m streamlit run app/streamlit_app.py
```

Full pipeline then dashboard:
```bash
cd ttc_pulse
export PYTHONPATH=src
python -m ttc_pulse.pipeline.load_dataset
python -m streamlit run app/streamlit_app.py
```

## Manual Build Order

Step 1:
```bash
PYTHONPATH=src python -m ttc_pulse.bronze.build_bronze_tables
```

Step 2:
```bash
PYTHONPATH=src python -m ttc_pulse.gtfs.build_dimensions
PYTHONPATH=src python -m ttc_pulse.gtfs.build_bridge
PYTHONPATH=src python -m ttc_pulse.aliasing.build_route_alias
PYTHONPATH=src python -m ttc_pulse.aliasing.build_station_alias
PYTHONPATH=src python -m ttc_pulse.aliasing.build_incident_code_dim
PYTHONPATH=src python -m ttc_pulse.aliasing.build_review_tables
PYTHONPATH=src python -m ttc_pulse.normalization.normalize_bus
PYTHONPATH=src python -m ttc_pulse.normalization.normalize_streetcar
PYTHONPATH=src python -m ttc_pulse.normalization.normalize_subway
PYTHONPATH=src python -m ttc_pulse.normalization.normalize_gtfsrt_entities
PYTHONPATH=src python -m ttc_pulse.facts.build_fact_delay_events_norm
PYTHONPATH=src python -m ttc_pulse.facts.build_fact_gtfsrt_alerts_norm
PYTHONPATH=src python -m ttc_pulse.normalization.register_step2_tables
```

Step 3:
```bash
PYTHONPATH=src python -m ttc_pulse.marts.build_gold_rankings
```

## Live Alerts

Poll once:
```bash
PYTHONPATH=src python -m ttc_pulse.alerts.poll_service_alerts --allow-network --register-manifest
```

Parse snapshots:
```bash
PYTHONPATH=src python -m ttc_pulse.alerts.parse_service_alerts
```

Full sidecar cycle:
```bash
PYTHONPATH=src python -m ttc_pulse.alerts.run_sidecar_cycle --allow-network
```

## Suggested Read Order For New Contributors
1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/TECHDOC.md`
4. `docs/PROJECT_SCOPE.md`
5. `docs/STORYLINE.md`
