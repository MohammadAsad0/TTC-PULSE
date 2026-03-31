# Current Project Audit (2026-03-18)

Audit root: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse`  
DuckDB audited: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/data/ttc_pulse.duckdb`

## 1) Files saved locally by step (Step1/Step2/Step3/Step4)

Step1 runtime artifacts found (6 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/data/ttc_pulse.duckdb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/logs/ingestion_log.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/bus/bus_file_registry.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/subway/subway_file_registry.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/gtfs/gtfs_file_registry.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/gtfsrt/gtfsrt_snapshot_registry.csv`

Step2 runtime artifacts found (16 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/logs/step2_registration_log.csv`
- `15 parquet outputs under /Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/{silver,dimensions,bridge,reviews}` (listed in Section 3)

Step3 runtime artifacts found (17 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/logs/step3_gold_build_log.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/logs/step3_alerts_sidecar_log.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/outputs/final_metrics_summary.md`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/airflow/dags/poll_gtfsrt_alerts.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260317T203007Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/manifest.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/parsed/service_alert_entities.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/parsed/parse_manifest.csv`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/parsed/parse_summary.json`
- `8 parquet outputs under /Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold` (listed in Section 3)

Step4 runtime/UI artifacts found (12 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/streamlit_app.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/01_Linkage_QA.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/02_Reliability_Overview.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/03_Bus_Route_Ranking.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/04_Subway_Station_Ranking.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/05_Weekday_Hour_Heatmap.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/06_Monthly_Trends.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/07_Cause_Category_Mix.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/08_Live_Alert_Validation.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/09_Spatial_Hotspot_Map.py`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/requirements.txt`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/.streamlit/config.toml`

## 2) DuckDB tables created (with row counts)

Query source: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/data/ttc_pulse.duckdb`  
Total tables found in `main`: **38**

| table_name | row_count |
|---|---:|
| bridge_route_direction_stop | 17824 |
| bronze_bus | 776435 |
| bronze_gtfs_calendar | 8 |
| bronze_gtfs_calendar_dates | 7 |
| bronze_gtfs_routes | 229 |
| bronze_gtfs_shapes | 1025672 |
| bronze_gtfs_stop_times | 4249149 |
| bronze_gtfs_stops | 9417 |
| bronze_gtfs_trips | 133665 |
| bronze_gtfsrt_alerts | 0 |
| bronze_gtfsrt_entities | 0 |
| bronze_subway | 250558 |
| dim_incident_code | 267 |
| dim_route_alias | 865 |
| dim_route_gtfs | 229 |
| dim_service_gtfs | 8 |
| dim_station_alias | 13860 |
| dim_stop_gtfs | 9417 |
| fact_delay_events_norm | 1026993 |
| fact_gtfsrt_alerts_norm | 0 |
| gold_alert_validation | 0 |
| gold_delay_events_core | 1004682 |
| gold_linkage_quality | 955 |
| gold_route_time_metrics | 818661 |
| gold_spatial_hotspot | 0 |
| gold_station_time_metrics | 197434 |
| gold_time_reliability | 336 |
| gold_top_offender_ranking | 289 |
| incident_code_review | 267 |
| raw_bus_file_registry | 100 |
| raw_gtfs_file_registry | 7 |
| raw_gtfsrt_snapshot_registry | 2 |
| raw_subway_file_registry | 61 |
| route_alias_review | 381 |
| silver_bus_events | 776435 |
| silver_gtfsrt_alert_entities | 0 |
| silver_subway_events | 250558 |
| station_alias_review | 13809 |

## 3) Parquet outputs created (group by folder: silver/dimensions/bridge/reviews/gold)

Total parquet files across required folders: **23**

`silver` (5 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/silver/fact_delay_events_norm.parquet` (1026993 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/silver/fact_gtfsrt_alerts_norm.parquet` (0 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/silver/silver_bus_events.parquet` (776435 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/silver/silver_gtfsrt_alert_entities.parquet` (0 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/silver/silver_subway_events.parquet` (250558 rows)

`dimensions` (6 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/dimensions/dim_incident_code.parquet` (267 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/dimensions/dim_route_alias.parquet` (865 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/dimensions/dim_route_gtfs.parquet` (229 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/dimensions/dim_service_gtfs.parquet` (8 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/dimensions/dim_station_alias.parquet` (13860 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/dimensions/dim_stop_gtfs.parquet` (9417 rows)

`bridge` (1 file):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/bridge/bridge_route_direction_stop.parquet` (17824 rows)

`reviews` (3 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/reviews/incident_code_review.parquet` (267 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/reviews/route_alias_review.parquet` (381 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/reviews/station_alias_review.parquet` (13809 rows)

`gold` (8 files):
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_alert_validation.parquet` (0 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_delay_events_core.parquet` (1004682 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_linkage_quality.parquet` (955 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_route_time_metrics.parquet` (818661 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_spatial_hotspot.parquet` (0 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_station_time_metrics.parquet` (197434 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_time_reliability.parquet` (336 rows)
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/gold/gold_top_offender_ranking.parquet` (289 rows)

## 4) docs files written/updated by step

Docs base path: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/docs`  
All listed files currently exist (missing=0 in local check).

Step1 docs touched (21):
- `architecture/overview.md`
- `architecture/layer_contracts.md`
- `architecture/data_flow.md`
- `layers/raw_layer.md`
- `layers/bronze_layer.md`
- `layers/silver_layer.md`
- `layers/gold_layer.md`
- `decisions/design_decisions.md`
- `decisions/alias_strategy.md`
- `decisions/confidence_gating.md`
- `pipelines/airflow_dag.md`
- `pipelines/agent_pipeline.md`
- `qa_and_review/review_tables.md`
- `qa_and_review/linkage_quality.md`
- `qa_and_review/known_caveats.md`
- `dashboard/panel_descriptions.md`
- `dashboard/ux_decisions.md`
- `source_inventory.md`
- `step1_summary.md`
- `changelog/agent_run_logs/step1_run.md`
- `changelog/CHANGELOG.md`

Step2 docs touched (8):
- `layers/silver_layer.md`
- `architecture/layer_contracts.md`
- `decisions/alias_strategy.md`
- `qa_and_review/review_tables.md`
- `qa_and_review/known_caveats.md`
- `step2_summary.md`
- `changelog/agent_run_logs/step2_run.md`
- `changelog/CHANGELOG.md`

Step3 docs touched (8):
- `layers/gold_layer.md`
- `decisions/design_decisions.md`
- `decisions/confidence_gating.md`
- `pipelines/airflow_dag.md`
- `qa_and_review/linkage_quality.md`
- `step3_summary.md`
- `changelog/agent_run_logs/step3_run.md`
- `changelog/CHANGELOG.md`

Step4 docs touched (10):
- `dashboard/panel_descriptions.md`
- `dashboard/ux_decisions.md`
- `architecture/data_flow.md`
- `pipelines/agent_pipeline.md`
- `architecture/overview.md`
- `architecture.md`
- `data_dictionary.md`
- `runbook.md`
- `changelog/agent_run_logs/step4_run.md`
- `changelog/CHANGELOG.md`

## 5) What remains incomplete

- GTFS-RT protobuf decoding dependency is not available in current environment (`google.transit.gtfs_realtime_pb2` import missing), so parsed alerts are fallback metadata rows only.
- Alert normalization pipeline output is still empty (`fact_gtfsrt_alerts_norm = 0`), so `gold_alert_validation` is empty (`0` rows) and alert QA pages are empty-state only.
- Spatial hotspot remains deferred by confidence gate; `gold_spatial_hotspot` is schema-present but `0` rows (`built_with_caveats` in Step3 log).
- Airflow side-car integration is partial: `airflow/dags/poll_gtfsrt_alerts.py` contains `hook_pending` tasks; parallel scaffold DAG `airflow/dags/ttc_gtfsrt_alerts_pipeline.py` still has placeholder callables.
- Dashboard runtime dependencies are not fully installed in current venv (`streamlit` and `altair` missing), so app launch is blocked despite clean syntax.

## 6) Readiness assessment for next step

Status: **Conditionally ready**.

- Ready for next-step hardening on bus/subway reliability analytics (Silver/Gold non-alert marts are materialized and non-empty).
- Not ready for full live-alert decisioning or full dashboard launch until dependencies and alert normalization wiring are completed.
- Recommended immediate next-step gate to clear:
  1. Install runtime deps in venv (`streamlit`, `altair`, and GTFS-RT protobuf bindings).
  2. Complete alert fact normalization hook and Gold alert validation refresh wiring.
  3. Resolve hotspot confidence gate failure criteria before enabling map publication.

## 7) Runnable status check

Checks run:
- DuckDB table/row audit query against `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/data/ttc_pulse.duckdb`: **PASS**.
- Runtime file existence:
  - `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/streamlit_app.py`: **exists**
  - `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages` with 9 page modules: **exists**
  - `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/requirements.txt`: **exists**
  - `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/.streamlit/config.toml` (optional): **exists**
- Lightweight syntax compile check using `../.venv-ttc/bin/python` on dashboard entry + all pages + `src/ttc_pulse/dashboard/*.py`: **PASS** (`checked=14`, `failed=0`).
- Runtime dependency smoke check in same venv:
  - `duckdb`, `pandas`, `pyarrow`: **import ok**
  - `streamlit`, `altair`: **missing**
  - `python -m streamlit --version`: **FAIL** (`No module named streamlit`)

Runnable verdict:
- **Code syntax status:** PASS
- **Local app runnable status:** FAIL (dependency gap)
