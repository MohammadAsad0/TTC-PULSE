# TTC Pulse Final Project Summary

## Final Runtime Position
- Date: 2026-03-18
- Runtime lock: Streamlit + DuckDB/Parquet
- Spark status: excluded from MVP
- Scope lock: bus delay, subway delay, static GTFS, GTFS-RT Service Alerts

## Delivered Data Assets
### Step 2 registered Silver assets (from `logs/step2_registration_log.csv`)
- `silver_bus_events`: 776435
- `silver_subway_events`: 250558
- `silver_gtfsrt_alert_entities`: 0
- `fact_delay_events_norm`: 1026993
- `fact_gtfsrt_alerts_norm`: 0
- `dim_route_gtfs`: 229
- `dim_stop_gtfs`: 9417
- `dim_service_gtfs`: 8
- `dim_route_alias`: 865
- `dim_station_alias`: 13860
- `dim_incident_code`: 267
- `bridge_route_direction_stop`: 17824
- `route_alias_review`: 381
- `station_alias_review`: 13809
- `incident_code_review`: 267

### Step 3 Gold marts (from `logs/step3_gold_build_log.csv`)
- `gold_delay_events_core`: 1004682
- `gold_linkage_quality`: 955
- `gold_route_time_metrics`: 818661
- `gold_station_time_metrics`: 197434
- `gold_time_reliability`: 336
- `gold_top_offender_ranking`: 289
- `gold_alert_validation`: 0
- `gold_spatial_hotspot`: 0 (`built_with_caveats`)

## Dashboard and Pipeline Status
- Streamlit app shell exists with Linkage QA placeholder page.
- Gold marts and metrics artifacts are materialized and query-ready.
- Alerts side-car code path exists with offline-safe polling, parse outputs, and Airflow DAG hooks.

## Critical Caveats
- `gold_alert_validation` is currently empty because normalized alert facts are empty (`fact_gtfsrt_alerts_norm = 0`).
- `gold_spatial_hotspot` remains deferred because confidence gate did not pass; schema-only zero-row output is emitted.
- Alerts parser can emit fallback metadata rows when protobuf decoder dependency is unavailable.

## Step 4 Documentation Guarantees
- Architecture, data flow, pipeline, dashboard contracts, runbook, and data dictionary are aligned with executable runtime behavior.
- All core docs explicitly enforce Streamlit + DuckDB/Parquet lock, Spark exclusion, confidence-gated hotspot release, and alert-validation empty-state caveat handling.
