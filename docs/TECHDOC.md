# TTC Pulse Technical Document

## Purpose
Code-first doc for teammates: what to read, how the pipeline runs, what DuckDB does, and how the dashboard consumes the marts.

## Read Order
1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `src/ttc_pulse/pipeline/load_dataset.py`
4. `src/ttc_pulse/bronze/build_bronze_tables.py`
5. `src/ttc_pulse/normalization/normalize_bus.py`
6. `src/ttc_pulse/normalization/normalize_subway.py`
7. `src/ttc_pulse/facts/build_fact_delay_events_norm.py`
8. `src/ttc_pulse/marts/build_gold_rankings.py`
9. `src/ttc_pulse/dashboard/loaders.py`
10. `app/streamlit_app.py`

## Execution Order (from `pipeline/load_dataset.py`)
1. `run_step1`
2. `build_dimensions`
3. `build_bridge`
4. `build_route_alias`
5. `build_station_alias`
6. `build_incident_code_dim`
7. `build_review_tables`
8. `normalize_bus`
9. `normalize_streetcar`
10. `normalize_subway`
11. `normalize_gtfsrt_entities`
12. `build_fact_delay_events_norm`
13. `build_fact_gtfsrt_alerts_norm`
14. `register_step2_tables`
15. `build_all_gold_marts`

## DuckDB’s Role
- load CSVs / register Parquet
- joins against GTFS dimensions
- grouped metrics, percentiles, window rankings
- fast local analytics without a server

## ETL / Preprocessing
- Step 1 (Bronze): discover sources, write registries, load bronze with lineage and schema-drift tolerance.
- Step 2 (Silver): GTFS dimensions/bridge; normalize route/station/incident; canonical events and facts; review queues.
- Step 3 (Gold): stakeholder marts (route/station/time metrics, rankings, alert validation, linkage quality, spatial).

## Data Model
- Dimensions: `dim_route_gtfs`, `dim_stop_gtfs`, `dim_service_gtfs`
- Alias dims: `dim_route_alias`, `dim_station_alias`, `dim_incident_code`
- Bridge: `bridge_route_direction_stop`
- Facts: `fact_delay_events_norm`, `fact_gtfsrt_alerts_norm`
- Review: `route_alias_review`, `station_alias_review`, `incident_code_review`

## Mode Strategy
- Bus: route-first (noisy location text)
- Subway: station-first (cleaner identity)
- Streetcar: extension scope

## Gold Marts
`gold_delay_events_core`, `gold_route_time_metrics`, `gold_station_time_metrics`, `gold_time_reliability`, `gold_top_offender_ranking`, `gold_alert_validation`, `gold_spatial_hotspot`, `gold_linkage_quality`.

## Dashboard
Entry: `app/streamlit_app.py`
Shared: `dashboard/loaders.py`, `dashboard/charts.py`, `dashboard/formatting.py`, `dashboard/storytelling.py`
AI: `dashboard/ai_explain.py`, `app/pages/08_AI_Chat_Bot.py`

## Evaluation / QA
Artifact-driven: build logs, mart outputs, linkage quality, alert validation, archived QA/regression reports. No strong `pytest` suite yet.

## Debugging Order
1. registry / manifest
2. Bronze
3. Silver event
4. fact
5. Gold mart
6. dashboard loader
7. page code