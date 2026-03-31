# TTC Pulse — Finalized Detailed Architecture

## 1) Scope Lock
- In scope: historical `bus + subway`, static merged `GTFS`, live `GTFS-RT Service Alerts`.
- Out of scope: vehicle positions, trip updates, streetcar core modeling, traffic causality, ridership inference, unnecessary ML.

## 2) Layered Data Model
- `Raw (immutable)`: preserve original Excel/CSV/GTFS files as-is.
- `Bronze`: row-preserving extracted tables with lineage metadata.
- `Silver`: canonical normalized model (facts + dimensions + alias tables + linkage quality).
- `Gold`: reliability marts and alert-validation marts.

## 3) Canonical Data Contracts
### Dimensions
- `dim_route_gtfs(route_id, route_short_name, route_long_name, route_type, mode)`
- `dim_stop_gtfs(stop_id, stop_name, stop_lat, stop_lon)`
- `dim_service_gtfs(service_id, weekday_flags..., start_date, end_date, exception_type)`
- `dim_route_alias(raw_route_token, route_short_name_norm, route_id_gtfs, mapping_method)`
- `dim_station_alias(station_raw, station_canonical, station_group, stop_id_gtfs_candidate, mapping_method)`
- `dim_incident_code(mode, incident_code_raw, incident_text_raw, incident_category)`

### Bridge
- `bridge_route_direction_stop(route_id, direction_id, stop_id, min_stop_sequence, max_stop_sequence, trip_count_serving_stop)`

### Facts
- `fact_delay_events_norm`
  - `event_id, mode, service_date, event_ts, day_name, hour_bin, month_bin`
  - `route_label_raw, route_short_name_norm, route_id_gtfs`
  - `line_code_raw, line_code_norm`
  - `location_text_raw, station_text_raw, station_canonical`
  - `incident_text_raw, incident_code_raw, incident_category`
  - `min_delay, min_gap`
  - `direction_raw, direction_norm, vehicle_id_raw`
  - `match_method, match_confidence, link_status`
  - `source_file, source_sheet, source_row_id, ingest_ts`
- `fact_gtfsrt_alerts_norm`
  - `snapshot_ts, feed_ts, alert_id`
  - `route_id_gtfs, stop_id_gtfs, trip_id_gtfs`
  - `cause, effect, header_text, description_text`
  - `selector_scope, match_status, match_notes`

## 4) Reliability Metrics
- Frequency: `count(*)`
- Severity: `median(min_delay)`, `p90(min_delay)`
- Regularity: `p90(min_gap)`
- Cause mix: category distribution score
- Composite score:
  - `S(u,t) = w1*z(Freq) + w2*z(Sev90) + w3*z(Reg90) + w4*CauseMix`
- Rule: always expose component metrics with composite.

## 5) GTFS Bridge Strategy
- Treat GTFS as a bridge asset family:
  - route dictionary
  - stop dictionary
  - service calendar
  - route-direction-stop topology
  - alias translators
- Join policy:
  - Bus: route-first, then corridor/intersection, then high-confidence stop.
  - Subway: station+line canonicalization first; map to GTFS route/stop via alias dims.

## 6) Spatial + Temporal Strategy
- Spatial:
  - Subway: station-level first.
  - Bus: route-level first; corridor/intersection second; stop-level only when confidence is high.
  - H3 deferred until location quality proves sufficient.
- Temporal:
  - event timestamp base
  - hour x weekday bins
  - daily + monthly aggregates

## 7) Live Validation Layer (Alerts Only)
- Poll GTFS-RT Service Alerts every 30 seconds via in-app APScheduler.
- Persist raw snapshot + flattened selectors.
- Validate `route_id`/`stop_id` against static GTFS.
- Trip-level validation deferred (selectors currently sparse/absent).

## 8) Scheduler Decision
- Airflow: include **one simple DAG** only for alert polling pipeline:
  1. fetch snapshot
  2. persist raw
  3. flatten selectors
  4. validate vs GTFS
  5. upsert alert-validation mart
  6. log run metrics
- No multi-DAG expansion in MVP.

## 9) Dashboard Build Order
1. Linkage QA panel
2. Reliability overview
3. Bus route ranking
4. Subway station ranking
5. Weekday x hour heatmap
6. Monthly trends
7. Cause/category mix
8. Live alert validation panel
9. Confidence-filtered spatial hotspot map

## 10) Minimum Shippable Architecture
- Raw sources preserved as-is
- Silver normalized facts/dims with GTFS bridge + alias tables
- Gold reliability + alert validation marts
- Streamlit/Dash dashboard
- One Airflow DAG for 30-min Service Alerts polling

## 12) Final Verdict
- Approved final structure:
  - immutable raw layer
  - Parquet-based cleaned analytical layer
  - compact dimensional model (fact + dimensions)
  - GTFS translator bridge
  - one simple Airflow DAG
  - Spark excluded for MVP
