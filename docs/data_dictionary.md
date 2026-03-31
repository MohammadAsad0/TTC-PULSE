# TTC Pulse Data Dictionary (Step 4)

## Purpose
This dictionary captures the key fields required to interpret and trace core datasets across Raw, Bronze, Silver, and Gold.

## Global Lineage Keys
These keys are mandatory for end-to-end traceability on normalized event data and should be preserved wherever available:
- `source_file`
- `source_sheet`
- `source_row_id`
- `ingested_at`

## Raw Layer Key Fields
| Table / Artifact | Key fields | Notes |
|---|---|---|
| `raw.raw_bus_delay_events` | `raw_id`, `source_file`, `source_row_id`, `payload_json`, `ingested_at` | Raw bus payload capture. |
| `raw.raw_subway_delay_events` | `raw_id`, `source_file`, `source_row_id`, `payload_json`, `ingested_at` | Raw subway payload capture. |
| `raw.raw_gtfs_routes` | `route_id`, `route_short_name`, `route_type`, `source_file`, `ingested_at` | GTFS route dictionary source. |
| `raw.raw_gtfs_trips` | `trip_id`, `route_id`, `service_id`, `direction_id`, `ingested_at` | GTFS trip-level source. |
| `raw.raw_gtfs_stop_times` | `trip_id`, `stop_id`, `stop_sequence`, `arrival_time`, `departure_time` | GTFS topology source. |
| `raw.raw_gtfs_stops` | `stop_id`, `stop_name`, `stop_lat`, `stop_lon` | GTFS stop dictionary source. |
| `raw.raw_gtfs_calendar` / `raw.raw_gtfs_calendar_dates` | `service_id`, `start_date`, `end_date`, `date`, `exception_type` | Service calendar source. |
| `raw.raw_gtfsrt_alert_snapshots` | `snapshot_ts`, `source_url`, `feed_header_ts`, `raw_bytes`, `ingested_at` | Raw GTFS-RT snapshot bytes. |

## Bronze Layer Key Fields
| Table | Key fields | Notes |
|---|---|---|
| `bronze.bronze_bus_events` | `bronze_id`, `source_file`, `source_row_id`, `source_schema_version`, `row_hash`, `payload_json` | Row-preserving bus extract. |
| `bronze.bronze_subway_events` | `bronze_id`, `source_file`, `source_row_id`, `source_schema_version`, `row_hash`, `payload_json` | Row-preserving subway extract. |
| `bronze.bronze_gtfs_*` | GTFS source columns + `source_file`, `ingested_at` | Bronze copies of GTFS static files. |
| `bronze.bronze_gtfsrt_alert_entities` | `snapshot_ts`, `alert_id`, `route_id`, `stop_id`, `trip_id`, `selector_scope`, `ingested_at` | Parsed alert selectors before Silver normalization. |

## Silver Layer Key Fields
### Canonical dimensions and bridge
| Table | Key fields |
|---|---|
| `silver.dim_route_gtfs` | `route_id`, `route_short_name`, `route_type`, `mode`, `is_active` |
| `silver.dim_stop_gtfs` | `stop_id`, `stop_name`, `stop_lat`, `stop_lon` |
| `silver.dim_service_gtfs` | `service_id`, weekday flags, `start_date`, `end_date` |
| `silver.dim_route_alias` | `route_token_raw`, `route_id_gtfs`, `mapping_method`, `mapping_confidence`, `is_active` |
| `silver.dim_station_alias` | `station_raw`, `station_canonical`, `stop_id_gtfs_candidate`, `mapping_method`, `mapping_confidence` |
| `silver.dim_incident_code` | `mode`, `incident_code_raw`, `incident_text_raw`, `incident_category`, `is_active` |
| `silver.bridge_route_direction_stop` | `route_id`, `direction_id`, `stop_id`, `min_stop_sequence`, `max_stop_sequence` |

### Review queues
| Table | Key fields |
|---|---|
| `silver.route_alias_review` | `route_token_raw`, `proposed_route_id_gtfs`, `reason`, `review_status`, `reviewed_at` |
| `silver.station_alias_review` | `station_raw`, `proposed_station_canonical`, `proposed_stop_id_gtfs`, `review_status` |
| `silver.incident_code_review` | `incident_code_raw`, `proposed_incident_category`, `review_status`, `reviewed_at` |

### Normalized events and facts
| Table | Key fields |
|---|---|
| `silver.silver_bus_events` | `event_id`, `service_date`, `route_id_gtfs`, `incident_category`, `min_delay`, `match_method`, `match_confidence`, `link_status`, lineage keys |
| `silver.silver_subway_events` | `event_id`, `service_date`, `line_code_norm`, `station_canonical`, `incident_category`, `match_method`, `match_confidence`, `link_status`, lineage keys |
| `silver.silver_gtfsrt_alert_entities` | `snapshot_ts`, `alert_id`, `route_id_gtfs`, `stop_id_gtfs`, `trip_id_gtfs`, `selector_scope`, `match_status` |
| `silver.fact_delay_events_norm` | `event_id`, `mode`, `service_date`, `hour_bin`, `route_id_gtfs`, `station_canonical`, `incident_category`, `min_delay`, `min_gap`, linkage fields, lineage keys |
| `silver.fact_gtfsrt_alerts_norm` | `alert_event_id`, `snapshot_ts`, `alert_id`, `route_id_gtfs`, `stop_id_gtfs`, `trip_id_gtfs`, `selector_scope`, `match_status`, `ingested_at` |

## Gold Layer Key Fields
| Table | Key fields | Notes |
|---|---|---|
| `gold.gold_delay_events_core` | `mode`, `service_date`, `hour_bin`, `route_id_gtfs`, `station_canonical`, `incident_category`, `event_count` | Base trend mart. |
| `gold.gold_linkage_quality` | `mode`, `period_start`, `period_end`, `match_method`, `link_status`, `row_count`, `pct_of_mode_rows` | Trust/QA mart. |
| `gold.gold_route_time_metrics` | `mode`, `route_id_gtfs`, `service_date`, `hour_bin`, `frequency`, `severity_p90`, `regularity_p90`, `cause_mix_score`, `composite_score` | Route reliability mart. |
| `gold.gold_station_time_metrics` | `line_code_norm`, `station_canonical`, `service_date`, `hour_bin`, component metrics, `composite_score` | Station reliability mart. |
| `gold.gold_time_reliability` | `mode`, `day_name`, `hour_bin`, component metrics, `composite_score` | Temporal reliability mart. |
| `gold.gold_top_offender_ranking` | `ranking_date`, `mode`, `entity_type`, `entity_id`, component metrics, `composite_score`, `rank_position` | Prioritization mart. |
| `gold.gold_alert_validation` | `snapshot_ts`, `alert_id`, `route_id_gtfs`, `stop_id_gtfs`, `selector_scope`, `match_status` | Alert selector validity mart. |
| `gold.gold_spatial_hotspot` | `mode`, `spatial_unit_type`, `spatial_unit_id`, centroid fields, component metrics, `composite_score`, `confidence_score` | Confidence-gated spatial mart. |

## Caveat Flags for Consumers
- `gold_alert_validation` may be empty when upstream alert facts are empty.
- `gold_spatial_hotspot` may be intentionally empty when confidence gate fails.
- For ranking interpretation, always inspect component metrics with `composite_score`.
