# Silver Layer (Step 2 Canonical Implementation)

## Status
Step 2 establishes the canonical Silver schema in `docs/schema_ddl.sql`.
The schema defines 15 Silver tables: dimensions, bridge, review queues, normalized mode tables, and final facts.

Step 2 scope is schema-level implementation and documentation alignment.
Population jobs and refresh orchestration are explicitly deferred to Step 3+.

## Purpose
Silver is the canonical normalization layer that:
- harmonizes bus, subway, and GTFS-RT entities into stable analytical facts;
- preserves raw context fields for auditability; and
- records linkage quality (`match_method`, `match_confidence`, `link_status`) on normalized event records.

## Step 2 Canonical Silver Tables
Core dimensions:
- `dim_route_gtfs`
- `dim_stop_gtfs`
- `dim_service_gtfs`
- `dim_route_alias`
- `dim_station_alias`
- `dim_incident_code`

Bridge:
- `bridge_route_direction_stop`

Review queues:
- `route_alias_review`
- `station_alias_review`
- `incident_code_review`

Mode-normalized staging tables:
- `silver_bus_events`
- `silver_subway_events`
- `silver_gtfsrt_alert_entities`

Canonical facts:
- `fact_delay_events_norm`
- `fact_gtfsrt_alerts_norm`

## Route/Station Modeling Strategy
Bus modeling is route-first:
1. Normalize route token (`route_label_raw` -> `route_short_name_norm`).
2. Resolve `route_id_gtfs` from GTFS route dictionary or route alias dimension.
3. Optionally attach stop/station context from `location_text_raw` when confidence supports it.

Subway modeling is station-first:
1. Canonicalize station text (`station_text_raw` -> `station_canonical`) with line context (`line_code_norm`).
2. Resolve GTFS stop candidate via station alias dimension.
3. Backfill `route_id_gtfs` for downstream route-level comparability.

## Match Semantics (Step 2)
`match_method` indicates how the link was produced:
- `exact_gtfs_match`
- `token_gtfs_match`
- `alias_match`
- `route_only_match` (expected mainly in bus route-level linking)
- `unmatched_review` (no accepted candidate)

`match_confidence` is a numeric score (`DOUBLE`) on the selected match.
- High confidence: deterministic exact/token matches.
- Medium confidence: curated alias matches.
- Lower confidence: fallback/route-only matches.

`link_status` is the final link outcome:
- `matched`: accepted canonical link for analysis (subject to confidence gates).
- `ambiguous_review`: multiple plausible candidates; held for human review.
- `unmatched_review`: no accepted canonical candidate.

Rows with `ambiguous_review` and `unmatched_review` are retained and routed into review tables; they are never silently dropped.

## Fact-Level Contracts
`fact_delay_events_norm` combines bus and subway delay events with:
- shared temporal bins (`service_date`, `day_name`, `hour_bin`, `month_bin`);
- dual raw/canonical fields (`route_label_raw`, `station_text_raw`, `station_canonical`, etc.);
- incident normalization (`incident_code_raw`, `incident_category`);
- linkage quality columns and source lineage (`source_file`, `source_sheet`, `source_row_id`, `row_hash`).

`fact_gtfsrt_alerts_norm` stores alert-level canonicalized selectors with:
- alert identity/time (`snapshot_ts`, `feed_ts`, `alert_id`, active window);
- canonical selector IDs (`route_id_gtfs`, `stop_id_gtfs`, `trip_id_gtfs`);
- validation outcome fields (`match_status`, `match_notes`).

## Step 3+ Remaining Work
- Build and schedule Silver population jobs from Bronze inputs.
- Seed and govern alias dimensions using reviewer-approved mappings.
- Enforce confidence gating in Gold marts and dashboard filters.
- Implement recurring review triage and promotion loop for unresolved mappings.
