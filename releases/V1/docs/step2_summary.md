# Step 2 Summary (Silver Canonical Contracts)

- Run date: `2026-03-18`
- Scope: Step 2 documentation alignment for canonical Silver layer
- Source of truth validated: `docs/schema_ddl.sql`

## Step 2 Outcomes
Step 2 locks and documents the Silver canonical model with 15 schema objects:
- 6 dimensions (`dim_route_gtfs`, `dim_stop_gtfs`, `dim_service_gtfs`, `dim_route_alias`, `dim_station_alias`, `dim_incident_code`)
- 1 topology bridge (`bridge_route_direction_stop`)
- 3 review queues (`route_alias_review`, `station_alias_review`, `incident_code_review`)
- 3 mode-normalized Silver tables (`silver_bus_events`, `silver_subway_events`, `silver_gtfsrt_alert_entities`)
- 2 final canonical facts (`fact_delay_events_norm`, `fact_gtfsrt_alerts_norm`)

## Modeling and Linkage Semantics
- Bus normalization is route-first, then optional stop/station enrichment.
- Subway normalization is station-first with line context, then route backfill.
- Required linkage fields are standardized in normalized event tables and final delay fact:
  - `match_method`
  - `match_confidence`
  - `link_status`
- Unresolved/ambiguous mappings are retained and routed to review tables (not dropped).

## Caveats and Unresolved Mapping Status
- Silver schema is implemented at DDL/contract level.
- Operational population jobs and review tooling are not yet complete.
- Current active runtime data is still primarily Step 1 raw/bronze materialization.

## Explicit Step 3+ Remaining Scope
- Implement executable Silver transforms from Bronze inputs.
- Populate alias dimensions from approved review decisions.
- Materialize Gold marts using confidence-gated rules.
- Add QA monitoring for unresolved mapping backlog and linkage quality trends.
