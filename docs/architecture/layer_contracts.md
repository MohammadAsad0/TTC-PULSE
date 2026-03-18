# Layer Contracts (Step 2)

## Contract Philosophy
- Contracts prioritize traceability over early optimization.
- Any transformation that can lose context must emit lineage and review signals.
- Step 2 formalizes the canonical Silver schema contract; Gold remains implementation-pending.

## Raw Contract
Required behavior:
- Preserve every source file byte-for-byte after ingestion.
- No schema normalization, no type casting, no deduplication.
- Maintain immutable storage semantics (append-only arrivals, no in-place edits).

Required provenance:
- `source_family` (`bus`, `subway`, `gtfs_static`, `gtfsrt_alerts`)
- `source_file`
- `source_sheet` (nullable)
- `ingested_at`
- `file_checksum` (recommended)

Failure policy:
- Corrupt or unreadable files are quarantined with error metadata.
- Raw files are never rewritten during retries.

## Bronze Contract
Required behavior:
- Row-preserving extraction from Raw into structured tables.
- One Bronze row must map to one source record whenever source format permits.
- Schema drift is allowed and tracked; no forced canonical collapse here.

Mandatory lineage columns on every Bronze table:
- `source_file`
- `source_sheet` (nullable)
- `source_row_id`
- `ingested_at`
- `source_schema_version` (nullable but recommended)
- `row_hash` (recommended)

Rules:
- Ambiguous parse outcomes are retained with parse status flags, not dropped.
- Duplicate source rows are retained unless flagged; dedup logic belongs to Silver/Gold use cases.

## Silver Contract (Implemented in Step 2 at Schema Level)
Canonical entities in `silver` schema:
- Dimensions: `dim_route_gtfs`, `dim_stop_gtfs`, `dim_service_gtfs`, `dim_route_alias`, `dim_station_alias`, `dim_incident_code`
- Bridge: `bridge_route_direction_stop`
- Review tables: `route_alias_review`, `station_alias_review`, `incident_code_review`
- Mode-normalized tables: `silver_bus_events`, `silver_subway_events`, `silver_gtfsrt_alert_entities`
- Facts: `fact_delay_events_norm`, `fact_gtfsrt_alerts_norm`

Required behavior:
- Preserve raw text fields alongside canonical fields in normalized rows.
- Persist linkage quality columns: `match_method`, `match_confidence`, `link_status`.
- Route unresolved and ambiguous mappings to review queues with reasons.
- Keep unresolved rows queryable for QA; no silent drops.

Mode-specific modeling policy:
- Bus is route-first: resolve `route_id_gtfs` before optional stop/station enrichment.
- Subway is station-first: canonicalize station + line context before route backfill.

Match semantics policy:
- `match_method` captures the selected linkage strategy (`exact_gtfs_match`, `token_gtfs_match`, `alias_match`, `route_only_match`, `unmatched_review`).
- `match_confidence` stores numeric confidence (`DOUBLE`) for accepted or attempted linkage.
- `link_status` is final state (`matched`, `ambiguous_review`, `unmatched_review`).

## Gold Contract (Design Contract; Build Deferred to Step 3+)
Target marts:
- `gold_delay_events_core`
- `gold_linkage_quality`
- `gold_route_time_metrics`
- `gold_station_time_metrics`
- `gold_time_reliability`
- `gold_top_offender_ranking`
- `gold_alert_validation`
- `gold_spatial_hotspot` (deferred behind confidence gate)

Rules:
- Always expose component metrics: frequency, severity, regularity, cause mix.
- Composite reliability score is optional and must remain explainable.
- Spatial outputs are blocked until confidence thresholds are met.

## Step 2 vs Step 3+ Boundary
Step 2 completed:
- Canonical Silver schema contract is defined in DDL with dimensions, bridge, review tables, normalized event tables, and facts.
- Linkage semantics are standardized across bus/subway delay normalization.

Step 3+ remaining:
- Implement ETL/ELT jobs that populate Silver tables from Bronze tables.
- Automate review queue ingestion and alias promotion workflow.
- Materialize Gold marts and wire confidence-gated dashboard outputs.

## Contract Validation Checklist
- Raw immutability confirmed
- Bronze row counts reconcile against extraction inputs
- Bronze lineage columns complete
- Silver DDL contains all canonical and review tables
- Silver fact schemas include `match_method`, `match_confidence`, `link_status`
- Gold metrics reproducible from documented formulas once marts are implemented
