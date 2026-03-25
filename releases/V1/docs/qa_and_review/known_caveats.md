# Known Caveats (Step 2)

## Canonicalization Caveats
Bus linkage:
- Route-first linking is robust for route analytics, but many rows can still end at `route_only_match` when location text is weak.
- Free-text location tokens can produce ambiguous stop candidates; these must stay in review queues.

Subway linkage:
- Station-first normalization depends on curated station aliases and line context quality.
- Legacy or shorthand station names can map to multiple stop candidates and require `station_alias_review`.

Incident normalization:
- Incident code/text drift across years can prevent stable category assignment without reviewer promotion into `dim_incident_code`.

## GTFS/GTFS-RT Caveats
- GTFS bridge topology (`bridge_route_direction_stop`) may vary by service pattern; stop-level joins should remain confidence-gated.
- GTFS-RT selectors are often route-heavy and sparse for stop/trip IDs, limiting immediate trip-level validation quality.
- Polling cadence can still miss short-lived alerts between snapshots.

## Current Implementation Boundary Caveats
- Step 2 implements Silver schema contracts in DDL; population/transformation jobs are not yet automated.
- Existing DuckDB runtime tables remain primarily Step 1 raw/bronze objects; canonical Silver facts are not yet materialized in routine runs.
- Review queues are schema-defined but operational triage tooling is not yet wired.

## Unresolved Mapping Policy
- Unmatched and ambiguous rows are retained, not dropped.
- Required outcome flags:
  - `match_method` records attempted linkage strategy.
  - `match_confidence` records score of the chosen/attempted mapping.
  - `link_status` records `matched`, `ambiguous_review`, or `unmatched_review`.
- All unresolved mapping classes must remain queryable for QA trend analysis.

## Step 3+ What Remains
- Implement Step 2 Silver transformation logic as executable jobs.
- Populate and version alias dimensions from approved review decisions.
- Enforce confidence gates in Gold marts and dashboard panels.
- Add review backlog and linkage-quality monitoring with alerting thresholds.
