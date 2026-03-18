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

## Dashboard and Metric Caveats
- The subway drill-down failure came from querying `gold_station_time_metrics` with a nonexistent `mode` column; that mart is station-first and should be queried through canonical station fields only.
- `Composite Score` is the default stakeholder ranking lens, but it is not the only valid exploratory lens.
- `Cause Mix` is inherently less stable at fine-grain slices and should be interpreted as a comparative signal, not a causal proof.
- Fine-grain drill slices can make the composite unstable; fallback to incident count, average delay, or `p90` delay is acceptable if the UI explains it inline.

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

## V2 Realignment Caveats
- `Linkage QA` is no longer visible in the sidebar, but it remains archived at `app/pages_archive/01_Linkage_QA.py` for recovery and QA use.
- Reliability Overview now filters by year range, so KPI values are slice-dependent and should not be compared directly to the full-history baseline without checking the selected window.
- Bus and subway ranking pages now recompute on selected date ranges, which makes Top N more flexible but also less comparable to the prior single-snapshot ranking contract.
- The weekday-hour heatmap is intentionally raw-stat oriented; it no longer uses composite score or cause mix as a primary lens.
- Cause-by-weekday and cause-by-hour views are spread/composition views, not causal inference outputs.
- Live Alert Validation is now an operations board; snapshot timestamps are capture context only and must not be interpreted as forecast horizons.
- Spatial bus hotspots are provisional route-centroid estimates derived from GTFS bridge geometry and route metrics.
- The spatial page uses a 1 to 69 Top N bound because the current subway hotspot table contains 69 mapped rows.
