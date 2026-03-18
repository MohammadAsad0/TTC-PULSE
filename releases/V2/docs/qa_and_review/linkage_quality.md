# Linkage Quality (Step 3 Stakeholder Interpretation Guide)

## Objective
Measure and communicate how trustworthy Silver-to-GTFS linkage is before stakeholders consume reliability rankings and hotspot outputs.

## Core Indicators
- Route match rate
- Station/stop match rate
- Match method distribution
- Confidence tier distribution
- Unmatched share trend
- Ambiguous share trend

## Metric Definitions
- Route match rate = rows with resolved route mapping / total eligible rows
- Station match rate = rows with resolved station or stop mapping / total eligible rows
- Unmatched share = rows with `link_status = unmatched_review` / total eligible rows
- Ambiguous share = rows with `link_status = ambiguous_review` / total eligible rows

## Terminology
- `match_method`: linkage strategy (`exact_gtfs_match`, `token_gtfs_match`, `alias_match`, `route_only_match`, `unmatched_review`)
- `match_confidence`: confidence score/tier for selected linkage
- `link_status`: final outcome (`matched`, `ambiguous_review`, `unmatched_review`)
- `lineage_complete`: required lineage keys exist for traceability

## Gold Mart Contract
- `gold_linkage_quality` aggregates linkage indicators by mode and period window.
- Every view must show both absolute counts (`row_count`) and percentages (`pct_of_mode_rows`) to avoid false confidence from small sample sizes.

## Interpretation for Stakeholders
Route reliability consumers:
- High route match with weaker station match still supports route-level ranking and trend analysis.
- Rising `route_only_match` share is acceptable for route views but should trigger station-linkage cleanup planning.

Station reliability consumers:
- Station-focused rankings are credible only when station/stop match quality is high and ambiguity is low.
- If ambiguous share increases, station-level actions should be tagged as provisional.

Program and QA leads:
- Trend direction matters more than one-day spikes; monitor rolling windows.
- Any persistent rise in unmatched/ambiguous share indicates review queue or alias coverage debt.

## Relationship to Confidence Gates
- Route-level outputs can tolerate lower-confidence linkage with clear labeling.
- Station-level outputs are stricter.
- Spatial hotspot remains deferred in Step 3 and should not be interpreted as absent risk; it is withheld pending confidence evidence.

## Step 3 Caveats and Step 4 Handoff
- Step 3 defines interpretation policy and trust thresholds, but automated stakeholder-facing alerts on quality drift are still pending.
- Step 4 will operationalize trend alerts, review backlog triggers, and hotspot release checks tied to confidence-gating thresholds.
