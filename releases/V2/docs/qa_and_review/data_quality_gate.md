# Data Quality Gate (Wave 3 Hardening)

## Scope
- Applies to bus stakeholder outputs, station reliability outputs, and QA/review surfaces.
- The bus stakeholder gate is strict: only GTFS-backed bus routes may appear in stakeholder-facing route views.

## Bus Gate Rules
- A bus route is eligible for stakeholder views only when the route is backed by GTFS static route data.
- Rows without a resolvable GTFS-backed bus route key remain queryable, but they stay in QA/review outputs only.
- Unmatched bus rows must not be promoted into ranking panels or drill-down pages.

## Sentinel Handling
- `min_delay >= 999` is a sentinel and must be treated as invalid for severity aggregation.
- `min_gap >= 999` is a sentinel and must be treated as invalid for regularity aggregation.
- Sentinel rows are retained for auditability but excluded from sev/reg rollups.

## Visibility Rules
- Unmatched and ambiguous mappings remain visible in QA/review surfaces.
- Stakeholder dashboards show only filtered, decision-grade data plus the accompanying caveat text.
- QA surfaces should keep the raw distribution visible so mapping debt is still measurable.

## Temporal Labeling Intent
- Subway drill-down views should carry explicit time labels for the selected window.
- Temporal labels should align station detail, heatmap context, and ranking summaries.
