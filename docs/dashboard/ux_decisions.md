# UX Decisions (V2 Realignment)

## Product Framing
- Proposal alignment over internal pipeline language.
- Stakeholder-first analytical path:
  - overview
  - rankings
  - temporal patterns
  - cause composition
  - live validation
  - spatial context
  - drill-down detail

## Navigation Decisions
- Removed Linkage QA from visible sidebar because it is internal QA, not proposal-promised stakeholder narrative.
- Kept archived page file for rollback and QA access (`app/pages_archive/01_Linkage_QA.py`).

## Control Design Decisions
- Reliability Overview now uses mode + year-range controls.
- Ranking pages use calendar date ranges instead of one ranking snapshot date.
- Top-N ranking bars use dynamic height and scroll-safe containers to avoid label clipping.
- Weekday heatmap metric dropdown is raw-stat only:
  - frequency
  - min delay p90
  - min gap p90

## Interpretability Rules
- Composite remains available and unchanged where previously defined.
- Non-composite metric selection changes primary ranking/charting metric only.
- Drill pages keep context breadcrumbs, reset/back behavior, and fallback messaging.

## Live Validation UX
- Live alert page redesigned as an operations status board:
  - KPI row
  - validation status distribution
  - selector scope validity split
  - snapshot capture timeline
  - recent alerts table

## Spatial UX
- Subway hotspots remain confidence-gated.
- Bus spatial mode is shown as provisional route-centroid mapping with explicit warning.
- Map/table are linked via shared mode, metric, top-N, and text filtering controls.

## Known Caveat Handling
- Sparse fine-grain slices may trigger metric fallback.
- Bus spatial points are approximate geometric centroids, not incident geocodes.
- Live snapshot timestamp represents capture time only.
