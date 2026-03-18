# Proposal Alignment Notes (V2 Realignment)

Source reference:
- `docs/proposal_extracted.md`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/proposal.pdf`

Run date:
- 2026-03-18

## Proposal-Stated Dashboard Intent
- Linked dashboard views with map, heatmap, timeline, and rankings.
- Reliability represented through four pillars:
  - frequency
  - severity
  - regularity
  - modality/cause mix
- Stakeholder interpretation centered on where/when/why/how severe.
- GTFS-RT alerts as validation layer, not full operations modeling.

## V2 Realignment Decisions Applied
- Removed proposal-misaligned shell copy and removed Linkage QA from visible sidebar.
- Added date-range ranking for:
  - bus routes
  - subway stations
- Added year-range filtering to reliability overview across bus and subway.
- Converted weekday heatmap to raw reliability statistics view:
  - frequency
  - min delay p90
  - min gap p90
- Expanded cause page with whole-history cause spread by:
  - weekday
  - hour-of-day
- Redesigned live alerts page into operations-board format.
- Redesigned spatial page with subway + provisional bus mode and explicit confidence caveat.
- Removed historical coverage sentence from drill-down views.

## Alignment Outcome
- Proposal-required analytical narrative is now prioritized over internal pipeline QA messaging.
- Dashboard now reads as a reliability observatory flow rather than a mart-status demo shell.
- Composite score support is preserved while exposing raw pillar-based views where needed.

## Remaining Gaps vs Proposal
- Coordinated cross-filter interaction between all charts (global brushing) is still partial.
- Full city-wide H3 workflow remains deferred; current spatial bus mode is provisional route-centroid.
- Live alert overlay with direct hotspot hit-rate annotation is not yet a dedicated visual.

## Locked Scope Compliance
- No vehicle-position analytics added.
- No trip-update modeling added.
- No unnecessary ML introduced.
- Core bus + subway + static GTFS + GTFS-RT Service Alerts scope preserved.
