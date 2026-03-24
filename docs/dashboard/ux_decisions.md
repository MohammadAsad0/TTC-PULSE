# UX Decisions (V4 Storytelling Redesign)

## Product Framing
- Storytelling-first over exploratory-first.
- Main audience: stakeholder/evaluator/professor who should grasp findings in under 10 minutes.
- Core narrative order:
  - what is happening
  - where it breaks
  - when it breaks
  - why it breaks
  - deep evidence for one hotspot
  - live alignment check
  - methodology appendix

## Frozen Navigation Decisions
- Reduced visible dashboard from many standalone analytic pages to 7 narrative pages.
- Archived previous V3 page set at `app/pages_archive/v3_pre_v4/`.
- Demoted QA diagnostics from opening navigation to final appendix page.

## Presentation vs Exploration
- Added shared mode toggle on every page:
  - `Presentation`: concise controls, fewer tables, one takeaway emphasis.
  - `Exploration`: deeper controls, expanded tables/charts.
- Default mode is Presentation to support walkthrough speed and narrative clarity.

## Merge / Remove Decisions
- Merged split ranking pages into `Recurring Hotspots` with mode-based entity scope.
- Merged temporal views into `Time Patterns` (weekday-hour + monthly support trend).
- Merged bus/subway drill pathways into one `Drill-Down Explorer` page while preserving:
  - route-first bus logic
  - station-first subway logic
- Demoted standalone linkage QA into `Technical Appendix`.

## Narrative Framing Pattern
- Every page starts with:
  - `Audience Question`
  - `Takeaway`
- Every page ends with a `Next Question` transition hint to the next story step.

## Interpretability Rules
- Composite score can be shown only with component context (frequency/severity/regularity/cause mix).
- Date windows are explicit and page-global.
- Empty-window states show clear info messaging and stop downstream rendering.

## Live Validation UX
- `Live Alert Alignment` remains explicit in main flow as the historical-vs-live claim check.
- Selector validity and capture timing stay visible.
- Dense alert rows are available in Exploration mode and not required in Presentation mode.

## Known Caveat Handling
- Bus spatial points remain provisional route-centroid approximations.
- Sparse or empty live-alert states are shown as caveated no-data informational states.
- QA remains visible and auditable, but not mixed into core story pages.
