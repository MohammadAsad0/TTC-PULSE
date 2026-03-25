# Design Decisions (Step 3)

## Decision Log
DD-001: Data platform
- Decision: Use DuckDB plus Parquet for MVP analytics.
- Rationale: Current data volume and query concurrency fit single-node local-first execution.
- Consequence: Fast iteration now; distributed stack postponed until clear scale pressure.

DD-002: Dashboard runtime
- Decision: Keep Streamlit as the delivery target for the first production dashboard.
- Rationale: Strong velocity for explainable analytics-first UI.
- Consequence: Prioritizes transparent metric views over custom front-end complexity.

DD-003: Scheduler scope (local-first side-car)
- Decision: Use local OS-native schedulers for GTFS-RT Service Alerts side-car cadence (`launchd` on macOS, Windows Task Scheduler on Windows) at 30-minute intervals.
- Rationale: Project is local-first and not moving to server-grade orchestration; Airflow runtime overhead is unnecessary for one recurring job.
- Consequence: Side-car scheduling remains lightweight and portable across teammate operating systems while Silver/Gold historical materialization remains manual.

DD-004: Layering and traceability
- Decision: Preserve Raw immutable, Bronze row-preserving with lineage, and retain unresolved mappings in Silver.
- Rationale: Auditability is required for confidence gates and stakeholder trust.
- Consequence: Lossy cleanup in upstream layers is disallowed.

DD-005: Scope lock
- Decision: Core scope remains bus, subway, static GTFS, and GTFS-RT Service Alerts.
- Rationale: Maintains a focused reliability narrative with controlled implementation risk.
- Consequence: Streetcar and additional feeds remain out of MVP core contracts.

DD-006: Gold mart set
- Decision: Lock eight Gold marts as the stakeholder contract (`gold_delay_events_core`, `gold_linkage_quality`, `gold_route_time_metrics`, `gold_station_time_metrics`, `gold_time_reliability`, `gold_top_offender_ranking`, `gold_alert_validation`, `gold_spatial_hotspot`).
- Rationale: Creates stable dashboard and QA interfaces while jobs are still being operationalized.
- Consequence: Downstream consumers can develop against fixed mart names and semantics.

DD-007: Reliability framework and composite formula
- Decision: Standardize reliability to Frequency + Severity + Regularity + Cause Mix, with optional composite scoring.
- Rationale: Prevents opaque ranking by forcing explainable component-level interpretation.
- Consequence: Composite score may be used for prioritization only when component metrics are visible in the same view.

DD-008: Confidence-gated spatial release
- Decision: Keep `gold_spatial_hotspot` deferred in Step 3.
- Rationale: Spatial views are sensitive to weak station/stop linkage quality.
- Consequence: Spatial hotspot activation is moved to Step 4 and requires threshold evidence.

DD-009: Exclude Spark
- Decision: Spark remains excluded from MVP.
- Rationale: Overhead is unjustified for present workload and team size.
- Consequence: Revisit only if SLA, concurrency, or data-size constraints materially change.

DD-010: Metric selector standard
- Decision: Major analytical pages expose a shared `Metric to analyze` control with `Composite Score`, `Frequency`, `Severity`, `Regularity`, and `Cause Mix`.
- Rationale: The dashboard needs multiple explainable lenses without losing the composite baseline.
- Consequence: Composite behavior stays unchanged when selected; other metrics become the primary ranking/chart field.

DD-011: Subway drill-down correction
- Decision: Remove the invalid dependency on `mode` in subway drill-down queries against `gold_station_time_metrics`.
- Rationale: The mart is station-first and does not carry a `mode` column.
- Consequence: Subway drill-down stays aligned with the actual mart contract and remains station-first end to end.

DD-012: Fine-grain fallback policy
- Decision: Allow drill pages to fall back from composite to simpler metrics when the selected slice is sparse or unstable.
- Rationale: Fine slices can produce brittle composite rankings that are hard to interpret.
- Consequence: The UI remains stable and explanatory, but must state the fallback inline.

DD-013: V2 navigation cleanup
- Decision: Remove `Linkage QA` from visible sidebar navigation and keep it archived outside the main app pages.
- Rationale: The project proposal emphasizes stakeholder-facing reliability views, not an internal QA landing page.
- Consequence: QA remains recoverable, but the default user path is now aligned to the proposal narrative.

DD-014: Range-based analytical controls
- Decision: Use year-range controls for reliability overview and calendar date ranges for ranking pages.
- Rationale: Analysts need to compare windows across the full historical record, not only a single ranking snapshot.
- Consequence: Ranking views now communicate stability over time while preserving the existing composite score behavior.

DD-015: Raw temporal heatmap lens
- Decision: Restrict the weekday-hour heatmap to raw frequency, `Min Delay P90`, and `Min Gap P90`.
- Rationale: This view should surface readable temporal pressure patterns without conflating them with composite ranking logic.
- Consequence: Composite and cause-mix lenses move to the other analysis pages where they add more value.

DD-016: Operations-board alert framing
- Decision: Redesign Live Alert Validation as an operations board with KPI, validity, timeline, and recent-alert sections.
- Rationale: The proposal calls for validation, not a raw table dump.
- Consequence: Snapshot timestamps are treated as capture context only, not as forecast labels.

DD-017: Provisional bus hotspots
- Decision: Allow bus mode on the spatial page as provisional route-centroid mapping with an explicit confidence warning.
- Rationale: The spatial story is still useful for buses, but the evidence is weaker than subway station hotspots.
- Consequence: Bus hotspots are directional and exploratory, not final geospatial truth.

DD-018: Security posture by deployment mode (course-scope)
- Decision: Do not ship a full in-app authentication system for the course project; instead document and enforce operating modes (`local development`, `shared/demo`, `public deployment`) with explicit handling guidance.
- Rationale: The dashboard is primarily academic and local-first, and full IAM would add complexity beyond MVP scope.
- Consequence: Security responsibility shifts to deployment context controls; local mode remains open by design, shared/demo mode must use restricted access boundaries, and public mode is explicitly unsupported without adding an external auth gateway.

## Deferred to Step 4+
- Production scheduling for Silver/Gold transform jobs and dependency chaining.
- Hotspot release decision after confidence-gate thresholds are met and documented.
- Postgres/PostGIS serving tier if multi-user API requirements emerge.
- Trip-level GTFS-RT validation once selector coverage improves.
- Full in-app authentication and role-based access controls for public multi-user deployments.
