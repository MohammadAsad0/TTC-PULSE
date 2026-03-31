# CHANGELOG

## 2026-03-18 - Dashboard Metric Selector and Subway Drill-Down Fix

### Added
- `changelog/agent_run_logs/dashboard_metric_selector_update.md`
- `changelog/agent_run_logs/subway_drilldown_bugfix.md`

### Updated
- `dashboard/panel_descriptions.md`
- `dashboard/ux_decisions.md`
- `qa_and_review/known_caveats.md`
- `decisions/design_decisions.md`
- `changelog/CHANGELOG.md`

### Summary
- Documented the subway drill-down SQL failure caused by querying `gold_station_time_metrics` with a nonexistent `mode` column.
- Locked the shared `Metric to analyze` contract for the major analytical pages with five options: `Composite Score`, `Frequency`, `Severity`, `Regularity`, and `Cause Mix`.
- Preserved composite-score behavior exactly when `Composite Score` is selected.
- Added fallback guidance for sparse or unstable fine-grain slices so pages can switch to simpler metrics without crashing.
- Captured the interpretation caveat that `Cause Mix` is comparative and weakens at very fine temporal slices.

## 2026-03-18 - Wave 3 Docs Hardening: Bus Gate and QA Visibility

### Added
- `qa_and_review/data_quality_gate.md`
- `changelog/agent_run_logs/post_audit_hardening_run.md`

### Updated
- `dashboard/panel_descriptions.md`
- `dashboard/ux_decisions.md`
- `decisions/confidence_gating.md`
- `changelog/CHANGELOG.md`

### Hardening Summary
- Locked the strict bus stakeholder gate: stakeholder-facing bus route views require GTFS-backed route keys.
- Documented sentinel handling for bus severity and regularity calculations: `min_delay >= 999` and `min_gap >= 999` are invalid and excluded from sev/reg rollups.
- Kept unmatched mappings visible only in QA/review surfaces and out of stakeholder ranking panels.
- Captured the new subway drill-down intent and temporal labeling requirement for station-focused detail views.

## 2026-03-18 - Bus Reliability Drill-Down Docs and Navigation Update

### Updated
- `dashboard/panel_descriptions.md`
- `dashboard/ux_decisions.md`
- `app/streamlit_app.py`

### Summary
- Added `Bus Reliability Drill-Down` to the dashboard page inventory and locked its route-to-detail drill flow.
- Documented the fallback rule for missing Gold marts as parquet-backed reads.
- Updated the app shell page order to include the new drill-down page.

## 2026-03-18 - Spatial Hotspot Map Enablement Fix

### Updated
- `src/ttc_pulse/marts/build_gold_station_metrics.py`
- `decisions/confidence_gating.md`
- `changelog/agent_run_logs/hotspot_fix_run.md`

### Fix summary
- Resolved `gold_spatial_hotspot` publication blockage by tuning gate thresholds for observed full-history subway linkage behavior:
  - `station_linkage_coverage >= 0.80` (unchanged)
  - `ambiguous_share < 0.15` (previously `< 0.10`)
  - `high_confidence_rows >= 1000` (new)
- Fixed station centroid mapping logic to use normalized GTFS subway stop keys (`serves_subway = TRUE`) rather than raw stop-name equality.
- Rebuilt station and spatial marts so hotspot rows include valid centroid coordinates.

## 2026-03-18 - Step 4 Final Documentation Alignment

### Added
- `architecture.md` final architecture brief aligned to executable runtime boundaries.
- `data_dictionary.md` with Raw/Bronze/Silver/Gold key fields and lineage keys.
- `runbook.md` with deployment and run instructions for Step 1 -> Step 3 flows, side-car operations, and dashboard launch.
- `changelog/agent_run_logs/step4_run.md` with Step 4 execution trace and validation notes.
- `reports/final_project_summary.md` with final runtime outcomes, caveats, and delivery status.

### Updated
- `architecture/overview.md`
- `architecture/data_flow.md`
- `pipelines/agent_pipeline.md`
- `dashboard/panel_descriptions.md`
- `dashboard/ux_decisions.md`
- `changelog/CHANGELOG.md`

### Step 4 Documentation Outcomes
- Locked final documentation language to Streamlit + DuckDB/Parquet runtime and explicit Spark exclusion.
- Aligned architecture and pipeline docs to implemented module entry points, logs, and artifact outputs.
- Finalized dashboard panel and UX contracts with explicit empty/deferred behavior rules.
- Captured critical caveats explicitly:
  - `gold_alert_validation` can be empty when normalized alert fact rows are absent.
  - `gold_spatial_hotspot` remains deferred when confidence gate fails.
- Added final handoff docs for architecture brief, data dictionary, runbook, and project summary.

## 2026-03-18 - Step 3 Gold and Confidence Documentation Alignment

### Added
- `changelog/agent_run_logs/step3_run.md` with Step 3 execution trace, caveats, and Step 4 handoff.
- `step3_summary.md` with Gold stakeholder intent, reliability policy, hotspot deferral, and next-step boundaries.

### Updated
- `layers/gold_layer.md`
- `decisions/design_decisions.md`
- `decisions/confidence_gating.md`
- `pipelines/airflow_dag.md`
- `qa_and_review/linkage_quality.md`

### Step 3 Documentation Outcomes
- Defined Gold mart-level stakeholder intent across reliability, linkage quality, alert validation, and deferred spatial hotspot outputs.
- Locked reliability framework with composite scoring formula and explicit component visibility policy.
- Formalized confidence-gating interpretation for route vs station outputs and explicit hotspot deferral conditions.
- Clarified Airflow as a single 30-minute GTFS-RT Service Alerts side-car with bounded scope.
- Added stakeholder-focused linkage quality interpretation guidance.
- Documented Step 3 caveats and explicit Step 4 implementation handoff.

## 2026-03-18 - Step 2 Silver Documentation Alignment

### Added
- `changelog/agent_run_logs/step2_run.md` with Step 2 execution trace, DDL validation notes, and Step 3+ boundary.
- `step2_summary.md` with canonical Silver inventory, linkage semantics, and remaining implementation scope.

### Updated
- `layers/silver_layer.md`
- `architecture/layer_contracts.md`
- `decisions/alias_strategy.md`
- `qa_and_review/review_tables.md`
- `qa_and_review/known_caveats.md`

### Step 2 Documentation Outcomes
- Documented Silver canonical implementation at schema level:
  - 6 dimensions
  - 1 route-direction-stop bridge
  - 3 review tables
  - 3 mode-normalized Silver staging tables
  - 2 canonical fact tables
- Standardized and documented linkage semantics:
  - `match_method`
  - `match_confidence`
  - `link_status`
- Captured route-first bus and station-first subway normalization strategy.
- Added explicit unresolved mapping policy and review queue governance.
- Added explicit statement of Step 3+ remaining work (population jobs, review tooling, Gold materialization, confidence-gated outputs).

## 2026-03-18 - Step 1 Documentation Baseline

### Added
- `source_inventory.md` with canonical source mapping, scope notice, constraints, and assumptions.
- `step1_summary.md` with locked outcomes, caveats, and implementation priorities.
- `changelog/agent_run_logs/step1_run.md` with detailed run trace and validation notes.

### Expanded and refreshed
- Architecture docs:
  - `architecture/overview.md`
  - `architecture/layer_contracts.md`
  - `architecture/data_flow.md`
- Layer docs:
  - `layers/raw_layer.md`
  - `layers/bronze_layer.md`
  - `layers/silver_layer.md`
  - `layers/gold_layer.md`
- Decision docs:
  - `decisions/design_decisions.md`
  - `decisions/alias_strategy.md`
  - `decisions/confidence_gating.md`
- Pipeline docs:
  - `pipelines/airflow_dag.md`
  - `pipelines/agent_pipeline.md`
- QA and review docs:
  - `qa_and_review/review_tables.md`
  - `qa_and_review/linkage_quality.md`
  - `qa_and_review/known_caveats.md`
- Dashboard docs:
  - `dashboard/panel_descriptions.md`
  - `dashboard/ux_decisions.md`

### Locked decisions documented
- Scope: bus + subway + static GTFS + GTFS-RT Service Alerts only.
- Raw immutable, Bronze row-preserving plus lineage.
- DuckDB plus Parquet backend and Streamlit dashboard target.
- Spark excluded from MVP.
- Silver and Gold documented as design contracts only in Step 1.
