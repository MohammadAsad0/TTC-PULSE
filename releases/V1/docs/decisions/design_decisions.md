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

DD-003: Scheduler scope (Airflow side-car)
- Decision: Keep exactly one Airflow DAG for GTFS-RT Service Alerts polling on a 30-minute cadence.
- Rationale: Alerts ingestion is the only cadence-sensitive workload in current scope.
- Consequence: Silver/Gold historical materialization remains outside Airflow in Step 3.

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

## Deferred to Step 4+
- Production scheduling for Silver/Gold transform jobs and dependency chaining.
- Hotspot release decision after confidence-gate thresholds are met and documented.
- Postgres/PostGIS serving tier if multi-user API requirements emerge.
- Trip-level GTFS-RT validation once selector coverage improves.
