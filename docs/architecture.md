# TTC Pulse Final Architecture Brief

## Stack Lock
- Dashboard runtime: Streamlit.
- Data platform: DuckDB + Parquet.
- Scheduler scope: GTFS-RT Service Alerts side-car at 30-minute cadence (`launchd` on macOS, Windows Task Scheduler on Windows).
- Spark: excluded from MVP.

## Scope Lock
- Included: bus delay history, subway delay history, static GTFS merged feed, GTFS-RT Service Alerts.
- Excluded: GTFS-RT vehicle positions, GTFS-RT trip updates, streetcar core modeling, distributed Spark stack.

## Layered Model
| Layer | Primary output |
|---|---|
| Raw | File registries and immutable source tracking |
| Bronze | Row-preserving extracted tables with lineage |
| Silver | Canonical dimensions, bridge, review tables, normalized events, canonical facts |
| Gold | Reliability marts, linkage QA mart, alert validation mart, confidence-gated hotspot mart |
| Dashboard | Streamlit pages consuming Gold contracts |

## Core Reliability Contract
- Component metrics: `frequency`, `severity_p90`, `regularity_p90`, `cause_mix_score`.
- Composite formula: weighted standardized score of components.
- Rule: no `composite_score` without component metrics in the same view/export.

## Critical Runtime Caveats
- Alert validation mart may be empty when normalized alert fact rows are zero.
- Spatial hotspot mart must remain deferred when confidence gate fails; builder emits schema-only zero-row output.
- Alerts parser may emit fallback metadata rows when protobuf decoding dependency is missing.

## Operational Artifacts
- Step 1 ingestion log: `logs/ingestion_log.csv`
- Step 2 registration log: `logs/step2_registration_log.csv`
- Step 3 Gold build log: `logs/step3_gold_build_log.csv`
- Final metrics summary: `outputs/final_metrics_summary.md`

## Implementation Boundary
Data contracts and marts are materialized; Streamlit experience is scaffold-level and should be completed against the locked contracts in `docs/dashboard/`.
