# UX Decisions (Step 4 Runtime Alignment)

## Product Priorities
- Keep trust-first flow: linkage quality before ranked recommendations.
- Keep filter model small and shared: mode, date window, entity type, confidence context.
- Keep explainability mandatory for all ranking outputs.

## Explainability and Transparency Rules
- `composite_score` is never shown alone; component metrics must be co-visible.
- Ranking views must show linkage context (`match_method` / `link_status` distribution or equivalent QA signal).
- Caveats that materially affect interpretation must be shown inline, not hidden in external docs.

## Empty and Deferred State Rules
- Live Alert Validation must render an explicit "data unavailable for current window" state when normalized alert facts are zero.
- Live Alert Validation empty output must not be framed as "no alerts occurred."
- Spatial Hotspot must render only when confidence gate passes.
- If hotspot gate fails, show deferred-state message and suppress map/ranking exposure.

## Information Architecture
- Entry view: Linkage QA + high-level reliability summary.
- Decision view: route rankings, Bus Reliability Drill-Down, and station rankings with component drilldown.
- Operations view: live alert validation and selector-quality status.

## Runtime Constraints
- UI runtime remains Streamlit for MVP.
- Data source remains DuckDB/Parquet marts only.
- Spark-backed UI paths are intentionally excluded.

## Step 4 Boundary
UX contracts are now final for MVP handoff; current Streamlit code is scaffold-level and should be implemented to this behavior.
