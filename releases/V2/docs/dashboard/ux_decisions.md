# UX Decisions (Step 4 Runtime Alignment)

## Product Priorities
- Keep trust-first flow: linkage quality before ranked recommendations.
- Keep filter model small and shared: mode, date window, entity type, confidence context.
- Keep explainability mandatory for all ranking outputs.
- Keep bus stakeholder outputs GTFS-backed only; unresolved bus mappings stay out of stakeholder views.
- Keep analytical pages metric-selectable rather than composite-only.

## Explainability and Transparency Rules
- `composite_score` is never shown alone; component metrics must be co-visible.
- Ranking views must show linkage context (`match_method` / `link_status` distribution or equivalent QA signal).
- Caveats that materially affect interpretation must be shown inline, not hidden in external docs.
- Unmatched records remain visible in QA/review surfaces only; they are not surfaced in stakeholder ranking panels.
- `Composite Score` must preserve the current ranking/chart logic exactly when selected.
- `Frequency`, `Severity`, `Regularity`, and `Cause Mix` switch the primary charting metric but keep the same supporting context visible.

## Empty and Deferred State Rules
- Live Alert Validation must render an explicit "data unavailable for current window" state when normalized alert facts are zero.
- Live Alert Validation empty output must not be framed as "no alerts occurred."
- Spatial Hotspot must render only when confidence gate passes.
- If hotspot gate fails, show deferred-state message and suppress map/ranking exposure.
- Subway Station Ranking and drill-down views should preserve temporal labels so station detail remains aligned to the selected time window.
- Subway drill-down pages must not depend on a `mode` column in `gold_station_time_metrics`; station queries should use canonical station fields only.
- If a selected metric is sparse or unstable at a drill level, the UI should explain the fallback and switch to a simpler measure rather than failing.

## Information Architecture
- Entry view: Linkage QA + high-level reliability summary.
- Decision view: route rankings, Bus Reliability Drill-Down, station rankings, and temporal drilldown labels.
- Operations view: live alert validation and selector-quality status.

## Runtime Constraints
- UI runtime remains Streamlit for MVP.
- Data source remains DuckDB/Parquet marts only.
- Spark-backed UI paths are intentionally excluded.

## Metric Interpretation Note
- `Severity` maps to delay intensity, not event count.
- `Regularity` maps to gap or headway irregularity, not delay magnitude.
- `Cause Mix` is best used as a comparative ranking aid, especially at coarse slices.

## Step 4 Boundary
UX contracts are now final for MVP handoff; current Streamlit code is scaffold-level and should be implemented to this behavior.
