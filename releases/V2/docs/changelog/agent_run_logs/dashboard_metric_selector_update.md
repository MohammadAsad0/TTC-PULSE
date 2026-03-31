# Dashboard Metric Selector Update

Date: 2026-03-18

## Scope
Document the shared `Metric to analyze` contract for the major analytical pages and the associated fallback behavior.

## Change Summary
- Added a top-level metric selector with five options:
  - `Composite Score`
  - `Frequency`
  - `Severity`
  - `Regularity`
  - `Cause Mix`
- Preserved current behavior exactly when `Composite Score` is selected.
- Defined the selector as a presentation-layer switch over the primary ranking/chart metric, while keeping all component metrics visible in summary cards and tooltips.
- Documented the fallback rule for sparse or unstable slices: prefer incident count, average delay, or `p90` delay instead of failing.

## Notes
- `Cause Mix` should be treated as a comparative composition signal rather than a literal causal explanation.
- The selector is intended to make the dashboard useful beyond a single composite view without changing the stakeholder contract.
