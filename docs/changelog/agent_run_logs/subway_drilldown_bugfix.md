# Subway Reliability Drill-Down Bugfix

Date: 2026-03-18

## Scope
Document the fix for the Subway Reliability Drill Down failure.

## Bug
- The subway drill-down page queried `gold_station_time_metrics` with `WHERE mode = 'subway'`.
- `gold_station_time_metrics` does not contain a `mode` column, so the query failed at runtime.

## Fix Strategy
- Remove the invalid dependence on `mode` in subway drill-down queries.
- Keep the drill-down station-first by filtering on canonical station fields only.
- Preserve the existing interaction contract:
  - station ranking
  - year
  - month
  - day or weekday
  - time bins

## Why This Fix
- It aligns the page with the actual mart schema.
- It avoids schema drift in the Gold layer.
- It keeps subway behavior consistent with the station-first modeling strategy already documented for TTC Pulse.

## Caveat
- This fix is corrective, not structural; the mart schema remains unchanged.
- Temporal drill slices can still become sparse, so fallback behavior remains necessary for fine-grain views.
