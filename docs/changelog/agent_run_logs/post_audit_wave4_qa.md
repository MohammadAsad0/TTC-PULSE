# Post-Audit Wave 4 QA Run

## Scope
Validated the V1 hardening pass for stakeholder bus route gating, sentinel exclusion, drill-down parity, and runtime smoke stability.

## Checks Performed
- Verified that `714` is absent from stakeholder bus ranking outputs.
- Verified that bus route mart output only contains GTFS-backed route IDs.
- Verified that bus severity/regularity sentinels at `>=999` are excluded before Gold aggregation.
- Verified static app compile smoke for shell, bus drill-down, subway drill-down, overview, live alerts, and QA modules.
- Verified that both drill-down pages carry breadcrumb, back, reset, and state persistence logic.

## Outcome
PASS. No code changes were required during this QA pass.

## Notes
- The analytics core still contains one `714` row for lineage purposes, but it is not exposed through stakeholder bus ranking/drill paths.
- Interactive browser clicks were not exercised in this run.
