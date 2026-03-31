# Post-Audit Fix Verification

## Scope
This QA pass verifies the V1 hardening changes for TTC Pulse:
`714` leakage removal from stakeholder bus views, sentinel handling for bus severity/regularity, dashboard drill-down parity, and runtime smoke stability.

## Executed Checks
| Check | Result | Evidence |
|---|---|---|
| `714` absent from stakeholder bus ranking | PASS | `gold_top_offender_ranking.parquet` route rows with `entity_id='714'` = `0` |
| `714` absent from stakeholder bus drill path | PASS | Bus drill page seeds selection from ranking output and resets when selected route is not in data |
| GTFS-backed bus routes only | PASS | `gold_route_time_metrics.parquet` has `0` bus route IDs missing from `dim_route_gtfs` bus dictionary |
| Sentinel values excluded from severity/regularity | PASS | Bus `>=999` sentinels are nulled before Gold aggregation; Gold outputs contain `0` bus rows with `severity_p90>=999` or `regularity_p90>=999` |
| App shell and pages compile | PASS | `python3 -m py_compile` succeeded for shell, bus drill, subway drill, overview, live alerts, and QA modules |
| Subway drill-down parity and state hooks | PASS | Station-first drill page includes state keys, breadcrumb, back/reset, and selection cascade |
| Historical vs snapshot labeling | PASS | Dashboard copy distinguishes historical coverage from alert snapshot timestamps |

## Evidence Snippets
- Gold source sanitation applies the bus sentinel gate in [`src/ttc_pulse/marts/_gold_utils.py`](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/src/ttc_pulse/marts/_gold_utils.py#L213).
- Route mart filtering requires GTFS-backed bus membership in [`src/ttc_pulse/marts/build_gold_route_metrics.py`](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/src/ttc_pulse/marts/build_gold_route_metrics.py#L31).
- Bus drill-down resets selection when the chosen route is absent from the ranked set in [`app/pages/10_Bus_Reliability_Drill_Down.py`](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/10_Bus_Reliability_Drill_Down.py#L711).
- Subway drill-down mirrors the same interaction contract in [`app/pages/11_Subway_Reliability_Drill_Down.py`](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/11_Subway_Reliability_Drill_Down.py#L736).

## Key Data Results
- `gold_top_offender_ranking`: `0` bus route rows for `714`.
- `gold_route_time_metrics`: `0` GTFS-mismatched bus route IDs.
- `gold_delay_events_core`: `0` bus rows with `min_delay_p90>=999` or `min_gap_p90>=999`.
- `gold_route_time_metrics`: `628,823` bus rows spanning `202` GTFS-backed bus routes.
- `gold_delay_events_core`: `1` bus analytics-core row still contains `route_id_gtfs='714'`, but it is not reachable from stakeholder route ranking because the drill-down is seeded from the GTFS-backed ranked set.

## Residual Risks
- Interactive click-through smoke testing was not executed in a live Streamlit session during this QA pass.
- The analytics core still contains one historical `714` row; this is acceptable for audit lineage, but it means direct manual state injection could still surface it if a user bypasses the UI contract.
- Subway drill-down parity is implemented statically; its runtime behavior should still be confirmed in a browser session.

## Final Verdict
PASS. The stakeholder-facing bus ranking and drill-down paths are now GTFS-gated, bus sentinel values are excluded from severity/regularity rollups, and the dashboard shell plus drill pages compile cleanly.
