# Recovered Resume Point

Recovered on: 2026-03-19

## Current repo state

Repo:
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse`

Branch:
- `om`

Important rollback marker:
- git tag `v2`
- snapshot folder `releases/V2/`

Recovered thread archive root:
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/_codex_recovered_threads`

## Highest-confidence current state

The strongest saved state is:
- V2 UI and analytics realignment completed on 2026-03-18
- final docs and reports aligned to executable runtime
- Gold marts materialized
- dashboard pages present
- GTFS-RT side-car exists but alert fact population remains incomplete in normal operation

Primary state references:
- `reports/final_project_summary.md`
- `docs/changelog/agent_run_logs/step_review_current.md`
- `docs/proposal_alignment_notes.md`
- `reports/v2_freeze_report.md`
- `docs/changelog/agent_run_logs/v2_ui_realignment_run.md`

## Last clear unresolved items

From the audit and final summary:
- `fact_gtfsrt_alerts_norm = 0`
- `gold_alert_validation = 0`
- `gold_spatial_hotspot` is confidence-gated / caveated
- local dashboard runtime may still be blocked if `streamlit` and related deps are missing in the active venv
- GTFS-RT protobuf decode availability is environment-dependent

## Recommended next coding sequence

1. Verify the active Python environment for `streamlit`, `altair`, and GTFS-RT protobuf bindings.
2. Run the alert parse + normalization path end to end and identify why alert facts remain empty.
3. Rebuild `fact_gtfsrt_alerts_norm` and `gold_alert_validation`.
4. Smoke-test the Streamlit app after dependency and alert-path fixes.
5. Only after alerts are live, decide whether spatial hotspot publication should remain caveated or be tightened again.

## Practical restart brief

If resuming coding from here, the most useful prompt is:

`Resume TTC Pulse from the recovered V2 state. Keep the Streamlit + DuckDB/Parquet runtime, preserve the proposal-aligned dashboard, and focus on clearing the remaining alert-pipeline and app-runnability blockers without expanding scope beyond bus + subway + static GTFS + GTFS-RT Service Alerts.`

## Related recovery docs

- `docs/recovery/MASTER_PROMPT_RECOVERED.md`
- `docs/recovery/SUBTHREAD_RECOVERY_SUMMARY.md`
