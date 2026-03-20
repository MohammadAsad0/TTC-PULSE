# Recovered Master Prompt

Recovered on: 2026-03-19

Recovered thread archive root:
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/_codex_recovered_threads`

Primary recovered parent thread:
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/_codex_recovered_threads/sessions/2026-03-18_17-42-14_main_Your_task_is_to_understand_the_files_list_them_as_well_with_short_summary_of_wha_019cc970-19a4-74e0-a425-6a2b2b810d69.jsonl`

Companion architecture synthesis thread:
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/_codex_recovered_threads/sessions/2026-03-17_14-51-16_Cicero_You_are_Subagent_6_Orchestrator_for_TTC_Pulse._Produce_one_decision-ready_archit_019cfc46-f12b-7cf0-b204-6765be66d34e.jsonl`

## Clean reconstruction

Project: `TTC Pulse / TTC Reliability Observatory`

Objective:
Build an interactive visual analytics system for TTC service reliability using:
1. historical TTC bus delay logs
2. historical TTC subway delay logs
3. static GTFS as the bridge layer
4. GTFS-Realtime Service Alerts as the real-time validation layer

Core research story:
TTC reliability is not a single average. It is a spatiotemporal pattern of:
- where service breaks
- when it breaks
- why it breaks
- how bad or irregular it becomes

Methodology to preserve:
Historical Logs + Static GTFS + GTFS-RT Alerts -> Clean / Normalize -> Space-Time Aggregation -> Reliability Metrics -> Hotspots + Rankings -> Alert Validation -> Linked Dashboard

Reliability pillars:
1. Frequency = incident count per route/station/time bin
2. Severity = median and p90 of `Min Delay`
3. Regularity = p90 of `Min Gap` as proxy for headway irregularity / bunching
4. Modality / Cause Mix = distribution of incident codes within each hotspot/time window

Composite score:
`S(u,t) = w1*z(Freq) + w2*z(Sev90) + w3*z(Reg) + w4*Modality`

Preserve component metrics and keep rankings explainable.

Spatial strategy:
- Primary: H3 hex-binning for city-wide hotspot maps
- Fallback:
  - subway = station-level aggregation
  - bus = route/segment-level aggregation if geocoding is noisy

Temporal strategy:
- hour-of-day x day-of-week
- monthly / seasonal slicing

Real-time validation:
- use GTFS-Realtime Service Alerts only
- do not default to full vehicle-position or trip-update modeling

Out of scope:
- full vehicle-position analytics
- full trip-update analytics
- per-stop ridership exposure modeling
- causal traffic claims
- unnecessary ML
- distributed infrastructure unless clearly necessary

Preferred implementation direction:
- Python ETL
- modular scripts
- optional Airflow-style pipeline structure
- PostgreSQL/PostGIS-compatible outputs if possible
- reproducible notebooks for EDA
- practical build within course timeline

Immediate milestone from the recovered prompt:
- basic EDA
- notebook/Colab/GitHub files shared
- short summary of findings
- enough evidence to finalize architecture in the next meeting

Recovered ownership notes for Om:
- merged GTFS analysis
- optional live GTFS alerts feasibility
- bridge-layer thinking for historical <-> GTFS <-> live alerts alignment
- methodology / architecture support

## Recovered implementation direction

The recovered architecture synthesis converged on:
- GTFS-anchored normalization
- DuckDB + Parquet runtime
- Streamlit dashboard runtime
- route-level bus linking first
- station-level subway linking
- route-first GTFS-RT alert validation
- explicit quarantine of unresolved bus code semantics
- QA-first visibility for low-confidence linkage

## Current interpretation

This recovered prompt is consistent with the implemented repo state under:
- `docs/`
- `reports/`
- `releases/V2/`
- `app/`
- `src/ttc_pulse/`

It should be treated as the canonical recovered working brief unless a newer user prompt overrides it.
