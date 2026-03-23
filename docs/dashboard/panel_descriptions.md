# Dashboard Panel Descriptions (V4 Storytelling Redesign)

## Runtime Lock
- Dashboard runtime: Streamlit (`app/streamlit_app.py` + `app/pages/*`).
- Query/storage runtime: DuckDB + Parquet fallback.
- Spark excluded from MVP runtime.

## Story-First Sidebar Order
| Sidebar order | Panel | Why it exists | Primary source(s) |
|---|---|---|---|
| 1 | Story Overview | Frame the TTC Pulse argument in <2 minutes. | `gold_delay_events_core`, `gold_route_time_metrics`, `gold_station_time_metrics` |
| 2 | Recurring Hotspots | Show where recurring risk concentrates (routes/stations + spatial context). | `gold_route_time_metrics`, `gold_station_time_metrics`, `gold_spatial_hotspot`, `bridge_route_direction_stop` |
| 3 | Time Patterns | Show when disruptions recur (weekday-hour + monthly support trend). | `gold_delay_events_core`, `gold_route_time_metrics`, `gold_station_time_metrics` |
| 4 | Cause Signatures | Show why hotspots fail (dominant incident categories over time). | `gold_delay_events_core` |
| 5 | Drill-Down Explorer | Route-first/station-first deep dive for one selected hotspot. | `gold_route_time_metrics`, `gold_station_time_metrics` |
| 6 | Live Alert Alignment | Validate historical hotspot story against live GTFS-RT alert outcomes. | `gold_alert_validation`, `gold_linkage_quality` |
| 7 | QA / Methodology | Expose trust diagnostics without cluttering core story pages. | `gold_linkage_quality`, Gold table snapshots |

## Archived Pages (V3 -> V4)
- V3 operational pages were moved to `app/pages_archive/v3_pre_v4/`.
- Purpose: preserve rollback while enforcing the reduced v4 narrative structure.

## Presentation vs Exploration Contract
- Every page supports:
  - `Presentation` mode: concise narrative, fewer controls, minimal tables.
  - `Exploration` mode: deeper controls, expanded supporting tables/charts.
- Story flow in presentation mode is optimized for a <10 minute walkthrough.

## Analytical Integrity Contract
- Bus logic remains route-first (`route_id_gtfs`).
- Subway logic remains station-first (`station_canonical`).
- Composite views include component context:
  - frequency
  - severity_p90
  - regularity_p90
  - cause_mix_score
- Date-window controls are explicit and page-global for each narrative panel.

## Caveat Visibility Rules
- Bus spatial context remains provisional route-centroid mapping and is labeled directionally.
- Live alert outputs are validation context, not historical score inputs.
- Empty-window or empty-source states must show clear no-data messaging and stop downstream rendering.
