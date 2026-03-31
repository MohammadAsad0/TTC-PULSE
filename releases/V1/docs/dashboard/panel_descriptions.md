# Dashboard Panel Descriptions (Step 4 Runtime Contract)

## Runtime Lock
- Dashboard runtime: Streamlit.
- Storage and query runtime: DuckDB + Parquet.
- Spark remains excluded from TTC Pulse MVP.

## Current UI Build Status
- `app/streamlit_app.py` provides the shell.
- `app/pages/01_Linkage_QA.py` exists as a placeholder page.
- Remaining panels below are locked implementation contracts against existing Gold marts.

## Panel Contracts
| Panel | Goal | Primary mart(s) | Step 4 status |
|---|---|---|---|
| Linkage QA | Expose mapping trust before reliability rankings are consumed. | `gold_linkage_quality` | Contract locked; page scaffold exists. |
| Reliability Overview | Summarize mode-level reliability trend and intensity. | `gold_delay_events_core`, `gold_time_reliability` | Contract locked; mart data available. |
| Bus Route Ranking | Rank route-level risk with explainable components. | `gold_route_time_metrics`, `gold_top_offender_ranking` | Contract locked; mart data available. |
| Bus Reliability Drill-Down | Follow the route ranking into incident-level and time-window detail for the selected bus route. | `gold_route_time_metrics`, `gold_delay_events_core`, `gold_top_offender_ranking` | Contract locked; route drill flow page. |
| Subway Station Ranking | Rank station-level reliability risk with confidence-aware context. | `gold_station_time_metrics`, `gold_top_offender_ranking` | Contract locked; mart data available. |
| Weekday x Hour Heatmap | Show temporal concentration of reliability pressure. | `gold_time_reliability` | Contract locked; mart data available. |
| Monthly Trends | Track longitudinal changes by route/station over month bins. | `gold_route_time_metrics`, `gold_station_time_metrics` | Contract locked; mart data available. |
| Cause/Category Mix | Show disruption composition by incident category. | `gold_delay_events_core` | Contract locked; mart data available. |
| Live Alert Validation | Validate GTFS-RT selectors against static GTFS IDs. | `gold_alert_validation` | Contract locked; mart may be empty. |
| Spatial Hotspot | Map localized subway hotspots for high-confidence records only. | `gold_spatial_hotspot` | Confidence-gated; deferred when gate fails. |

## Mandatory Caveat Messaging
- `Live Alert Validation` can be empty when no rows exist in `fact_gtfsrt_alerts_norm` / `silver_gtfsrt_alert_entities`.
- `Spatial Hotspot` must stay deferred when confidence gate fails; current behavior emits a schema-only zero-row `gold_spatial_hotspot`.
- `Bus Reliability Drill-Down` should open from `Bus Route Ranking` and keep the selected route in context while drilling into supporting incident detail.
- When a Gold mart is missing, the app shell and pages fall back to parquet-backed reads where available.
- Any panel that displays `composite_score` must display `frequency`, `severity_p90`, `regularity_p90`, and `cause_mix_score` in the same view.
