# Dashboard Panel Descriptions (V2 Realignment)

## Runtime Lock
- Dashboard runtime: Streamlit.
- Query/storage runtime: DuckDB + Parquet fallback.
- Spark excluded from MVP runtime.

## Visible Panel Order
| Sidebar order | Panel | Primary purpose | Main source(s) |
|---|---|---|---|
| 1 | Reliability Overview | Monthwise reliability trend with mode + year-range controls. | `gold_delay_events_core` |
| 2 | Bus Route Ranking | Date-range bus route ranking with metric selector and scroll-safe Top N view. | `gold_route_time_metrics` |
| 3 | Subway Station Ranking | Date-range subway station ranking with metric selector and scroll-safe Top N view. | `gold_station_time_metrics` |
| 4 | Weekday Hour Heatmap | Raw temporal view with only frequency/min-delay-p90/min-gap-p90. | `gold_delay_events_core` |
| 5 | Monthly Trends | Multi-entity monthly trend analysis. | `gold_route_time_metrics`, `gold_station_time_metrics` |
| 6 | Cause Category Mix | Category composition with top-N mix + weekday/hour spread views. | `gold_delay_events_core` |
| 7 | Live Alert Validation | Ops board for selector quality and snapshot capture behavior. | `gold_alert_validation` |
| 8 | Spatial Hotspot Map | Interactive hotspot map with subway + provisional bus mode. | `gold_spatial_hotspot`, `gold_route_time_metrics`, `bridge_route_direction_stop` |
| 9 | Bus Reliability Drill-Down | Guided route-first drill story. | `gold_route_time_metrics`, `gold_delay_events_core` |
| 10 | Subway Reliability Drill-Down | Guided station-first drill story. | `gold_station_time_metrics`, `gold_top_offender_ranking` |

## Explicit V2 Changes
- Linkage QA page was removed from active sidebar navigation and archived at `app/pages_archive/01_Linkage_QA.py`.
- Home shell proposal-misaligned runtime copy was removed.
- Ranking pages now use calendar date ranges and compute rankings on demand over selected windows.
- Weekday heatmap no longer exposes composite/cause-mix metric toggles.
- Live alerts page now follows an operations-board layout.
- Spatial page now supports `bus` mode as provisional route-centroid mapping and labels confidence caveat inline.

## Metric Selector Contract
- Ranking and drill pages still expose:
  - `Composite Score`
  - `Frequency`
  - `Severity`
  - `Regularity`
  - `Cause Mix`
- Composite behavior remains unchanged when selected.
- At fine granularity where composite is unstable, drill pages fall back to interpretable raw metrics and show inline explanation.

## Heatmap Metric Contract
- Weekday heatmap metric selector is intentionally restricted to:
  - `Frequency`
  - `Min Delay P90`
  - `Min Gap P90`
- The heatmap is raw-stat oriented, not z-score/composite oriented.

## Spatial Confidence Note
- `subway` hotspots come from confidence-gated `gold_spatial_hotspot`.
- `bus` hotspots are provisional route-centroid points computed from GTFS bridge geometry and route metrics.
- Bus map is for pattern directionality, not precise incident localization.
