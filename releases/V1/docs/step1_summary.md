# Step 1 Summary (Ingestion/Foundation)

- Run ID: `20260317T194704Z`
- Ingested at (UTC): `2026-03-17T19:47:04Z`
- DuckDB path: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/data/ttc_pulse.duckdb`

## Row Counts

- `bronze_bus`: **776435**
- `bronze_gtfs_calendar`: **8**
- `bronze_gtfs_calendar_dates`: **7**
- `bronze_gtfs_routes`: **229**
- `bronze_gtfs_shapes`: **1025672**
- `bronze_gtfs_stop_times`: **4249149**
- `bronze_gtfs_stops`: **9417**
- `bronze_gtfs_trips`: **133665**
- `bronze_gtfsrt_alerts`: **0**
- `bronze_gtfsrt_entities`: **0**
- `bronze_subway`: **250558**
- `raw_bus_file_registry`: **100**
- `raw_gtfs_file_registry`: **7**
- `raw_gtfsrt_snapshot_registry`: **2**
- `raw_subway_file_registry`: **61**

## Assumptions / Notes

- GTFS-RT snapshots were discovered but protobuf parsing is not implemented in Step 1; shell bronze_gtfsrt tables remain empty.
