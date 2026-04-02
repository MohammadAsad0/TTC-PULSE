# Step 1 Summary (Ingestion/Foundation)

- Run ID: `20260402T210142Z`
- Ingested at (UTC): `2026-04-02T21:01:42Z`
- DuckDB path: `D:/fSemester_2_Winter_(2026)/6414-Data Analytics and Visualization/test/TTC-PULSE/data/ttc_pulse.duckdb`

## Row Counts

- `bronze_bus`: **776435**
- `bronze_gtfs_calendar`: **8**
- `bronze_gtfs_calendar_dates`: **7**
- `bronze_gtfs_routes`: **229**
- `bronze_gtfs_shapes`: **1025672**
- `bronze_gtfs_stop_times`: **4249149**
- `bronze_gtfs_stops`: **9417**
- `bronze_gtfs_trips`: **133665**
- `bronze_gtfsrt_alerts`: **2227**
- `bronze_gtfsrt_entities`: **5984**
- `bronze_streetcar`: **170840**
- `bronze_subway`: **250558**
- `raw_bus_file_registry`: **1900**
- `raw_gtfs_file_registry`: **133**
- `raw_gtfsrt_snapshot_registry`: **15295**
- `raw_streetcar_file_registry`: **2128**
- `raw_subway_file_registry`: **1159**

## Assumptions / Notes

- GTFS-RT bronze tables were populated from alerts/parsed/service_alert_entities.csv.
