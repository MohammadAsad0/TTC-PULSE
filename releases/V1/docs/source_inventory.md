# Source Inventory (Step 1 Ingestion/Foundation)

- Run ID: `20260317T194704Z`
- Ingested at (UTC): `2026-03-17T19:47:04Z`

## Source Roots Used

- Bus source root: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/02_bus_delay/csv`
- Subway source root: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/03_subway_delay/csv`
- GTFS source root: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged`
- GTFS-RT candidate roots:
  - `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots`
  - `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/eda/gtfsrt_eda_outputs/raw`

## Discovery Metrics

- Bus files discovered: **100**
- Subway files discovered: **61**
- GTFS files discovered: **7**
- GTFS-RT snapshot files discovered: **2**

## Raw Registry Outputs

- Bus registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/bus/bus_file_registry.csv` (table `raw_bus_file_registry`)
- Subway registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/subway/subway_file_registry.csv` (table `raw_subway_file_registry`)
- GTFS registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/gtfs/gtfs_file_registry.csv` (table `raw_gtfs_file_registry`)
- GTFS-RT registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/gtfsrt/gtfsrt_snapshot_registry.csv` (table `raw_gtfsrt_snapshot_registry`)

## GTFS File Map

- `routes` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/routes.txt`
- `trips` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/trips.txt`
- `stop_times` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/stop_times.txt`
- `stops` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/stops.txt`
- `calendar` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/calendar.txt`
- `calendar_dates` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/calendar_dates.txt`
- `shapes` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/shapes.txt`

## GTFS-RT Snapshot Paths

- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/eda/gtfsrt_eda_outputs/raw/alerts_20260311T065928Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/eda/gtfsrt_eda_outputs/raw/alerts_20260311T073525Z.pb`
