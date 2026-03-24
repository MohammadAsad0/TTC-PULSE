# Source Inventory (Step 1 Ingestion/Foundation)

- Run ID: `20260324T025223Z`
- Ingested at (UTC): `2026-03-24T02:52:23Z`

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
- GTFS-RT snapshot files discovered: **46**

## Raw Registry Outputs

- Bus registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/bus/bus_file_registry.csv` (table `raw_bus_file_registry`)
- Subway registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/subway/subway_file_registry.csv` (table `raw_subway_file_registry`)
- GTFS registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/gtfs/gtfs_file_registry.csv` (table `raw_gtfs_file_registry`)
- GTFS-RT registry CSV: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/raw/gtfsrt/gtfsrt_snapshot_registry.csv` (table `raw_gtfsrt_snapshot_registry`)

## GTFS File Map

- `routes` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/csv/routes.csv`
- `trips` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/csv/trips.csv`
- `stop_times` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/csv/stop_times.csv`
- `stops` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/csv/stops.csv`
- `calendar` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/csv/calendar.csv`
- `calendar_dates` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/csv/calendar_dates.csv`
- `shapes` -> `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/datasets/01_gtfs_merged/csv/shapes.csv`

## GTFS-RT Snapshot Paths

- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/eda/gtfsrt_eda_outputs/raw/alerts_20260311T065928Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/eda/gtfsrt_eda_outputs/raw/alerts_20260311T073525Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260317T203007Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260317T213822Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T015030Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T042630Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T044357Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T044438Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T051358Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T054358Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T061359Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T064400Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T071400Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T074401Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T081401Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T084402Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T091403Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T094403Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T101404Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T104405Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T111405Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T114406Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T121406Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T124407Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T131408Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T134408Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T141409Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T144409Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T151410Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T154410Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T161411Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T164412Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T171412Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T174413Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T181413Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T184414Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T191415Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T224822Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T231823Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260323T234824Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260324T001825Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260324T004825Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260324T011826Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260324T014826Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260324T021827Z.pb`
- `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/alerts/raw_snapshots/alerts_20260324T024828Z.pb`
