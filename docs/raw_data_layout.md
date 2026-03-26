# Raw Data Layout

The project reads raw CSV files directly from this repository. No external datasets workspace is required.

## Required Folders

- `data/bus/`: raw bus delay CSV files
- `data/subway/`: raw subway delay CSV files plus the subway delay code lookup CSV
- `data/gtfs/`: static GTFS CSV files such as `routes.csv`, `stops.csv`, and `trips.csv`

## Loading Notes

- All bus CSV files in `data/bus/` are ingested.
- All subway event CSV files in `data/subway/` are ingested.
- The subway delay code file is read separately as a lookup, not as event data.
- GTFS enrichments are added only when the expected GTFS CSV files are present.
