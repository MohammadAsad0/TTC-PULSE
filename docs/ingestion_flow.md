# Ingestion Flow

## Flow Summary

1. Discover all CSV files in `data/bus/`, `data/subway/`, and `data/gtfs/`.
2. Read the raw bus and subway CSV files directly into pandas dataframes.
3. Add a `source_file` column to preserve file lineage.
4. Load GTFS routes and stops when they are available.
5. Clean bus records using route and date rules.
6. Clean subway records using line, station, and date rules.
7. Enrich subway incident codes using `ttc-subway-delay-codes__01_Sheet_1.csv`.
8. Enrich bus and subway rows with GTFS route IDs where a practical lookup exists.
9. Expose the cleaned frames in the Streamlit app and in the verification command.

## Notes

- The project does not rely on an external datasets workspace.
- The raw repository layout is the source of truth.
- GTFS is optional for startup, but GTFS enrichments are added when the reference files exist.
