# Project Structure

## Top-Level Folders

- `app/`: Streamlit dashboard entrypoint
- `data/bus/`: raw TTC bus CSV files
- `data/subway/`: raw TTC subway CSV files, including the subway delay code lookup file
- `data/gtfs/`: static GTFS reference CSV files
- `src/ttc_pulse/`: small ingestion, cleaning, enrichment, and summary modules
- `docs/`: lightweight project documentation
- `logs/`: reserved for run logs and validation notes
- `outputs/`: reserved for generated outputs or exports

## Source Modules

- `src/ttc_pulse/paths.py`: repository path helpers
- `src/ttc_pulse/io.py`: CSV discovery and loading helpers
- `src/ttc_pulse/gtfs.py`: GTFS lookups, subway code lookup parsing, and normalization helpers
- `src/ttc_pulse/cleaning.py`: bus and subway cleaning rules
- `src/ttc_pulse/service.py`: top-level dataset loading and overview summaries
- `src/ttc_pulse/verification.py`: CLI verification command
