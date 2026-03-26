# How TTC Pulse Reads the Dataset

## Short Answer
The project reads the TTC datasets in two stages:

1. It discovers matching source CSV files from the external `datasets/` workspace using YAML configs in `configs/`.
2. It loads those CSV files into DuckDB bronze tables with DuckDB `read_csv_auto(...)`, then writes normalized row-level parquet files in `silver/` that the Streamlit app can query directly.

## Source Discovery
Bus and subway ingestion start from:

- `configs/schema_bus.yml`
- `configs/schema_subway.yml`

Those files define:

- `source_root`: where the raw files live relative to the workspace
- `include_patterns`: which filenames should be picked up
- `exclude_patterns`: which files should be ignored
- `raw_registry_table`: the DuckDB table used to register discovered files

The discovery logic lives in:

- `src/ttc_pulse/ingestion/ingest_bus.py`
- `src/ttc_pulse/ingestion/ingest_subway.py`
- `src/ttc_pulse/utils/project_setup.py`

`resolve_project_paths()` finds the repository root, then points `workspace_root` to the parent folder so the code can read source files from `../datasets/...`.

## Step 1: Register Files and Build Bronze
Step 1 runs through `src/ttc_pulse/bronze/build_bronze_tables.py`.

For bus and subway, the flow is:

1. Discover files with `discover_files(...)`.
2. Record file metadata in raw registry CSVs:
   - `raw/bus/bus_file_registry.csv`
   - `raw/subway/subway_file_registry.csv`
3. Insert the same registry metadata into DuckDB raw registry tables.
4. Build `bronze_bus` and `bronze_subway` by reading all discovered CSVs with DuckDB:

```sql
read_csv_auto(
    [...file paths...],
    all_varchar = TRUE,
    union_by_name = TRUE,
    filename = TRUE,
    ignore_errors = TRUE
)
```

Important behavior:

- `all_varchar = TRUE` keeps the bronze load schema-tolerant across yearly source drift.
- `union_by_name = TRUE` lets files with slightly different column layouts load together.
- `filename = TRUE` preserves source lineage.
- The bronze build adds `source_file`, `source_row_id`, `ingested_at`, and `row_hash`.

This means the project is not manually parsing each CSV column in Python. Python orchestrates discovery and metadata. DuckDB performs the bulk CSV read.

## Step 2: Normalize Into Silver Event Tables
The row-level datasets used by the dashboard are created in:

- `src/ttc_pulse/normalization/normalize_bus.py`
- `src/ttc_pulse/normalization/normalize_subway.py`

These jobs read from `bronze_bus` and `bronze_subway`, then:

- parse `Date` and `Time` into `service_date` and `event_ts`
- preserve the original text fields such as route, station, line, incident, and direction values
- add normalized lookup fields like `route_id_gtfs`, `route_short_name_norm`, `line_code_norm`, and `station_canonical`
- keep lineage columns such as `source_file`, `source_row_id`, `ingested_at`, and `row_hash`

Outputs:

- `silver/silver_bus_events.parquet`
- `silver/silver_subway_events.parquet`

These are the row-level datasets shown in the app because they still retain the original event text while also exposing parsed dates and audit fields.

## How Streamlit Reads the Data
The dashboard loader code is in `src/ttc_pulse/dashboard/loaders.py`.

There are two patterns in the app:

- Gold summary pages use `query_table(...)` to read DuckDB tables when available and fall back to `gold/*.parquet`.
- The new Dataset Explorer page reads the row-level silver parquet files directly:
  - `silver/silver_bus_events.parquet`
  - `silver/silver_subway_events.parquet`

This fallback matters because the Streamlit app can still run even when `data/ttc_pulse.duckdb` has not been rebuilt in the current workspace, as long as the parquet artifacts already exist.

## New Dataset Explorer Page
When Streamlit runs, the new page lets you:

- choose `bus` or `subway`
- choose a `service_date` range
- inspect the row-level dataset for that window
- download the current filtered view as CSV

The page intentionally shows the normalized row-level silver dataset rather than only the Gold aggregates, so you can inspect the actual event records behind the charts.
