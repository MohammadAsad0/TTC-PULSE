# TTC Pulse

TTC Pulse is a small Streamlit project that reads raw TTC bus, subway, and GTFS CSV files directly from this repository, cleans them, and exposes them through a lightweight dashboard.

## Project Layout

```text
TTC-PULSE/
|-- datasets/
|   |-- 01_gtfs_merged/
|   |   |-- routes.txt
|   |   |-- trips.txt
|   |   |-- stop_times.txt
|   |   |-- stops.txt
|   |   |-- calendar.txt
|   |   |-- calendar_dates.txt
|   |   `-- shapes.txt
|   |-- 02_bus_delay/
|   |   `-- csv/
|   `-- 03_subway_delay/
|       `-- csv/
`-- ttc_pulse/
```

## Raw Data Layout

The app expects the raw files to stay in this repository:

```text
data/
|-- bus/      # raw bus CSV files
|-- subway/   # raw subway CSV files
`-- gtfs/     # static GTFS CSV files
```

Before launching Streamlit, create a `.env` file in the project root:

```env
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-5.4-mini
```

Then run:

```bash
export PYTHONPATH=src
python -m ttc_pulse.bronze.build_bronze_tables
python -m ttc_pulse.gtfs.build_dimensions
python -m ttc_pulse.gtfs.build_bridge
python -m ttc_pulse.aliasing.build_route_alias
python -m ttc_pulse.aliasing.build_station_alias
python -m ttc_pulse.aliasing.build_incident_code_dim
python -m ttc_pulse.aliasing.build_review_tables
python -m ttc_pulse.normalization.normalize_bus
python -m ttc_pulse.normalization.normalize_subway
python -m ttc_pulse.normalization.normalize_gtfsrt_entities
python -m ttc_pulse.facts.build_fact_delay_events_norm
python -m ttc_pulse.facts.build_fact_gtfsrt_alerts_norm
python -m ttc_pulse.normalization.register_step2_tables
python -m ttc_pulse.marts.build_gold_rankings
streamlit run app/streamlit_app.py
```

If the Gold parquet outputs are already present in the repo or your local copy, you can often skip directly to:

```bash
streamlit run app/streamlit_app.py
```

## Step-by-Step Execution

### 1. Build Raw and Bronze

```bash
export PYTHONPATH=src
python -m ttc_pulse.bronze.build_bronze_tables
```

This creates:
- raw registries in `raw/`
- Bronze parquet outputs in `bronze/`
- source inventory and step summary docs
- `data/ttc_pulse.duckdb`

Important outputs:
- `logs/ingestion_log.csv`
- `docs/source_inventory.md`
- `docs/step1_summary.md`

### 2. Build Silver, Dimensions, Bridge, and Reviews

Run in this order:

```bash
export PYTHONPATH=src
python -m ttc_pulse.gtfs.build_dimensions
python -m ttc_pulse.gtfs.build_bridge
python -m ttc_pulse.aliasing.build_route_alias
python -m ttc_pulse.aliasing.build_station_alias
python -m ttc_pulse.aliasing.build_incident_code_dim
python -m ttc_pulse.aliasing.build_review_tables
python -m ttc_pulse.normalization.normalize_bus
python -m ttc_pulse.normalization.normalize_subway
python -m ttc_pulse.normalization.normalize_gtfsrt_entities
python -m ttc_pulse.facts.build_fact_delay_events_norm
python -m ttc_pulse.facts.build_fact_gtfsrt_alerts_norm
python -m ttc_pulse.normalization.register_step2_tables
```

Important outputs:
- `silver/*.parquet`
- `dimensions/*.parquet`
- `bridge/*.parquet`
- `reviews/*.parquet`
- `logs/step2_registration_log.csv`

### 3. Build Gold Marts

```bash
export PYTHONPATH=src
python -m ttc_pulse.marts.build_gold_rankings
```

This orchestrates the Gold build and writes:
- `gold/gold_delay_events_core.parquet`
- `gold/gold_linkage_quality.parquet`
- `gold/gold_route_time_metrics.parquet`
- `gold/gold_station_time_metrics.parquet`
- `gold/gold_time_reliability.parquet`
- `gold/gold_top_offender_ranking.parquet`
- `gold/gold_alert_validation.parquet`
- `gold/gold_spatial_hotspot.parquet`

Important outputs:
- `logs/step3_gold_build_log.csv`
- `outputs/final_metrics_summary.md`

### 4. Launch the Dashboard

Make sure `.env` exists in the project root before this step (`OPENAI_API_KEY` and `OPENAI_MODEL`).

```bash
export PYTHONPATH=src
streamlit run app/streamlit_app.py
```

Default local URL:

- `http://localhost:8501`

## Verification

Run the lightweight verification command to confirm the raw files load and the cleaned datasets have date coverage:

```powershell
$env:PYTHONPATH = "src"
python -m ttc_pulse.verification
```

This prints:

- file counts discovered in `data/bus`, `data/subway`, and `data/gtfs`
- cleaned row counts
- date coverage for bus and subway
- whether GTFS route and stop lookup tables were loaded

For more detail:
- [Runbook](docs/runbook.md): operational steps to run, refresh, troubleshoot, and recover the pipelines/dashboard.
- [Architecture](docs/architecture.md): system design, data flow across raw/bronze/silver/gold, and component responsibilities.
- [Data Dictionary](docs/data_dictionary.md): table/column definitions, metric meanings, and key assumptions.

- `Start Here`: landing page modeled after the shared mockup
- `Dataset Explorer`: choose bus or subway, filter by service date range, and inspect cleaned row-level records
- `Overview`: quick counts, date coverage, and route/station summaries

The current dashboard includes:
- Reliability Overview
- Bus Route Ranking
- Subway Station Ranking
- Weekday Hour Heatmap
- Monthly Trends
- Cause Category Mix
- Live Alert Validation
- Spatial Hotspot Map
- Bus Reliability Drill-Down
- Subway Reliability Drill-Down
- AI-chat bot

## AI Chat Bot

A dashboard page named **AI-chat bot** is available from the app sidebar.

### OpenAI setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Create a `.env` file in the project root:

```env
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-5.4-mini
```

### Recommended model

- `gpt-5.4-mini` (recommended default): better cost/latency for interactive dashboard chat.
- `gpt-5.4`: higher quality for deeper analysis and longer reasoning.

The chatbot uses the loaded TTC dataset context (DuckDB/parquet Gold tables) to answer:
- reliability pattern questions
- practical data-driven mitigation ideas
- forward-looking delay risk discussions (with explicit assumptions)



