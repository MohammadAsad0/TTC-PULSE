# TTC Pulse

TTC Pulse is a small Streamlit project that reads raw TTC bus, subway, and GTFS CSV files directly from this repository, cleans them, and exposes them through a lightweight dashboard.

## Project Layout

```text
TTC-PULSE/
|-- app/
|   `-- streamlit_app.py
|-- data/
|   |-- bus/
|   |-- subway/
|   `-- gtfs/
|-- docs/
|-- logs/
|-- outputs/
|-- src/
|   `-- ttc_pulse/
`-- requirements.txt
```

## Raw Data Layout

The app expects the raw files to stay in this repository:

```text
data/
|-- bus/      # raw bus CSV files
|-- subway/   # raw subway CSV files
`-- gtfs/     # static GTFS CSV files
```

## Environment Setup

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Run The App

PowerShell:

```powershell
streamlit run app/streamlit_app.py
```

The app bootstraps the local `src/` folder automatically, so `PYTHONPATH` does not need to be set.

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

## Dashboard Pages

- `Start Here`: landing page modeled after the shared mockup
- `Dataset Explorer`: choose bus or subway, filter by service date range, and inspect cleaned row-level records
- `Overview`: quick counts, date coverage, and route/station summaries

## Cleaning Rules

- Bus rows are dropped when the route field is missing, the date is missing, or the route resolves to `0`
- Subway rows are dropped when line, station, or date is missing
- Route, line, station, direction, incident code, and vehicle fields are standardized when present
- Subway delay codes are enriched from `data/subway/ttc-subway-delay-codes__01_Sheet_1.csv`
- Subway GTFS route IDs are inferred from the cleaned subway line value using static GTFS routes

More detail is documented in [docs/project_structure.md](docs/project_structure.md), [docs/raw_data_layout.md](docs/raw_data_layout.md), [docs/ingestion_flow.md](docs/ingestion_flow.md), and [docs/cleaning_rules.md](docs/cleaning_rules.md).
