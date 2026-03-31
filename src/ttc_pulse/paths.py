from __future__ import annotations

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
BUS_DIR = DATA_DIR / "bus"
SUBWAY_DIR = DATA_DIR / "subway"
GTFS_DIR = DATA_DIR / "gtfs"
DOCS_DIR = ROOT_DIR / "docs"
LOGS_DIR = ROOT_DIR / "logs"
OUTPUTS_DIR = ROOT_DIR / "outputs"
CACHE_DIR = OUTPUTS_DIR / "processed"
BUS_PARQUET = CACHE_DIR / "bus_clean.parquet"
SUBWAY_PARQUET = CACHE_DIR / "subway_clean.parquet"
GTFS_ROUTES_PARQUET = CACHE_DIR / "gtfs_routes.parquet"
GTFS_STOPS_PARQUET = CACHE_DIR / "gtfs_stops.parquet"
DUCKDB_PATH = DATA_DIR / "ttc_pulse.duckdb"
