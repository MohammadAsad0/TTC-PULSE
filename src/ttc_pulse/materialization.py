from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ttc_pulse.cleaning import load_and_clean_all
from ttc_pulse.io import list_csv_files, load_csv_files
from ttc_pulse.paths import (
    BUS_DIR,
    BUS_PARQUET,
    CACHE_DIR,
    DUCKDB_PATH,
    GTFS_DIR,
    GTFS_ROUTES_PARQUET,
    GTFS_STOPS_PARQUET,
    SUBWAY_DIR,
    SUBWAY_PARQUET,
)


METADATA_PATH = CACHE_DIR / "materialization_meta.json"


def _is_subway_event_file(path: Path) -> bool:
    return "codes" not in path.name.lower()


def _source_files() -> list[Path]:
    files = []
    files.extend(list_csv_files(BUS_DIR))
    files.extend(list_csv_files(SUBWAY_DIR, predicate=_is_subway_event_file))
    files.extend(list_csv_files(GTFS_DIR))
    return files


def _source_signature() -> dict[str, Any]:
    files = _source_files()
    latest_mtime = max((path.stat().st_mtime for path in files), default=0.0)
    return {
        "latest_mtime": latest_mtime,
        "bus_files": len(list_csv_files(BUS_DIR)),
        "subway_files": len(list_csv_files(SUBWAY_DIR, predicate=_is_subway_event_file)),
        "gtfs_files": len(list_csv_files(GTFS_DIR)),
    }


def _artifacts_exist() -> bool:
    required = [BUS_PARQUET, SUBWAY_PARQUET, GTFS_ROUTES_PARQUET, GTFS_STOPS_PARQUET, METADATA_PATH]
    return all(path.exists() for path in required)


def _read_metadata() -> dict[str, Any]:
    if not METADATA_PATH.exists():
        return {}
    return json.loads(METADATA_PATH.read_text(encoding="utf-8"))


def _write_metadata(payload: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _sanitize_for_storage(frame: pd.DataFrame) -> pd.DataFrame:
    clean = frame.copy()
    for column in clean.columns:
        if pd.api.types.is_object_dtype(clean[column]):
            clean[column] = clean[column].astype("string")
    return clean


def artifacts_are_fresh() -> bool:
    if not _artifacts_exist():
        return False
    current = _source_signature()
    meta = _read_metadata()
    cached_mtime = float(meta.get("latest_mtime", 0.0))
    return cached_mtime >= float(current["latest_mtime"])


def _load_raw() -> tuple[pd.DataFrame, pd.DataFrame]:
    bus_raw = load_csv_files(BUS_DIR)
    subway_raw = load_csv_files(SUBWAY_DIR, predicate=_is_subway_event_file)
    return bus_raw, subway_raw


def materialize_clean_datasets(force: bool = False) -> dict[str, Any]:
    if not force and artifacts_are_fresh():
        meta = _read_metadata()
        meta["materialized"] = False
        meta["source"] = "cache"
        return meta

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    bus_raw, subway_raw = _load_raw()
    cleaned = load_and_clean_all(bus_raw=bus_raw, subway_raw=subway_raw)

    bus = _sanitize_for_storage(cleaned["bus"])
    subway = _sanitize_for_storage(cleaned["subway"])
    gtfs_routes = _sanitize_for_storage(cleaned["gtfs_routes"])
    gtfs_stops = _sanitize_for_storage(cleaned["gtfs_stops"])

    bus.to_parquet(BUS_PARQUET, index=False)
    subway.to_parquet(SUBWAY_PARQUET, index=False)
    gtfs_routes.to_parquet(GTFS_ROUTES_PARQUET, index=False)
    gtfs_stops.to_parquet(GTFS_STOPS_PARQUET, index=False)

    with duckdb.connect(str(DUCKDB_PATH)) as con:
        con.register("bus_df", bus)
        con.register("subway_df", subway)
        con.register("gtfs_routes_df", gtfs_routes)
        con.register("gtfs_stops_df", gtfs_stops)
        con.execute("CREATE OR REPLACE TABLE bus_clean AS SELECT * FROM bus_df")
        con.execute("CREATE OR REPLACE TABLE subway_clean AS SELECT * FROM subway_df")
        con.execute("CREATE OR REPLACE TABLE gtfs_routes AS SELECT * FROM gtfs_routes_df")
        con.execute("CREATE OR REPLACE TABLE gtfs_stops AS SELECT * FROM gtfs_stops_df")

    sig = _source_signature()
    payload = {
        **sig,
        "rows_bus": int(len(bus)),
        "rows_subway": int(len(subway)),
        "materialized": True,
        "source": "rebuild",
    }
    _write_metadata(payload)
    return payload


def load_materialized_datasets() -> dict[str, Any]:
    bus = pd.read_parquet(BUS_PARQUET) if BUS_PARQUET.exists() else pd.DataFrame()
    subway = pd.read_parquet(SUBWAY_PARQUET) if SUBWAY_PARQUET.exists() else pd.DataFrame()
    gtfs_routes = pd.read_parquet(GTFS_ROUTES_PARQUET) if GTFS_ROUTES_PARQUET.exists() else pd.DataFrame()
    gtfs_stops = pd.read_parquet(GTFS_STOPS_PARQUET) if GTFS_STOPS_PARQUET.exists() else pd.DataFrame()

    for frame in [bus, subway]:
        if "service_date" in frame.columns:
            frame["service_date"] = pd.to_datetime(frame["service_date"], errors="coerce")

    return {
        "bus": bus,
        "subway": subway,
        "gtfs_routes": gtfs_routes,
        "gtfs_stops": gtfs_stops,
        "file_inventory": {
            "bus_files": len(list_csv_files(BUS_DIR)),
            "subway_files": len(list_csv_files(SUBWAY_DIR, predicate=_is_subway_event_file)),
            "gtfs_files": len(list_csv_files(GTFS_DIR)),
        },
    }


def load_datasets_for_app(force_refresh: bool = False) -> dict[str, Any]:
    materialize_clean_datasets(force=force_refresh)
    return load_materialized_datasets()
