from __future__ import annotations

import pandas as pd

from ttc_pulse.cleaning import load_and_clean_all
from ttc_pulse.io import list_csv_files, load_csv_files
from ttc_pulse.materialization import load_datasets_for_app, materialize_clean_datasets
from ttc_pulse.paths import BUS_DIR, GTFS_DIR, SUBWAY_DIR


def _is_subway_event_file(path) -> bool:
    return "codes" not in path.name.lower()


def load_clean_datasets() -> dict[str, object]:
    bus_raw = load_csv_files(BUS_DIR)
    subway_raw = load_csv_files(SUBWAY_DIR, predicate=_is_subway_event_file)
    cleaned = load_and_clean_all(bus_raw=bus_raw, subway_raw=subway_raw)
    cleaned["file_inventory"] = {
        "bus_files": len(list_csv_files(BUS_DIR)),
        "subway_files": len(list_csv_files(SUBWAY_DIR, predicate=_is_subway_event_file)),
        "gtfs_files": len(list_csv_files(GTFS_DIR)),
    }
    return cleaned


def load_fast_datasets(force_refresh: bool = False) -> dict[str, object]:
    return load_datasets_for_app(force_refresh=force_refresh)


def refresh_fast_artifacts() -> dict[str, object]:
    return materialize_clean_datasets(force=True)


def _coverage_row(label: str, frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {"dataset": label, "rows": 0, "min_date": None, "max_date": None, "coverage": "No data"}
    return {
        "dataset": label,
        "rows": len(frame),
        "min_date": frame["service_date"].min().date(),
        "max_date": frame["service_date"].max().date(),
        "coverage": f"{frame['service_date'].min().date()} to {frame['service_date'].max().date()}",
    }


def build_overview(bus_rows: pd.DataFrame, subway_rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    coverage = pd.DataFrame([_coverage_row("Bus", bus_rows), _coverage_row("Subway", subway_rows)])

    bus_routes = (
        bus_rows.groupby("route_short_name_norm", dropna=False)
        .size()
        .reset_index(name="rows")
        .rename(columns={"route_short_name_norm": "route"})
        .sort_values("rows", ascending=False)
        .head(10)
    )

    subway_stations = (
        subway_rows.groupby("station_norm", dropna=False)
        .size()
        .reset_index(name="rows")
        .rename(columns={"station_norm": "station"})
        .sort_values("rows", ascending=False)
        .head(10)
    )

    subway_lines = (
        subway_rows.groupby("line_norm", dropna=False)
        .size()
        .reset_index(name="rows")
        .rename(columns={"line_norm": "line"})
        .sort_values("rows", ascending=False)
        .head(10)
    )

    return {
        "coverage": coverage,
        "bus_routes": bus_routes,
        "subway_stations": subway_stations,
        "subway_lines": subway_lines,
    }
