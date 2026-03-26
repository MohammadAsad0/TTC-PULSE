from __future__ import annotations

import pandas as pd

from ttc_pulse.gtfs import (
    build_station_lookup,
    build_subway_code_lookup,
    load_gtfs_tables,
    map_subway_route_ids,
    normalize_code,
    normalize_direction,
    normalize_route_short_name,
    normalize_station_name,
    normalize_subway_line,
    normalize_vehicle,
)


def _coalesce_columns(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    series = pd.Series(index=frame.index, dtype="object")
    for column in columns:
        if column in frame.columns:
            series = series.where(series.notna(), frame[column])
    return series


def clean_bus_data(raw: pd.DataFrame, gtfs_routes: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    bus = raw.copy()
    bus["service_date"] = pd.to_datetime(
        _coalesce_columns(bus, ["Date", "Report Date"]),
        errors="coerce",
        format="mixed",
    ).dt.normalize()
    bus["route_label_raw"] = _coalesce_columns(bus, ["Line", "Route"])
    bus["route_short_name_norm"] = bus["route_label_raw"].map(normalize_route_short_name)
    bus["station_text_raw"] = _coalesce_columns(bus, ["Station", "Location"])
    bus["station_text_norm"] = bus["station_text_raw"].map(normalize_station_name)
    bus["direction_raw"] = _coalesce_columns(bus, ["Bound", "Direction"])
    bus["direction_norm"] = bus["direction_raw"].map(normalize_direction)
    bus["incident_code_raw"] = _coalesce_columns(bus, ["Code", "Incident"])
    bus["incident_code_norm"] = bus["incident_code_raw"].map(normalize_code)
    bus["vehicle_raw"] = _coalesce_columns(bus, ["Vehicle"])
    bus["vehicle_norm"] = bus["vehicle_raw"].map(normalize_vehicle)
    bus["min_delay"] = pd.to_numeric(_coalesce_columns(bus, ["Min Delay", " Min Delay", "Delay"]), errors="coerce")
    bus["min_gap"] = pd.to_numeric(_coalesce_columns(bus, ["Min Gap", "Gap"]), errors="coerce")

    bus = bus.dropna(subset=["service_date", "route_short_name_norm"]).copy()
    bus = bus.loc[bus["route_short_name_norm"] != "0"].copy()

    route_lookup = gtfs_routes.loc[gtfs_routes["route_type"].astype("string") == "3", ["route_id", "route_short_name_norm"]].copy()
    route_lookup["route_id_gtfs"] = route_lookup["route_id"].astype("string")
    route_lookup = route_lookup.drop_duplicates(subset=["route_short_name_norm"])
    bus = bus.merge(route_lookup[["route_short_name_norm", "route_id_gtfs"]], on="route_short_name_norm", how="left")

    keep = [
        "service_date",
        "Time",
        "Day",
        "route_label_raw",
        "route_short_name_norm",
        "route_id_gtfs",
        "station_text_raw",
        "station_text_norm",
        "incident_code_raw",
        "incident_code_norm",
        "min_delay",
        "min_gap",
        "direction_raw",
        "direction_norm",
        "vehicle_raw",
        "vehicle_norm",
        "source_file",
    ]
    existing = [column for column in keep if column in bus.columns]
    return bus[existing].sort_values(["service_date", "route_short_name_norm", "source_file"]).reset_index(drop=True)


def clean_subway_data(raw: pd.DataFrame, gtfs_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame()

    subway = raw.copy()
    subway["service_date"] = pd.to_datetime(subway.get("Date"), errors="coerce", format="mixed").dt.normalize()
    subway["station_norm"] = subway.get("Station", pd.Series(index=subway.index, dtype="object")).map(normalize_station_name)
    subway["line_norm"] = subway.get("Line", pd.Series(index=subway.index, dtype="object")).map(normalize_subway_line)
    subway["direction_norm"] = subway.get("Bound", pd.Series(index=subway.index, dtype="object")).map(normalize_direction)
    subway["incident_code_norm"] = subway.get("Code", pd.Series(index=subway.index, dtype="object")).map(normalize_code)
    subway["vehicle_norm"] = subway.get("Vehicle", pd.Series(index=subway.index, dtype="object")).map(normalize_vehicle)

    subway = subway.dropna(subset=["service_date", "station_norm", "line_norm"]).copy()
    subway = subway.loc[subway["line_norm"].ne("") & subway["station_norm"].ne("")].copy()
    subway["route_id_gtfs"] = subway["line_norm"].map(map_subway_route_ids)

    code_lookup = build_subway_code_lookup()
    if not code_lookup.empty:
        subway = subway.merge(code_lookup, on="incident_code_norm", how="left")

    station_lookup = build_station_lookup(gtfs_tables["stops"])
    subway = subway.merge(station_lookup, left_on="station_norm", right_on="station_norm", how="left")

    keep = [
        "service_date",
        "Time",
        "Day",
        "Station",
        "station_norm",
        "stop_id_gtfs",
        "stop_name_gtfs",
        "Line",
        "line_norm",
        "route_id_gtfs",
        "Code",
        "incident_code_norm",
        "incident_code_description",
        "Min Delay",
        "Min Gap",
        "Bound",
        "direction_norm",
        "Vehicle",
        "vehicle_norm",
        "source_file",
    ]
    existing = [column for column in keep if column in subway.columns]
    return subway[existing].sort_values(["service_date", "line_norm", "station_norm", "source_file"]).reset_index(drop=True)


def load_and_clean_all(bus_raw: pd.DataFrame, subway_raw: pd.DataFrame) -> dict[str, pd.DataFrame]:
    gtfs_tables = load_gtfs_tables()
    return {
        "bus": clean_bus_data(bus_raw, gtfs_tables["routes"]),
        "subway": clean_subway_data(subway_raw, gtfs_tables),
        "gtfs_routes": gtfs_tables["routes"],
        "gtfs_stops": gtfs_tables["stops"],
    }
