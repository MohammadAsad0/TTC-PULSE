from __future__ import annotations

import re

import pandas as pd

from ttc_pulse.paths import GTFS_DIR, SUBWAY_DIR


SUBWAY_ROUTE_ALIASES = {
    "YU": "1",
    "YUS": "1",
    "BD": "2",
    "SHP": "4",
    "SRT": None,
}


def normalize_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    text = re.sub(r"\s+", " ", text)
    return text or None


def normalize_station_name(value: object) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    text = text.replace(" STN", " STATION")
    return text


def normalize_vehicle(value: object) -> str | None:
    text = normalize_text(value)
    if not text or text == "0" or text == "NONE":
        return None
    return text


def normalize_direction(value: object) -> str | None:
    text = normalize_text(value)
    if not text or text == "NONE":
        return None
    text = text.replace("BOUND", "")
    return text.strip() or None


def normalize_code(value: object) -> str | None:
    return normalize_text(value)


def normalize_route_short_name(value: object) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    match = re.match(r"^([A-Z0-9]+)", text)
    if not match:
        return None
    route = match.group(1)
    return None if route == "0" else route


def normalize_subway_line(value: object) -> str | None:
    text = normalize_text(value)
    if not text or text == "NONE":
        return None

    normalized = text.replace(" - ", "/").replace(" / ", "/").replace("-", "/")
    candidates: list[str] = []
    if "YONGE" in normalized or "UNIVERSITY" in normalized or re.search(r"\bYU(S)?\b", normalized):
        candidates.append("YU")
    if "BLOOR" in normalized or "DANFORTH" in normalized or re.search(r"\bBD\b", normalized):
        candidates.append("BD")
    if "SHEPPARD" in normalized or re.search(r"\bSHP\b", normalized):
        candidates.append("SHP")
    if "SCARBOROUGH" in normalized or re.search(r"\bSRT\b", normalized):
        candidates.append("SRT")

    unique = []
    for item in candidates:
        if item not in unique:
            unique.append(item)
    if not unique:
        return normalized
    return "/".join(unique)



def load_gtfs_tables() -> dict[str, pd.DataFrame]:
    routes = pd.read_csv(GTFS_DIR / "routes.csv") if (GTFS_DIR / "routes.csv").exists() else pd.DataFrame()
    stops = pd.read_csv(GTFS_DIR / "stops.csv") if (GTFS_DIR / "stops.csv").exists() else pd.DataFrame()
    if not routes.empty:
        routes["route_short_name_norm"] = routes["route_short_name"].map(normalize_text)
        routes["route_long_name_norm"] = routes["route_long_name"].map(normalize_text)
    if not stops.empty:
        stops["stop_name_norm"] = stops["stop_name"].map(normalize_station_name)
        stops["station_name_norm"] = stops["stop_name_norm"].str.extract(r"([A-Z' ]+ STATION)", expand=False)
    return {"routes": routes, "stops": stops, "trips": pd.DataFrame()}



def build_subway_code_lookup() -> pd.DataFrame:
    code_file = next((path for path in SUBWAY_DIR.glob("*codes*.csv")), None)
    if code_file is None:
        return pd.DataFrame(columns=["incident_code_norm", "incident_code_description"])

    raw = pd.read_csv(code_file)
    pairs = []
    for code_col, desc_col in [("Unnamed: 2", "Unnamed: 3"), ("Unnamed: 6", "Unnamed: 7")]:
        if code_col in raw.columns and desc_col in raw.columns:
            subset = raw[[code_col, desc_col]].rename(
                columns={code_col: "incident_code_norm", desc_col: "incident_code_description"}
            )
            pairs.append(subset)

    if not pairs:
        return pd.DataFrame(columns=["incident_code_norm", "incident_code_description"])

    lookup = pd.concat(pairs, ignore_index=True)
    lookup["incident_code_norm"] = lookup["incident_code_norm"].map(normalize_code)
    lookup["incident_code_description"] = lookup["incident_code_description"].astype("string").str.strip()
    lookup = lookup.dropna(subset=["incident_code_norm", "incident_code_description"]).drop_duplicates()
    return lookup



def map_subway_route_ids(line_value: object) -> str | None:
    line_norm = normalize_subway_line(line_value)
    if not line_norm:
        return None
    route_ids = [SUBWAY_ROUTE_ALIASES.get(part) for part in line_norm.split("/") if SUBWAY_ROUTE_ALIASES.get(part)]
    unique_ids = []
    for route_id in route_ids:
        if route_id not in unique_ids:
            unique_ids.append(route_id)
    return ",".join(unique_ids) if unique_ids else None



def build_station_lookup(stops: pd.DataFrame) -> pd.DataFrame:
    if stops.empty:
        return pd.DataFrame(columns=["station_norm", "stop_id_gtfs", "stop_name_gtfs"])

    exact_station = stops.loc[stops["stop_name_norm"].str.endswith("STATION", na=False), ["stop_id", "stop_name_norm"]].copy()
    exact_station["station_norm"] = exact_station["stop_name_norm"]
    exact_station["stop_id_gtfs"] = exact_station["stop_id"].astype("string")
    exact_station["stop_name_gtfs"] = exact_station["stop_name_norm"]
    lookup = (
        exact_station[["station_norm", "stop_id_gtfs", "stop_name_gtfs"]]
        .sort_values(["station_norm", "stop_id_gtfs"])
        .drop_duplicates(subset=["station_norm"])
    )
    return lookup
