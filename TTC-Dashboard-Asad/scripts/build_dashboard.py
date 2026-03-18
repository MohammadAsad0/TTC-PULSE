#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

# Update these paths as needed to point to your local data and output locations.
DATA_PATH = "/Users/muhammadasad/Library/CloudStorage/OneDrive-YorkUniversity/Omkumar Patel's files - 6414-Data-Visualization"
OUTPUT_PATH = "/Users/muhammadasad/Documents/TTC-PULSE/dist"

DEFAULT_DATA_ROOT = Path(
    DATA_PATH + "/Code/Dataset"
)
DEFAULT_OUTPUT = Path(OUTPUT_PATH + "/index.html")

WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

MODE_CONFIG = {
    "bus": {
        "recent_path": Path("02_bus_delay/TTC Bus Delay Data since 2025.csv"),
        "history_dir": Path("02_bus_delay/csv"),
        "history_pattern": "ttc-bus-delay-data-*.csv",
        "color": "#d62828",
    },
    "streetcar": {
        "recent_path": Path("04_streetcar_delay/ttc-streetcar-delay-data-since-2025.csv"),
        "history_dir": Path("04_streetcar_delay/csv"),
        "history_pattern": "ttc-streetcar-delay-data-*.csv",
        "color": "#f77f00",
    },
    "subway": {
        "recent_path": Path("03_subway_delay/TTC Subway Delay Data since 2025.csv"),
        "history_dir": Path("03_subway_delay/csv"),
        "history_pattern": "ttc-subway-delay-*.csv",
        "color": "#003049",
    },
}

CODE_LOOKUP_FILES = {
    "bus": Path("04_streetcar_delay/Code Descriptions (1).csv"),
    "streetcar": Path("03_subway_delay/code-descriptions.csv"),
    "subway": Path("02_bus_delay/Code Descriptions.csv"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a self-contained TTC analytics dashboard."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="Root directory that contains the TTC CSV folders.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Where to write the generated HTML dashboard.",
    )
    return parser.parse_args()


def read_csv(path: Path, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig", **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1", **kwargs)


def clean_text(value: object) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return re.sub(r"\s+", " ", text)


def parse_hour(value: object) -> int | None:
    text = clean_text(value)
    if not text:
        return None
    match = re.match(r"^(\d{1,2})", text)
    if not match:
        return None
    hour = int(match.group(1))
    if 0 <= hour <= 23:
        return hour
    return None


def normalize_route_key(mode: str, line_value: object) -> str | None:
    text = clean_text(line_value)
    if not text:
        return None

    if mode == "subway":
        token = text.upper().replace(" ", "")
        has_yu = "YU" in token or "YUS" in token
        has_bd = "BD" in token
        has_sheppard = "SHP" in token or "SRT" in token
        flags = [has_yu, has_bd, has_sheppard]
        if sum(bool(flag) for flag in flags) > 1:
            return "MULTI"
        if has_yu:
            return "1"
        if has_bd:
            return "2"
        if has_sheppard:
            return "4"
        return None

    match = re.match(r"^\s*(\d{1,3})", text)
    if match:
        return str(int(match.group(1)))
    return None


def load_code_lookup(data_root: Path) -> dict[str, dict[str, str]]:
    lookups: dict[str, dict[str, str]] = {}
    for mode, relative_path in CODE_LOOKUP_FILES.items():
        frame = read_csv(data_root / relative_path)
        cols = {str(col).strip().upper(): col for col in frame.columns}
        code_col = cols.get("CODE")
        desc_col = cols.get("DESCRIPTION")
        if not code_col or not desc_col:
            lookups[mode] = {}
            continue
        frame = frame[[code_col, desc_col]].dropna()
        frame[code_col] = frame[code_col].astype(str).str.strip().str.upper()
        frame[desc_col] = frame[desc_col].astype(str).str.strip()
        lookups[mode] = dict(zip(frame[code_col], frame[desc_col]))
    return lookups


def standardize_frame(mode: str, path: Path, recent: bool) -> pd.DataFrame:
    raw = read_csv(path)
    rename_map = {}
    if "Report Date" in raw.columns:
        rename_map["Report Date"] = "Date"
    if "Route" in raw.columns:
        rename_map["Route"] = "Line"
    if "Location" in raw.columns:
        rename_map["Location"] = "Station"
    if "Incident" in raw.columns:
        rename_map["Incident"] = "Code"
    if "Direction" in raw.columns:
        rename_map["Direction"] = "Bound"
    raw = raw.rename(columns=rename_map)

    expected_cols = ["Date", "Line", "Time", "Day", "Station", "Code", "Min Delay", "Min Gap"]
    present_cols = [col for col in expected_cols if col in raw.columns]
    frame = raw[present_cols].copy()

    def series_or_default(column: str, default: object = None) -> pd.Series:
        if column in frame.columns:
            return frame[column]
        return pd.Series([default] * len(frame), index=frame.index)

    frame["mode"] = mode
    frame["date"] = pd.to_datetime(series_or_default("Date"), errors="coerce")
    frame["month"] = frame["date"].dt.to_period("M").astype(str)
    frame["hour"] = series_or_default("Time").map(parse_hour)
    frame["weekday"] = series_or_default("Day").map(clean_text)
    if frame["weekday"].isna().any():
        frame.loc[frame["weekday"].isna(), "weekday"] = frame.loc[
            frame["weekday"].isna(), "date"
        ].dt.day_name()
    frame["location"] = series_or_default("Station").map(clean_text)
    frame["line_raw"] = series_or_default("Line").map(clean_text)
    frame["route_key"] = frame["line_raw"].map(lambda value: normalize_route_key(mode, value))
    frame["code"] = series_or_default("Code").map(clean_text)
    frame["delay_minutes"] = pd.to_numeric(series_or_default("Min Delay", 0), errors="coerce").fillna(0)
    frame["gap_minutes"] = pd.to_numeric(series_or_default("Min Gap", 0), errors="coerce").fillna(0)
    frame["is_recent"] = recent

    frame = frame.dropna(subset=["date"])
    frame = frame[frame["month"].ne("NaT")]
    frame = frame[frame["delay_minutes"] >= 0]
    return frame[
        [
            "mode",
            "date",
            "month",
            "hour",
            "weekday",
            "location",
            "line_raw",
            "route_key",
            "code",
            "delay_minutes",
            "gap_minutes",
            "is_recent",
        ]
    ].copy()


def valid_history_file(path: Path) -> bool:
    name = path.name.lower()
    banned_terms = ("readme", "code", "since", ".ds_store")
    return not any(term in name for term in banned_terms)


def aggregate_monthly(frame: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        frame.groupby(["mode", "month"], as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
            avg_delay=("delay_minutes", "mean"),
        )
        .sort_values(["mode", "month"])
    )
    grouped["avg_delay"] = grouped["avg_delay"].round(2)
    grouped["total_delay"] = grouped["total_delay"].round(1)
    return grouped


def build_historical_monthly(data_root: Path) -> tuple[pd.DataFrame, dict[str, tuple[str, str]]]:
    monthly_parts: list[pd.DataFrame] = []
    coverage: dict[str, list[pd.Timestamp]] = defaultdict(list)

    for mode, config in MODE_CONFIG.items():
        for path in sorted((data_root / config["history_dir"]).glob(config["history_pattern"])):
            if not valid_history_file(path):
                continue
            frame = standardize_frame(mode, path, recent=False)
            if frame.empty:
                continue
            monthly_parts.append(aggregate_monthly(frame))
            coverage[mode].extend([frame["date"].min(), frame["date"].max()])

    monthly = (
        pd.concat(monthly_parts, ignore_index=True)
        .groupby(["mode", "month"], as_index=False)
        .agg(
            incidents=("incidents", "sum"),
            total_delay=("total_delay", "sum"),
            weighted_delay=("avg_delay", "sum"),
        )
        .sort_values(["mode", "month"])
    )
    monthly["avg_delay"] = (monthly["total_delay"] / monthly["incidents"]).round(2)
    monthly["total_delay"] = monthly["total_delay"].round(1)
    monthly = monthly.drop(columns=["weighted_delay"])

    normalized_coverage = {}
    for mode, timestamps in coverage.items():
        valid = [stamp for stamp in timestamps if pd.notna(stamp)]
        normalized_coverage[mode] = (
            min(valid).strftime("%Y-%m-%d"),
            max(valid).strftime("%Y-%m-%d"),
        )
    return monthly, normalized_coverage


def build_recent_frames(data_root: Path) -> dict[str, pd.DataFrame]:
    recent_frames: dict[str, pd.DataFrame] = {}
    for mode, config in MODE_CONFIG.items():
        frame = standardize_frame(mode, data_root / config["recent_path"], recent=True)
        recent_frames[mode] = frame
    return recent_frames


def build_gtfs_service(data_root: Path) -> pd.DataFrame:
    routes = read_csv(
        data_root / "01_gtfs_merged/csv/routes.csv",
        usecols=["route_id", "route_short_name", "route_long_name", "route_type"],
    )
    trips = read_csv(
        data_root / "01_gtfs_merged/csv/trips.csv",
        usecols=["trip_id", "route_id"],
    )
    trips["trip_id"] = trips["trip_id"].astype(str)
    trips["route_id"] = trips["route_id"].astype(str)
    trip_counts = trips.groupby("route_id").size().rename("scheduled_trips")
    trip_to_route = trips.set_index("trip_id")["route_id"]

    stop_event_counter: defaultdict[str, int] = defaultdict(int)
    unique_stops: defaultdict[str, set[str]] = defaultdict(set)
    for chunk in pd.read_csv(
        data_root / "01_gtfs_merged/csv/stop_times.csv",
        usecols=["trip_id", "stop_id"],
        chunksize=300_000,
        encoding="utf-8-sig",
    ):
        chunk["trip_id"] = chunk["trip_id"].astype(str)
        chunk["route_id"] = chunk["trip_id"].map(trip_to_route)
        chunk = chunk.dropna(subset=["route_id"])
        counts = chunk.groupby("route_id").size()
        for route_id, count in counts.items():
            stop_event_counter[str(route_id)] += int(count)
        for route_id, stops in chunk.groupby("route_id")["stop_id"]:
            unique_stops[str(route_id)].update(stops.dropna().astype(str).tolist())

    service = routes.copy()
    service["route_id"] = service["route_id"].astype(str)
    service["scheduled_trips"] = service["route_id"].map(trip_counts).fillna(0).astype(int)
    service["stop_events"] = service["route_id"].map(stop_event_counter).fillna(0).astype(int)
    service["unique_stops"] = service["route_id"].map(
        lambda route_id: len(unique_stops.get(str(route_id), set()))
    )
    service["route_short_name"] = service["route_short_name"].astype(str).str.strip()

    route_mode_map = {3: "bus", 0: "streetcar", 1: "subway"}
    service["mode"] = service["route_type"].map(route_mode_map)
    service = service.dropna(subset=["mode"]).copy()
    service["route_key"] = service.apply(
        lambda row: (
            str(row["route_id"])
            if str(row["mode"]) == "subway"
            else normalize_route_key(str(row["mode"]), row["route_short_name"])
        ),
        axis=1,
    )
    service["route_display"] = (
        service["route_short_name"].astype(str).str.strip()
        + " "
        + service["route_long_name"].astype(str).str.strip()
    ).str.strip()
    return service[
        [
            "mode",
            "route_id",
            "route_key",
            "route_short_name",
            "route_long_name",
            "route_display",
            "scheduled_trips",
            "stop_events",
            "unique_stops",
        ]
    ].copy()


def modal_label(series: pd.Series) -> str | None:
    cleaned = series.dropna().astype(str).str.strip()
    if cleaned.empty:
        return None
    return cleaned.value_counts().index[0]


def build_route_benchmark(recent_all: pd.DataFrame, gtfs_service: pd.DataFrame) -> pd.DataFrame:
    route_metrics = (
        recent_all.dropna(subset=["route_key"])
        .groupby(["mode", "route_key"], as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
            avg_delay=("delay_minutes", "mean"),
            hotspots=("location", pd.Series.nunique),
            primary_label=("line_raw", modal_label),
        )
    )
    route_metrics["avg_delay"] = route_metrics["avg_delay"].round(2)
    route_metrics["total_delay"] = route_metrics["total_delay"].round(1)

    benchmark = route_metrics.merge(
        gtfs_service,
        on=["mode", "route_key"],
        how="left",
        suffixes=("", "_gtfs"),
    )
    benchmark["route_display"] = benchmark["route_display"].fillna(benchmark["primary_label"])
    safe_trips = pd.to_numeric(benchmark["scheduled_trips"], errors="coerce").replace(0, pd.NA)
    benchmark["incidents_per_1000_trips"] = (
        pd.to_numeric(benchmark["incidents"], errors="coerce") / safe_trips * 1000
    )
    benchmark["delay_minutes_per_trip"] = (
        pd.to_numeric(benchmark["total_delay"], errors="coerce") / safe_trips
    )
    benchmark["incidents_per_1000_trips"] = benchmark["incidents_per_1000_trips"].astype("Float64").round(2)
    benchmark["delay_minutes_per_trip"] = benchmark["delay_minutes_per_trip"].astype("Float64").round(3)
    return benchmark.sort_values(["mode", "total_delay"], ascending=[True, False]).reset_index(
        drop=True
    )


def build_heatmap(recent_all: pd.DataFrame) -> pd.DataFrame:
    heatmap = (
        recent_all.dropna(subset=["weekday", "hour"])
        .groupby(["mode", "weekday", "hour"], as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
        )
    )
    heatmap["weekday"] = pd.Categorical(heatmap["weekday"], WEEKDAY_ORDER, ordered=True)
    heatmap = heatmap.sort_values(["mode", "weekday", "hour"]).reset_index(drop=True)
    heatmap["total_delay"] = heatmap["total_delay"].round(1)
    return heatmap


def build_hotspots(recent_all: pd.DataFrame) -> pd.DataFrame:
    hotspots = (
        recent_all.dropna(subset=["location"])
        .groupby(["mode", "location"], as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
            avg_delay=("delay_minutes", "mean"),
        )
    )
    hotspots["avg_delay"] = hotspots["avg_delay"].round(2)
    hotspots["total_delay"] = hotspots["total_delay"].round(1)
    return hotspots.sort_values(["mode", "incidents"], ascending=[True, False]).reset_index(
        drop=True
    )


def build_causes(recent_all: pd.DataFrame, code_lookup: dict[str, dict[str, str]]) -> pd.DataFrame:
    cause_rows = []
    for mode, frame in recent_all.groupby("mode"):
        lookup = code_lookup.get(mode, {})
        subset = frame.dropna(subset=["code"]).copy()
        subset["code_key"] = subset["code"].str.upper()
        grouped = (
            subset.groupby("code_key", as_index=False)
            .agg(
                incidents=("delay_minutes", "size"),
                total_delay=("delay_minutes", "sum"),
            )
            .sort_values("incidents", ascending=False)
        )
        grouped["mode"] = mode
        grouped["description"] = grouped["code_key"].map(lookup)
        grouped["label"] = grouped.apply(
            lambda row: (
                f"{row['code_key']} - {row['description']}"
                if pd.notna(row["description"])
                else row["code_key"]
            ),
            axis=1,
        )
        grouped["total_delay"] = grouped["total_delay"].round(1)
        cause_rows.append(grouped.rename(columns={"code_key": "code"}))
    return pd.concat(cause_rows, ignore_index=True)


def build_coverage_summary(
    historical_monthly: pd.DataFrame,
    historical_coverage: dict[str, tuple[str, str]],
    recent_frames: dict[str, pd.DataFrame],
    route_benchmark: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    for mode in MODE_CONFIG:
        hist = historical_monthly[historical_monthly["mode"] == mode]
        recent = recent_frames[mode]
        benchmarked = route_benchmark[
            (route_benchmark["mode"] == mode) & route_benchmark["scheduled_trips"].notna()
        ]
        rows.append(
            {
                "mode": mode,
                "all_time_incidents": int(hist["incidents"].sum()),
                "all_time_delay": round(float(hist["total_delay"].sum()), 1),
                "all_time_avg_delay": round(
                    float(hist["total_delay"].sum()) / max(float(hist["incidents"].sum()), 1.0),
                    2,
                ),
                "recent_incidents": int(len(recent)),
                "recent_delay": round(float(recent["delay_minutes"].sum()), 1),
                "recent_avg_delay": round(float(recent["delay_minutes"].mean()), 2),
                "recent_start": recent["date"].min().strftime("%Y-%m-%d"),
                "recent_end": recent["date"].max().strftime("%Y-%m-%d"),
                "history_start": historical_coverage[mode][0],
                "history_end": historical_coverage[mode][1],
                "benchmarked_routes": int(len(benchmarked)),
            }
        )
    return pd.DataFrame(rows)


def to_records(frame: pd.DataFrame) -> list[dict]:
    return json.loads(frame.to_json(orient="records"))


def build_dashboard_payload(data_root: Path) -> dict:
    historical_monthly, historical_coverage = build_historical_monthly(data_root)
    recent_frames = build_recent_frames(data_root)
    recent_all = pd.concat(recent_frames.values(), ignore_index=True)
    recent_monthly = aggregate_monthly(recent_all)
    code_lookup = load_code_lookup(data_root)
    gtfs_service = build_gtfs_service(data_root)
    route_benchmark = build_route_benchmark(recent_all, gtfs_service)
    heatmap = build_heatmap(recent_all)
    hotspots = build_hotspots(recent_all)
    causes = build_causes(recent_all, code_lookup)
    coverage = build_coverage_summary(
        historical_monthly, historical_coverage, recent_frames, route_benchmark
    )

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_root": str(data_root),
        "mode_colors": {mode: config["color"] for mode, config in MODE_CONFIG.items()},
        "historical_monthly": to_records(historical_monthly),
        "recent_monthly": to_records(recent_monthly),
        "heatmap": to_records(heatmap),
        "route_benchmark": to_records(route_benchmark),
        "hotspots": to_records(hotspots),
        "causes": to_records(causes),
        "coverage": to_records(coverage),
    }


def dashboard_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>TTC Ridership and Service Dynamics Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=Source+Sans+3:wght@400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --paper: #f8f4ec;
      --ink: #11212d;
      --muted: #51606d;
      --red: #d62828;
      --orange: #f77f00;
      --blue: #003049;
      --panel: rgba(255, 255, 255, 0.78);
      --line: rgba(17, 33, 45, 0.12);
      --shadow: 0 18px 50px rgba(17, 33, 45, 0.12);
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      font-family: "Source Sans 3", Georgia, serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(214, 40, 40, 0.10), transparent 25%),
        radial-gradient(circle at top right, rgba(247, 127, 0, 0.13), transparent 25%),
        linear-gradient(180deg, #fffaf3 0%, #f5efe3 100%);
      min-height: 100vh;
    }}

    .shell {{
      width: min(1280px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 48px;
    }}

    .hero {{
      display: grid;
      gap: 18px;
      grid-template-columns: 1.25fr 0.75fr;
      margin-bottom: 22px;
    }}

    .hero-card, .panel {{
      background: var(--panel);
      backdrop-filter: blur(18px);
      border: 1px solid rgba(255, 255, 255, 0.65);
      border-radius: 26px;
      box-shadow: var(--shadow);
    }}

    .hero-card {{
      padding: 28px;
    }}

    .kicker {{
      display: inline-flex;
      padding: 7px 12px;
      border-radius: 999px;
      background: rgba(214, 40, 40, 0.10);
      color: var(--red);
      font-size: 0.92rem;
      font-weight: 700;
      letter-spacing: 0.02em;
      text-transform: uppercase;
      margin-bottom: 14px;
    }}

    h1, h2, h3 {{
      font-family: "Space Grotesk", "Avenir Next", sans-serif;
      margin: 0;
    }}

    h1 {{
      font-size: clamp(2rem, 4vw, 3.5rem);
      line-height: 0.98;
      margin-bottom: 14px;
      max-width: 12ch;
    }}

    .lede {{
      margin: 0;
      font-size: 1.08rem;
      line-height: 1.55;
      color: var(--muted);
      max-width: 70ch;
    }}

    .hero-side {{
      padding: 24px;
      display: flex;
      flex-direction: column;
      gap: 16px;
      justify-content: space-between;
    }}

    .hero-side .stamp {{
      color: var(--muted);
      font-size: 0.95rem;
      line-height: 1.45;
    }}

    .method-note {{
      background: linear-gradient(135deg, rgba(0, 48, 73, 0.95), rgba(17, 33, 45, 0.95));
      color: white;
      padding: 16px 18px;
      border-radius: 20px;
    }}

    .method-note strong {{
      display: block;
      margin-bottom: 8px;
      font-family: "Space Grotesk", sans-serif;
    }}

    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 0 0 18px;
    }}

    .chip {{
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      padding: 11px 16px;
      border-radius: 999px;
      font-weight: 700;
      cursor: pointer;
      transition: 160ms ease;
    }}

    .chip.active {{
      border-color: transparent;
      color: white;
      background: linear-gradient(135deg, var(--red), #aa1f1f);
      transform: translateY(-1px);
    }}

    .metrics {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }}

    .metric {{
      padding: 18px 20px;
    }}

    .metric .label {{
      color: var(--muted);
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-size: 0.82rem;
    }}

    .metric .value {{
      font-family: "Space Grotesk", sans-serif;
      font-size: clamp(1.5rem, 3vw, 2.4rem);
      margin-top: 6px;
      margin-bottom: 4px;
    }}

    .metric .sub {{
      color: var(--muted);
      font-size: 0.96rem;
    }}

    .grid {{
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 18px;
      margin-bottom: 18px;
    }}

    .stack {{
      display: grid;
      gap: 18px;
    }}

    .panel {{
      padding: 18px;
      overflow: hidden;
    }}

    .panel-head {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }}

    .panel-head p {{
      margin: 0;
      color: var(--muted);
      font-size: 0.95rem;
    }}

    .chart {{
      min-height: 380px;
    }}

    .chart.compact {{
      min-height: 330px;
    }}

    .insight-list {{
      display: grid;
      gap: 10px;
      margin: 10px 0 0;
      padding: 0;
      list-style: none;
    }}

    .insight-list li {{
      padding: 13px 14px;
      border-radius: 16px;
      background: rgba(17, 33, 45, 0.04);
      line-height: 1.45;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }}

    th, td {{
      text-align: left;
      padding: 11px 8px;
      border-bottom: 1px solid var(--line);
    }}

    th {{
      font-family: "Space Grotesk", sans-serif;
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
    }}

    .footnote {{
      color: var(--muted);
      font-size: 0.94rem;
      line-height: 1.5;
      margin-top: 10px;
    }}

    @media (max-width: 1080px) {{
      .hero, .grid {{
        grid-template-columns: 1fr;
      }}

      .metrics {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
    }}

    @media (max-width: 640px) {{
      .shell {{
        width: min(100vw - 18px, 100%);
        padding-top: 18px;
      }}

      .hero-card, .hero-side, .panel, .metric {{
        border-radius: 22px;
      }}

      .metrics {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-card">
        <div class="kicker">Interactive Visualization &amp; Analytics</div>
        <h1>TTC Ridership and Service Dynamics</h1>
        <p class="lede">
          This dashboard blends all-time disruption history with a recent-period service benchmark.
          It uses TTC delay records from 2014 through January 2026 plus GTFS schedule structure to show
          where the network experiences the most friction and which high-service corridors absorb the most disruption.
        </p>
      </div>
      <aside class="hero-card hero-side">
        <div class="stamp">
          <strong>Generated:</strong> {payload["generated_at"]}<br>
          <strong>Source:</strong> TTC CSV folders supplied for the project<br>
          <strong>Dashboard file:</strong> Self-contained HTML
        </div>
        <div class="method-note">
          <strong>Ridership note</strong>
          Direct ridership counts are not present in the provided CSVs. The dashboard uses GTFS scheduled trips and route stop coverage as a service-demand proxy, so “ridership-sensitive” findings should be read as exposure to passenger demand rather than exact boardings.
        </div>
      </aside>
    </section>

    <div class="controls" id="mode-controls"></div>

    <section class="metrics" id="metrics"></section>

    <section class="grid">
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>All-Time Disruption Trend</h2>
            <p>Monthly incident volume and accumulated delay minutes across the available history.</p>
          </div>
        </div>
        <div id="trend-chart" class="chart"></div>
      </div>
      <div class="stack">
        <div class="panel">
          <div class="panel-head">
            <div>
              <h2>Analyst Readout</h2>
              <p>High-signal observations based on the selected mode.</p>
            </div>
          </div>
          <ul id="insights" class="insight-list"></ul>
        </div>
        <div class="panel">
          <div class="panel-head">
            <div>
              <h2>Coverage Snapshot</h2>
              <p>Data horizon, recent window, and GTFS benchmark coverage.</p>
            </div>
          </div>
          <div id="coverage-table"></div>
        </div>
      </div>
    </section>

    <section class="grid">
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>Service Exposure vs. Disruption Load</h2>
            <p>Each bubble is a route. Higher vertical position means more incidents per 1,000 scheduled trips.</p>
          </div>
        </div>
        <div id="route-chart" class="chart"></div>
      </div>
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>Highest Pressure Locations</h2>
            <p>Recent-period hotspots by incident count and delay minutes.</p>
          </div>
        </div>
        <div id="hotspot-chart" class="chart compact"></div>
      </div>
    </section>

    <section class="grid">
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>Day and Hour Heatmap</h2>
            <p>Delay-minute intensity across the week for the recent 2025-2026 period.</p>
          </div>
        </div>
        <div id="heatmap-chart" class="chart"></div>
      </div>
      <div class="stack">
        <div class="panel">
          <div class="panel-head">
            <div>
              <h2>Dominant Delay Codes</h2>
              <p>Most frequent recent causes in the selected mode.</p>
            </div>
          </div>
          <div id="cause-chart" class="chart compact"></div>
        </div>
        <div class="panel">
          <div class="panel-head">
            <div>
              <h2>Top Routes Table</h2>
              <p>Recent routes with the largest disruption burden after GTFS benchmarking.</p>
            </div>
          </div>
          <div id="route-table"></div>
          <div class="footnote">
            The source code-description lookup tables appear to be cross-labeled across TTC folders. This dashboard remaps them by observed code overlap before displaying descriptions.
          </div>
        </div>
      </div>
    </section>
  </div>

  <script>
    const DASHBOARD = {data_json};
    const MODES = ["all", "bus", "streetcar", "subway"];
    const MODE_LABELS = {{
      all: "All Modes",
      bus: "Bus",
      streetcar: "Streetcar",
      subway: "Subway"
    }};

    let activeMode = "all";

    const formatNumber = (value, digits = 0) =>
      new Intl.NumberFormat("en-CA", {{
        maximumFractionDigits: digits,
        minimumFractionDigits: digits
      }}).format(value ?? 0);

    const titleCase = (text) => text.charAt(0).toUpperCase() + text.slice(1);

    const byMode = (rows) => activeMode === "all" ? rows : rows.filter((row) => row.mode === activeMode);

    function renderControls() {{
      const host = document.getElementById("mode-controls");
      host.innerHTML = "";
      MODES.forEach((mode) => {{
        const button = document.createElement("button");
        button.className = `chip ${{mode === activeMode ? "active" : ""}}`;
        button.textContent = MODE_LABELS[mode];
        button.addEventListener("click", () => {{
          activeMode = mode;
          document.querySelectorAll(".chip").forEach((chip) => chip.classList.remove("active"));
          button.classList.add("active");
          renderAll();
        }});
        host.appendChild(button);
      }});
    }}

    function summarizeCoverage() {{
      const rows = byMode(DASHBOARD.coverage);
      if (activeMode === "all") {{
        const allTimeIncidents = rows.reduce((sum, row) => sum + row.all_time_incidents, 0);
        const recentIncidents = rows.reduce((sum, row) => sum + row.recent_incidents, 0);
        const recentDelay = rows.reduce((sum, row) => sum + row.recent_delay, 0);
        const benchmarkedRoutes = rows.reduce((sum, row) => sum + row.benchmarked_routes, 0);
        const starts = rows.map((row) => row.history_start).sort();
        const ends = rows.map((row) => row.history_end).sort();
        return {{
          all_time_incidents: allTimeIncidents,
          recent_incidents: recentIncidents,
          recent_delay: recentDelay,
          benchmarked_routes: benchmarkedRoutes,
          history_start: starts[0],
          history_end: ends[ends.length - 1]
        }};
      }}
      return rows[0];
    }}

    function renderMetrics() {{
      const coverage = summarizeCoverage();
      const recentRows = byMode(DASHBOARD.recent_monthly);
      const allTimeRows = byMode(DASHBOARD.historical_monthly);
      const avgRecentDelay = recentRows.reduce((sum, row) => sum + row.total_delay, 0) /
        Math.max(recentRows.reduce((sum, row) => sum + row.incidents, 0), 1);
      const lastMonth = recentRows.map((row) => row.month).sort().slice(-1)[0];
      const cards = [
        {{
          label: "All-Time Incidents",
          value: formatNumber(coverage.all_time_incidents),
          sub: `${{coverage.history_start}} to ${{coverage.history_end}}`
        }},
        {{
          label: "Recent Incidents",
          value: formatNumber(coverage.recent_incidents),
          sub: `2025-01 to 2026-01 monitoring window`
        }},
        {{
          label: "Recent Delay Minutes",
          value: formatNumber(coverage.recent_delay),
          sub: `Average ${{formatNumber(avgRecentDelay, 2)}} minutes per incident`
        }},
        {{
          label: "Benchmarked Routes",
          value: formatNumber(coverage.benchmarked_routes),
          sub: `GTFS service baseline aligned with delay feeds`
        }},
      ];
      const host = document.getElementById("metrics");
      host.innerHTML = cards.map((card) => `
        <article class="panel metric">
          <div class="label">${{card.label}}</div>
          <div class="value">${{card.value}}</div>
          <div class="sub">${{card.sub}}</div>
        </article>
      `).join("");
    }}

    function renderTrendChart() {{
      const rows = activeMode === "all" ? DASHBOARD.historical_monthly : byMode(DASHBOARD.historical_monthly);
      const modes = activeMode === "all" ? ["bus", "streetcar", "subway"] : [activeMode];
      const traces = modes.map((mode) => {{
        const subset = rows.filter((row) => row.mode === mode);
        return {{
          x: subset.map((row) => row.month),
          y: subset.map((row) => row.total_delay),
          mode: "lines",
          name: MODE_LABELS[mode],
          line: {{
            color: DASHBOARD.mode_colors[mode],
            width: 3
          }},
          hovertemplate:
            "<b>%{{x}}</b><br>Total delay: %{{y:,.0f}} min<br>Incidents: %{{customdata[0]:,.0f}}<br>Avg delay: %{{customdata[1]:.2f}} min<extra></extra>",
          customdata: subset.map((row) => [row.incidents, row.avg_delay])
        }};
      }});

      Plotly.newPlot("trend-chart", traces, {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 52, r: 16, t: 12, b: 46 }},
        xaxis: {{ gridcolor: "rgba(17,33,45,0.08)" }},
        yaxis: {{ title: "Delay Minutes", gridcolor: "rgba(17,33,45,0.08)" }},
        legend: {{ orientation: "h", y: 1.15 }},
        hovermode: "x unified"
      }}, {{ displayModeBar: false, responsive: true }});
    }}

    function renderRouteChart() {{
      const rows = byMode(DASHBOARD.route_benchmark)
        .filter((row) => row.scheduled_trips && row.incidents_per_1000_trips)
        .sort((a, b) => b.total_delay - a.total_delay);

      const modes = activeMode === "all" ? ["bus", "streetcar", "subway"] : [activeMode];
      const traces = modes.map((mode) => {{
        const subset = rows.filter((row) => row.mode === mode);
        return {{
          type: "scatter",
          mode: "markers",
          name: MODE_LABELS[mode],
          x: subset.map((row) => row.scheduled_trips),
          y: subset.map((row) => row.incidents_per_1000_trips),
          text: subset.map((row) => row.route_display),
          customdata: subset.map((row) => [row.total_delay, row.avg_delay, row.unique_stops, row.incidents]),
          marker: {{
            size: subset.map((row) => Math.max(12, Math.sqrt(row.total_delay) / 2.2)),
            color: DASHBOARD.mode_colors[mode],
            opacity: 0.72,
            line: {{ width: 1, color: "rgba(255,255,255,0.8)" }}
          }},
          hovertemplate:
            "<b>%{{text}}</b><br>Scheduled trips: %{{x:,.0f}}<br>Incidents / 1k trips: %{{y:.2f}}<br>Total delay: %{{customdata[0]:,.0f}} min<br>Avg delay: %{{customdata[1]:.2f}} min<br>Unique stops: %{{customdata[2]:,.0f}}<br>Incidents: %{{customdata[3]:,.0f}}<extra></extra>"
        }};
      }});

      Plotly.newPlot("route-chart", traces, {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 52, r: 16, t: 14, b: 48 }},
        xaxis: {{ title: "Scheduled Trips (GTFS Snapshot)", gridcolor: "rgba(17,33,45,0.08)" }},
        yaxis: {{ title: "Incidents per 1,000 Trips", gridcolor: "rgba(17,33,45,0.08)" }},
        legend: {{ orientation: "h", y: 1.15 }}
      }}, {{ displayModeBar: false, responsive: true }});
    }}

    function renderHotspots() {{
      const rows = byMode(DASHBOARD.hotspots).sort((a, b) => b.incidents - a.incidents).slice(0, 12);
      Plotly.newPlot("hotspot-chart", [{{
        type: "bar",
        orientation: "h",
        y: rows.map((row) => row.location).reverse(),
        x: rows.map((row) => row.incidents).reverse(),
        marker: {{
          color: rows.map((row) => DASHBOARD.mode_colors[row.mode] || "#d62828")
        }},
        customdata: rows.map((row) => [row.total_delay, row.avg_delay]).reverse(),
        hovertemplate:
          "<b>%{{y}}</b><br>Incidents: %{{x:,.0f}}<br>Total delay: %{{customdata[0]:,.0f}} min<br>Avg delay: %{{customdata[1]:.2f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 160, r: 16, t: 8, b: 36 }},
        xaxis: {{ gridcolor: "rgba(17,33,45,0.08)", title: "Incidents" }},
        yaxis: {{ automargin: true }}
      }}, {{ displayModeBar: false, responsive: true }});
    }}

    function renderHeatmap() {{
      const rows = byMode(DASHBOARD.heatmap);
      const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
      const hours = Array.from({{ length: 24 }}, (_, index) => index);
      const lookup = new Map(rows.map((row) => [`${{row.weekday}}-${{row.hour}}`, row.total_delay]));
      const z = days.map((day) => hours.map((hour) => lookup.get(`${{day}}-${{hour}}`) || 0));

      Plotly.newPlot("heatmap-chart", [{{
        type: "heatmap",
        x: hours,
        y: days,
        z,
        colorscale: [
          [0, "#fff0d9"],
          [0.35, "#fcbf49"],
          [0.7, "#f77f00"],
          [1, "#d62828"]
        ],
        hovertemplate:
          "<b>%{{y}}</b><br>Hour: %{{x}}:00<br>Total delay: %{{z:,.0f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 84, r: 16, t: 10, b: 42 }},
        xaxis: {{ title: "Hour of Day" }},
        yaxis: {{ autorange: "reversed" }}
      }}, {{ displayModeBar: false, responsive: true }});
    }}

    function renderCauses() {{
      const rows = byMode(DASHBOARD.causes).sort((a, b) => b.incidents - a.incidents).slice(0, 10);
      Plotly.newPlot("cause-chart", [{{
        type: "bar",
        x: rows.map((row) => row.incidents),
        y: rows.map((row) => row.label),
        orientation: "h",
        marker: {{ color: rows.map((row) => DASHBOARD.mode_colors[row.mode] || "#d62828") }},
        customdata: rows.map((row) => [row.total_delay]),
        hovertemplate:
          "<b>%{{y}}</b><br>Incidents: %{{x:,.0f}}<br>Total delay: %{{customdata[0]:,.0f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 180, r: 16, t: 8, b: 36 }},
        xaxis: {{ title: "Incidents", gridcolor: "rgba(17,33,45,0.08)" }},
        yaxis: {{ automargin: true }}
      }}, {{ displayModeBar: false, responsive: true }});
    }}

    function renderInsights() {{
      const recentRows = byMode(DASHBOARD.recent_monthly);
      const routeRows = byMode(DASHBOARD.route_benchmark).filter((row) => row.scheduled_trips && row.incidents >= 25);
      const hotspotRows = byMode(DASHBOARD.hotspots);
      const heatRows = byMode(DASHBOARD.heatmap);
      const causeRows = byMode(DASHBOARD.causes);

      const topMonth = [...recentRows].sort((a, b) => b.total_delay - a.total_delay)[0];
      const topRoute = [...routeRows].sort((a, b) => (b.incidents_per_1000_trips || 0) - (a.incidents_per_1000_trips || 0))[0];
      const topHotspot = [...hotspotRows].sort((a, b) => b.incidents - a.incidents)[0];
      const peakCell = [...heatRows].sort((a, b) => b.total_delay - a.total_delay)[0];
      const topCause = [...causeRows].sort((a, b) => b.incidents - a.incidents)[0];

      const insights = [
        topMonth ? `<strong>Peak month:</strong> ${{topMonth.month}} recorded ${{formatNumber(topMonth.total_delay)}} delay minutes from ${{formatNumber(topMonth.incidents)}} incidents.` : null,
        topRoute ? `<strong>Most pressure-exposed route:</strong> ${{topRoute.route_display}} posted ${{formatNumber(topRoute.incidents_per_1000_trips, 2)}} incidents per 1,000 scheduled trips.` : null,
        topHotspot ? `<strong>Top hotspot:</strong> ${{topHotspot.location}} logged ${{formatNumber(topHotspot.incidents)}} recent incidents totaling ${{formatNumber(topHotspot.total_delay)}} delay minutes.` : null,
        peakCell ? `<strong>Peak operating window:</strong> ${{peakCell.weekday}} around ${{String(peakCell.hour).padStart(2, "0")}}:00 shows the highest delay-minute concentration.` : null,
        topCause ? `<strong>Leading cause code:</strong> ${{topCause.label}} appears ${{formatNumber(topCause.incidents)}} times in the recent monitoring window.` : null
      ].filter(Boolean).slice(0, 4);

      document.getElementById("insights").innerHTML = insights.map((text) => `<li>${{text}}</li>`).join("");
    }}

    function renderCoverageTable() {{
      const rows = byMode(DASHBOARD.coverage);
      document.getElementById("coverage-table").innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Mode</th>
              <th>History</th>
              <th>Recent Window</th>
              <th>Benchmarked Routes</th>
            </tr>
          </thead>
          <tbody>
            ${{rows.map((row) => `
              <tr>
                <td>${{MODE_LABELS[row.mode]}}</td>
                <td>${{row.history_start}} to ${{row.history_end}}</td>
                <td>${{formatNumber(row.recent_incidents)}} incidents</td>
                <td>${{formatNumber(row.benchmarked_routes)}}</td>
              </tr>
            `).join("")}}
          </tbody>
        </table>
      `;
    }}

    function renderRouteTable() {{
      const rows = byMode(DASHBOARD.route_benchmark)
        .filter((row) => row.scheduled_trips && row.incidents_per_1000_trips)
        .sort((a, b) => b.total_delay - a.total_delay)
        .slice(0, 10);

      document.getElementById("route-table").innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Route</th>
              <th>Incidents</th>
              <th>Total Delay</th>
              <th>Trips</th>
              <th>Incidents / 1k Trips</th>
            </tr>
          </thead>
          <tbody>
            ${{rows.map((row) => `
              <tr>
                <td>${{row.route_display}}</td>
                <td>${{formatNumber(row.incidents)}}</td>
                <td>${{formatNumber(row.total_delay)}} min</td>
                <td>${{formatNumber(row.scheduled_trips)}}</td>
                <td>${{formatNumber(row.incidents_per_1000_trips, 2)}}</td>
              </tr>
            `).join("")}}
          </tbody>
        </table>
      `;
    }}

    function renderAll() {{
      renderMetrics();
      renderTrendChart();
      renderRouteChart();
      renderHotspots();
      renderHeatmap();
      renderCauses();
      renderInsights();
      renderCoverageTable();
      renderRouteTable();
    }}

    renderControls();
    renderAll();
  </script>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    payload = build_dashboard_payload(args.data_root)
    html = dashboard_html(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    print(f"Wrote dashboard to {args.output}")


if __name__ == "__main__":
    main()
