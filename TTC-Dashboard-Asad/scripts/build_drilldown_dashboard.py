#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

import build_dashboard as base

# Update these paths as needed to point to your local data and output locations.
OUTPUT_PATH = "/Users/muhammadasad/Documents/TTC-PULSE/dist"

DEFAULT_OUTPUT = Path(OUTPUT_PATH + "/drilldown_dashboard.html")
DELAY_BAND_ORDER = ["0-4 min", "5-14 min", "15-29 min", "30-59 min", "60+ min"]
MONTH_ORDER = list(range(1, 13))
MAX_LOCATION_DRILLDOWN_PER_MODE = 400


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate an advanced TTC drilldown dashboard."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=base.DEFAULT_DATA_ROOT,
        help="Root directory that contains the TTC CSV folders.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Where to write the generated HTML dashboard.",
    )
    return parser.parse_args()


def iter_source_frames(data_root: Path) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for mode, config in base.MODE_CONFIG.items():
        for path in sorted((data_root / config["history_dir"]).glob(config["history_pattern"])):
            if not base.valid_history_file(path):
                continue
            frame = base.standardize_frame(mode, path, recent=False)
            if not frame.empty:
                frames.append(frame)
        recent = base.standardize_frame(mode, data_root / config["recent_path"], recent=True)
        if not recent.empty:
            frames.append(recent)
    return frames


def add_time_columns(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["year"] = enriched["date"].dt.year.astype(int)
    enriched["month_number"] = enriched["date"].dt.month.astype(int)
    enriched["month_name"] = enriched["date"].dt.strftime("%b")
    enriched["year_month"] = enriched["date"].dt.to_period("M").astype(str)
    enriched["delay_band"] = pd.cut(
        enriched["delay_minutes"],
        bins=[-0.1, 4.99, 14.99, 29.99, 59.99, float("inf")],
        labels=DELAY_BAND_ORDER,
        ordered=True,
    )
    return enriched


def build_all_data(data_root: Path) -> pd.DataFrame:
    frames = iter_source_frames(data_root)
    if not frames:
        raise RuntimeError("No TTC source files were loaded.")
    all_data = pd.concat(frames, ignore_index=True)
    all_data = add_time_columns(all_data)
    return all_data


def build_route_display_lookup(all_data: pd.DataFrame, gtfs_service: pd.DataFrame) -> dict[tuple[str, str], str]:
    lookup: dict[tuple[str, str], str] = {}
    gtfs_rows = gtfs_service.dropna(subset=["route_key"]).drop_duplicates(["mode", "route_key"])
    for row in gtfs_rows.itertuples(index=False):
        lookup[(row.mode, row.route_key)] = row.route_display

    fallback = (
        all_data.dropna(subset=["route_key", "line_raw"])
        .groupby(["mode", "route_key"])["line_raw"]
        .agg(base.modal_label)
    )
    for (mode, route_key), label in fallback.items():
        lookup.setdefault((mode, route_key), label if label else f"{route_key}")
    return lookup


def with_route_display(frame: pd.DataFrame, route_lookup: dict[tuple[str, str], str]) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["route_display"] = [
        route_lookup.get((mode, route_key))
        for mode, route_key in zip(enriched["mode"], enriched["route_key"])
    ]
    return enriched


def aggregate_metrics(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    grouped = (
        frame.groupby(group_cols, as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
            avg_delay=("delay_minutes", "mean"),
            median_delay=("delay_minutes", "median"),
            peak_delay=("delay_minutes", "max"),
        )
        .sort_values(group_cols)
    )
    grouped["total_delay"] = grouped["total_delay"].round(1)
    grouped["avg_delay"] = grouped["avg_delay"].round(2)
    grouped["median_delay"] = grouped["median_delay"].round(2)
    return grouped


def build_overview_tables(all_data: pd.DataFrame) -> dict[str, pd.DataFrame]:
    yearly_mode = aggregate_metrics(all_data, ["mode", "year"])
    monthly_mode = aggregate_metrics(all_data, ["mode", "year_month"])
    mode_delay_bands = (
        all_data.groupby(["mode", "year", "delay_band"], observed=True, as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
        )
        .sort_values(["mode", "year", "delay_band"])
    )
    mode_delay_bands["total_delay"] = mode_delay_bands["total_delay"].round(1)
    combo = yearly_mode.copy()
    combo["frequency_rank"] = combo.groupby("mode")["incidents"].rank(ascending=False, method="dense")
    combo["delay_rank"] = combo.groupby("mode")["total_delay"].rank(ascending=False, method="dense")
    return {
        "yearly_mode": yearly_mode,
        "monthly_mode": monthly_mode,
        "mode_delay_bands": mode_delay_bands,
        "yearly_combo": combo,
    }


def build_route_tables(
    all_data: pd.DataFrame,
    route_lookup: dict[tuple[str, str], str],
) -> dict[str, pd.DataFrame]:
    route_data = with_route_display(all_data.dropna(subset=["route_key"]).copy(), route_lookup)
    route_summary = (
        route_data.groupby(["mode", "route_key", "route_display"], as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
            avg_delay=("delay_minutes", "mean"),
            median_delay=("delay_minutes", "median"),
            peak_delay=("delay_minutes", "max"),
            locations=("location", pd.Series.nunique),
            active_years=("year", pd.Series.nunique),
            latest_year=("year", "max"),
        )
        .sort_values(["mode", "total_delay"], ascending=[True, False])
    )
    route_summary["total_delay"] = route_summary["total_delay"].round(1)
    route_summary["avg_delay"] = route_summary["avg_delay"].round(2)
    route_summary["median_delay"] = route_summary["median_delay"].round(2)

    route_yearly = aggregate_metrics(route_data, ["mode", "route_key", "route_display", "year"])
    route_monthly = aggregate_metrics(
        route_data, ["mode", "route_key", "route_display", "year_month"]
    )
    route_delay_bands = (
        route_data.groupby(
            ["mode", "route_key", "route_display", "delay_band"],
            observed=True,
            as_index=False,
        )
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
        )
        .sort_values(["mode", "route_display", "delay_band"])
    )
    route_delay_bands["total_delay"] = route_delay_bands["total_delay"].round(1)
    route_heatmap = (
        route_data.dropna(subset=["weekday", "hour"])
        .groupby(
            ["mode", "route_key", "route_display", "weekday", "hour"],
            as_index=False,
        )
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
        )
    )
    route_heatmap["weekday"] = pd.Categorical(
        route_heatmap["weekday"], base.WEEKDAY_ORDER, ordered=True
    )
    route_heatmap = route_heatmap.sort_values(
        ["mode", "route_display", "weekday", "hour"]
    ).reset_index(drop=True)
    route_heatmap["total_delay"] = route_heatmap["total_delay"].round(1)

    return {
        "route_summary": route_summary,
        "route_yearly": route_yearly,
        "route_monthly": route_monthly,
        "route_delay_bands": route_delay_bands,
        "route_heatmap": route_heatmap,
    }


def build_location_tables(all_data: pd.DataFrame) -> dict[str, pd.DataFrame]:
    location_data = all_data.dropna(subset=["location"]).copy()
    location_summary = (
        location_data.groupby(["mode", "location"], as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
            avg_delay=("delay_minutes", "mean"),
            median_delay=("delay_minutes", "median"),
            peak_delay=("delay_minutes", "max"),
            active_years=("year", pd.Series.nunique),
        )
        .sort_values(["mode", "incidents"], ascending=[True, False])
    )
    location_summary["total_delay"] = location_summary["total_delay"].round(1)
    location_summary["avg_delay"] = location_summary["avg_delay"].round(2)
    location_summary["median_delay"] = location_summary["median_delay"].round(2)

    retained_summary = (
        location_summary.groupby("mode", group_keys=False)
        .head(MAX_LOCATION_DRILLDOWN_PER_MODE)
        .reset_index(drop=True)
    )
    allowed_locations = {
        (row.mode, row.location) for row in retained_summary.itertuples(index=False)
    }

    def keep_allowed(frame: pd.DataFrame) -> pd.DataFrame:
        return frame[
            [
                (mode, location) in allowed_locations
                for mode, location in zip(frame["mode"], frame["location"])
            ]
        ].copy()

    location_yearly = aggregate_metrics(location_data, ["mode", "location", "year"])
    location_monthly = aggregate_metrics(location_data, ["mode", "location", "year_month"])
    location_delay_bands = (
        location_data.groupby(
            ["mode", "location", "delay_band"],
            observed=True,
            as_index=False,
        )
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
        )
        .sort_values(["mode", "location", "delay_band"])
    )
    location_delay_bands["total_delay"] = location_delay_bands["total_delay"].round(1)

    location_calendar = (
        location_data.groupby(["mode", "location", "month_number", "month_name"], as_index=False)
        .agg(
            incidents=("delay_minutes", "size"),
            total_delay=("delay_minutes", "sum"),
            avg_delay=("delay_minutes", "mean"),
        )
        .sort_values(["mode", "location", "month_number"])
    )
    location_calendar["avg_delay"] = location_calendar["avg_delay"].round(2)
    location_calendar["total_delay"] = location_calendar["total_delay"].round(1)

    return {
        "location_summary": retained_summary,
        "location_yearly": keep_allowed(location_yearly),
        "location_monthly": keep_allowed(location_monthly),
        "location_delay_bands": keep_allowed(location_delay_bands),
        "location_calendar": keep_allowed(location_calendar),
    }


def build_coverage(all_data: pd.DataFrame) -> pd.DataFrame:
    coverage_rows = []
    for mode in base.MODE_CONFIG:
        subset = all_data[all_data["mode"] == mode]
        recent = subset[subset["is_recent"]]
        coverage_rows.append(
            {
                "mode": mode,
                "history_start": subset["date"].min().strftime("%Y-%m-%d"),
                "history_end": subset["date"].max().strftime("%Y-%m-%d"),
                "all_time_incidents": int(len(subset)),
                "all_time_delay": round(float(subset["delay_minutes"].sum()), 1),
                "recent_incidents": int(len(recent)),
                "recent_delay": round(float(recent["delay_minutes"].sum()), 1),
            }
        )
    return pd.DataFrame(coverage_rows)


def validate_aggregations(payload: dict) -> None:
    coverage = {row["mode"]: row for row in payload["coverage"]}
    yearly = pd.DataFrame(payload["yearly_mode"])
    route_summary = pd.DataFrame(payload["route_summary"])

    for mode, row in coverage.items():
        mode_yearly = yearly[yearly["mode"] == mode]
        incidents_from_yearly = int(mode_yearly["incidents"].sum())
        delay_from_yearly = round(float(mode_yearly["total_delay"].sum()), 1)
        if incidents_from_yearly != row["all_time_incidents"]:
            raise ValueError(f"Yearly incident check failed for {mode}.")
        if abs(delay_from_yearly - row["all_time_delay"]) > 0.1:
            raise ValueError(f"Yearly delay check failed for {mode}.")

        if mode == "subway":
            continue
        mode_routes = route_summary[route_summary["mode"] == mode]
        if int(mode_routes["incidents"].sum()) < row["all_time_incidents"] * 0.85:
            raise ValueError(f"Route coverage unexpectedly low for {mode}.")


def to_records(frame: pd.DataFrame) -> list[dict]:
    return json.loads(frame.to_json(orient="records"))


def build_payload(data_root: Path) -> dict:
    all_data = build_all_data(data_root)
    gtfs_service = base.build_gtfs_service(data_root)
    route_lookup = build_route_display_lookup(all_data, gtfs_service)
    overview = build_overview_tables(all_data)
    route_tables = build_route_tables(all_data, route_lookup)
    location_tables = build_location_tables(all_data)
    coverage = build_coverage(all_data)

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_root": str(data_root),
        "mode_colors": {mode: config["color"] for mode, config in base.MODE_CONFIG.items()},
        "coverage": to_records(coverage),
        "yearly_mode": to_records(overview["yearly_mode"]),
        "monthly_mode": to_records(overview["monthly_mode"]),
        "mode_delay_bands": to_records(overview["mode_delay_bands"]),
        "yearly_combo": to_records(overview["yearly_combo"]),
        "route_summary": to_records(route_tables["route_summary"]),
        "route_yearly": to_records(route_tables["route_yearly"]),
        "route_monthly": to_records(route_tables["route_monthly"]),
        "route_delay_bands": to_records(route_tables["route_delay_bands"]),
        "route_heatmap": to_records(route_tables["route_heatmap"]),
        "location_summary": to_records(location_tables["location_summary"]),
        "location_yearly": to_records(location_tables["location_yearly"]),
        "location_monthly": to_records(location_tables["location_monthly"]),
        "location_delay_bands": to_records(location_tables["location_delay_bands"]),
        "location_calendar": to_records(location_tables["location_calendar"]),
    }
    validate_aggregations(payload)
    return payload


def dashboard_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    delay_bands_json = json.dumps(DELAY_BAND_ORDER, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>TTC Drilldown Explorer</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=Source+Sans+3:wght@400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --paper: #f8f4ec;
      --ink: #10212d;
      --muted: #536572;
      --panel: rgba(255,255,255,0.82);
      --line: rgba(16,33,45,0.10);
      --bus: #d62828;
      --streetcar: #f77f00;
      --subway: #003049;
      --shadow: 0 22px 60px rgba(16,33,45,0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Source Sans 3", Georgia, serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(214,40,40,0.12), transparent 24%),
        radial-gradient(circle at 85% 0%, rgba(247,127,0,0.16), transparent 22%),
        linear-gradient(180deg, #fffaf4 0%, #f4eee3 100%);
    }}
    .shell {{
      width: min(1400px, calc(100vw - 28px));
      margin: 0 auto;
      padding: 24px 0 44px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid rgba(255,255,255,0.7);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
      padding: 18px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1.25fr 0.75fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    h1, h2, h3 {{
      font-family: "Space Grotesk", sans-serif;
      margin: 0;
    }}
    h1 {{
      font-size: clamp(2.2rem, 4vw, 3.8rem);
      line-height: 0.95;
      margin-bottom: 14px;
      max-width: 12ch;
    }}
    .kicker {{
      display: inline-block;
      border-radius: 999px;
      padding: 7px 12px;
      background: rgba(214,40,40,0.1);
      color: #b42020;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 700;
      font-size: 0.86rem;
      margin-bottom: 14px;
    }}
    .lede, .meta, .mini-note {{
      color: var(--muted);
      line-height: 1.55;
      margin: 0;
    }}
    .hero-side {{
      display: grid;
      gap: 14px;
      align-content: start;
    }}
    .mode-controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 16px;
    }}
    .chip {{
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      padding: 10px 16px;
      border-radius: 999px;
      font-weight: 700;
      cursor: pointer;
    }}
    .chip.active {{
      background: linear-gradient(135deg, #d62828, #9e1d1d);
      color: white;
      border-color: transparent;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }}
    .metric .label {{
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-size: 0.8rem;
      font-weight: 700;
    }}
    .metric .value {{
      font-family: "Space Grotesk", sans-serif;
      font-size: clamp(1.6rem, 2.8vw, 2.3rem);
      margin-top: 7px;
      margin-bottom: 4px;
    }}
    .grid-2 {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .grid-main {{
      display: grid;
      grid-template-columns: 0.8fr 1.2fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 8px;
    }}
    .panel-head p {{
      margin: 0;
      color: var(--muted);
      font-size: 0.94rem;
    }}
    .chart {{
      min-height: 340px;
    }}
    .chart-tall {{
      min-height: 390px;
    }}
    .selector-card {{
      display: grid;
      gap: 14px;
    }}
    .entity-pill {{
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(16,33,45,0.03);
    }}
    .entity-pill strong {{
      display: block;
      font-family: "Space Grotesk", sans-serif;
      margin-bottom: 3px;
    }}
    .split {{
      display: grid;
      grid-template-columns: 0.95fr 1.05fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .detail-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.94rem;
    }}
    th, td {{
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
    }}
    th {{
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-size: 0.79rem;
      font-family: "Space Grotesk", sans-serif;
    }}
    .footnote {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.5;
    }}
    @media (max-width: 1120px) {{
      .hero, .grid-2, .grid-main, .split, .detail-grid {{
        grid-template-columns: 1fr;
      }}
      .metrics {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
    }}
    @media (max-width: 680px) {{
      .metrics {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="panel">
        <div class="kicker">Advanced Drilldown Dashboard</div>
        <h1>TTC Multi-Level Route and Location Explorer</h1>
        <p class="lede">
          This second dashboard keeps the original work intact and adds year-wise, month-wise, mode-wise,
          frequency-wise, and delay-time-wise drilldowns. Click a route or a location to inspect its detailed trend profile.
        </p>
      </div>
      <div class="hero-side">
        <div class="panel">
          <p class="meta"><strong>Generated:</strong> {payload["generated_at"]}</p>
          <p class="meta"><strong>Validation:</strong> Cross-checks run across yearly, route, and location aggregates before output.</p>
          <p class="meta"><strong>Coverage:</strong> Historical archives plus the recent 2025-01-01 to 2026-01-31 window.</p>
        </div>
        <div class="panel">
          <p class="mini-note">
            Direct ridership counts are still not present in the source files, so route exposure remains a service proxy built from TTC GTFS.
            All route and location drilldowns use observed delay records from the supplied CSV archive only. The location explorer keeps the top hotspot catalog per mode so the dashboard stays responsive.
          </p>
        </div>
      </div>
    </section>

    <div id="mode-controls" class="mode-controls"></div>

    <section id="summary-metrics" class="metrics"></section>

    <section class="grid-2">
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>Year-Wise Mode Trend</h2>
            <p>Delay frequency and total delay by year for the selected transport mode.</p>
          </div>
        </div>
        <div id="yearly-mode-chart" class="chart-tall"></div>
      </div>
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>Frequency vs Delay Combination</h2>
            <p>Each point is a year, combining incident frequency, average delay, and total delay minutes.</p>
          </div>
        </div>
        <div id="combo-chart" class="chart-tall"></div>
      </div>
    </section>

    <section class="grid-2">
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>All-Time Month Trend</h2>
            <p>Month-by-month delay evolution across the selected mode.</p>
          </div>
        </div>
        <div id="monthly-mode-chart" class="chart"></div>
      </div>
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>Delay-Time Band Mix</h2>
            <p>How short, medium, and severe delays change through time.</p>
          </div>
        </div>
        <div id="delay-band-chart" class="chart"></div>
      </div>
    </section>

    <section id="overview-insight-anchor" class="panel" style="margin-bottom: 18px;">
      <div class="panel-head">
        <div>
          <h2>Clicked Visual Insight</h2>
          <p>Click any year, month, or delay-band point above and this section will update with deeper findings.</p>
        </div>
      </div>
      <div class="detail-grid">
        <div>
          <div id="overview-selection" class="entity-pill"></div>
          <ul id="overview-insight-list" class="footnote" style="margin: 12px 0 0; padding-left: 20px;"></ul>
        </div>
        <div id="overview-detail-table"></div>
      </div>
    </section>

    <section class="split">
      <div class="panel selector-card">
        <div class="panel-head">
          <div>
            <h2>Route Explorer</h2>
            <p>Click a route bar to open its yearly and monthly detail view.</p>
          </div>
        </div>
        <div id="route-selection" class="entity-pill"></div>
        <div id="route-list-chart" class="chart"></div>
      </div>
      <div class="panel" id="route-detail-anchor">
        <div class="panel-head">
          <div>
            <h2>Route Detail</h2>
            <p>Detailed analysis for the currently selected route.</p>
          </div>
        </div>
        <div class="detail-grid">
          <div id="route-yearly-chart" class="chart"></div>
          <div id="route-delay-band-chart" class="chart"></div>
          <div id="route-monthly-chart" class="chart"></div>
          <div id="route-heatmap-chart" class="chart"></div>
        </div>
        <div id="route-summary-table"></div>
      </div>
    </section>

    <section class="split">
      <div class="panel selector-card">
        <div class="panel-head">
          <div>
            <h2>Location Explorer</h2>
            <p>Click a location bar to inspect its separate yearly and monthly pattern.</p>
          </div>
        </div>
        <div id="location-selection" class="entity-pill"></div>
        <div id="location-list-chart" class="chart"></div>
      </div>
      <div class="panel" id="location-detail-anchor">
        <div class="panel-head">
          <div>
            <h2>Location Detail</h2>
            <p>Separate detail view for the selected station, stop, loop, or corridor location.</p>
          </div>
        </div>
        <div class="detail-grid">
          <div id="location-yearly-chart" class="chart"></div>
          <div id="location-delay-band-chart" class="chart"></div>
          <div id="location-monthly-chart" class="chart"></div>
          <div id="location-calendar-chart" class="chart"></div>
        </div>
        <div id="location-summary-table"></div>
      </div>
    </section>
  </div>

  <script>
    const DATA = {data_json};
    const DELAY_BANDS = {delay_bands_json};
    const MODE_LABELS = {{ all: "All Modes", bus: "Bus", streetcar: "Streetcar", subway: "Subway" }};
    const MODES = ["all", "bus", "streetcar", "subway"];
    let activeMode = "all";
    let selectedRouteKey = null;
    let selectedLocation = null;
    let selectedOverview = null;

    const formatNumber = (value, digits = 0) =>
      new Intl.NumberFormat("en-CA", {{
        maximumFractionDigits: digits,
        minimumFractionDigits: digits
      }}).format(value ?? 0);

    const byMode = (rows) => activeMode === "all" ? rows : rows.filter((row) => row.mode === activeMode);

    const modeColor = (mode) => DATA.mode_colors[mode] || "#d62828";

    function scrollToId(id) {{
      const el = document.getElementById(id);
      if (el) {{
        el.scrollIntoView({{ behavior: "smooth", block: "start" }});
      }}
    }}

    function renderModeControls() {{
      const host = document.getElementById("mode-controls");
      host.innerHTML = "";
      MODES.forEach((mode) => {{
        const button = document.createElement("button");
        button.className = `chip ${{mode === activeMode ? "active" : ""}}`;
        button.textContent = MODE_LABELS[mode];
        button.addEventListener("click", () => {{
          activeMode = mode;
          selectedRouteKey = null;
          selectedLocation = null;
          renderAll();
        }});
        host.appendChild(button);
      }});
    }}

    function ensureSelections() {{
      const routes = byMode(DATA.route_summary).sort((a, b) => b.total_delay - a.total_delay);
      const locations = byMode(DATA.location_summary).sort((a, b) => b.incidents - a.incidents);
      const routeKeys = new Set(routes.map((row) => `${{row.mode}}||${{row.route_key}}`));
      const locationKeys = new Set(locations.map((row) => `${{row.mode}}||${{row.location}}`));
      if (!selectedRouteKey || !routeKeys.has(selectedRouteKey)) {{
        selectedRouteKey = routes.length ? `${{routes[0].mode}}||${{routes[0].route_key}}` : null;
      }}
      if (!selectedLocation || !locationKeys.has(selectedLocation)) {{
        selectedLocation = locations.length ? `${{locations[0].mode}}||${{locations[0].location}}` : null;
      }}
      const yearly = byMode(DATA.yearly_mode).sort((a, b) => b.year - a.year);
      if (!selectedOverview) {{
        selectedOverview = yearly.length ? {{ kind: "year", value: yearly[0].year }} : null;
      }}
    }}

    function renderMetrics() {{
      const rows = byMode(DATA.coverage);
      const summary = activeMode === "all"
        ? {{
            incidents: rows.reduce((sum, row) => sum + row.all_time_incidents, 0),
            delay: rows.reduce((sum, row) => sum + row.all_time_delay, 0),
            recentIncidents: rows.reduce((sum, row) => sum + row.recent_incidents, 0),
            recentDelay: rows.reduce((sum, row) => sum + row.recent_delay, 0),
            start: rows.map((row) => row.history_start).sort()[0],
            end: rows.map((row) => row.history_end).sort().slice(-1)[0],
          }}
        : {{
            incidents: rows[0].all_time_incidents,
            delay: rows[0].all_time_delay,
            recentIncidents: rows[0].recent_incidents,
            recentDelay: rows[0].recent_delay,
            start: rows[0].history_start,
            end: rows[0].history_end,
          }};

      const cards = [
        {{ label: "All-Time Incidents", value: formatNumber(summary.incidents), sub: `${{summary.start}} to ${{summary.end}}` }},
        {{ label: "All-Time Delay Minutes", value: formatNumber(summary.delay), sub: "Observed TTC delay minutes from the source CSVs" }},
        {{ label: "Recent Incidents", value: formatNumber(summary.recentIncidents), sub: "Recent window: 2025-01-01 to 2026-01-31" }},
        {{ label: "Recent Delay Minutes", value: formatNumber(summary.recentDelay), sub: "Validated against mode-level summaries" }},
      ];
      document.getElementById("summary-metrics").innerHTML = cards.map((card) => `
        <div class="panel metric">
          <div class="label">${{card.label}}</div>
          <div class="value">${{card.value}}</div>
          <div class="mini-note">${{card.sub}}</div>
        </div>
      `).join("");
    }}

    function renderYearlyMode() {{
      const rows = byMode(DATA.yearly_mode);
      const traces = activeMode === "all"
        ? ["bus", "streetcar", "subway"].map((mode) => {{
            const subset = rows.filter((row) => row.mode === mode);
            return {{
              x: subset.map((row) => row.year),
              y: subset.map((row) => row.incidents),
              type: "bar",
              name: MODE_LABELS[mode],
              marker: {{ color: modeColor(mode) }},
              customdata: subset.map((row) => [row.total_delay, row.avg_delay]),
              hovertemplate: "<b>%{{x}}</b><br>Incidents: %{{y:,.0f}}<br>Total delay: %{{customdata[0]:,.0f}} min<br>Avg delay: %{{customdata[1]:.2f}} min<extra></extra>"
            }};
          }})
        : [{{
            x: rows.map((row) => row.year),
            y: rows.map((row) => row.incidents),
            type: "bar",
            marker: {{ color: modeColor(activeMode) }},
            customdata: rows.map((row) => [row.total_delay, row.avg_delay]),
            hovertemplate: "<b>%{{x}}</b><br>Incidents: %{{y:,.0f}}<br>Total delay: %{{customdata[0]:,.0f}} min<br>Avg delay: %{{customdata[1]:.2f}} min<extra></extra>"
          }}];

      Plotly.newPlot("yearly-mode-chart", traces, {{
        barmode: activeMode === "all" ? "group" : "relative",
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 52, r: 12, t: 12, b: 42 }},
        xaxis: {{ title: "Year" }},
        yaxis: {{ title: "Incident Frequency", gridcolor: "rgba(16,33,45,0.08)" }},
        legend: {{ orientation: "h", y: 1.12 }}
      }}, {{ displayModeBar: false, responsive: true }});
      document.getElementById("yearly-mode-chart").on("plotly_click", (event) => {{
        selectedOverview = {{ kind: "year", value: event.points[0].x }};
        renderOverviewInsight();
        scrollToId("overview-insight-anchor");
      }});
    }}

    function renderComboChart() {{
      const rows = byMode(DATA.yearly_combo);
      const traces = activeMode === "all"
        ? ["bus", "streetcar", "subway"].map((mode) => {{
            const subset = rows.filter((row) => row.mode === mode);
            return {{
              x: subset.map((row) => row.incidents),
              y: subset.map((row) => row.avg_delay),
              text: subset.map((row) => row.year),
              mode: "markers+text",
              type: "scatter",
              textposition: "top center",
              name: MODE_LABELS[mode],
              marker: {{
                color: modeColor(mode),
                size: subset.map((row) => Math.max(14, Math.sqrt(row.total_delay) / 7)),
                opacity: 0.72,
              }},
              customdata: subset.map((row) => [row.total_delay]),
              hovertemplate: "<b>%{{text}}</b><br>Incidents: %{{x:,.0f}}<br>Avg delay: %{{y:.2f}} min<br>Total delay: %{{customdata[0]:,.0f}} min<extra></extra>"
            }};
          }})
        : [{{
            x: rows.map((row) => row.incidents),
            y: rows.map((row) => row.avg_delay),
            text: rows.map((row) => row.year),
            mode: "markers+text",
            textposition: "top center",
            type: "scatter",
            marker: {{
              color: modeColor(activeMode),
              size: rows.map((row) => Math.max(14, Math.sqrt(row.total_delay) / 7)),
              opacity: 0.75,
            }},
            customdata: rows.map((row) => [row.total_delay]),
            hovertemplate: "<b>%{{text}}</b><br>Incidents: %{{x:,.0f}}<br>Avg delay: %{{y:.2f}} min<br>Total delay: %{{customdata[0]:,.0f}} min<extra></extra>"
          }}];

      Plotly.newPlot("combo-chart", traces, {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 52, r: 12, t: 12, b: 42 }},
        xaxis: {{ title: "Incident Frequency", gridcolor: "rgba(16,33,45,0.08)" }},
        yaxis: {{ title: "Average Delay (Minutes)", gridcolor: "rgba(16,33,45,0.08)" }},
        legend: {{ orientation: "h", y: 1.12 }}
      }}, {{ displayModeBar: false, responsive: true }});
      document.getElementById("combo-chart").on("plotly_click", (event) => {{
        selectedOverview = {{ kind: "year", value: Number(event.points[0].text) }};
        renderOverviewInsight();
        scrollToId("overview-insight-anchor");
      }});
    }}

    function renderMonthlyMode() {{
      const rows = byMode(DATA.monthly_mode);
      const modes = activeMode === "all" ? ["bus", "streetcar", "subway"] : [activeMode];
      const traces = modes.map((mode) => {{
        const subset = rows.filter((row) => row.mode === mode);
        return {{
          x: subset.map((row) => row.year_month),
          y: subset.map((row) => row.total_delay),
          mode: "lines",
          line: {{ color: modeColor(mode), width: 3 }},
          name: MODE_LABELS[mode],
          customdata: subset.map((row) => [row.incidents, row.avg_delay]),
          hovertemplate: "<b>%{{x}}</b><br>Total delay: %{{y:,.0f}} min<br>Incidents: %{{customdata[0]:,.0f}}<br>Avg delay: %{{customdata[1]:.2f}} min<extra></extra>"
        }};
      }});
      Plotly.newPlot("monthly-mode-chart", traces, {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 52, r: 12, t: 10, b: 40 }},
        xaxis: {{ title: "Month" }},
        yaxis: {{ title: "Total Delay Minutes", gridcolor: "rgba(16,33,45,0.08)" }},
        legend: {{ orientation: "h", y: 1.12 }}
      }}, {{ displayModeBar: false, responsive: true }});
      document.getElementById("monthly-mode-chart").on("plotly_click", (event) => {{
        selectedOverview = {{ kind: "month", value: event.points[0].x }};
        renderOverviewInsight();
        scrollToId("overview-insight-anchor");
      }});
    }}

    function renderDelayBandChart() {{
      const rows = byMode(DATA.mode_delay_bands);
      const traces = DELAY_BANDS.map((band) => {{
        const subset = rows.filter((row) => row.delay_band === band);
        const x = [...new Set(subset.map((row) => row.year))].sort((a, b) => a - b);
        return {{
          x,
          y: x.map((year) => {{
            const yearRows = subset.filter((row) => row.year === year);
            return yearRows.reduce((sum, row) => sum + row.incidents, 0);
          }}),
          stackgroup: "one",
          mode: "lines",
          name: band,
          line: {{ width: 1.5 }},
          hovertemplate: `<b>${{band}}</b><br>Year: %{{x}}<br>Incidents: %{{y:,.0f}}<extra></extra>`
        }};
      }});
      Plotly.newPlot("delay-band-chart", traces, {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 52, r: 12, t: 10, b: 40 }},
        xaxis: {{ title: "Year" }},
        yaxis: {{ title: "Incident Frequency", gridcolor: "rgba(16,33,45,0.08)" }},
        legend: {{ orientation: "h", y: 1.15 }}
      }}, {{ displayModeBar: false, responsive: true }});
      document.getElementById("delay-band-chart").on("plotly_click", (event) => {{
        selectedOverview = {{
          kind: "band-year",
          value: event.points[0].x,
          band: event.points[0].data.name
        }};
        renderOverviewInsight();
        scrollToId("overview-insight-anchor");
      }});
    }}

    function topRow(rows, metric) {{
      const copy = [...rows];
      copy.sort((a, b) => (b[metric] || 0) - (a[metric] || 0));
      return copy[0] || null;
    }}

    function renderOverviewInsight() {{
      if (!selectedOverview) return;

      let title = "";
      let statRows = [];
      let topRoute = null;
      let topLocation = null;
      let bullets = [];

      if (selectedOverview.kind === "year") {{
        const year = Number(selectedOverview.value);
        statRows = byMode(DATA.yearly_mode).filter((row) => row.year === year);
        topRoute = topRow(byMode(DATA.route_yearly).filter((row) => row.year === year), "total_delay");
        topLocation = topRow(byMode(DATA.location_yearly).filter((row) => row.year === year), "incidents");
        const totals = statRows.reduce((acc, row) => {{
          acc.incidents += row.incidents;
          acc.delay += row.total_delay;
          return acc;
        }}, {{ incidents: 0, delay: 0 }});
        title = "Year " + year;
        bullets = [
          `${{formatNumber(totals.incidents)}} incidents and ${{formatNumber(totals.delay)}} total delay minutes across the selected mode scope.`,
          topRoute ? `Top delay route: ${{topRoute.route_display}} with ${{formatNumber(topRoute.total_delay)}} delay minutes.` : "No route summary available for this year.",
          topLocation ? `Top incident location: ${{topLocation.location}} with ${{formatNumber(topLocation.incidents)}} incidents.` : "No location summary available for this year."
        ];
      }} else if (selectedOverview.kind === "month") {{
        const month = selectedOverview.value;
        statRows = byMode(DATA.monthly_mode).filter((row) => row.year_month === month);
        topRoute = topRow(byMode(DATA.route_monthly).filter((row) => row.year_month === month), "total_delay");
        topLocation = topRow(byMode(DATA.location_monthly).filter((row) => row.year_month === month), "incidents");
        const totals = statRows.reduce((acc, row) => {{
          acc.incidents += row.incidents;
          acc.delay += row.total_delay;
          return acc;
        }}, {{ incidents: 0, delay: 0 }});
        title = "Month " + month;
        bullets = [
          `${{formatNumber(totals.incidents)}} incidents and ${{formatNumber(totals.delay)}} total delay minutes in this month.`,
          topRoute ? `Top delay route: ${{topRoute.route_display}} with ${{formatNumber(topRoute.total_delay)}} delay minutes.` : "No route summary available for this month.",
          topLocation ? `Top incident location: ${{topLocation.location}} with ${{formatNumber(topLocation.incidents)}} incidents.` : "No location summary available for this month."
        ];
      }} else if (selectedOverview.kind === "band-year") {{
        const year = Number(selectedOverview.value);
        const band = selectedOverview.band;
        statRows = byMode(DATA.mode_delay_bands).filter((row) => row.year === year && row.delay_band === band);
        topRoute = topRow(byMode(DATA.route_yearly).filter((row) => row.year === year), "peak_delay");
        topLocation = topRow(byMode(DATA.location_yearly).filter((row) => row.year === year), "peak_delay");
        const totals = statRows.reduce((acc, row) => {{
          acc.incidents += row.incidents;
          acc.delay += row.total_delay;
          return acc;
        }}, {{ incidents: 0, delay: 0 }});
        title = band + " in " + year;
        bullets = [
          `${{formatNumber(totals.incidents)}} incidents fell into this delay band in the selected year.`,
          `${{formatNumber(totals.delay)}} total delay minutes were accumulated inside this band-year slice.`,
          topRoute ? `Highest single-route peak delay that year: ${{topRoute.route_display}} at ${{formatNumber(topRoute.peak_delay)}} minutes.` : "No route peak summary available.",
          topLocation ? `Highest location peak delay that year: ${{topLocation.location}} at ${{formatNumber(topLocation.peak_delay)}} minutes.` : "No location peak summary available."
        ];
      }}

      document.getElementById("overview-selection").innerHTML = `
        <strong>${{title}}</strong>
        <span>${{MODE_LABELS[activeMode]}} focus. Click other overview visuals to replace this in-depth slice.</span>
      `;
      document.getElementById("overview-insight-list").innerHTML = bullets.map((item) => `<li>${{item}}</li>`).join("");
      document.getElementById("overview-detail-table").innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Mode</th>
              <th>Incidents</th>
              <th>Total Delay</th>
              <th>Avg Delay</th>
            </tr>
          </thead>
          <tbody>
            ${{statRows.map((row) => `
              <tr>
                <td>${{MODE_LABELS[row.mode] || row.mode}}</td>
                <td>${{formatNumber(row.incidents)}}</td>
                <td>${{formatNumber(row.total_delay)}} min</td>
                <td>${{formatNumber(row.avg_delay, 2)}} min</td>
              </tr>
            `).join("")}}
          </tbody>
        </table>
      `;
    }}

    function renderRouteList() {{
      const rows = byMode(DATA.route_summary).sort((a, b) => b.total_delay - a.total_delay).slice(0, 18);
      const chart = document.getElementById("route-list-chart");
      Plotly.newPlot(chart, [{{
        type: "bar",
        orientation: "h",
        y: rows.map((row) => row.route_display).reverse(),
        x: rows.map((row) => row.total_delay).reverse(),
        customdata: rows.map((row) => [`${{row.mode}}||${{row.route_key}}`, row.incidents]).reverse(),
        marker: {{ color: rows.map((row) => modeColor(row.mode)).reverse() }},
        hovertemplate: "<b>%{{y}}</b><br>Total delay: %{{x:,.0f}} min<br>Incidents: %{{customdata[1]:,.0f}}<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 180, r: 12, t: 10, b: 36 }},
        xaxis: {{ title: "Total Delay Minutes", gridcolor: "rgba(16,33,45,0.08)" }},
        yaxis: {{ automargin: true }}
      }}, {{ displayModeBar: false, responsive: true }});
      chart.on("plotly_click", (event) => {{
        selectedRouteKey = event.points[0].customdata[0];
        renderRouteDetail();
        scrollToId("route-detail-anchor");
      }});
    }}

    function routeRows(kind) {{
      if (!selectedRouteKey) return [];
      const [mode, routeKey] = selectedRouteKey.split("||");
      return DATA[kind].filter((row) => row.mode === mode && String(row.route_key) === routeKey);
    }}

    function renderRouteDetail() {{
      const summary = routeRows("route_summary")[0];
      if (!summary) {{
        document.getElementById("route-selection").innerHTML = "<strong>No route selected</strong>";
        return;
      }}
      document.getElementById("route-selection").innerHTML = `
        <strong>${{summary.route_display}}</strong>
        <span>${{MODE_LABELS[summary.mode]}} route with ${{formatNumber(summary.incidents)}} incidents and ${{formatNumber(summary.total_delay)}} delay minutes.</span>
      `;

      const yearly = routeRows("route_yearly");
      const monthly = routeRows("route_monthly");
      const delayBands = routeRows("route_delay_bands");
      const heatmapRows = routeRows("route_heatmap");

      Plotly.newPlot("route-yearly-chart", [
        {{
          x: yearly.map((row) => row.year),
          y: yearly.map((row) => row.incidents),
          type: "bar",
          name: "Incidents",
          marker: {{ color: modeColor(summary.mode) }},
        }},
        {{
          x: yearly.map((row) => row.year),
          y: yearly.map((row) => row.avg_delay),
          type: "scatter",
          mode: "lines+markers",
          yaxis: "y2",
          name: "Avg Delay",
          line: {{ color: "#10212d", width: 2.5 }}
        }}
      ], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 48, r: 44, t: 10, b: 40 }},
        xaxis: {{ title: "Year" }},
        yaxis: {{ title: "Incidents", gridcolor: "rgba(16,33,45,0.08)" }},
        yaxis2: {{ title: "Avg Delay", overlaying: "y", side: "right" }},
        legend: {{ orientation: "h", y: 1.15 }}
      }}, {{ displayModeBar: false, responsive: true }});

      Plotly.newPlot("route-monthly-chart", [{{
        x: monthly.map((row) => row.year_month),
        y: monthly.map((row) => row.total_delay),
        mode: "lines",
        line: {{ color: modeColor(summary.mode), width: 3 }},
        customdata: monthly.map((row) => [row.incidents, row.avg_delay]),
        hovertemplate: "<b>%{{x}}</b><br>Total delay: %{{y:,.0f}} min<br>Incidents: %{{customdata[0]:,.0f}}<br>Avg delay: %{{customdata[1]:.2f}}<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 48, r: 12, t: 10, b: 40 }},
        xaxis: {{ title: "Month" }},
        yaxis: {{ title: "Total Delay Minutes", gridcolor: "rgba(16,33,45,0.08)" }},
      }}, {{ displayModeBar: false, responsive: true }});

      Plotly.newPlot("route-delay-band-chart", [{{
        type: "bar",
        x: delayBands.map((row) => row.delay_band),
        y: delayBands.map((row) => row.incidents),
        marker: {{ color: delayBands.map(() => modeColor(summary.mode)) }},
        customdata: delayBands.map((row) => [row.total_delay]),
        hovertemplate: "<b>%{{x}}</b><br>Incidents: %{{y:,.0f}}<br>Total delay: %{{customdata[0]:,.0f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 42, r: 12, t: 10, b: 50 }},
        xaxis: {{ title: "Delay Band" }},
        yaxis: {{ title: "Incident Frequency", gridcolor: "rgba(16,33,45,0.08)" }}
      }}, {{ displayModeBar: false, responsive: true }});

      const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
      const hours = Array.from({{ length: 24 }}, (_, i) => i);
      const lookup = new Map(heatmapRows.map((row) => [`${{row.weekday}}-${{row.hour}}`, row.total_delay]));
      const z = days.map((day) => hours.map((hour) => lookup.get(`${{day}}-${{hour}}`) || 0));
      Plotly.newPlot("route-heatmap-chart", [{{
        type: "heatmap",
        x: hours,
        y: days,
        z,
        colorscale: [[0, "#fff0d9"], [0.45, "#fcbf49"], [0.75, "#f77f00"], [1, "#d62828"]],
        hovertemplate: "<b>%{{y}}</b><br>Hour: %{{x}}:00<br>Total delay: %{{z:,.0f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 72, r: 12, t: 10, b: 38 }},
        xaxis: {{ title: "Hour of Day" }},
        yaxis: {{ autorange: "reversed" }}
      }}, {{ displayModeBar: false, responsive: true }});

      document.getElementById("route-summary-table").innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Incidents</td><td>${{formatNumber(summary.incidents)}}</td></tr>
            <tr><td>Total Delay</td><td>${{formatNumber(summary.total_delay)}} min</td></tr>
            <tr><td>Average Delay</td><td>${{formatNumber(summary.avg_delay, 2)}} min</td></tr>
            <tr><td>Median Delay</td><td>${{formatNumber(summary.median_delay, 2)}} min</td></tr>
            <tr><td>Peak Delay</td><td>${{formatNumber(summary.peak_delay)}} min</td></tr>
            <tr><td>Active Years</td><td>${{formatNumber(summary.active_years)}}</td></tr>
            <tr><td>Distinct Locations</td><td>${{formatNumber(summary.locations)}}</td></tr>
          </tbody>
        </table>
      `;
    }}

    function renderLocationList() {{
      const rows = byMode(DATA.location_summary).sort((a, b) => b.incidents - a.incidents).slice(0, 18);
      const chart = document.getElementById("location-list-chart");
      Plotly.newPlot(chart, [{{
        type: "bar",
        orientation: "h",
        y: rows.map((row) => row.location).reverse(),
        x: rows.map((row) => row.incidents).reverse(),
        customdata: rows.map((row) => [`${{row.mode}}||${{row.location}}`, row.total_delay]).reverse(),
        marker: {{ color: rows.map((row) => modeColor(row.mode)).reverse() }},
        hovertemplate: "<b>%{{y}}</b><br>Incidents: %{{x:,.0f}}<br>Total delay: %{{customdata[1]:,.0f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 185, r: 12, t: 10, b: 36 }},
        xaxis: {{ title: "Incident Frequency", gridcolor: "rgba(16,33,45,0.08)" }},
        yaxis: {{ automargin: true }}
      }}, {{ displayModeBar: false, responsive: true }});
      chart.on("plotly_click", (event) => {{
        selectedLocation = event.points[0].customdata[0];
        renderLocationDetail();
        scrollToId("location-detail-anchor");
      }});
    }}

    function locationRows(kind) {{
      if (!selectedLocation) return [];
      const [mode, location] = selectedLocation.split("||");
      return DATA[kind].filter((row) => row.mode === mode && row.location === location);
    }}

    function renderLocationDetail() {{
      const summary = locationRows("location_summary")[0];
      if (!summary) {{
        document.getElementById("location-selection").innerHTML = "<strong>No location selected</strong>";
        return;
      }}
      document.getElementById("location-selection").innerHTML = `
        <strong>${{summary.location}}</strong>
        <span>${{MODE_LABELS[summary.mode]}} location with ${{formatNumber(summary.incidents)}} incidents and ${{formatNumber(summary.total_delay)}} delay minutes.</span>
      `;

      const yearly = locationRows("location_yearly");
      const monthly = locationRows("location_monthly");
      const delayBands = locationRows("location_delay_bands");
      const calendar = locationRows("location_calendar");

      Plotly.newPlot("location-yearly-chart", [
        {{
          x: yearly.map((row) => row.year),
          y: yearly.map((row) => row.incidents),
          type: "bar",
          marker: {{ color: modeColor(summary.mode) }},
          name: "Incidents"
        }},
        {{
          x: yearly.map((row) => row.year),
          y: yearly.map((row) => row.avg_delay),
          type: "scatter",
          mode: "lines+markers",
          yaxis: "y2",
          name: "Avg Delay",
          line: {{ color: "#10212d", width: 2.4 }}
        }}
      ], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 48, r: 44, t: 10, b: 40 }},
        xaxis: {{ title: "Year" }},
        yaxis: {{ title: "Incidents", gridcolor: "rgba(16,33,45,0.08)" }},
        yaxis2: {{ title: "Avg Delay", overlaying: "y", side: "right" }},
        legend: {{ orientation: "h", y: 1.15 }}
      }}, {{ displayModeBar: false, responsive: true }});

      Plotly.newPlot("location-monthly-chart", [{{
        x: monthly.map((row) => row.year_month),
        y: monthly.map((row) => row.total_delay),
        mode: "lines",
        line: {{ color: modeColor(summary.mode), width: 3 }},
        customdata: monthly.map((row) => [row.incidents, row.avg_delay]),
        hovertemplate: "<b>%{{x}}</b><br>Total delay: %{{y:,.0f}} min<br>Incidents: %{{customdata[0]:,.0f}}<br>Avg delay: %{{customdata[1]:.2f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 48, r: 12, t: 10, b: 40 }},
        xaxis: {{ title: "Month" }},
        yaxis: {{ title: "Total Delay Minutes", gridcolor: "rgba(16,33,45,0.08)" }}
      }}, {{ displayModeBar: false, responsive: true }});

      Plotly.newPlot("location-delay-band-chart", [{{
        type: "bar",
        x: delayBands.map((row) => row.delay_band),
        y: delayBands.map((row) => row.incidents),
        marker: {{ color: delayBands.map(() => modeColor(summary.mode)) }},
        customdata: delayBands.map((row) => [row.total_delay]),
        hovertemplate: "<b>%{{x}}</b><br>Incidents: %{{y:,.0f}}<br>Total delay: %{{customdata[0]:,.0f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 42, r: 12, t: 10, b: 50 }},
        xaxis: {{ title: "Delay Band" }},
        yaxis: {{ title: "Incident Frequency", gridcolor: "rgba(16,33,45,0.08)" }}
      }}, {{ displayModeBar: false, responsive: true }});

      Plotly.newPlot("location-calendar-chart", [{{
        x: calendar.map((row) => row.month_name),
        y: calendar.map((row) => row.avg_delay),
        type: "bar",
        marker: {{ color: modeColor(summary.mode) }},
        customdata: calendar.map((row) => [row.incidents, row.total_delay]),
        hovertemplate: "<b>%{{x}}</b><br>Avg delay: %{{y:.2f}} min<br>Incidents: %{{customdata[0]:,.0f}}<br>Total delay: %{{customdata[1]:,.0f}} min<extra></extra>"
      }}], {{
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        margin: {{ l: 48, r: 12, t: 10, b: 40 }},
        xaxis: {{ title: "Month of Year" }},
        yaxis: {{ title: "Average Delay", gridcolor: "rgba(16,33,45,0.08)" }}
      }}, {{ displayModeBar: false, responsive: true }});

      document.getElementById("location-summary-table").innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Incidents</td><td>${{formatNumber(summary.incidents)}}</td></tr>
            <tr><td>Total Delay</td><td>${{formatNumber(summary.total_delay)}} min</td></tr>
            <tr><td>Average Delay</td><td>${{formatNumber(summary.avg_delay, 2)}} min</td></tr>
            <tr><td>Median Delay</td><td>${{formatNumber(summary.median_delay, 2)}} min</td></tr>
            <tr><td>Peak Delay</td><td>${{formatNumber(summary.peak_delay)}} min</td></tr>
            <tr><td>Active Years</td><td>${{formatNumber(summary.active_years)}}</td></tr>
          </tbody>
        </table>
      `;
    }}

    function renderAll() {{
      renderModeControls();
      ensureSelections();
      renderMetrics();
      renderYearlyMode();
      renderComboChart();
      renderMonthlyMode();
      renderDelayBandChart();
      renderOverviewInsight();
      renderRouteList();
      renderRouteDetail();
      renderLocationList();
      renderLocationDetail();
    }}

    renderAll();
  </script>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    payload = build_payload(args.data_root)
    html = dashboard_html(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    print(f"Wrote drilldown dashboard to {args.output}")


if __name__ == "__main__":
    main()
