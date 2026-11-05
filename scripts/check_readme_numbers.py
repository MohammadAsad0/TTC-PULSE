"""Print every figure in the README from the committed files.

Run from the repository root:
    python3 scripts/check_readme_numbers.py

Each line names the file or table that gives the number. Nothing here reads
the raw source data, which is not in the repository.
"""

from __future__ import annotations

import csv
import hashlib
import os
import re
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SNAPSHOT_NAME_PATTERN = re.compile(r"^alerts_\d{8}T\d{6}Z\.pb$")
REGISTRIES = [
    "raw/bus/bus_file_registry.csv",
    "raw/subway/subway_file_registry.csv",
    "raw/gtfs/gtfs_file_registry.csv",
    "raw/gtfsrt/gtfsrt_snapshot_registry.csv",
]
ROW_COUNT_TABLES = [
    "silver/fact_delay_events_norm.parquet",
    "silver/fact_gtfsrt_alerts_norm.parquet",
    "dimensions/dim_route_gtfs.parquet",
    "dimensions/dim_stop_gtfs.parquet",
    "dimensions/dim_service_gtfs.parquet",
    "dimensions/dim_route_alias.parquet",
    "dimensions/dim_station_alias.parquet",
    "dimensions/dim_incident_code.parquet",
    "bridge/bridge_route_direction_stop.parquet",
    "gold/gold_delay_events_core.parquet",
    "gold/gold_route_time_metrics.parquet",
    "gold/gold_station_time_metrics.parquet",
    "gold/gold_time_reliability.parquet",
    "gold/gold_top_offender_ranking.parquet",
    "gold/gold_alert_validation.parquet",
    "gold/gold_spatial_hotspot.parquet",
    "gold/gold_linkage_quality.parquet",
]


def parquet(path: str) -> str:
    return f"read_parquet('{(REPO_ROOT / path).as_posix()}')"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def print_fact_checks(con: duckdb.DuckDBPyConnection) -> None:
    fact = parquet("silver/fact_delay_events_norm.parquet")
    print("== Delay events (silver/fact_delay_events_norm.parquet)")
    total, min_date, max_date = con.execute(
        f"SELECT count(*), min(service_date), max(service_date) FROM {fact}"
    ).fetchone()
    print(f"events: {total:,}  first service date: {min_date}  last service date: {max_date}")
    for mode, count in con.execute(f"SELECT mode, count(*) FROM {fact} GROUP BY mode ORDER BY mode").fetchall():
        print(f"  {mode}: {count:,}")
    bus_total, bus_matched = con.execute(
        f"""SELECT count(*), sum(CASE WHEN link_status = 'matched' THEN 1 ELSE 0 END)
            FROM {fact} WHERE mode = 'bus'"""
    ).fetchone()
    print(f"bus events matched to a GTFS route: {bus_matched:,} of {bus_total:,} ({100 * bus_matched / bus_total:.1f}%)")
    print(f"bus events not matched: {bus_total - bus_matched:,} ({100 * (bus_total - bus_matched) / bus_total:.1f}%)")
    for method, count in con.execute(
        f"SELECT match_method, count(*) FROM {fact} GROUP BY match_method ORDER BY match_method"
    ).fetchall():
        print(f"match_method {method}: {count:,} ({100 * count / total:.1f}%)")
    subway_stations, subway_lines = con.execute(
        f"""SELECT count(DISTINCT station_canonical), count(DISTINCT line_code_norm)
            FROM {fact} WHERE mode = 'subway'"""
    ).fetchone()
    streetcar_routes = con.execute(
        f"SELECT count(DISTINCT route_short_name_norm) FROM {fact} WHERE mode = 'streetcar'"
    ).fetchone()[0]
    bus_keys = con.execute(f"SELECT count(DISTINCT route_short_name_norm) FROM {fact} WHERE mode = 'bus'").fetchone()[0]
    bus_keys_matched = con.execute(
        f"""SELECT count(DISTINCT route_short_name_norm) FROM {fact}
            WHERE mode = 'bus' AND link_status = 'matched'"""
    ).fetchone()[0]
    print(f"coverage: subway stations {subway_stations}, subway lines {subway_lines}, "
          f"streetcar routes {streetcar_routes}, bus route keys {bus_keys}, "
          f"bus route keys matched to GTFS {bus_keys_matched}")
    source_ids = con.execute(
        f"SELECT count(*), count(source_file), count(source_row_id), count(row_hash) FROM {fact}"
    ).fetchone()
    print(f"rows with source_file, source_row_id and row_hash: {source_ids[1]:,}, {source_ids[2]:,}, {source_ids[3]:,} of {source_ids[0]:,}")
    unique_sources = con.execute(
        f"SELECT mode, count(DISTINCT (source_file, source_row_id)) FROM {fact} GROUP BY mode ORDER BY mode"
    ).fetchall()
    print("distinct (source_file, source_row_id) per mode: " + ", ".join(f"{mode} {count:,}" for mode, count in unique_sources))


def print_gold_checks(con: duckdb.DuckDBPyConnection) -> None:
    print("== Gold marts: event totals by mode (README SQL example)")
    for mode, events in con.execute(
        f"SELECT mode, sum(event_count) FROM {parquet('gold/gold_delay_events_core.parquet')} GROUP BY mode ORDER BY mode"
    ).fetchall():
        print(f"  {mode}: {events:,}")
    print("== Gold marts: linkage quality rows by mode and link status")
    for mode, link_status, rows in con.execute(
        f"""SELECT mode, link_status, sum(row_count) FROM {parquet('gold/gold_linkage_quality.parquet')}
            GROUP BY mode, link_status ORDER BY mode, link_status"""
    ).fetchall():
        print(f"  {mode} {link_status}: {rows:,}")
    print("== Gold route scores: recompute composite_score from its four inputs (weights 0.35, 0.30, 0.20, 0.15)")
    route = parquet("gold/gold_route_time_metrics.parquet")
    rows, max_diff = con.execute(
        f"""WITH z AS (
              SELECT *,
                COALESCE((frequency - avg(frequency) OVER w) / NULLIF(stddev_samp(frequency) OVER w, 0), 0.0) AS zf,
                COALESCE((severity_p90 - avg(severity_p90) OVER w) / NULLIF(stddev_samp(severity_p90) OVER w, 0), 0.0) AS zs,
                COALESCE((regularity_p90 - avg(regularity_p90) OVER w) / NULLIF(stddev_samp(regularity_p90) OVER w, 0), 0.0) AS zr
              FROM {route}
              WINDOW w AS (PARTITION BY mode, service_date, hour_bin))
            SELECT count(*), max(abs(composite_score - (0.35 * zf + 0.30 * zs + 0.20 * zr + 0.15 * coalesce(cause_mix_score, 0.0))))
            FROM z"""
    ).fetchone()
    print(f"route rows: {rows:,}  largest difference: {max_diff:.1e}")


def print_alert_checks(con: duckdb.DuckDBPyConnection) -> None:
    alerts = parquet("gold/gold_alert_validation.parquet")
    print("== Alert validation (gold/gold_alert_validation.parquet)")
    alert_ids, rows = con.execute(f"SELECT count(DISTINCT alert_id), count(*) FROM {alerts}").fetchone()
    matched = con.execute(f"SELECT count(*) FROM {alerts} WHERE match_status = 'matched'").fetchone()[0]
    snapshots = con.execute(f"SELECT count(DISTINCT snapshot_ts) FROM {alerts}").fetchone()[0]
    print(f"alerts: {alert_ids}  entity rows: {rows:,}  matched rows: {matched:,} ({100 * matched / rows:.1f}%)")
    print(f"distinct snapshot timestamps: {snapshots}")


def print_registry_checks() -> None:
    print("== Source registries (raw/*/ *_registry.csv)")
    for registry in REGISTRIES:
        with (REPO_ROOT / registry).open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))
        header, body = rows[0], rows[1:]
        valid_last = sum(1 for row in body if SHA256_PATTERN.match(row[-1]))
        longer = sum(1 for row in body if len(row) != len(header))
        print(f"{registry}: rows {len(body):,}, last field is a SHA-256 value in {valid_last:,}, "
              f"rows with a different field count from the header ({len(header)}): {longer:,}")
    print("== GTFS-RT snapshots (alerts/raw_snapshots/*.pb against the GTFS-RT registry)")
    with (REPO_ROOT / REGISTRIES[3]).open(newline="", encoding="utf-8") as handle:
        registry_rows = list(csv.reader(handle))[1:]
    recorded: dict[str, set[str]] = {}
    for row in registry_rows:
        name = next((field for field in row if SNAPSHOT_NAME_PATTERN.match(os.path.basename(field))), None)
        if name:
            recorded.setdefault(os.path.basename(name), set()).add(row[-1])
    snapshots = sorted(path.name for path in (REPO_ROOT / "alerts/raw_snapshots").glob("*.pb"))
    listed = [name for name in snapshots if name in recorded]
    matched = [
        name for name in listed
        if file_sha256(REPO_ROOT / "alerts/raw_snapshots" / name) in recorded[name]
    ]
    print(f"committed snapshot files: {len(snapshots)}  listed in the registry: {len(listed)}  "
          f"SHA-256 matches the recorded value: {len(matched)}  does not match: {len(listed) - len(matched)}")


def print_code_checks() -> None:
    print("== Code")
    modules = [
        path for path in (REPO_ROOT / "src/ttc_pulse").rglob("*.py") if path.name != "__init__.py"
    ]
    print(f"Python modules in src/ttc_pulse (excluding __init__.py): {len(modules)}")
    packages = ["ingestion", "normalization", "aliasing", "bronze", "facts", "gtfs", "marts", "alerts", "qa"]
    pipeline = [path for path in modules if path.parent.name in packages]
    print(f"modules in the pipeline packages ({', '.join(packages)}): {len(pipeline)}")


def print_row_counts(con: duckdb.DuckDBPyConnection) -> None:
    print("== Row counts of the tables in the README data model")
    for table in ROW_COUNT_TABLES:
        count = con.execute(f"SELECT count(*) FROM {parquet(table)}").fetchone()[0]
        print(f"{count:>12,}  {table}")


def main() -> None:
    con = duckdb.connect()
    print_fact_checks(con)
    print_gold_checks(con)
    print_alert_checks(con)
    print_registry_checks()
    print_code_checks()
    print_row_counts(con)


if __name__ == "__main__":
    main()
