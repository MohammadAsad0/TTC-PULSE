from __future__ import annotations

import argparse

from ttc_pulse.service import load_fast_datasets, refresh_fast_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify TTC Pulse data loading and artifacts.")
    parser.add_argument("--refresh", action="store_true", help="Force rebuild parquet and duckdb artifacts before checks.")
    args = parser.parse_args()

    if args.refresh:
        meta = refresh_fast_artifacts()
        print("Artifacts refresh status:", meta)

    data = load_fast_datasets(force_refresh=False)
    inventory = data["file_inventory"]
    bus = data["bus"]
    subway = data["subway"]
    gtfs_routes = data["gtfs_routes"]
    gtfs_stops = data["gtfs_stops"]

    print("TTC Pulse verification")
    print(f"Bus files found: {inventory['bus_files']}")
    print(f"Subway files found: {inventory['subway_files']}")
    print(f"GTFS files found: {inventory['gtfs_files']}")
    print(f"Cleaned bus rows: {len(bus)}")
    print(f"Cleaned subway rows: {len(subway)}")

    if not bus.empty:
        print(f"Bus date coverage: {bus['service_date'].min().date()} to {bus['service_date'].max().date()}")
    else:
        print("Bus date coverage: no rows")

    if not subway.empty:
        print(f"Subway date coverage: {subway['service_date'].min().date()} to {subway['service_date'].max().date()}")
    else:
        print("Subway date coverage: no rows")

    print(f"GTFS routes loaded: {len(gtfs_routes)}")
    print(f"GTFS stops loaded: {len(gtfs_stops)}")


if __name__ == "__main__":
    main()
