from __future__ import annotations

from ttc_pulse.service import load_clean_datasets


def main() -> None:
    data = load_clean_datasets()
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
