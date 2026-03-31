# Cleaning Rules

## Bus

- Read every CSV file in `data/bus/`.
- Parse `Date` into `service_date`.
- Treat `Line` as the raw route label.
- Extract a normalized route short name from the route label.
- Drop rows where the normalized route is missing.
- Drop rows where the parsed date is missing.
- Drop rows where the normalized route resolves to `0`.
- Standardize station, direction, incident code, and vehicle fields when present.
- Map normalized bus routes to `route_id_gtfs` using static GTFS bus routes.

## Subway

- Read every CSV file in `data/subway/`.
- Parse `Date` into `service_date`.
- Normalize `Line`, `Station`, `Bound`, `Code`, and `Vehicle`.
- Drop rows where normalized line is missing.
- Drop rows where normalized station is missing.
- Drop rows where parsed date is missing.
- Enrich incident codes using `ttc-subway-delay-codes__01_Sheet_1.csv`.
- Infer `route_id_gtfs` from the normalized subway line using GTFS subway routes:
  - `YU` or `YUS` -> `1`
  - `BD` -> `2`
  - `SHP` -> `4`
  - `SRT` has no current GTFS route ID in the provided static GTFS files

## Practical Standardization

- Text fields are trimmed, uppercased, and whitespace-normalized.
- `Bound = None` and `Vehicle = 0` are treated as missing.
- The dashboard keeps row-level raw columns alongside normalized fields to make source checks easier.
