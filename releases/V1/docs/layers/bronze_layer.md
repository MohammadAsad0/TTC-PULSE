# Bronze Layer (Step 1)

## Purpose
Bronze is the first tabular representation of Raw sources.
Its core job is structured parsing without information loss.
Bronze is row-preserving and lineage-first.

## Step 1 Scope
Bronze contracts cover:
- Bus delay event extracts
- Subway delay event extracts
- Static GTFS table copies
- GTFS-RT Service Alerts parsed entities

## Row-Preserving Contract
- One source record maps to one Bronze record whenever source format allows deterministic extraction.
- Multi-row explosion (for repeated fields) must carry parent source keys.
- No quality filtering, business rule pruning, or confidence gating at Bronze.

## Mandatory Columns
All Bronze tables must include:
- `source_file`
- `source_sheet` (nullable)
- `source_row_id`
- `ingested_at`

Recommended:
- `source_schema_version`
- `row_hash`
- `parse_status`
- `parse_error_reason` (nullable)

## Proposed Bronze Families
| Family | Example table contract | Notes |
|---|---|---|
| Bus delays | `bronze_bus_delay_events` | Preserve raw columns plus normalized timestamp parse outputs |
| Subway delays | `bronze_subway_delay_events` | Preserve line/location text and code fields |
| GTFS static | `bronze_gtfs_routes`, `bronze_gtfs_stops`, `bronze_gtfs_trips`, etc. | One table per GTFS source file |
| GTFS-RT alerts | `bronze_gtfsrt_alert_snapshots`, `bronze_gtfsrt_informed_entities` | Keep snapshot timestamp and selector payload lineage |

## Schema Drift Handling
- Drift is expected across years and source revisions.
- Bronze retains original variant columns and records schema profile metadata.
- Canonical column harmonization is deferred to Silver.

## QA Checks
- Row-count reconciliation against parseable source rows.
- Null-rate checks for lineage columns.
- Duplicate key diagnostics for (`source_file`, `source_row_id`) pairs.
- Parse failure tracking with explicit status codes.

## Caveats
- Some source rows may be structurally parseable but semantically unclear.
- GTFS-RT protobuf flattening can generate sparse selector fields.
- Bronze tables are not analytics-ready; they are traceability-ready.

## Step 1 Status
- Bronze contracts are fully documented.
- Bronze implementation is pending.
