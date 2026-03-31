# Raw Layer (Step 1)

## Purpose
The Raw layer is the system-of-record landing zone.
It preserves source artifacts exactly as delivered and establishes file-level provenance for every downstream row.

## Scope
Raw includes only Step 1 in-scope families:
- Bus delay history
- Subway delay history
- Static GTFS merged feed
- GTFS-RT Service Alerts snapshots

Streetcar files may exist in repository-level datasets but are excluded from TTC Pulse MVP scope and should not flow into TTC Pulse Raw contracts.

## Storage Layout
- `ttc_pulse/raw/bus/`
- `ttc_pulse/raw/subway/`
- `ttc_pulse/raw/gtfs/`
- `ttc_pulse/raw/gtfsrt/`

## Immutability Contract
- No in-place file edits.
- No schema harmonization.
- No row-level transformation.
- No deduplication.
- Retries append new arrival artifacts or manifests; they do not rewrite original payloads.

## Provenance Requirements
Every Raw load run should emit or update metadata with:
- `source_family`
- `source_file`
- `source_sheet` (nullable)
- `arrived_at`
- `ingested_at`
- `file_size_bytes`
- `file_checksum` (recommended)
- `ingest_run_id`

## Source Mapping (Current Repository)
| Source family | Canonical input location | Raw target folder |
|---|---|---|
| Bus delay | `datasets/02_bus_delay/` | `ttc_pulse/raw/bus/` |
| Subway delay | `datasets/03_subway_delay/` | `ttc_pulse/raw/subway/` |
| Static GTFS | `datasets/01_gtfs_merged/` | `ttc_pulse/raw/gtfs/` |
| GTFS-RT alerts | live poll side-car | `ttc_pulse/raw/gtfsrt/` |

## Validation Checks
- File count and checksum reconciliation against source pickup manifest.
- Reject partial copy states (size mismatch).
- Log unsupported file formats without deleting source artifacts.

## Caveats
- Historical datasets contain year-to-year schema drift and naming inconsistencies by design.
- GTFS merged feed freshness must be monitored before production-grade alert validation.
- Raw is expected to include only placeholders until ingestion jobs are implemented.

## Step 1 Status
- Raw folder scaffold exists.
- Contract is documented and locked.
- Automated raw ingestion logic is pending implementation in later steps.
