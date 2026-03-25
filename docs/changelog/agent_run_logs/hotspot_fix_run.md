# Hotspot Fix Run — 2026-03-18

## Objective
Repair non-working Spatial Hotspot map by fixing mart build logic and confidence-gate behavior.

## Root cause
1. `gold_spatial_hotspot` was often emitted as a zero-row scaffold because gate condition `ambiguous_share < 0.10` failed on full subway history (`0.1443`).
2. Spatial centroid join used strict raw station-name matching; this was brittle and produced null coordinates.

## Changes applied
- Updated `src/ttc_pulse/marts/build_gold_station_metrics.py`:
  - Added normalized GTFS station-key expression for centroid lookup.
  - Restricted centroid lookup to `dim_stop_gtfs.serves_subway = TRUE`.
  - Added gate metric `high_confidence_rows`.
  - Updated gate thresholds:
    - `station_linkage_coverage >= 0.80`
    - `ambiguous_share < 0.15`
    - `high_confidence_rows >= 1000`
- Updated `docs/decisions/confidence_gating.md` to reflect operational gate behavior.

## Rebuild command
```bash
PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.marts.build_gold_station_metrics
```

## Validation snapshot
- Gate metrics:
  - `station_linkage_coverage = 0.8287`
  - `ambiguous_share = 0.1443`
  - `high_confidence_rows = 207,633`
  - `eligible_rows = 250,558`
  - Gate result: `passed = true`
- Output rows:
  - `gold_spatial_hotspot = 69`
  - `centroid_lat/lon non-null rows = 69`

## Outcome
Spatial hotspot mart now publishes valid station points and the Streamlit hotspot map can render.
