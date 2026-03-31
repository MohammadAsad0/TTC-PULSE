# GTFS-RT Alerts Decode Refresh Run

## Run Metadata
- Run date (UTC): 2026-03-17T21:30:54Z
- Project: `ttc_pulse`
- Objective: regenerate parsed GTFS-RT Service Alert entities via protobuf decode path and refresh downstream alert marts.

## Execution
1. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.alerts.parse_service_alerts`
2. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.alerts.load_parsed_into_bronze`
3. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.normalization.normalize_gtfsrt_entities`
4. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.facts.build_fact_gtfsrt_alerts_norm`
5. `PYTHONPATH=src ../.venv-ttc/bin/python -m ttc_pulse.marts.build_gold_alert_validation`

## Parse Mode Distribution
| Scope | Parse mode | Count |
|---|---|---:|
| Parsed entity rows (`alerts/parsed/service_alert_entities.csv`) | `protobuf` | 522 |
| Parsed snapshots (`alerts/parsed/parse_manifest.csv`) | `protobuf` | 3 |

## Final Row Counts (Post Refresh)
| Table | Row count |
|---|---:|
| `bronze_gtfsrt_entities` | 522 |
| `silver_gtfsrt_alert_entities` | 522 |
| `fact_gtfsrt_alerts_norm` | 522 |
| `gold_alert_validation` | 522 |

## Decoded Selector Presence
- `route_id` selectors: present (`522 / 522` parsed rows; `522 / 522` silver rows).
- `stop_id` selectors: present (`39 / 522` parsed rows; `39 / 522` silver rows).
- `route_id + stop_id` jointly present: `39` rows in parsed and silver outputs.

## Artifacts
- `logs/alerts_decode_refresh_log.csv`
- `alerts/parsed/service_alert_entities.csv`
- `alerts/parsed/parse_manifest.csv`
- `alerts/parsed/parse_summary.json`
