# TTC Pulse Project Scope

## Proposal Baseline
- historical bus delay logs
- historical subway delay logs
- static GTFS reference layer
- GTFS-Realtime service alerts as validation
- linked dashboard

## Delivered Core (current branch)
- end-to-end ingestion through Gold marts
- GTFS-backed normalization
- route / station / time reliability marts
- top-offender ranking
- live-alert alignment outputs
- Streamlit dashboard

## Extensions Present
- streetcar integration
- Dataset Explorer
- AI Explain
- AI Chat Bot

## Delivery Matrix
| Deliverable | Status | Note |
| --- | --- | --- |
| Bus reliability analysis | Delivered | Core strength |
| Subway reliability analysis | Delivered | Core strength |
| GTFS normalization bridge | Delivered | Trust layer |
| Explainable metrics | Delivered | Frequency, severity, regularity, cause mix |
| Route/station ranking | Delivered | Stakeholder value |
| Temporal pattern analysis | Delivered | Stakeholder value |
| Live GTFS-RT validation | Delivered  | Validates history |
| Spatial hotspot analysis | Partial | Stronger for subway than bus |
| Full H3 citywide hotspots | Not fully delivered | Optional |
| Traffic extension | Not delivered | Optional |
| AI features | Delivered as extension | Stakeholder value |

## Strengths
- integrated multi-source reliability view
- route-first and station-first framing
- explainable scoring
- live-alert validation layer
- bus spatial precision
- fully linked dashboard interactions
- AI contribution
- streetcar scope

## Objective
Decision-support reliability observatory turning noisy TTC operational data into route, station, time, cause, and live-alert intelligence.

## Schedulers for Realtime Alerts
Airflow's application was heavy and not aligned to our project, local schedulers (launchd) used instead.