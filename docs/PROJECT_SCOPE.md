# TTC Pulse Project Scope

## Proposal Baseline
- historical bus delay logs
- historical subway delay logs
- static GTFS as the reference layer
- GTFS-Realtime service alerts as the live validation layer
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
| Live GTFS-RT validation | Delivered materially | Current branch improvement |
| Spatial hotspot analysis | Partial | Stronger for subway than bus |
| Full H3 citywide hotspots | Not fully delivered | Do not overclaim |
| Traffic extension | Not delivered | Optional |
| AI features | Delivered as extension | Not core thesis |

## Strengths
- integrated multi-source reliability view
- route-first and station-first framing
- explainable scoring
- live-alert validation layer

## Cautions
- bus spatial precision
- fully linked dashboard interactions
- AI as main contribution
- streetcar as core scope

## Positioning
Decision-support reliability observatory turning noisy TTC operational data into route, station, time, cause, and live-alert intelligence.
