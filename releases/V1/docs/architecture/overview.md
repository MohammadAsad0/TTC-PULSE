# TTC Pulse Architecture Overview (Step 4 Finalized Runtime)

## Purpose
This document states the finalized MVP architecture that is currently implemented as a DuckDB/Parquet data stack with Streamlit delivery contracts and an alerts side-car.

## Scope Lock
In scope:
- TTC bus delay history
- TTC subway delay history
- Static GTFS merged feed
- GTFS-RT Service Alerts

Out of scope:
- GTFS-RT Vehicle Positions
- GTFS-RT Trip Updates
- Streetcar core modeling
- Traffic causality / ridership inference
- Spark-based distributed execution

## Runtime Lock
- Data engine: DuckDB (`data/ttc_pulse.duckdb`) with Parquet artifacts by layer.
- Delivery runtime: Streamlit (`app/streamlit_app.py` + pages).
- Scheduler boundary: alerts side-car cadence at 30 minutes.
- Spark remains excluded.

## Layer Status
| Layer | Responsibility | Step 4 status |
|---|---|---|
| Raw | Source registries and immutable source tracking | Implemented for file registries and ingestion metadata |
| Bronze | Row-preserving extraction with lineage | Implemented in DuckDB Step 1 build flow |
| Silver | Canonical dims/bridge/reviews/events/facts | Implemented as Parquet outputs and DuckDB-registrable tables |
| Gold | Stakeholder marts for reliability and validation | Implemented as Parquet outputs with run logs |
| Dashboard | Consumption of Gold marts | Streamlit shell + linkage page scaffold; contracts locked |
| Scheduler | GTFS-RT side-car | Side-car DAG plus placeholder DAG coexist; hooks partially pending |

## Architecture Invariants
- Raw and Bronze preserve traceability; unresolved mappings are retained, not dropped.
- Reliability scoring requires component visibility alongside composite score.
- Confidence gates control sensitive outputs, especially spatial hotspot release.
- GTFS-RT side-car remains bounded to alerts ingestion/validation and does not own full batch orchestration.

## Operational Caveats
- `gold_alert_validation` can be empty when normalized alert fact rows are absent.
- `gold_spatial_hotspot` remains deferred (zero-row scaffold) when confidence gate fails.
- If protobuf decoder bindings are unavailable, alerts parsing falls back to binary metadata rows with explicit caveats.
- Streamlit package may require environment install in the active venv before local app launch.

## Glossary
| Term | Definition |
|---|---|
| Lineage | Ability to trace transformed rows back to source file and source row context. |
| `source_file` / `source_sheet` / `source_row_id` | Core lineage keys carried through normalized event data. |
| `match_method` | Linking strategy used for GTFS/entity resolution. |
| `match_confidence` | Numeric confidence score for chosen mapping. |
| `link_status` | Final mapping state: `matched`, `ambiguous_review`, `unmatched_review`. |
| Confidence gate | Release control that blocks low-trust outputs (for example, spatial hotspot). |
