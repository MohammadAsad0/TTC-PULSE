# Alias Strategy (Step 2)

## Why Alias Dimensions Remain Central
- Historical delay feeds contain inconsistent route/station tokens across years.
- GTFS IDs are stable; source delay text is not.
- Step 2 canonical schema requires alias dimensions and review tables to make mapping auditable instead of heuristic-only.

## Implemented Alias Tables (Schema Level)
Route aliases (`dim_route_alias`):
- `mode`, `route_token_raw`, `route_short_name_norm`, `route_id_gtfs`
- `mapping_method`, `mapping_confidence`, `is_active`
- reviewer provenance (`reviewed_by`, `reviewed_at`)

Station aliases (`dim_station_alias`):
- `station_raw`, `station_canonical`, `station_group`, `stop_id_gtfs_candidate`
- `mapping_method`, `mapping_confidence`, `is_active`
- reviewer provenance (`reviewed_by`, `reviewed_at`)

Incident normalization (`dim_incident_code`) is governed in the same pattern and paired with `incident_code_review`.

## Resolution Order by Mode
Bus route-first resolution:
1. Normalize route token from `route_label_raw`.
2. Attempt direct GTFS route match (`route_id` / `route_short_name`).
3. Resolve through `dim_route_alias`.
4. If route resolves, optionally attempt location-to-stop linkage.
5. Route unresolved/ambiguous outcomes to `route_alias_review`.

Subway station-first resolution:
1. Normalize `station_text_raw` (plus line context from `line_code_raw/line_code_norm`).
2. Resolve station canonical form and stop candidate from `dim_station_alias`.
3. Backfill route mapping from station-line relationship.
4. Route unresolved/ambiguous outcomes to `station_alias_review`.

## Linkage Semantics in Facts
Step 2 standardizes these fields in normalized event tables and facts:
- `match_method`: linkage strategy (`exact_gtfs_match`, `token_gtfs_match`, `alias_match`, `route_only_match`, `unmatched_review`)
- `match_confidence`: numeric confidence score for selected/attempted mapping
- `link_status`: final outcome (`matched`, `ambiguous_review`, `unmatched_review`)

Rule:
- `route_only_match` is acceptable for route-level bus analytics but should not power station-level hotspot outputs.

## Review Queue Governance
Review tables capture unresolved mappings with proposed candidate and reason:
- `route_alias_review`: unresolved route tokens
- `station_alias_review`: unresolved station/stop mappings
- `incident_code_review`: unresolved incident category mappings

Required policy:
- unresolved or ambiguous mappings are inserted into review queues;
- promoted mappings must carry reviewer metadata;
- historical review rows are retained for audit and reproducibility.

## Step 3+ Remaining Work
- Populate alias dimensions with seed dictionaries and reviewer-approved mappings.
- Implement operational review workflow (triage, approve/reject, promotion rerun).
- Add enforcement tests for alias conflicts, inactive aliases, and confidence downgrade rules.
