# Review Tables (Step 2)

## Purpose
Review tables are the control surface for unresolved canonicalization.
Step 2 defines concrete Silver review schemas so ambiguous/unmatched mappings remain auditable and promotable.

## Implemented Review Tables (Silver Schema)
`route_alias_review`:
- `review_id`, `mode`
- `route_token_raw`, `proposed_route_id_gtfs`
- `reason`, `review_status`, `reviewed_by`, `reviewed_at`

`station_alias_review`:
- `review_id`
- `station_raw`, `proposed_station_canonical`, `proposed_stop_id_gtfs`
- `reason`, `review_status`, `reviewed_by`, `reviewed_at`

`incident_code_review`:
- `review_id`, `mode`
- `incident_code_raw`, `incident_text_raw`, `proposed_incident_category`
- `reason`, `review_status`, `reviewed_by`, `reviewed_at`

## How Rows Reach Review Queues
Rows are inserted when normalized events end with:
- `link_status = 'ambiguous_review'`
- `link_status = 'unmatched_review'`
- or incident mapping cannot produce a stable `incident_category`

The attempted strategy is still preserved in event facts through:
- `match_method`
- `match_confidence`
- `link_status`

## Review Status Semantics
Standard lifecycle:
- `open`: waiting for analyst decision
- `approved`: candidate accepted and eligible for alias/code promotion
- `rejected`: candidate not accepted
- `needs_context`: insufficient evidence; hold until additional context exists

## Promotion Contract
When `approved`:
1. Add/activate a row in `dim_route_alias`, `dim_station_alias`, or `dim_incident_code`.
2. Capture reviewer provenance (`reviewed_by`, `reviewed_at`) in promoted record.
3. Re-run normalization so previously unresolved rows can graduate to `matched`.

Rejected/held rows remain in review tables for audit trail and model-tuning feedback.

## Step 3+ Remaining Work
- Implement automated queue writes from normalization jobs.
- Build reviewer tooling for decision capture.
- Add KPI marts for backlog, turnaround time, and approval/rejection trends.
