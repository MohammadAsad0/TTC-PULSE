# Gold Layer (Step 3 Stakeholder Mart Contract)

## Status
Step 3 locks Gold mart definitions, reliability scoring policy, and confidence-gated release rules.
Gold tables are defined in `docs/schema_ddl.sql`, but recurring materialization and dashboard wiring remain Step 4 work.

## Purpose
Gold converts canonical Silver facts into stakeholder-facing marts that support reliability decisions, data-quality trust checks, and live alert validation.

## Gold Mart Definitions and Stakeholder Intent
| Gold mart | Stakeholder intent |
|---|---|
| `gold_delay_events_core` | Baseline time-sliced event facts for trend panels and drill-down context. |
| `gold_linkage_quality` | Trust panel for linkage quality before consuming ranked reliability outputs. |
| `gold_route_time_metrics` | Route-level reliability risk scoring for transit operations and planning. |
| `gold_station_time_metrics` | Station-level reliability risk scoring for subway-focused interventions. |
| `gold_time_reliability` | Day and hour reliability patterns for staffing and service window review. |
| `gold_top_offender_ranking` | Prioritized intervention queue (routes/stations) with transparent scoring inputs. |
| `gold_alert_validation` | GTFS-RT Service Alerts selector validity and feed quality monitoring. |
| `gold_spatial_hotspot` | Localized hotspot map for corridor/stop analysis; deferred behind confidence gate. |

## Reliability Framework (Step 3)
Required reliability components:
- Frequency: `frequency` (event count)
- Severity: `severity_median`, `severity_p90`
- Regularity: `regularity_p90` (gap/recovery risk proxy)
- Cause mix: `cause_mix_score`

Composite reliability formula:
- `composite_score = wf * z(frequency) + ws * z(severity_p90) + wr * z(regularity_p90) + wc * cause_mix_score`
- Weight terms (`wf`, `ws`, `wr`, `wc`) must be versioned, reproducible, and sum to 1.0.

Component visibility policy:
- Composite score is allowed only as a summary/ranking aid.
- Any table/chart showing `composite_score` must also display component metrics in the same view/export.
- Users must be able to interpret rank movement from component values, not composite score alone.

## Confidence-Gated Spatial Policy
- Route-level marts may include `route_only_match` where explicitly labeled.
- Station-level marts require high-confidence mappings.
- `gold_spatial_hotspot` stays disabled in Step 3 and is not promoted until confidence thresholds pass (see `docs/decisions/confidence_gating.md`).

## Step 3 Caveats and Step 4 Handoff
- Gold schemas are contract-ready, but not yet scheduled as production mart jobs.
- Composite weighting is policy-defined but not yet calibrated from observed KPI baselines.
- Spatial hotspot output remains intentionally deferred.
- Step 4 will implement mart population jobs, gating enforcement in runtime filters, and hotspot release only after threshold evidence is published.
