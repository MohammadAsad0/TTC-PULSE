# Confidence Gating (Step 3 Operational Policy)

## Purpose
Confidence gates prevent low-quality mappings from leaking into high-impact stakeholder outputs, especially station rankings and spatial hotspot visualizations.

## Match Tiers (Ordered)
- `exact_gtfs_match`
- `token_gtfs_match`
- `alias_match`
- `route_only_match`
- `unmatched_review`

## Output Gating Matrix
| Output type | Allowed tiers in Step 3 |
|---|---|
| Route-level reliability trend | `exact_gtfs_match`, `token_gtfs_match`, `alias_match`, `route_only_match` |
| Station-level reliability ranking | `exact_gtfs_match`, `token_gtfs_match`, high-confidence `alias_match` |
| Stop/corridor spatial hotspot | Allowed only when spatial gate passes; otherwise table is emitted as zero-row scaffold |
| QA/review exports | All tiers, including `unmatched_review` and `ambiguous_review` |

## Spatial Hotspot Release Gate
`gold_spatial_hotspot` is published only when all conditions are met:
- Station/stop linkage coverage is at least `0.80`.
- Ambiguous mapping share is below `0.15`.
- High-confidence subway matched rows are at least `1000`.
- Unmatched share trend is stable or improving over rolling windows.
- Linkage-quality evidence is published in run logs/changelog for the release window.

## Reliability and Visibility Rules
- Low-confidence rows are retained in Silver for auditability and QA.
- Gold route-level marts may include `route_only_match` rows, but station/spatial outputs may not.
- When confidence filters are applied, stakeholder views must show both:
  - filtered metrics used for decisions
  - all-record QA baseline for transparency

## Operational Behavior
- If the gate fails, the mart builder writes a schema-only zero-row `gold_spatial_hotspot` output and records gate metrics in run logs.
- If the gate passes, the mart builder publishes hotspot rows using subway-station normalized keys to derive centroid coordinates from GTFS stop geometry.
- Dashboard runtime still checks for empty outputs and shows an explicit deferred-state message when the gate is not met.
