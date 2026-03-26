# Final Metrics Summary

Generated at (UTC): 2026-03-26T17:06:56Z

## Gold Row Counts

| Table | Row count | Status |
|---|---:|---|
| gold_delay_events_core | 933327 | built |
| gold_linkage_quality | 467 | built |
| gold_route_time_metrics | 755793 | built |
| gold_station_time_metrics | 187162 | built |
| gold_time_reliability | 336 | built |
| gold_top_offender_ranking | 259 | built |
| gold_alert_validation | 3252 | built |
| gold_spatial_hotspot | 0 | built_with_caveats |

## Composite Scoring Policy

S = 0.35*z(freq) + 0.30*z(sev90) + 0.20*z(reg90) + 0.15*cause_mix (weights version: step3-default-v1)

## Metric Caveats

- Spatial hotspot confidence gate not met; emitted schema-only zero-row scaffold.
- Gate metrics: station_linkage_coverage=0.8449 (threshold 0.80), ambiguous_share=0.1551 (threshold < 0.15), high_confidence_rows=197011 (threshold >= 1000), eligible_rows=233179
