# Final Metrics Summary

Generated at (UTC): 2026-03-24T02:52:13Z

## Gold Row Counts

| Table | Row count | Status |
|---|---:|---|
| gold_delay_events_core | 1004682 | built_with_caveats |
| gold_linkage_quality | 955 | built |
| gold_route_time_metrics | 762679 | built_with_caveats |
| gold_station_time_metrics | 197434 | built |
| gold_time_reliability | 336 | built |
| gold_top_offender_ranking | 264 | built |
| gold_alert_validation | 675 | built_with_caveats |
| gold_spatial_hotspot | 69 | built_with_caveats |

## Composite Scoring Policy

S = 0.35*z(freq) + 0.30*z(sev90) + 0.20*z(reg90) + 0.15*cause_mix (weights version: step3-default-v1)

## Metric Caveats

- fact_delay_events_norm: table was missing in DuckDB and was loaded from silver/fact_delay_events_norm.parquet.
- dim_route_gtfs: table was missing in DuckDB and was loaded from dimensions/dim_route_gtfs.parquet.
- fact_gtfsrt_alerts_norm: table was missing in DuckDB and was loaded from silver/fact_gtfsrt_alerts_norm.parquet.
- dim_stop_gtfs: table was missing in DuckDB and was loaded from dimensions/dim_stop_gtfs.parquet.
