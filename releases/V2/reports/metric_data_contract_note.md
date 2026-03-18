# Metric Data Contract Note

The updated analytical pages consume a shared five-metric contract:
`Composite Score`, `Frequency`, `Severity`, `Regularity`, and `Cause Mix`.

Backend availability by mart is:
`gold_top_offender_ranking`, `gold_route_time_metrics`, and `gold_time_reliability` expose all five metrics plus `mode`.
`gold_station_time_metrics` exposes the same metric set but does not include `mode`; subway drill-down must query it through `station_canonical` and temporal filters only.

Drill granularities use these aliases when the mart surface does not already match the selector label:
`incident_count -> frequency`, `p90_delay -> severity_p90`, `p90_gap -> regularity_p90`.
`Cause Mix` resolves to `cause_mix_score`, which is a comparative composition signal rather than a causal attribution measure.

Composite behavior remains unchanged when `Composite Score` is selected. When the selected metric is sparse or unstable at a fine slice, the UI can fall back to a simpler available metric and show an inline notice instead of failing.
