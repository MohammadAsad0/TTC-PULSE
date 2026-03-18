# Dashboard Regression Report

## Scope
Validated the dashboard selector update and the subway drill-down fix for TTC Pulse. The checks covered pages 03, 04, 05, 06, 10, and 11, plus the relevant Gold schema and compile smoke.

## Findings
The subway drill bug is fixed. `gold_station_time_metrics` does not have a `mode` column, and the subway drill page now queries that table through `station_canonical` plus temporal filters instead of `WHERE mode = 'subway'`. The schema proof shows `HAS_MODE False`, with columns: `line_code_norm`, `station_canonical`, `service_date`, `hour_bin`, `frequency`, `severity_median`, `severity_p90`, `regularity_p90`, `cause_mix_score`, `composite_score`.

The `Metric to analyze` selector is present on all required pages with the shared 5-option contract. Evidence is visible in [03_Bus_Route_Ranking.py](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/03_Bus_Route_Ranking.py#L55), [04_Subway_Station_Ranking.py](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/04_Subway_Station_Ranking.py#L75), [05_Weekday_Hour_Heatmap.py](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/05_Weekday_Hour_Heatmap.py#L53), [06_Monthly_Trends.py](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/06_Monthly_Trends.py#L75), [10_Bus_Reliability_Drill_Down.py](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/10_Bus_Reliability_Drill_Down.py#L746), and [11_Subway_Reliability_Drill_Down.py](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/11_Subway_Reliability_Drill_Down.py#L786).

Composite behavior is preserved where it was already special-cased. Bus and subway ranking/drill pages keep their legacy composite sort/title branches when `Composite Score` is selected, while the other metrics drive the main ranking/chart metric through the shared resolver. The heatmap and monthly-trend pages also route `Composite Score` through the same column path without changing the underlying data contract.

Fallback handling is in place for sparse or unstable slices. The shared metric resolver returns user-facing fallback messages, and the drill pages emit captions when composite becomes unstable at fine granularity. The subway drill now resolves year/month/day/weekday/time-bin slices without touching the nonexistent `mode` column on `gold_station_time_metrics`.

## Validation
| Check | Result | Evidence |
|---|---|---|
| `gold_station_time_metrics` has no `mode` column | PASS | `HAS_MODE False` from DuckDB `describe` output |
| Subway drill page no longer depends on invalid `mode` filter on `gold_station_time_metrics` | PASS | See [11_Subway_Reliability_Drill_Down.py](/Users/om-college/Work/2%20Canada/York/Winter26/DataViz/Project/ttc_pulse/app/pages/11_Subway_Reliability_Drill_Down.py#L896) and nearby drill queries |
| `Metric to analyze` selector exists on pages 03, 04, 05, 06, 10, 11 | PASS | Shared selector calls in all six pages |
| Composite behavior preserved | PASS | Legacy composite branches remain in pages 03/04/10/11 |
| Metric selector drives ranking/chart metric | PASS | `resolve_metric_choice` is used on the target pages; 05/06 use resolved metric column for heatmap/trends |
| Fallback behavior exists and is user-facing | PASS | Resolver/info/caption paths present in pages 03/04/10/11 |
| Basic compile smoke on touched modules | PASS | `py_compile` succeeded for all touched modules |
| Live click testing | NOT RUN | Interactive Streamlit smoke is not available in this sandbox |

## Residual Risk
Interactive click-through behavior still needs a browser session. The static code path is in place, but the runtime selection loop should be checked once outside the sandbox.
