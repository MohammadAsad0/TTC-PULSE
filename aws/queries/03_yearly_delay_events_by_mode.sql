-- Delay events per calendar year and mode.
-- 2026 holds January only.
SELECT
  year(service_date) AS service_year,
  mode,
  sum(event_count) AS delay_events
FROM gold_delay_events_core
GROUP BY year(service_date), mode
ORDER BY service_year, mode;
