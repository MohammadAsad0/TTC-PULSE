-- Share of delay events that link to GTFS, by mode and link status.
-- Bus: the matched share is the GTFS route match rate.
SELECT
  mode,
  link_status,
  sum(row_count) AS events,
  round(100.0 * sum(row_count) / sum(sum(row_count)) OVER (PARTITION BY mode), 2) AS pct_of_mode
FROM gold_linkage_quality
GROUP BY mode, link_status
ORDER BY mode, link_status;
