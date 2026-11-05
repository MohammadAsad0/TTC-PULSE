-- Top 10 bus routes by delay events, 2014 to January 2026.
-- The gold mart holds event counts, not total delay minutes.
SELECT
  route_id_gtfs AS route_id,
  sum(event_count) AS delay_events
FROM gold_delay_events_core
WHERE mode = 'bus'
  AND route_id_gtfs IS NOT NULL
GROUP BY route_id_gtfs
ORDER BY delay_events DESC
LIMIT 10;
