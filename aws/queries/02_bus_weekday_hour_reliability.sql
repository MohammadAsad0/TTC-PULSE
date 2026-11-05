-- Bus service by weekday and hour: the 20 weekday-hour cells with the highest composite score.
-- The composite score is a weighted z-score blend. A higher score means more delay pressure.
SELECT
  day_name,
  hour_bin,
  frequency,
  round(composite_score, 3) AS composite_score
FROM gold_time_reliability
WHERE mode = 'bus'
ORDER BY composite_score DESC
LIMIT 20;
