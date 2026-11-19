-- As 10 zonas de embarque com mais viagens, no período publicado.
SELECT
  pu_zone,
  pu_borough,
  SUM(trip_count) AS trips
FROM demand_hourly
GROUP BY pu_zone, pu_borough
ORDER BY trips DESC, pu_zone
LIMIT 10
