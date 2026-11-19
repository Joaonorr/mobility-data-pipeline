-- Demanda total de viagens por hora de embarque, somando todos os dias e zonas.
SELECT
  pickup_hour,
  SUM(trip_count) AS trips
FROM demand_hourly
GROUP BY pickup_hour
ORDER BY pickup_hour
