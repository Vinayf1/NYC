-- 1. Revenue by borough: which pickup borough produced the most revenue in January 2025.
SELECT pickup_borough, ROUND(SUM(total_revenue), 2) AS revenue
FROM taxi_pipeline.daily_borough_summary
WHERE year = '2025' AND month = '01'
GROUP BY pickup_borough
ORDER BY revenue DESC;

-- 2. Daily trend: how total trip volume changed day by day across the month.
SELECT trip_date, SUM(trip_count) AS trips
FROM taxi_pipeline.daily_borough_summary
WHERE year = '2025' AND month = '01'
GROUP BY trip_date
ORDER BY trip_date;

-- 3. Data quality: how many rows were rejected, and for which reason.
SELECT reject_reason, COUNT(*) AS rows_rejected
FROM taxi_pipeline.yellow_trips_quarantine
WHERE year = '2025' AND month = '01'
GROUP BY reject_reason
ORDER BY rows_rejected DESC;
