/*
  city_monthly_stats — Mart model (materialized as TABLE)
  Monthly aggregated statistics per city.
  Designed for trend analysis, seasonal comparison, and executive dashboards.
  One row = one city × one calendar month.
*/

WITH daily AS (
    SELECT * FROM {{ ref('daily_weather') }}
),

monthly AS (
    SELECT
        city,
        month_start,
        year_number,
        month_number,
        COUNT(*)                            AS days_with_data,

        -- Temperature stats
        ROUND(AVG(temp_avg_c), 1)           AS avg_temp_c,
        ROUND(MAX(temp_max_c), 1)           AS max_temp_c,
        ROUND(MIN(temp_min_c), 1)           AS min_temp_c,
        ROUND(AVG(temp_range_c), 1)         AS avg_temp_range_c,

        -- Precipitation
        ROUND(SUM(precipitation_mm), 1)     AS total_precipitation_mm,
        COUNT(*) FILTER (
            WHERE precipitation_mm > 0
        )                                   AS rainy_days,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE precipitation_mm > 0)
            / COUNT(*), 1
        )                                   AS rainy_days_pct,

        -- Wind
        ROUND(MAX(wind_max_kmh), 1)         AS max_wind_kmh,
        ROUND(AVG(wind_max_kmh), 1)         AS avg_wind_kmh,

        -- Most frequent weather description
        MODE() WITHIN GROUP (
            ORDER BY weather_description
        )                                   AS dominant_weather

    FROM daily
    GROUP BY city, month_start, year_number, month_number
)

SELECT * FROM monthly
ORDER BY city, month_start
