/*
  daily_weather — Mart model (materialized as TABLE)
  Production-ready daily weather table with enriched columns.
  Used by dashboards, reports, and downstream consumers.

  Built on top of stg_weather — never reads raw tables directly.
*/

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

enriched AS (
    SELECT
        city,
        observation_date,

        -- Temperatures
        temp_max_c,
        temp_min_c,
        temp_avg_c,
        temp_range_c,

        -- Precipitation
        precipitation_mm,
        CASE
            WHEN precipitation_mm = 0     THEN 'Dry'
            WHEN precipitation_mm < 5     THEN 'Light rain'
            WHEN precipitation_mm < 20    THEN 'Moderate rain'
            ELSE                               'Heavy rain'
        END                             AS precipitation_category,

        -- Wind
        wind_max_kmh,
        CASE
            WHEN wind_max_kmh < 20        THEN 'Calm'
            WHEN wind_max_kmh < 40        THEN 'Moderate'
            WHEN wind_max_kmh < 60        THEN 'Strong'
            ELSE                               'Storm'
        END                             AS wind_category,

        -- Weather
        weather_code,
        weather_description,

        -- Time dimensions for easy filtering
        DATE_TRUNC('week',  observation_date)::DATE AS week_start,
        DATE_TRUNC('month', observation_date)::DATE AS month_start,
        EXTRACT(DOW FROM observation_date)::INT     AS day_of_week,  -- 0=Sunday
        EXTRACT(MONTH FROM observation_date)::INT   AS month_number,
        EXTRACT(YEAR  FROM observation_date)::INT   AS year_number,

        loaded_at
    FROM staging
)

SELECT * FROM enriched
ORDER BY city, observation_date
