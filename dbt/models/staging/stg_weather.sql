/*
  stg_weather — Staging model
  Cleans and standardizes the raw meteo_journaliere table:
    - Renames French columns to English for downstream consumers
    - Adds a human-readable weather description from the WMO code
    - Casts types explicitly for safety
*/

WITH source AS (
    SELECT * FROM {{ source('meteo', 'meteo_journaliere') }}
),

renamed AS (
    SELECT
        id,
        ville                           AS city,
        date                            AS observation_date,
        temp_max_c::NUMERIC(5, 1)       AS temp_max_c,
        temp_min_c::NUMERIC(5, 1)       AS temp_min_c,
        temp_moyenne_c::NUMERIC(5, 1)   AS temp_avg_c,
        temp_max_c - temp_min_c         AS temp_range_c,
        precipitation_mm::NUMERIC(6, 1) AS precipitation_mm,
        vent_max_kmh::NUMERIC(6, 1)     AS wind_max_kmh,
        code_meteo                      AS weather_code,
        -- Human-readable label derived from WMO weather interpretation codes
        CASE
            WHEN code_meteo = 0             THEN 'Clear sky'
            WHEN code_meteo IN (1, 2, 3)    THEN 'Partly cloudy'
            WHEN code_meteo IN (45, 48)     THEN 'Foggy'
            WHEN code_meteo BETWEEN 51 AND 57 THEN 'Drizzle'
            WHEN code_meteo BETWEEN 61 AND 67 THEN 'Rain'
            WHEN code_meteo BETWEEN 71 AND 77 THEN 'Snow'
            WHEN code_meteo BETWEEN 80 AND 82 THEN 'Rain showers'
            WHEN code_meteo BETWEEN 95 AND 99 THEN 'Thunderstorm'
            ELSE 'Unknown'
        END                             AS weather_description,
        insere_le                       AS loaded_at
    FROM source
)

SELECT * FROM renamed
