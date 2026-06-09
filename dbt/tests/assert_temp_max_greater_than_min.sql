/*
  Custom dbt test: temp_max_c must always be >= temp_min_c.
  Returns rows that FAIL the test (dbt expects 0 rows to pass).
*/

SELECT
    city,
    observation_date,
    temp_max_c,
    temp_min_c
FROM {{ ref('stg_weather') }}
WHERE temp_max_c < temp_min_c
