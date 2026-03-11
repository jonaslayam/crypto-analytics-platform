{{ config(
    materialized='table',
    schema='OCI_GOLD'
) }}

WITH date_range AS (
    SELECT 
        TRUNC(MIN(event_time)) as min_date,
        TRUNC(MAX(event_time)) + 365 as max_date
    FROM {{ ref('stg_crypto_prices') }}
),

date_spine AS (
    SELECT 
        min_date + LEVEL - 1 AS date_day
    FROM date_range
    CONNECT BY LEVEL <= (max_date - min_date + 1)
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD') AS date_key,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Q') AS quarter,
    TO_CHAR(date_day, 'IW') AS week_of_year,
    CASE 
        WHEN TO_CHAR(date_day, 'D', 'NLS_DATE_LANGUAGE=ENGLISH') IN ('1', '7') THEN 1 
        ELSE 0 
    END AS is_weekend
FROM date_spine