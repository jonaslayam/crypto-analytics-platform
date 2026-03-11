{{ config(
    materialized='table',
    schema='OCI_GOLD'
) }}

WITH prices_ranked AS (
    SELECT 
        CAST(asset_id AS VARCHAR2(100)) as asset_id,
        CAST(price_usd AS NUMBER(18, 8)) as price_usd,
        CAST(processed_at AS TIMESTAMP) as processed_at,
        TO_CHAR(processed_at, 'YYYYMMDD') as date_key,
        ROW_NUMBER() OVER (
            PARTITION BY asset_id 
            ORDER BY processed_at DESC
        ) AS pos
    FROM {{ ref('stg_crypto_prices') }}
)

SELECT 
    asset_id,    -- Link to scd_assets_metadata
    date_key,    -- Link to dim_date
    price_usd,   -- Measure
    processed_at -- Event timestamp
FROM prices_ranked 
WHERE pos = 1