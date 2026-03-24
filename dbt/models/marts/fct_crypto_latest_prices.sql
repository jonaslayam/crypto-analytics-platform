{{ config(
    materialized='table',
    schema='OCI_GOLD'
) }}

-- depends_on: {{ ref('stg_crypto_prices') }}
-- depends_on: {{ ref('dim_assets') }}

WITH prices_ranked AS (
    SELECT 
        CAST(asset_id AS VARCHAR2(100)) as asset_id,
        CAST(price_usd AS NUMBER(18, 8)) as price_usd,
        CAST(event_time AS TIMESTAMP) as event_time,
        TO_CHAR(event_time, 'YYYYMMDD') as date_key,
        ROW_NUMBER() OVER (
            PARTITION BY asset_id 
            ORDER BY event_time DESC
        ) AS pos
    FROM {{ ref('stg_crypto_prices') }}
)

SELECT 
    d.asset_sk,     -- Link to dim_assets
    r.asset_id,       -- Link to scd_assets_metadata
    r.date_key,       -- Link to dim_date
    r.price_usd,      -- Measure
    r.event_time,     -- Event timestamp
    CURRENT_TIMESTAMP AS PROCESSED_AT
FROM prices_ranked r
JOIN {{ ref('dim_assets') }} d ON r.asset_id = d.asset_id AND d.is_current = 1
WHERE r.pos = 1