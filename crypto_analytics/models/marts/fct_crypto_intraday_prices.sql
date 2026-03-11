{{ config(
    materialized='incremental',
    unique_key='fact_sk',
    schema='OCI_GOLD'
) }}

WITH base_prices AS (
    SELECT 
        STANDARD_HASH(CAST(asset_id AS VARCHAR2(100)) || TO_CHAR(event_time, 'YYYYMMDDHH24MISS'), 'MD5') as fact_sk,
        CAST(asset_id AS VARCHAR2(100)) as asset_id,
        CAST(price_usd AS NUMBER(18, 8)) as price_usd,
        CAST(event_time AS TIMESTAMP) as event_time,
        TO_CHAR(event_time, 'YYYYMMDD') as date_key
    FROM {{ ref('stg_crypto_prices') }}
    
    {% if is_incremental() %}
        WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
    {% endif %}
),

calculated_features AS (
    SELECT 
        fact_sk,
        asset_id,
        date_key,
        event_time,
        price_usd,
        
        -- 1. Tendency: Moving Average of 24 hours (6 data points if querying every 4h)
        AVG(price_usd) OVER (
            PARTITION BY asset_id 
            ORDER BY event_time 
            RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW
        ) AS ma_24h_usd,
        
        -- 2. Momentum: Price from the previous call (4 or 6 hours ago)
        LAG(price_usd) OVER (
            PARTITION BY asset_id 
            ORDER BY event_time
        ) AS last_price_usd
        
    FROM base_prices
),

final_features AS (
    SELECT 
        *,
        CASE 
            WHEN last_price_usd IS NOT NULL AND last_price_usd > 0 
            THEN ((price_usd - last_price_usd) / last_price_usd) * 100 
            ELSE 0 
        END AS pct_change_since_last
    FROM calculated_features
)

SELECT 
    f.fact_sk,
    d.asset_sk,
    f.date_key,
    f.event_time,
    f.price_usd,
    f.ma_24h_usd,
    f.pct_change_since_last
FROM final_features f
LEFT JOIN {{ ref('dim_assets') }} d
    ON f.asset_id = d.asset_id
    AND f.event_time >= d.valid_from 
    AND (f.event_time < d.valid_to OR d.valid_to IS NULL)