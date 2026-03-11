{{ config(
    materialized='incremental',
    unique_key='fact_sk',
    schema='OCI_GOLD'
) }}

WITH base_prices AS (
    SELECT 
        price_sk AS fact_sk,
        asset_id,
        price_usd,
        volume_usd_24h,
        event_time,
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
        volume_usd_24h,
        
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
        ) AS last_price_usd,

        -- 3. Volatility: Standard deviation of price over 24 hours
        STDDEV(price_usd) OVER (
            PARTITION BY asset_id 
            ORDER BY event_time 
            RANGE INTERVAL '24' HOUR PRECEDING
        ) AS stddev_24h_usd,

        -- 4. Volume Trend: Volume from the previous call
        LAG(volume_usd_24h) OVER (
            PARTITION BY asset_id 
            ORDER BY event_time
        ) AS last_volume_usd

    FROM base_prices
),

final_features AS (
    SELECT 
        cf.*,
        -- 5. Percentage change since last price
        CASE 
            WHEN cf.last_price_usd IS NOT NULL AND cf.last_price_usd > 0 
            THEN ((cf.price_usd - cf.last_price_usd) / cf.last_price_usd) * 100 
            ELSE 0 
        END AS pct_change_since_last,

        -- 6. Price to Moving Average ratio
        CASE 
            WHEN cf.ma_24h_usd > 0 THEN cf.price_usd / cf.ma_24h_usd 
            ELSE 1 
        END AS price_to_ma_ratio,

        -- 7. Volume percentage change
        CASE 
            WHEN cf.last_volume_usd > 0 
            THEN ((cf.volume_usd_24h - cf.last_volume_usd) / cf.last_volume_usd) * 100 
            ELSE 0 
        END AS volume_pct_change

    FROM calculated_features cf
)

SELECT 
    f.fact_sk,
    d.asset_sk,
    f.date_key,
    f.event_time,
    f.price_usd,
    f.ma_24h_usd,
    f.pct_change_since_last,
    f.price_to_ma_ratio,
    f.volume_pct_change,
    COALESCE(f.stddev_24h_usd, 0) AS stddev_24h_usd,
    EXTRACT(HOUR FROM f.event_time) AS hour_of_day,
    TO_CHAR(f.event_time, 'D') AS day_of_week
FROM final_features f
LEFT JOIN {{ ref('dim_assets') }} d
    ON f.asset_id = d.asset_id
    AND f.event_time >= d.valid_from 
    AND (f.event_time < d.valid_to OR d.valid_to IS NULL)