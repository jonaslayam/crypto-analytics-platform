{{ config(
    materialized='incremental',
    unique_key='fact_sk',
    schema='OCI_GOLD',
    on_schema_change='sync_all_columns'
) }}

-- depends_on: {{ ref('stg_crypto_prices') }}
-- depends_on: {{ ref('dim_assets') }}

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
        -- Works perfectly because event_time is a TIMESTAMP
        WHERE event_time > (SELECT MAX(event_time) - INTERVAL '72' HOUR FROM {{ this }})
    {% endif %}
),

calculated_features AS (
    SELECT 
        bp.*,
        -- 1. Avg and Std Dev for Z-Score
        AVG(bp.price_usd) OVER (
            PARTITION BY bp.asset_id 
            ORDER BY bp.event_time 
            RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW
        ) AS ma_24h_usd,

        STDDEV(bp.price_usd) OVER (
            PARTITION BY bp.asset_id 
            ORDER BY bp.event_time 
            RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW
        ) AS stddev_24h_usd,
        
        -- 2. Lags for Momentum
        -- Native Oracle syntax: INTERVAL 'H:MM' HOUR TO MINUTE
        LAST_VALUE(bp.price_usd) OVER (
            PARTITION BY bp.asset_id 
            ORDER BY bp.event_time 
            RANGE BETWEEN INTERVAL '6:30' HOUR TO MINUTE PRECEDING 
                      AND INTERVAL '5:30' HOUR TO MINUTE PRECEDING
        ) AS last_price_usd_6h,

        LAST_VALUE(bp.volume_usd_24h) OVER (
            PARTITION BY bp.asset_id 
            ORDER BY bp.event_time 
            RANGE BETWEEN INTERVAL '6:30' HOUR TO MINUTE PRECEDING 
                      AND INTERVAL '5:30' HOUR TO MINUTE PRECEDING
        ) AS last_volume_usd_6h

    FROM base_prices bp
),

rsi_logic AS (
    SELECT 
        cf.*,
        cf.price_usd - cf.last_price_usd_6h AS price_diff,

        CASE WHEN (cf.price_usd - cf.last_price_usd_6h) > 0 THEN (cf.price_usd - cf.last_price_usd_6h) ELSE 0 END AS gain,
        CASE WHEN (cf.price_usd - cf.last_price_usd_6h) < 0 THEN ABS(cf.price_usd - cf.last_price_usd_6h) ELSE 0 END AS loss

    FROM calculated_features cf
),

final_features AS (
    SELECT 
        rsi.*,
        -- 3. Z-Score: Is the price at an extreme?
        CASE 
            WHEN rsi.stddev_24h_usd > 0 THEN (rsi.price_usd - rsi.ma_24h_usd) / rsi.stddev_24h_usd 
            ELSE 0 
        END AS z_score_24h,

        -- 4. RSI (Relative Strength Index)
        100 - (100 / (1 + NULLIF(
            AVG(gain) OVER (PARTITION BY asset_id ORDER BY event_time RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW) / 
            NULLIF(AVG(loss) OVER (PARTITION BY asset_id ORDER BY event_time RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW), 0)
        , 0))) AS rsi_24h,

        -- 5. Targets (Looking into the future: FOLLOWING)
        FIRST_VALUE(price_usd) OVER (
            PARTITION BY asset_id 
            ORDER BY event_time 
            RANGE BETWEEN INTERVAL '5:30' HOUR TO MINUTE FOLLOWING 
                      AND INTERVAL '6:30' HOUR TO MINUTE FOLLOWING
        ) AS target_price_next_6h,

        FIRST_VALUE(price_usd) OVER (
            PARTITION BY asset_id 
            ORDER BY event_time 
            RANGE BETWEEN INTERVAL '23:30' HOUR TO MINUTE FOLLOWING 
                      AND INTERVAL '24:30' HOUR TO MINUTE FOLLOWING
        ) AS target_price_next_24h,

        CASE 
            WHEN FIRST_VALUE(price_usd) OVER (
                PARTITION BY asset_id 
                ORDER BY event_time 
                RANGE BETWEEN INTERVAL '5:30' HOUR TO MINUTE FOLLOWING 
                          AND INTERVAL '6:30' HOUR TO MINUTE FOLLOWING
            ) > price_usd 
            THEN 1 
            
            -- Optional but recommended for ML:
            -- If 6 hours haven't passed yet, we don't know if it went up or down.
            WHEN FIRST_VALUE(price_usd) OVER (
                PARTITION BY asset_id 
                ORDER BY event_time 
                RANGE BETWEEN INTERVAL '5:30' HOUR TO MINUTE FOLLOWING 
                          AND INTERVAL '6:30' HOUR TO MINUTE FOLLOWING
            ) IS NULL 
            THEN NULL
            
            ELSE 0 
        END AS target_direction_next_6h,

        -- Additional ratios
        CASE WHEN rsi.last_price_usd_6h > 0 THEN ((rsi.price_usd - rsi.last_price_usd_6h) / rsi.last_price_usd_6h) * 100 ELSE 0 END AS pct_change_since_last,
        CASE WHEN rsi.last_volume_usd_6h > 0 THEN ((rsi.volume_usd_24h - rsi.last_volume_usd_6h) / rsi.last_volume_usd_6h) * 100 ELSE 0 END AS volume_pct_change

    FROM rsi_logic rsi
), 
deduplication as (
    SELECT 
        f.fact_sk,
        d.asset_sk,
        f.date_key,
        f.event_time,
        f.price_usd,
        f.ma_24h_usd,
        f.z_score_24h,
        COALESCE(f.rsi_24h, 50) AS rsi_24h,
        f.pct_change_since_last,
        f.volume_pct_change,
        f.target_price_next_24h,
        f.target_price_next_6h,
        f.target_direction_next_6h,
        EXTRACT(HOUR FROM f.event_time) AS hour_of_day,
        TO_CHAR(f.event_time, 'D') AS day_of_week,
        ROW_NUMBER() OVER (PARTITION BY f.fact_sk ORDER BY f.event_time DESC) as dedupe_rn
    FROM final_features f
    JOIN {{ ref('dim_assets') }} d ON f.asset_id = d.asset_id AND d.is_current = 1
)
SELECT 
    CAST(fact_sk AS VARCHAR2(100)) AS fact_sk,
    asset_sk,
    date_key,
    event_time,
    price_usd,
    ma_24h_usd,
    z_score_24h,
    rsi_24h,
    pct_change_since_last,
    volume_pct_change,
    target_price_next_24h,
    target_price_next_6h,
    target_direction_next_6h,
    hour_of_day,
    day_of_week
FROM deduplication d
WHERE d.dedupe_rn = 1