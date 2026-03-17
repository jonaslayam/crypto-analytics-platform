{{ config(
    materialized='incremental',
    unique_key='fact_sk',
    schema='OCI_GOLD',
    on_schema_change='sync_all_columns'
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
        WHERE event_time > (SELECT MAX(event_time) - INTERVAL '30' HOUR FROM {{ this }})
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
        
        -- 2. Lags and Diff for Momentum and RSI
        LAG(bp.price_usd) OVER (PARTITION BY bp.asset_id ORDER BY bp.event_time) AS last_price_usd,
        LAG(bp.volume_usd_24h) OVER (PARTITION BY bp.asset_id ORDER BY bp.event_time) AS last_volume_usd,
        bp.price_usd - LAG(bp.price_usd) OVER (PARTITION BY bp.asset_id ORDER BY bp.event_time) AS price_diff
    FROM base_prices bp
),

rsi_logic AS (
    SELECT 
        cf.*,
        CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END AS gain,
        CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END AS loss
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

        -- 5. Target 
        -- (What the AI should predict: The price in 24h / 4 steps ahead)
        -- (What the AI should predict: The direction in 6h / 1 step ahead)
        -- (Target: 1 if price goes up, 0 if price goes down)
        LEAD(price_usd, 4) OVER (PARTITION BY asset_id ORDER BY event_time) AS target_price_next_24h,
        LEAD(price_usd, 1) OVER (PARTITION BY asset_id ORDER BY event_time) AS target_price_next_6h,
        CASE WHEN LEAD(price_usd, 1) OVER (PARTITION BY asset_id ORDER BY event_time) > price_usd THEN 1 ELSE 0 END AS target_direction_next_6h,

        -- Additional ratios
        CASE WHEN rsi.last_price_usd > 0 THEN ((rsi.price_usd - rsi.last_price_usd) / rsi.last_price_usd) * 100 ELSE 0 END AS pct_change_since_last,
        CASE WHEN rsi.last_volume_usd > 0 THEN ((rsi.volume_usd_24h - rsi.last_volume_usd) / rsi.last_volume_usd) * 100 ELSE 0 END AS volume_pct_change

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
    JOIN {{ ref('dim_assets') }} d
        ON f.asset_id = d.asset_id
        AND f.event_time >= d.valid_from 
        AND (f.event_time < d.valid_to OR d.valid_to IS NULL)
)
SELECT 
    fact_sk,
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
