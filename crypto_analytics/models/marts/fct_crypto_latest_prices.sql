WITH prices_ranked AS (
    SELECT 
        CAST(asset_id AS VARCHAR2(100)) as asset_id,
        CAST(symbol AS VARCHAR2(20)) as symbol,
        CAST(price_usd AS NUMBER(18, 8)) as price_usd,
        CAST(processed_at AS TIMESTAMP) as processed_at,
        ROW_NUMBER() OVER (
            PARTITION BY asset_id 
            ORDER BY processed_at DESC
        ) AS pos
    FROM {{ ref('stg_crypto_prices') }}
)

SELECT 
    asset_id,
    symbol,
    price_usd,
    processed_at
FROM prices_ranked 
WHERE pos = 1