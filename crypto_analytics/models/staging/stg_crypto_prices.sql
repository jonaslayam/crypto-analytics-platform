WITH source_data AS (
    SELECT * 
    FROM {{ source('oci_silver_source', 'CRYPTO_ASSETS_EXT') }}
),

staged AS (
    SELECT
        STANDARD_HASH(ASSET_ID || TO_CHAR(EVENT_TIME, 'YYYYMMDDHH24MISS'), 'MD5') AS price_sk,
        
        LOWER(ASSET_ID) AS asset_id,
        PRICE_USD       AS price_usd,
        EVENT_TIME      AS event_time
        
    FROM source_data
    
    WHERE EVENT_TIME IS NOT NULL AND ASSET_ID IS NOT NULL
)

SELECT * FROM staged