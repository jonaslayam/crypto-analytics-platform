WITH source_data AS (
    SELECT * FROM {{ source('oci_silver_source', 'CRYPTO_ASSETS_EXT') }}
),

staged AS (
    SELECT
        STANDARD_HASH(LOWER(ASSET_ID) || TO_CHAR(EVENT_TIME, 'YYYYMMDDHH24MISS'), 'MD5') AS price_sk,
        
        CAST(LOWER(ASSET_ID) AS VARCHAR2(100)) AS asset_id,
        CAST(PRICE_USD AS NUMBER(18, 8))       AS price_usd,
        CAST(VOLUME_USD_24H AS NUMBER(24, 8))  AS volume_usd_24h,
        CAST(EVENT_TIME AS TIMESTAMP)          AS event_time
        
    FROM source_data
    WHERE EVENT_TIME IS NOT NULL AND ASSET_ID IS NOT NULL
)

SELECT * FROM staged