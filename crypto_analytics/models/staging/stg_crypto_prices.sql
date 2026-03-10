SELECT
    ASSET_ID,
    SYMBOL,
    PRICE_USD,
    PROCESSED_AT
FROM {{ source('oci_silver_source', 'CRYPTO_ASSETS_EXT') }}