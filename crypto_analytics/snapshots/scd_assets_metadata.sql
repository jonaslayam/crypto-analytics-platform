{% snapshot scd_assets_metadata %}

{{
    config(
      target_schema='OCI_GOLD',
      strategy='check',
      unique_key='asset_id',
      check_cols=['name', 'explorer', 'symbol'],
      invalidate_hard_deletes=False,
    )
}}

WITH ranked_assets AS (
    SELECT 
        asset_id,
        symbol,
        name,
        explorer,
        processed_at AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY asset_id 
            ORDER BY event_time DESC
        ) as rn
    FROM {{ source('oci_silver_source', 'CRYPTO_ASSETS_EXT') }}
)

SELECT 
    asset_id,
    symbol,
    name,
    explorer,
    updated_at
FROM ranked_assets
WHERE rn = 1

{% endsnapshot %}