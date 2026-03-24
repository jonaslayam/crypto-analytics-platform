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

/* IMPORTANT FOR ORACLE ADW: 
   We avoid using the 'WITH' clause (CTE) here because it triggers ORA-32034 
   during dbt's internal MERGE operation. Instead, we use a standard subquery.
*/

SELECT 
    CAST(asset_id AS VARCHAR2(100)) as asset_id,
    symbol,
    name,
    explorer,
    updated_at
FROM (
    /* Subquery to rank assets by event_time to get the latest metadata per asset_id */
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
WHERE rn = 1

{% endsnapshot %}