{% snapshot scd_assets_metadata %}

{{
    config(
      target_schema='OCI_GOLD',
      strategy='check',
      unique_key='asset_id',
      check_cols=['name', 'explorer', 'symbol'],
      invalidate_hard_deletes=True,
    )
}}

select 
    asset_id,
    symbol,
    name,
    explorer,
    processed_at as updated_at
from {{ source('oci_silver_source', 'CRYPTO_ASSETS_EXT') }}

{% endsnapshot %}