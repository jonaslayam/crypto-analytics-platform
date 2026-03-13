{{ config(
    materialized='view',
    schema='OCI_GOLD'
) }}

SELECT
    dbt_scd_id AS asset_sk,
    asset_id,
    symbol,
    name,
    explorer,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE WHEN dbt_valid_to IS NULL THEN 1 ELSE 0 END AS is_current
FROM {{ ref('scd_assets_metadata') }}
