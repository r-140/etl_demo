{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='vendor_hk',
    tags=['data_vault', 'hub', 'silver']
) }}

-- Hub: Vendor

WITH source AS (
    SELECT DISTINCT
        vendor_id,
        valid_from AS load_date,
        'olap.dim_vendor' AS record_source
    FROM {{ source('olap', 'dim_vendor') }}
),

hashed AS (
    SELECT
        CAST(MD5(CAST(vendor_id AS String)) AS FixedString(32)) AS vendor_hk,
        vendor_id AS vendor_bk,
        load_date,
        record_source
    FROM source
)

SELECT * FROM hashed
{% if is_incremental() %}
WHERE vendor_hk NOT IN (SELECT vendor_hk FROM {{ this }})
{% endif %}
