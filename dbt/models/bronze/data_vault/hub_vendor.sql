{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='vendor_hk',
    tags=['data_vault', 'hub', 'bronze']
) }}

-- Hub: Vendor

WITH source AS (
    SELECT DISTINCT
        vendor_id,
        load_date,
        record_source
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
