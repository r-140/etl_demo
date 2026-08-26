{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='customer_hk',
    tags=['data_vault', 'silver', 'satellite']
  )
}}

-- Satellite: Customer Details
-- Contains all descriptive attributes for Customer
-- SCD Type 2 via load_date (no end_date in raw vault)
-- Hash diff for change detection

WITH source AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        segment,
        tier,
        registration_date,
        load_date,
        record_source
    FROM {{ ref('stg_customers') }}
),

hashed AS (
    SELECT
        -- Parent Hub Hash Key
        CAST(MD5(CAST(customer_id AS String)) AS FixedString(32)) AS customer_hk,

        -- Descriptive attributes
        first_name,
        last_name,
        email,
        phone,
        segment,
        tier,
        registration_date,

        -- Hash Diff: MD5 of all descriptive attributes for change detection
        CAST(MD5(CONCAT(
            COALESCE(first_name, ''), '|',
            COALESCE(last_name, ''), '|',
            COALESCE(email, ''), '|',
            COALESCE(phone, ''), '|',
            COALESCE(segment, ''), '|',
            COALESCE(tier, ''), '|',
            COALESCE(CAST(registration_date AS String), '')
        )) AS FixedString(32)) AS hash_diff,

        -- Audit columns
        load_date,
        record_source

    FROM source
)

SELECT * FROM hashed
{% if is_incremental() %}
WHERE (customer_hk, hash_diff) NOT IN (SELECT customer_hk, hash_diff FROM {{ this }})
{% endif %}

-- Data Vault Satellite tests:
-- 1. customer_hk + load_date is unique (composite key)
-- 2. customer_hk references hub_customer.customer_hk
-- 3. hash_diff changes when attributes change
-- 4. No business keys as descriptive attributes (only hk)
