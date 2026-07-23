{{ 
  config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='customer_hk',
    tags=['data_vault', 'bronze', 'hub']
  )
}}

-- Hub: Customer
-- Data Vault 2.0 principle: Contains ONLY business keys, no descriptive attributes
-- Hash key is calculated from business key for deterministic surrogate keys

WITH source AS (
    SELECT DISTINCT
        customer_id,
        load_date,
        record_source
    FROM {{ ref('stg_customers') }}
    WHERE customer_id IS NOT NULL
),

hashed AS (
    SELECT
        -- Hash Key: MD5 of business key (deterministic surrogate)
        CAST(MD5(CAST(customer_id AS String)) AS FixedString(32)) AS customer_hk,

        -- Business Key: the natural key from source
        customer_id AS customer_bk,

        -- Audit columns
        load_date,
        record_source

    FROM source
)

SELECT * FROM hashed

-- Data Vault tests that should pass:
-- 1. customer_hk is unique (no duplicate hash keys)
-- 2. customer_bk is unique (no duplicate business keys)  
-- 3. customer_hk is not null
-- 4. customer_bk is not null
-- 5. No descriptive attributes (only hk, bk, audit)
