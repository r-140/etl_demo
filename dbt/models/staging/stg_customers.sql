{{
  config(
    materialized='view',
    tags=['staging', 'customers']
  )
}}

-- Staging model for customer dimension
-- Filters to current records only (SCD Type 2)

WITH source AS (
    SELECT * FROM {{ source('olap', 'dim_customer') }}
),

current_records AS (
    SELECT
        customer_key,
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        segment,
        tier,
        registration_date,
        valid_from,
        valid_to,
        is_current,
        valid_from AS load_date,
        'olap.dim_customer' AS record_source,

        -- Derived fields
        CONCAT(first_name, ' ', last_name) AS display_name,
        CASE 
            WHEN tier = 'VIP' THEN 4
            WHEN tier = 'Gold' THEN 3
            WHEN tier = 'Silver' THEN 2
            ELSE 1
        END AS tier_rank

    FROM source
    WHERE is_current = 1
)

SELECT * FROM current_records
