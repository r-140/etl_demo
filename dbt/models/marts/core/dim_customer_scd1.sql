{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='customer_id',
    tags=['gold', 'kimball', 'scd1']
) }}

-- SCD Type 1: one row per business key; corrections overwrite prior values.
select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    segment,
    tier,
    registration_date,
    now() as dw_updated_at
from {{ ref('stg_customers') }}

