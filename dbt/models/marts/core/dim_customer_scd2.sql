{{ config(materialized='view', tags=['gold', 'kimball', 'scd2']) }}

-- SCD Type 2: dbt snapshots close the old version and create a new version.
-- valid_to is made explicit for easier point-in-time joins from facts.
select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_version_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    segment,
    tier,
    registration_date,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, toDateTime64('2299-12-31 00:00:00', 6)) as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('dim_customer_snapshot') }}
