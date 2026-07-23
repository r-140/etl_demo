{% snapshot dim_customer_snapshot %}
    {{
        config(
            target_schema=var('customer_schema') ~ '_snapshots',
            unique_key='customer_key',
            strategy='check',
            check_cols=['tier', 'segment', 'email', 'phone']
        )
    }}

    SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}
