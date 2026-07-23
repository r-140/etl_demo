{% macro test_hub_has_no_descriptive_attributes(model, hub_hash_key, hub_business_key) %}
    -- Data Vault test: Hub should contain ONLY hash key, business key, load_date, record_source
    -- Any other column is a descriptive attribute and violates DV principles

    WITH column_check AS (
        SELECT 
            COUNT(*) AS descriptive_column_count
        FROM information_schema.columns 
        WHERE table_name = '{{ model.name }}'
          AND table_schema = '{{ model.schema }}'
          AND column_name NOT IN (
              '{{ hub_hash_key }}',
              '{{ hub_business_key }}', 
              'load_date',
              'record_source'
          )
    )
    SELECT * FROM column_check WHERE descriptive_column_count > 0

{% endmacro %}

{% macro test_link_references_hubs(model, link_hash_key, hub_hash_keys) %}
    -- Data Vault test: All hub hash keys in Link must exist in respective Hubs
    -- This ensures referential integrity in the Raw Vault

    SELECT 
        l.{{ link_hash_key }}
    FROM {{ model }} l
    LEFT JOIN {{ ref('hub_customer') }} h ON l.customer_hk = h.customer_hk
    WHERE h.customer_hk IS NULL
      AND l.customer_hk IS NOT NULL

{% endmacro %}

{% macro test_hash_key_is_deterministic(model, hash_key_column, business_key_columns) %}
    -- Data Vault test: Hash key must be deterministic
    -- Same business key must always produce the same hash key

    SELECT 
        {{ business_key_columns | join(', ') }},
        COUNT(DISTINCT {{ hash_key_column }}) AS hash_key_count
    FROM {{ model }}
    GROUP BY {{ business_key_columns | join(', ') }}
    HAVING hash_key_count > 1

{% endmacro %}

{% macro test_satellite_hash_diff_detects_changes(model, parent_hash_key, hash_diff_column) %}
    -- Data Vault test: Hash diff must change when descriptive attributes change
    -- Same parent hash key + different hash diff = different records

    SELECT 
        {{ parent_hash_key }},
        {{ hash_diff_column }},
        COUNT(*) AS duplicate_count
    FROM {{ model }}
    GROUP BY {{ parent_hash_key }}, {{ hash_diff_column }}
    HAVING COUNT(*) > 1

{% endmacro %}

{% macro test_no_orphan_hubs(model, hub_hash_key) %}
    -- Data Vault test: No hub records should exist without at least one satellite or link
    -- (Optional - depends on business rules)

    SELECT h.{{ hub_hash_key }}
    FROM {{ model }} h
    LEFT JOIN {{ ref('sat_customer_details') }} s ON h.{{ hub_hash_key }} = s.{{ hub_hash_key }}
    WHERE s.{{ hub_hash_key }} IS NULL

{% endmacro %}

{% macro generate_hash_key(business_key, salt='') %}
    -- Generate deterministic hash key for Data Vault
    -- Uses MD5 with optional salt for additional uniqueness
    CAST(MD5(CONCAT('{{ salt }}', CAST({{ business_key }} AS String))) AS FixedString(32))
{% endmacro %}

{% macro generate_hash_diff(column_list) %}
    -- Generate hash diff for change detection in satellites
    -- Concatenates all descriptive columns and hashes them
    CAST(MD5(CONCAT(
        {% for col in column_list %}
        COALESCE(CAST({{ col }} AS String), '')
        {% if not loop.last %}, '|', {% endif %}
        {% endfor %}
    )) AS FixedString(32))
{% endmacro %}
