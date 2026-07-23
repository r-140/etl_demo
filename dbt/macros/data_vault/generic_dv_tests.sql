{% test data_vault_hub_structure(model, column_name) %}
    -- Generic test: Verify hub has correct structure (no descriptive attributes)
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
{% endtest %}

{% test data_vault_link_integrity(model, column_name) %}
    -- Generic test: Verify link hash key references exist in hubs
    SELECT l.{{ column_name }}
    FROM {{ model }} l
    WHERE l.{{ column_name }} IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM {{ ref('hub_customer') }} h 
          WHERE h.customer_hk = l.{{ column_name }}
      )
{% endtest %}

{% test data_vault_hash_diff_not_null(model, column_name) %}
    -- Generic test: Hash diff must not be null in satellites
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
{% endtest %}
