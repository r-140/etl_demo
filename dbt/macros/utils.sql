{% macro cents_to_dollars(column_name, precision=2) %}
    -- Convert cents to dollars with specified precision
    round({{ column_name }} / 100, {{ precision }})
{% endmacro %}

{% macro date_spine(datepart, start_date, end_date) %}
    -- Generate a date spine for time-series analysis
    SELECT 
        date
    FROM (
        SELECT 
            arrayJoin(range(
                toInt32(toDate('{{ start_date }}')),
                toInt32(toDate('{{ end_date }}'))
            )) AS date_int,
            toDate(date_int) AS date
    )
{% endmacro %}

{% macro generate_surrogate_key(fields) %}
    -- Generate surrogate key from multiple fields using farmHash
    farmFingerprint64(
        concat(
            {% for field in fields %}
                toString({{ field }})
                {% if not loop.last %}, '|', {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}

{% macro test_positive_values(model, column_name) %}
    -- Custom test: all values must be positive
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} <= 0
    OR {{ column_name }} IS NULL
{% endmacro %}

{% macro test_unique_combination(model, columns) %}
    -- Custom test: combination of columns must be unique
    SELECT 
        {% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
        COUNT(*) AS count
    FROM {{ model }}
    GROUP BY {% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
    HAVING COUNT(*) > 1
{% endmacro %}
