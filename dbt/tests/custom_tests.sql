-- Custom data quality tests

-- Test: All orders must have valid customer
SELECT o.order_id
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_key = c.customer_key
WHERE c.customer_key IS NULL

-- Test: No future dates
SELECT *
FROM {{ ref('fct_sales_daily') }}
WHERE date > today()

-- Test: Revenue consistency (line_total = quantity * unit_price - discount)
SELECT *,
    quantity * unit_price - discount_amount AS calculated_total
FROM {{ ref('stg_orders') }}
WHERE ABS(line_total - calculated_total) > 0.01
