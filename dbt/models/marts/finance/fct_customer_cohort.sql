{{
  config(
    materialized='table',
    tags=['marts', 'finance', 'cohort']
  )
}}

-- Customer cohort analysis
-- Tracks retention and LTV by acquisition month

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),

customer_first_order AS (
    SELECT
        customer_key,
        MIN(date) AS first_order_date,
        toStartOfMonth(MIN(date)) AS cohort_month
    FROM orders
    GROUP BY customer_key
),

cohort_activity AS (
    SELECT
        cfo.cohort_month,
        toStartOfMonth(o.date) AS activity_month,
        dateDiff('month', cfo.cohort_month, toStartOfMonth(o.date)) AS period_number,
        COUNT(DISTINCT o.customer_key) AS active_customers,
        SUM(o.line_total) AS revenue,
        COUNT(DISTINCT o.order_id) AS orders
    FROM orders o
    JOIN customer_first_order cfo ON o.customer_key = cfo.customer_key
    GROUP BY cfo.cohort_month, toStartOfMonth(o.date), period_number
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_key) AS cohort_size
    FROM customer_first_order
    GROUP BY cohort_month
)

SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.period_number,
    ca.active_customers,
    ca.active_customers / NULLIF(cs.cohort_size, 0) AS retention_rate,
    ca.revenue,
    ca.revenue / NULLIF(ca.active_customers, 0) AS revenue_per_customer,
    ca.orders,
    ca.orders / NULLIF(ca.active_customers, 0) AS orders_per_customer
FROM cohort_activity ca
JOIN cohort_sizes cs ON ca.cohort_month = cs.cohort_month
ORDER BY ca.cohort_month, ca.period_number
