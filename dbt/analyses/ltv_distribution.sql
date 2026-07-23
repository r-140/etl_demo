-- Ad-hoc analysis: Customer Lifetime Value distribution

WITH customer_metrics AS (
    SELECT * FROM {{ ref('int_customer_metrics') }}
)

SELECT
    CASE 
        WHEN lifetime_value < 100 THEN '< $100'
        WHEN lifetime_value < 500 THEN '$100 - $499'
        WHEN lifetime_value < 1000 THEN '$500 - $999'
        WHEN lifetime_value < 5000 THEN '$1K - $4.9K'
        WHEN lifetime_value < 10000 THEN '$5K - $9.9K'
        ELSE '$10K+'
    END AS ltv_bucket,
    COUNT(*) AS customer_count,
    AVG(total_orders) AS avg_orders,
    AVG(days_since_last_order) AS avg_days_since_order
FROM customer_metrics
GROUP BY ltv_bucket
ORDER BY MIN(lifetime_value)
