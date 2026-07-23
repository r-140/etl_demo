{{
  config(
    materialized='table',
    tags=['marts', 'marketing', 'recommendations']
  )
}}

-- Product affinity analysis for cross-sell recommendations
-- Finds products frequently bought together

WITH orders AS (
    SELECT 
        order_id,
        product_key,
        product_name,
        category_name
    FROM {{ ref('int_orders_enriched') }}
),

-- Self-join to find product pairs in same order
product_pairs AS (
    SELECT
        o1.product_key AS product_a_key,
        o1.product_name AS product_a_name,
        o1.category_name AS product_a_category,
        o2.product_key AS product_b_key,
        o2.product_name AS product_b_name,
        o2.category_name AS product_b_category,
        o1.order_id
    FROM orders o1
    JOIN orders o2 ON o1.order_id = o2.order_id
    WHERE o1.product_key < o2.product_key  -- Avoid duplicates and self-pairs
),

affinity_metrics AS (
    SELECT
        product_a_key,
        product_a_name,
        product_a_category,
        product_b_key,
        product_b_name,
        product_b_category,
        COUNT(DISTINCT order_id) AS co_occurrence_count,

        -- Lift calculation (would need total order counts per product in full implementation)
        co_occurrence_count * 1.0 / 
            (SELECT COUNT(DISTINCT order_id) FROM orders WHERE product_key = product_a_key) 
            AS confidence_a_to_b

    FROM product_pairs
    GROUP BY 
        product_a_key, product_a_name, product_a_category,
        product_b_key, product_b_name, product_b_category
    HAVING co_occurrence_count >= 10  -- Filter noise
)

SELECT
    product_a_key,
    product_a_name,
    product_a_category,
    product_b_key,
    product_b_name,
    product_b_category,
    co_occurrence_count,
    confidence_a_to_b,

    -- Recommendation strength
    CASE 
        WHEN confidence_a_to_b >= 0.5 THEN 'Strong'
        WHEN confidence_a_to_b >= 0.3 THEN 'Moderate'
        WHEN confidence_a_to_b >= 0.1 THEN 'Weak'
        ELSE 'Very Weak'
    END AS recommendation_strength,

    -- Cross-category flag (useful for cross-sell)
    CASE WHEN product_a_category != product_b_category THEN 1 ELSE 0 END AS cross_category

FROM affinity_metrics
WHERE confidence_a_to_b >= 0.1
ORDER BY co_occurrence_count DESC
