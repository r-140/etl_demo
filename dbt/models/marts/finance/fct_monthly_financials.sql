{{
  config(
    materialized='table',
    tags=['marts', 'finance', 'executive']
  )
}}

-- Monthly financial summary for executive reporting

WITH daily_sales AS (
    SELECT * FROM {{ ref('fct_sales_daily') }}
),

monthly AS (
    SELECT
        year,
        month,
        year_month,
        year_quarter,

        -- Revenue
        SUM(total_revenue) AS revenue,
        SUM(total_gross_revenue) AS gross_revenue,
        SUM(total_discounts) AS discounts,
        SUM(total_profit) AS gross_profit,

        -- Margins
        gross_profit / NULLIF(revenue, 0) AS gross_margin_pct,
        discounts / NULLIF(gross_revenue, 0) AS discount_rate,

        -- Volume
        SUM(total_orders) AS orders,
        SUM(total_quantity) AS units_sold,
        SUM(unique_customers) AS unique_customers,

        -- Averages
        AVG(avg_order_value) AS avg_order_value,
        AVG(avg_profit_margin) AS avg_profit_margin,

        -- Customer mix
        SUM(vip_customers) AS vip_customers,
        SUM(vip_revenue) AS vip_revenue,
        vip_revenue / NULLIF(revenue, 0) AS vip_revenue_pct,

        -- Payment mix
        SUM(credit_card_orders) AS credit_card_orders,
        SUM(debit_card_orders) AS debit_card_orders,
        SUM(paypal_orders) AS paypal_orders,

        -- MoM calculations (will be calculated in BI tool or with window functions)
        revenue - LAG(revenue) OVER (ORDER BY year_month) AS revenue_mom_change,
        orders - LAG(orders) OVER (ORDER BY year_month) AS orders_mom_change

    FROM daily_sales
    GROUP BY year, month, year_month, year_quarter
)

SELECT * FROM monthly
