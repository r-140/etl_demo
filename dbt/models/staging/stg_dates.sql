{{
  config(
    materialized='view',
    tags=['staging', 'dates']
  )
}}

-- Staging model for date dimension

WITH source AS (
    SELECT * FROM {{ source('olap', 'dim_date') }}
),

enriched AS (
    SELECT
        date_key,
        date,
        year,
        quarter,
        month,
        day,
        day_of_week,
        day_name,
        month_name,
        is_weekend,
        is_holiday,
        fiscal_year,
        fiscal_quarter,

        -- Derived fields
        CONCAT(CAST(year AS String), '-Q', CAST(quarter AS String)) AS year_quarter,
        CONCAT(CAST(year AS String), '-', LPAD(CAST(month AS String), 2, '0')) AS year_month,
        CASE 
            WHEN month IN (1, 2, 3) THEN 'Q1'
            WHEN month IN (4, 5, 6) THEN 'Q2'
            WHEN month IN (7, 8, 9) THEN 'Q3'
            ELSE 'Q4'
        END AS quarter_label,

        -- Date arithmetic helpers
        toYear(date) AS calendar_year,
        toMonth(date) AS calendar_month,
        toDayOfWeek(date) AS day_of_week_num

    FROM source
)

SELECT * FROM enriched
