-- Initialize ClickHouse OLAP database with Star Schema
-- Each customer gets their own database

-- Create databases for each customer
CREATE DATABASE IF NOT EXISTS customer_003;
CREATE DATABASE IF NOT EXISTS customer_004;

-- Date dimension (shared)
CREATE TABLE IF NOT EXISTS customer_003.dim_date (
    date_key UInt32,
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day UInt8,
    day_of_week UInt8,
    day_name String,
    month_name String,
    is_weekend UInt8,
    is_holiday UInt8,
    fiscal_year UInt16,
    fiscal_quarter UInt8
) ENGINE = MergeTree()
ORDER BY date_key;

-- Customer dimension with SCD Type 2
CREATE TABLE IF NOT EXISTS customer_003.dim_customer (
    customer_key UInt32,
    customer_id UInt32,
    first_name String,
    last_name String,
    full_name String,
    email String,
    phone String,
    segment String,
    tier String,
    registration_date Date,
    valid_from DateTime,
    valid_to DateTime,
    is_current UInt8
) ENGINE = MergeTree()
ORDER BY (customer_key, valid_from);

-- Product dimension
CREATE TABLE IF NOT EXISTS customer_003.dim_product (
    product_key UInt32,
    product_id UInt32,
    name String,
    description String,
    sku String,
    category_name String,
    category_path String,
    vendor_name String,
    vendor_id UInt32,
    price Decimal(10,2),
    cost Decimal(10,2),
    margin Decimal(10,2),
    status String,
    valid_from DateTime,
    valid_to DateTime,
    is_current UInt8
) ENGINE = MergeTree()
ORDER BY (product_key, valid_from);

-- Vendor dimension
CREATE TABLE IF NOT EXISTS customer_003.dim_vendor (
    vendor_key UInt32,
    vendor_id UInt32,
    name String,
    email String,
    phone String,
    address String,
    tax_id String,
    status String,
    valid_from DateTime,
    valid_to DateTime,
    is_current UInt8
) ENGINE = MergeTree()
ORDER BY (vendor_key, valid_from);

-- Fact Orders
CREATE TABLE IF NOT EXISTS customer_003.fact_orders (
    order_key UInt64,
    order_id UInt32,
    date_key UInt32,
    customer_key UInt32,
    product_key UInt32,
    vendor_key UInt32,
    quantity UInt32,
    unit_price Decimal(10,2),
    discount_percent Decimal(5,2),
    discount_amount Decimal(10,2),
    line_total Decimal(12,2),
    cost_amount Decimal(10,2),
    profit_amount Decimal(10,2),
    order_status String,
    payment_method String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDate(date_key))
ORDER BY (date_key, customer_key, product_key);

-- Pre-aggregated: Daily Sales Summary
CREATE TABLE IF NOT EXISTS customer_003.daily_sales_summary (
    date Date,
    date_key UInt32,
    total_orders UInt64,
    total_revenue Decimal(15,2),
    total_cost Decimal(15,2),
    total_profit Decimal(15,2),
    total_quantity UInt64,
    unique_customers UInt64,
    unique_products UInt64,
    avg_order_value Decimal(10,2),
    avg_discount_percent Decimal(5,2)
) ENGINE = SummingMergeTree()
ORDER BY (date_key);

-- Pre-aggregated: Weekly Customer LTV
CREATE TABLE IF NOT EXISTS customer_003.weekly_customer_ltv (
    week_start Date,
    week_key UInt32,
    customer_key UInt32,
    customer_segment String,
    weekly_spend Decimal(15,2),
    weekly_orders UInt64,
    weekly_quantity UInt64,
    avg_order_value Decimal(10,2),
    days_since_last_order UInt32,
    customer_lifetime_value Decimal(15,2)
) ENGINE = SummingMergeTree()
ORDER BY (week_key, customer_key);

-- Pre-aggregated: Monthly Product Performance
CREATE TABLE IF NOT EXISTS customer_003.monthly_product_performance (
    month_start Date,
    month_key UInt32,
    product_key UInt32,
    product_name String,
    category_name String,
    vendor_name String,
    units_sold UInt64,
    revenue Decimal(15,2),
    cost Decimal(15,2),
    profit Decimal(15,2),
    avg_selling_price Decimal(10,2),
    discount_rate Decimal(5,2),
    return_rate Decimal(5,2)
) ENGINE = SummingMergeTree()
ORDER BY (month_key, product_key);

-- Pre-aggregated: Vendor Performance
CREATE TABLE IF NOT EXISTS customer_003.vendor_performance (
    month_start Date,
    month_key UInt32,
    vendor_key UInt32,
    vendor_name String,
    total_orders UInt64,
    total_revenue Decimal(15,2),
    total_cost Decimal(15,2),
    total_profit Decimal(15,2),
    unique_products UInt64,
    avg_lead_time_days UInt32
) ENGINE = SummingMergeTree()
ORDER BY (month_key, vendor_key);

-- Create same tables for customer_004 (WHALE)
CREATE TABLE IF NOT EXISTS customer_004.dim_date AS customer_003.dim_date EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.dim_customer AS customer_003.dim_customer EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.dim_product AS customer_003.dim_product EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.dim_vendor AS customer_003.dim_vendor EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.fact_orders AS customer_003.fact_orders EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.daily_sales_summary AS customer_003.daily_sales_summary EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.weekly_customer_ltv AS customer_003.weekly_customer_ltv EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.monthly_product_performance AS customer_003.monthly_product_performance EMPTY;
CREATE TABLE IF NOT EXISTS customer_004.vendor_performance AS customer_003.vendor_performance EMPTY;
