# DBT Analytics Project

## Overview

This DBT project transforms raw OLAP data (star schema) into business-ready models for reporting and analysis.

## Architecture

```
Raw OLAP (ClickHouse)
    ├── fact_orders
    ├── dim_customer
    ├── dim_product
    ├── dim_vendor
    └── dim_date
        │
        ▼
    Staging (views)
        ├── stg_orders
        ├── stg_customers
        ├── stg_products
        └── stg_dates
            │
            ▼
    Intermediate (ephemeral)
        ├── int_orders_enriched
        ├── int_customer_metrics
        └── int_product_metrics
            │
            ▼
    Marts (tables/incremental)
        ├── Core
        │   ├── fct_sales_daily
        │   ├── dim_customer_enriched
        │   └── dim_product_enriched
        ├── Finance
        │   ├── fct_monthly_financials
        │   └── fct_customer_cohort
        └── Marketing
            ├── fct_customer_rfm
            └── fct_product_affinity
```

## Quick Start

### Prerequisites

```bash
# Install DBT with ClickHouse adapter
pip install dbt-clickhouse

# Verify installation
dbt --version
```

### Configuration

```bash
# Set environment variables (or use .env file)
export DBT_CUSTOMER_SCHEMA=customer_003
export DBT_CLICKHOUSE_HOST=localhost
export DBT_CLICKHOUSE_PORT=8123
export DBT_CLICKHOUSE_USER=etl
export DBT_CLICKHOUSE_PASSWORD=etl
export DBT_CLICKHOUSE_DATABASE=olap
```

### Running DBT

```bash
cd dbt

# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific model
dbt run --select fct_sales_daily

# Run with specific vars
dbt run --vars '{"customer_schema": "customer_004"}'

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Full refresh (rebuild incremental models)
dbt run --full-refresh
```

### Running in Docker

```bash
# Use docker profile
dbt run --profile docker

# Or set env vars for docker
docker exec -it etl-airflow-webserver bash
cd /opt/airflow/dbt
dbt run --profile docker
```

## Model Details

### Staging Models

| Model | Type | Description |
|-------|------|-------------|
| stg_orders | view | Cleaned fact_orders with derived metrics |
| stg_customers | view | Current customer records from SCD Type 2 |
| stg_products | view | Current product records with margin categories |
| stg_dates | view | Date dimension with derived fields |

### Intermediate Models

| Model | Type | Description |
|-------|------|-------------|
| int_orders_enriched | ephemeral | Orders joined with all dimensions |
| int_customer_metrics | ephemeral | Customer-level aggregations |
| int_product_metrics | ephemeral | Product-level aggregations |

### Mart Models

#### Core

| Model | Type | Refresh | Description |
|-------|------|---------|-------------|
| fct_sales_daily | incremental | Daily | Daily sales with customer/payment breakdowns |
| dim_customer_enriched | incremental | Daily | Customers with LTV, RFM, lifecycle |
| dim_product_enriched | incremental | Daily | Products with sales performance |

#### Finance

| Model | Type | Description |
|-------|------|-------------|
| fct_monthly_financials | table | Monthly P&L summary |
| fct_customer_cohort | table | Cohort retention and LTV analysis |

#### Marketing

| Model | Type | Description |
|-------|------|-------------|
| fct_customer_rfm | table | RFM segmentation with campaign recommendations |
| fct_product_affinity | table | Product cross-sell recommendations |

## Testing

```bash
# Run all tests
dbt test

# Run specific test
dbt test --select stg_orders

# Run custom tests only
dbt test --select test_type:custom
```

## Documentation

```bash
# Generate and serve docs
dbt docs generate
dbt docs serve --port 8080
```

## Multi-Tenant Support

The project supports multiple customers via the `customer_schema` variable:

```bash
# Run for customer_003 (default)
dbt run

# Run for customer_004
dbt run --vars '{"customer_schema": "customer_004"}'

# Run for all customers (in production, orchestrated by Airflow)
for schema in customer_001 customer_002 customer_003 customer_004; do
    dbt run --vars "{"customer_schema": "$schema"}"
done
```

## Integration with ETL Pipeline

This DBT project runs **after** the Spark ETL pipeline completes:

```
Spark ETL Pipeline
    ├── Extract (Delta Strategy)
    ├── Transform (Spark)
    └── Load (ClickHouse Star Schema)
            │
            ▼
    DBT Transformations
        ├── Staging (views)
        ├── Intermediate (ephemeral)
        └── Marts (tables)
                │
                ▼
    BI Tools / Reports
```

## Variables

| Variable | Default | Description |
|----------|---------|-------------|
| customer_schema | customer_003 | Target customer schema |
| lookback_days | 30 | Days for incremental filter |
| vip_threshold | 10000 | LTV threshold for VIP tier |
| gold_threshold | 5000 | LTV threshold for Gold tier |
| silver_threshold | 1000 | LTV threshold for Silver tier |
| currency_code | USD | Default currency |

## Macros

| Macro | Description |
|-------|-------------|
| cents_to_dollars | Convert cents to dollars |
| date_spine | Generate date series |
| generate_surrogate_key | Create hash-based surrogate keys |
| test_positive_values | Custom test for positive values |
| test_unique_combination | Custom test for composite uniqueness |
