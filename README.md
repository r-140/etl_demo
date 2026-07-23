# ETL Demo Platform

Production-grade ETL batch processing platform with Spark, multi-tenant OLTP/OLAP, dynamic delta strategies, and DBT analytics.

## Structure

```
etl_demo/
├── dbt/                          # DBT analytics project
│   ├── models/                   # Staging, intermediate, marts
│   ├── macros/                   # Reusable SQL macros
│   ├── tests/                    # Data quality tests
│   ├── seeds/                    # Static data
│   ├── snapshots/                # SCD Type 2 tracking
│   ├── analyses/                 # Ad-hoc queries
│   ├── dbt_project.yml           # Project config
│   ├── profiles.yml              # Connection profiles
│   └── README.md                 # DBT documentation
│
└── etl-batch-project/            # Spark ETL framework
    ├── etl-batch-core/           # Core framework
    ├── etl-batch-jobs/         # ETL job implementations
    ├── docker/                   # Infrastructure
    ├── k8s/                      # Kubernetes manifests
    ├── scripts/                    # Start/stop/validate
    ├── data-generator/           # Test data generator
    └── docs/                     # Architecture docs
```

## Quick Start

### 1. Start Infrastructure

```bash
cd etl-batch-project/docker
docker-compose up -d
```

### 2. Build ETL Project

```bash
cd etl-batch-project
mvn clean package -DskipTests
```

### 3. Run ETL Jobs

```bash
# Via Airflow
docker exec etl-airflow-webserver airflow dags trigger etl_batch_pipeline
```

### 4. Run DBT Transformations

```bash
cd dbt
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## Architecture

```
OLTP (PostgreSQL, 3NF) → Spark ETL (Java) → OLAP (ClickHouse, Star) → DBT → Marts
```

| Customer Size | Strategy | Storage |
|--------------|----------|---------|
| SMALL (< 1M) | TimestampDelta | PostgreSQL |
| MEDIUM (1-10M) | PartitionDelta | PostgreSQL |
| LARGE (10-100M) | DeltaLakeMerge | ClickHouse |
| WHALE (> 100M) | CdcDelta | ClickHouse |
