# ETL strategy demo

An educational Java/Spark and dbt project for comparing initial-load, delta-extraction, warehouse-history, and modeling patterns. It is intentionally a learning platform, not a claim of production-ready exactly-once infrastructure.

## What is demonstrated

- Full table reload and reconciliation
- Timestamp high-water marks
- Closed-partition extraction
- Landing-zone file discovery
- CDC envelopes and Delta Lake Change Data Feed
- PostgreSQL-backed extraction/job metadata
- SCD Type 1 and Type 2 customer dimensions
- Data Vault Raw Vault in silver and Kimball marts in gold

Read [the delta strategy guide](docs/DELTA_STRATEGIES.md) before running the examples. It explains each algorithm, initial-load handoff, deletes, late data, idempotency, and cursor failure modes. [The modeling guide](docs/WAREHOUSE_MODELING.md) explains the Vault/Kimball split and the SCD examples.

## Repository layout

```text
dbt/                         dbt-clickhouse analytics project
  models/staging/            source-shaped views and audit metadata
  models/silver/raw_vault/   hubs, links, and satellites
  models/marts/              Kimball-style gold models
  snapshots/                 dbt SCD2 capture
etl-batch-project/
  etl-batch-core/            extraction, pipeline, metadata, transforms
  etl-batch-jobs/            Spark job entry points
  etl-batch-orchestrator/    configuration validation/resolution CLI
  data-generator/            sample OLTP event/data producer
  docker/                    PostgreSQL, ClickHouse, Spark, monitoring
docs/                        strategy and modeling walkthroughs
```

## Build

Requirements: Java 17 and Maven 3.9+.

```bash
cd etl-batch-project
mvn clean test
mvn package -DskipTests
```

The Maven Spark/Scala/Delta artifacts are aligned with the Spark 3.5.1 Docker image (Scala 2.12, Delta 3.2).

## Infrastructure

```bash
cd etl-batch-project/docker
docker compose up -d postgres-oltp postgres-metadata clickhouse-olap minio
docker compose ps
```

Host ports are PostgreSQL OLTP `5432`, metadata `5433`, ClickHouse HTTP/native `8123`/`9000`, and MinIO API/console `9002`/`9001`.

Docker initialization runs only for empty named volumes. After changing an init SQL file, apply it manually or deliberately recreate only the relevant demo volume.

## dbt

```bash
python -m venv .venv
. .venv/bin/activate
pip install dbt-clickhouse
cd dbt
dbt deps
dbt debug
dbt run --select staging
dbt run --select silver
dbt snapshot --select dim_customer_snapshot
dbt run --select dim_customer_scd1 dim_customer_scd2
dbt test
```

The profile environment variables are documented in [dbt/README.md](dbt/README.md). The source schema must contain the tables in `dbt/models/staging/_sources.yml`.

## Current boundary

The implementations are deliberately compact. Some extraction classes still save watermarks before the pipeline target commit; the safe post-load cursor protocol is documented and should be the next refactor before non-demo use. Airflow remains optional Compose infrastructure, but no DAG is advertised because the repository does not yet contain an executable DAG.
