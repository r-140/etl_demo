# Warehouse modeling: bronze, silver, gold, and SCD

## Recommended learning layout

| Layer | Model used here | Purpose |
|---|---|---|
| Bronze/source | Source-shaped ClickHouse tables and dbt staging views | Preserve ingestion metadata and make only mechanical renames/casts |
| Silver | Data Vault 2.0 Raw Vault | Integrate business keys, relationships, and attribute history without applying reporting rules |
| Gold | Kimball dimensions/facts and marts | Stable, fast business-facing schemas and metrics |

Data Vault in silver and Kimball in gold is sensible for this pet project because it lets the same changes be studied twice: first as historized integration records (hub/link/satellite), then as consumer-friendly dimensions and facts. It is not automatically the right production choice. For one source and a small team, staging directly into Kimball is usually simpler; the Vault earns its cost when there are multiple changing sources, auditability requirements, and independent delivery timelines.

The former Vault directory was named `bronze`. It now lives under `models/silver/raw_vault`; bronze should not reinterpret source records into hubs and satellites.

## Raw Vault rules used here

- Hubs contain a deterministic hash key, business key, load timestamp, and record source.
- Links contain relationships between hub keys.
- Satellites contain descriptive attributes and a hash diff.
- Incremental hubs/links reject already loaded keys.
- Satellites append a row only for a previously unseen `(parent_hash_key, hash_diff)` pair.

For a fuller implementation, add effectivity satellites for relationship validity, a record-source discriminator to hashes when business keys are not globally unique, and a Business Vault/PIT layer to make point-in-time queries cheaper.

## SCD examples

SCD applies to the Kimball dimension, not to the extraction mechanism.

| Type | Model | Behaviour | Typical columns |
|---|---|---|---|
| Type 0 | Source immutable attributes | Never overwrite, for example original registration date | Original value only |
| Type 1 | `dim_customer_scd1` | Update the row in place | `dw_updated_at` |
| Type 2 | `dim_customer_snapshot` → `dim_customer_scd2` | Close the previous row and insert a new version | `valid_from`, `valid_to`, `is_current`, version surrogate key |

Run SCD2 in two steps:

```bash
dbt snapshot --select dim_customer_snapshot
dbt run --select dim_customer_scd2
```

After changing a customer’s tracked columns, rerun both commands. A point-in-time fact join is:

```sql
select f.*, d.segment, d.tier
from fact_orders f
join dim_customer_scd2 d
  on f.customer_id = d.customer_id
 and f.order_timestamp >= d.valid_from
 and f.order_timestamp < d.valid_to
```

Facts normally retain the dimension version surrogate key resolved at fact-load time. The range join above is useful for repair/backfill and explaining the concept.

## Important distinction: Vault satellites versus SCD2

Both retain history, but they serve different consumers. A Raw Vault satellite records source observations and normally has no updated-in-place end date. A Kimball SCD2 dimension assembles a convenient business row, adds explicit validity, and may apply business rules. Do not expose raw satellites as if they were finished reporting dimensions.

