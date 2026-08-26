# Initial-load and delta strategy guide

This project deliberately keeps two decisions separate:

1. **Extraction:** which source records/files changed since the last successful run?
2. **History:** when a dimension changed, should the warehouse overwrite it (SCD1) or retain versions (SCD2)?

## Strategy comparison

| Strategy | Initial load | Delta marker | Handles update | Handles delete | Main failure mode | Good fit |
|---|---|---|---|---|---|---|
| Full reload | Reads the complete table | Successful-run marker | Yes, by rebuilding | Yes | Cost and source load | Small/reference tables and reconciliation |
| Timestamp high-water mark | First run starts at epoch | `updated_at` | Yes | No | Equal timestamps, clock skew, hard deletes | Append/update tables with a reliable DB-managed timestamp |
| Partition delta | Reads historical partitions | Latest closed partition | Only if old partitions are revisited | No | Late data in an already closed partition | Event facts partitioned by event date |
| Landing zone | Reads all matching files | Processed file identity | Depends on file contract | Tombstone file required | Filename ordering and partial uploads | Partner feeds and object-storage drops |
| CDC | Snapshot followed by log changes | LSN/offset | Yes | Yes | Retention gap or incorrect offset commit | High-change OLTP sources |
| Delta Change Data Feed | Snapshot/table version zero | Delta table version | Yes | Yes | Vacuum removes required history | Lakehouse-to-lakehouse propagation |

## Full reload

`FullReloadStrategy` is both the simplest strategy and the correctness baseline. The target must be replaced atomically (write a new table/path, validate it, then swap) rather than truncated before extraction succeeds. Use it for the first load even when later runs use another strategy. A periodic full comparison is also useful for detecting missed deltas.

Try it by setting `deltaStrategy` to `FullReload`. `forceReload=true` reruns it after the successful-run marker exists.

## Timestamp high-water mark

`TimestampDeltaStrategy` queries rows newer than the stored timestamp. A production implementation should use a composite cursor `(updated_at, primary_key)`, persist the source database time captured at run start, and apply an overlap window. The overlap intentionally rereads a small range, so the target must deduplicate or merge by key. Source timestamps should be maintained by the database, not client clocks. Hard deletes require soft-delete columns or a separate tombstone feed.

## Partition delta

`PartitionDeltaStrategy` reads closed daily partitions after its watermark. It is efficient for immutable events, but yesterday is not necessarily final: mobile clients, retries, and upstream backfills create late data. A realistic schedule should reprocess the last N partitions and overwrite those target partitions atomically. Do not generate an unbounded SQL `IN` list after a long outage; process bounded ranges.

## Landing-zone files

`LandingZoneStrategy` demonstrates file discovery. A robust feed uses a manifest or control table containing file URI, size/checksum, batch ID, and status. Producers upload to a temporary name and publish/rename only when complete. Archiving a file during extraction is unsafe because transformation or load can still fail; archive or mark it committed only after the target transaction succeeds. Filename lexicographic order is educational shorthand, not a production checkpoint.

## CDC

`CdcDeltaStrategy` normalizes Debezium-like insert/update/delete envelopes. The initial snapshot and log stream must share a consistent boundary. Store offsets per source partition, retain transaction ordering, deduplicate by event identity, and advance offsets only after the target commit. Consumers must apply delete tombstones. PostgreSQL logical-slot reads are stateful operations; treating them as an ordinary repeatable JDBC query can consume changes prematurely, so Debezium/Kafka is the preferred executable pattern.

## Delta Lake Change Data Feed

`DeltaLakeMergeStrategy` reads Delta CDF versions; it is not a source-database extraction strategy. Enable CDF when creating the table, start the next read at `last_committed_version + 1`, and handle `update_preimage`/`update_postimage` deliberately. A first run creates or reads a snapshot; subsequent runs propagate CDF. The downstream MERGE key must match the business grain.

## Correct watermark protocol

All strategies should follow the same protocol:

1. Read the last **committed** cursor.
2. Capture an upper bound from the source.
3. Extract `(last_cursor, upper_bound]` (with an overlap where appropriate).
4. Validate, transform, and idempotently load.
5. Commit the cursor and target as one logical operation.

The current Java classes are compact demonstrations and persist some cursors during extraction. Before treating this as production infrastructure, move cursor advancement into the pipeline’s post-load commit. The guide calls this out explicitly so the demo does not teach unsafe exactly-once claims.

## Demo scenarios

1. Run a full load; rerun and observe it skip; use `forceReload` and observe replacement semantics.
2. Update two rows with the same timestamp and examine why a timestamp-only cursor needs a primary-key tie-breaker.
3. Insert a late order into yesterday’s partition and compare strict-watermark versus lookback reprocessing.
4. Drop two files out of lexical order and compare filename watermarks with a manifest table.
5. Insert, update, and delete one order; verify the normalized CDC operation and target tombstone.
6. Update a customer tier; compare SCD1’s single row with SCD2’s closed and current versions.

