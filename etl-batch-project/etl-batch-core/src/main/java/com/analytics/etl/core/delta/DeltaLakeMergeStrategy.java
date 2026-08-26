package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.storage.MetadataStore;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Delta Lake Merge Strategy.
 * Best for: LARGE/WHALE customers with Delta Lake as intermediate storage.
 * Uses Delta Lake's MERGE INTO for efficient upserts.
 * Supports time travel, schema evolution, and ACID transactions.
 */
public class DeltaLakeMergeStrategy implements DeltaStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeMergeStrategy.class);
    private static final String STRATEGY_NAME = "DeltaLakeMerge";

    private final MetadataStore metadataStore;

    public DeltaLakeMergeStrategy() {
        this.metadataStore = new MetadataStore.JdbcMetadataStore();
    }

    public DeltaLakeMergeStrategy(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public boolean supports(CustomerConfig config) {
        return config.getCustomerSize() == CustomerConfig.CustomerSize.LARGE ||
               config.getCustomerSize() == CustomerConfig.CustomerSize.WHALE;
    }

    @Override
    public Dataset<Row> extractDelta(SparkSession spark, CustomerConfig config,
                                     String sourceTable, Map<String, String> sourceOptions,
                                     ETLMetrics metrics) {

        LOG.info("[{}] Extracting delta for table '{}' using Delta Lake merge strategy",
                config.getCustomerId(), sourceTable);

        String deltaPath = sourceOptions.getOrDefault("deltaPath",
                "/data/delta/" + config.getCustomerId() + "/" + sourceTable);
        String mergeKey = sourceOptions.getOrDefault("mergeKey", "id");

        DeltaMetadata lastMeta = getLastExtractionMetadata(config, sourceTable);
        long lastVersion = lastMeta != null && lastMeta.getWatermarkValue() != null
                ? Long.parseLong(lastMeta.getWatermarkValue()) : 0L;

        metrics.recordGauge("delta.lake.last_version", lastVersion, config.getCustomerId(), sourceTable);

        Dataset<Row> deltaDF;

        try {
            // Try to read changes from Delta Lake table history
            DeltaTable deltaTable = DeltaTable.forPath(spark, deltaPath);

            if (lastMeta == null) {
                // Existing table but first consumer run: establish a complete baseline.
                deltaDF = spark.read().format("delta").load(deltaPath);
            } else {
                // The stored version is inclusive; resume at the following commit.
                deltaDF = spark.read()
                    .format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", lastVersion + 1)
                    .load(deltaPath);
            }

        } catch (Exception e) {
            LOG.warn("Delta table not found at {}, falling back to full read: {}", deltaPath, e.getMessage());

            // Fallback: read from source JDBC and write to Delta
            String schemaPrefix = config.getSchemaName() + ".";
            deltaDF = spark.read()
                .format("jdbc")
                .option("url", config.getConnectionProperties().get("url"))
                .option("dbtable", schemaPrefix + sourceTable)
                .option("user", config.getConnectionProperties().get("user"))
                .option("password", config.getConnectionProperties().get("password"))
                .option("numPartitions", String.valueOf(config.getParallelism()))
                .load();

            // Initialize Delta table
            deltaDF.write()
                .format("delta")
                .mode("overwrite")
                .save(deltaPath);

            DeltaTable deltaTable = DeltaTable.forPath(spark, deltaPath);
            lastVersion = deltaTable.history(1).select("version").collectAsList().get(0).getLong(0);
        }

        long count = deltaDF.count();
        LOG.info("[{}] Extracted {} delta records from Delta Lake '{}' (version {})",
                config.getCustomerId(), count, sourceTable, lastVersion);

        metrics.recordCounter("delta.records.extracted", count, config.getCustomerId(), sourceTable);

        if (count > 0) {
            // Get current version
            DeltaTable deltaTable = DeltaTable.forPath(spark, deltaPath);
            long currentVersion = deltaTable.history(1).select("version").collectAsList().get(0).getLong(0);

            DeltaMetadata newMeta = DeltaMetadata.builder()
                    .customerId(config.getCustomerId())
                    .sourceTable(sourceTable)
                    .strategyName(STRATEGY_NAME)
                    .recordCount(count)
                    .watermarkValue(String.valueOf(currentVersion))
                    .status(DeltaMetadata.ExtractionStatus.SUCCESS)
                    .build();
            persistExtractionMetadata(config, sourceTable, newMeta);
        }

        return deltaDF;
    }

    /**
     * Perform MERGE INTO operation for upserts
     */
    public void mergeIntoDelta(SparkSession spark, String deltaPath, Dataset<Row> sourceDF,
                               String mergeKey, ETLMetrics metrics) {

        DeltaTable targetTable = DeltaTable.forPath(spark, deltaPath);

        targetTable.as("target")
            .merge(sourceDF.as("source"), "target." + mergeKey + " = source." + mergeKey)
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute();

        Object updatedMetric = targetTable.history(1).select("operationMetrics")
                .collectAsList().get(0).getMap(0).get("numTargetRowsUpdated");
        long updatedCount = updatedMetric == null ? 0 : Long.parseLong(updatedMetric.toString());

        metrics.recordCounter("delta.lake.rows.updated", updatedCount, "global", deltaPath);
    }

    @Override
    public DeltaMetadata getLastExtractionMetadata(CustomerConfig config, String sourceTable) {
        return metadataStore.loadMetadata(config.getCustomerId(), sourceTable, STRATEGY_NAME);
    }

    @Override
    public void persistExtractionMetadata(CustomerConfig config, String sourceTable, DeltaMetadata metadata) {
        metadataStore.saveMetadata(metadata);
    }
}
