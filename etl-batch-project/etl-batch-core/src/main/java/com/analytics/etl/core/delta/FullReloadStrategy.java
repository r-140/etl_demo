package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.storage.MetadataStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * Full Reload Strategy.
 * Best for: Initial loads, small reference tables, or when delta is not available.
 * Reads entire table and overwrites target.
 * Can be combined with other strategies for initial load + delta maintenance.
 */
public class FullReloadStrategy implements DeltaStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(FullReloadStrategy.class);
    private static final String STRATEGY_NAME = "FullReload";

    private final MetadataStore metadataStore;

    public FullReloadStrategy() {
        this.metadataStore = new MetadataStore.JdbcMetadataStore();
    }

    public FullReloadStrategy(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public boolean supports(CustomerConfig config) {
        return true; // Universal fallback
    }

    @Override
    public Dataset<Row> extractDelta(SparkSession spark, CustomerConfig config,
                                     String sourceTable, Map<String, String> sourceOptions,
                                     ETLMetrics metrics) {

        LOG.info("[{}] Performing full reload for table '{}'", config.getCustomerId(), sourceTable);

        String schemaPrefix = config.getSchemaName() + ".";
        String fullTableName = schemaPrefix + sourceTable;

        // Check if this is an initial load or forced reload
        boolean isInitialLoad = Boolean.parseBoolean(sourceOptions.getOrDefault("initialLoad", "false"));
        boolean forceReload = Boolean.parseBoolean(sourceOptions.getOrDefault("forceReload", "false"));

        if (!isInitialLoad && !forceReload) {
            DeltaMetadata lastMeta = getLastExtractionMetadata(config, sourceTable);
            if (lastMeta != null && lastMeta.getStatus() == DeltaMetadata.ExtractionStatus.SUCCESS) {
                LOG.info("[{}] Table '{}' already loaded, skipping full reload. Use forceReload=true to override.",
                        config.getCustomerId(), sourceTable);
                return spark.emptyDataFrame();
            }
        }

        metrics.recordCounter("delta.reload.triggered", 1, config.getCustomerId(), sourceTable);

        Dataset<Row> sourceDF = spark.read()
            .format("jdbc")
            .option("url", config.getConnectionProperties().get("url"))
            .option("dbtable", fullTableName)
            .option("user", config.getConnectionProperties().get("user"))
            .option("password", config.getConnectionProperties().get("password"))
            .option("fetchsize", String.valueOf(config.getBatchSize()))
            .option("numPartitions", String.valueOf(config.getParallelism()))
            .load();

        long count = sourceDF.count();
        LOG.info("[{}] Full reload extracted {} records from '{}'", 
                config.getCustomerId(), count, sourceTable);

        metrics.recordCounter("delta.records.extracted", count, config.getCustomerId(), sourceTable);
        metrics.recordGauge("delta.reload.table_size", count, config.getCustomerId(), sourceTable);

        // Persist metadata
        DeltaMetadata newMeta = DeltaMetadata.builder()
                .customerId(config.getCustomerId())
                .sourceTable(sourceTable)
                .strategyName(STRATEGY_NAME)
                .recordCount(count)
                .watermarkValue(Instant.now().toString())
                .status(DeltaMetadata.ExtractionStatus.SUCCESS)
                .build();
        persistExtractionMetadata(config, sourceTable, newMeta);

        return sourceDF;
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
