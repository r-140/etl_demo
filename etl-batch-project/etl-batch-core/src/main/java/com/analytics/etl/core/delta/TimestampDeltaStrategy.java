package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.storage.MetadataStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * Timestamp-based delta extraction.
 * Best for: SMALL customers, tables with updated_at/created_at columns.
 * Extracts records where timestamp column > last extraction watermark.
 */
public class TimestampDeltaStrategy implements DeltaStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampDeltaStrategy.class);
    private static final String STRATEGY_NAME = "TimestampDelta";
    private static final String DEFAULT_TIMESTAMP_COLUMN = "updated_at";

    private final MetadataStore metadataStore;

    public TimestampDeltaStrategy() {
        this.metadataStore = new MetadataStore.JdbcMetadataStore();
    }

    public TimestampDeltaStrategy(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public boolean supports(CustomerConfig config) {
        return config.getCustomerSize() == CustomerConfig.CustomerSize.SMALL ||
               config.getCustomerSize() == CustomerConfig.CustomerSize.MEDIUM;
    }

    @Override
    public Dataset<Row> extractDelta(SparkSession spark, CustomerConfig config, 
                                     String sourceTable, Map<String, String> sourceOptions,
                                     ETLMetrics metrics) {

        LOG.info("[{}] Extracting delta for table '{}' using timestamp strategy", 
                config.getCustomerId(), sourceTable);

        String timestampColumn = sourceOptions.getOrDefault("timestampColumn", DEFAULT_TIMESTAMP_COLUMN);
        DeltaMetadata lastMeta = getLastExtractionMetadata(config, sourceTable);
        String lastWatermark = lastMeta != null ? lastMeta.getWatermarkValue() : "1970-01-01T00:00:00Z";

        metrics.recordGauge("delta.watermark.timestamp", Instant.parse(lastWatermark).toEpochMilli(), 
                config.getCustomerId(), sourceTable);

        // Build query with timestamp filter
        String schemaPrefix = config.getSchemaName() + ".";
        String fullTableName = schemaPrefix + sourceTable;

        Dataset<Row> sourceDF = spark.read()
            .format("jdbc")
            .option("url", config.getConnectionProperties().get("url"))
            .option("dbtable", "(SELECT * FROM " + fullTableName + 
                    " WHERE " + timestampColumn + " > '" + lastWatermark + "') as delta_subquery")
            .option("user", config.getConnectionProperties().get("user"))
            .option("password", config.getConnectionProperties().get("password"))
            .option("partitionColumn", sourceOptions.getOrDefault("partitionColumn", "id"))
            .option("lowerBound", sourceOptions.getOrDefault("lowerBound", "0"))
            .option("upperBound", sourceOptions.getOrDefault("upperBound", "1000000"))
            .option("numPartitions", String.valueOf(config.getParallelism()))
            .load();

        long count = sourceDF.count();
        LOG.info("[{}] Extracted {} delta records from '{}'", config.getCustomerId(), count, sourceTable);

        metrics.recordCounter("delta.records.extracted", count, config.getCustomerId(), sourceTable);

        // Update watermark with max timestamp
        if (count > 0) {
            String maxTimestamp = sourceDF.agg(functions.max(timestampColumn)).collectAsList()
                    .get(0).getString(0);
            DeltaMetadata newMeta = DeltaMetadata.builder()
                    .customerId(config.getCustomerId())
                    .sourceTable(sourceTable)
                    .strategyName(STRATEGY_NAME)
                    .recordCount(count)
                    .watermarkValue(maxTimestamp)
                    .status(DeltaMetadata.ExtractionStatus.SUCCESS)
                    .build();
            persistExtractionMetadata(config, sourceTable, newMeta);
        }

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
