package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.storage.MetadataStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Partition-based delta extraction.
 * Best for: MEDIUM customers, date-partitioned tables (e.g., daily partitions).
 * Extracts only partitions that haven't been processed yet.
 */
public class PartitionDeltaStrategy implements DeltaStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionDeltaStrategy.class);
    private static final String STRATEGY_NAME = "PartitionDelta";
    private static final String DEFAULT_PARTITION_COLUMN = "dt";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final MetadataStore metadataStore;

    public PartitionDeltaStrategy() {
        this.metadataStore = new MetadataStore.JdbcMetadataStore();
    }

    public PartitionDeltaStrategy(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public boolean supports(CustomerConfig config) {
        return config.getCustomerSize() == CustomerConfig.CustomerSize.MEDIUM ||
               config.getCustomerSize() == CustomerConfig.CustomerSize.LARGE;
    }

    @Override
    public Dataset<Row> extractDelta(SparkSession spark, CustomerConfig config,
                                     String sourceTable, Map<String, String> sourceOptions,
                                     ETLMetrics metrics) {

        LOG.info("[{}] Extracting delta for table '{}' using partition strategy",
                config.getCustomerId(), sourceTable);

        String partitionColumn = sourceOptions.getOrDefault("partitionColumn", DEFAULT_PARTITION_COLUMN);
        DeltaMetadata lastMeta = getLastExtractionMetadata(config, sourceTable);
        String lastPartition = lastMeta != null ? lastMeta.getWatermarkValue() : "1970-01-01";
        LocalDate lastDate = LocalDate.parse(lastPartition, DATE_FORMAT);
        LocalDate today = LocalDate.now();

        // Determine partitions to process (all unprocessed dates up to yesterday)
        StringBuilder partitionFilter = new StringBuilder();
        LocalDate current = lastDate.plusDays(1);
        while (current.isBefore(today)) {
            if (partitionFilter.length() > 0) partitionFilter.append(",");
            partitionFilter.append("'").append(current.format(DATE_FORMAT)).append("'");
            current = current.plusDays(1);
        }

        if (partitionFilter.length() == 0) {
            LOG.info("[{}] No new partitions to process for '{}'", config.getCustomerId(), sourceTable);
            return spark.emptyDataFrame();
        }

        metrics.recordGauge("delta.partitions.pending", 
                java.time.temporal.ChronoUnit.DAYS.between(lastDate, today) - 1,
                config.getCustomerId(), sourceTable);

        String schemaPrefix = config.getSchemaName() + ".";
        String fullTableName = schemaPrefix + sourceTable;

        String query = String.format(
            "(SELECT * FROM %s WHERE %s IN (%s)) as delta_subquery",
            fullTableName, partitionColumn, partitionFilter
        );

        Dataset<Row> sourceDF = spark.read()
            .format("jdbc")
            .option("url", config.getConnectionProperties().get("url"))
            .option("dbtable", query)
            .option("user", config.getConnectionProperties().get("user"))
            .option("password", config.getConnectionProperties().get("password"))
            .option("numPartitions", String.valueOf(config.getParallelism()))
            .load();

        long count = sourceDF.count();
        LOG.info("[{}] Extracted {} delta records from '{}' across partitions: {}",
                config.getCustomerId(), count, sourceTable, partitionFilter);

        metrics.recordCounter("delta.records.extracted", count, config.getCustomerId(), sourceTable);

        // Update watermark with latest processed partition
        if (count > 0) {
            Object latestPartitionValue = sourceDF.agg(org.apache.spark.sql.functions.max(partitionColumn))
                    .first().get(0);
            String latestPartition = latestPartitionValue.toString();
            DeltaMetadata newMeta = DeltaMetadata.builder()
                    .customerId(config.getCustomerId())
                    .sourceTable(sourceTable)
                    .strategyName(STRATEGY_NAME)
                    .recordCount(count)
                    .watermarkValue(latestPartition)
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
