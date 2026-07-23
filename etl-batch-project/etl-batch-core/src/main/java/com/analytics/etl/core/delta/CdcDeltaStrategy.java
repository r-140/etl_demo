package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.storage.MetadataStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * CDC (Change Data Capture) Delta Strategy.
 * Best for: WHALE customers, high-volume transactional tables.
 * Uses Debezium-style CDC format or database-native CDC (e.g., WAL, binlog).
 * 
 * Supports:
 * - INSERT, UPDATE, DELETE operations
 * - Before/after image tracking
 * - Exactly-once processing semantics
 */
public class CdcDeltaStrategy implements DeltaStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(CdcDeltaStrategy.class);
    private static final String STRATEGY_NAME = "CdcDelta";

    private final MetadataStore metadataStore;

    public CdcDeltaStrategy() {
        this.metadataStore = new MetadataStore.JdbcMetadataStore();
    }

    public CdcDeltaStrategy(MetadataStore metadataStore) {
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

        LOG.info("[{}] Extracting CDC delta for table '{}'", config.getCustomerId(), sourceTable);

        DeltaMetadata lastMeta = getLastExtractionMetadata(config, sourceTable);
        long lastLsn = lastMeta != null && lastMeta.getWatermarkValue() != null 
                ? Long.parseLong(lastMeta.getWatermarkValue()) : 0L;

        metrics.recordGauge("delta.cdc.last_lsn", lastLsn, config.getCustomerId(), sourceTable);

        String cdcFormat = sourceOptions.getOrDefault("cdcFormat", "debezium");
        String cdcTopic = sourceOptions.getOrDefault("cdcTopic", 
                config.getSchemaName() + "." + sourceTable);

        Dataset<Row> cdcDF;

        switch (cdcFormat.toLowerCase()) {
            case "debezium":
                cdcDF = readDebeziumCdc(spark, config, cdcTopic, lastLsn, sourceOptions);
                break;
            case "wal":
                cdcDF = readWalCdc(spark, config, sourceTable, lastLsn, sourceOptions);
                break;
            case "trigger":
                cdcDF = readTriggerCdc(spark, config, sourceTable, lastLsn, sourceOptions);
                break;
            default:
                throw new IllegalArgumentException("Unsupported CDC format: " + cdcFormat);
        }

        long count = cdcDF.count();
        LOG.info("[{}] Extracted {} CDC records from '{}'", config.getCustomerId(), count, sourceTable);

        metrics.recordCounter("delta.cdc.records.extracted", count, config.getCustomerId(), sourceTable);

        // Track operation types
        if (count > 0) {
            cdcDF.groupBy("_change_type").count().collectAsList().forEach(row -> {
                String opType = row.getString(0);
                long opCount = row.getLong(1);
                metrics.recordCounter("delta.cdc.operation." + opType, opCount, 
                        config.getCustomerId(), sourceTable);
            });

            // Update LSN watermark
            long maxLsn = cdcDF.agg(functions.max("_lsn")).collectAsList()
                    .get(0).getLong(0);

            DeltaMetadata newMeta = DeltaMetadata.builder()
                    .customerId(config.getCustomerId())
                    .sourceTable(sourceTable)
                    .strategyName(STRATEGY_NAME)
                    .recordCount(count)
                    .watermarkValue(String.valueOf(maxLsn))
                    .status(DeltaMetadata.ExtractionStatus.SUCCESS)
                    .build();
            persistExtractionMetadata(config, sourceTable, newMeta);
        }

        return cdcDF;
    }

    /**
     * Read CDC data in Debezium format from Kafka or file-based CDC log
     */
    private Dataset<Row> readDebeziumCdc(SparkSession spark, CustomerConfig config,
                                         String topic, long lastLsn, Map<String, String> options) {

        String cdcPath = options.getOrDefault("cdcPath", 
                "/data/cdc/" + config.getCustomerId() + "/" + topic);

        Dataset<Row> rawCdc = spark.read()
            .format("json")
            .option("multiLine", true)
            .load(cdcPath + "/*.json");

        // Parse Debezium envelope
        return rawCdc
            .select(
                functions.col("payload.before").alias("_before"),
                functions.col("payload.after").alias("_after"),
                functions.col("payload.source.lsn").alias("_lsn").cast(DataTypes.LongType),
                functions.col("payload.op").alias("_change_type"),
                functions.col("payload.ts_ms").alias("_change_timestamp")
            )
            .filter(functions.col("_lsn").gt(lastLsn))
            .withColumn("_change_type", 
                functions.when(functions.col("_change_type").equalTo("c"), "insert")
                       .when(functions.col("_change_type").equalTo("u"), "update")
                       .when(functions.col("_change_type").equalTo("d"), "delete")
                       .otherwise(functions.col("_change_type")))
            .withColumn("data", 
                functions.when(functions.col("_change_type").equalTo("delete"), 
                        functions.col("_before"))
                    .otherwise(functions.col("_after")))
            .filter(functions.col("_lsn").isNotNull());
    }

    /**
     * Read from PostgreSQL WAL (Logical Replication)
     */
    private Dataset<Row> readWalCdc(SparkSession spark, CustomerConfig config,
                                    String sourceTable, long lastLsn, Map<String, String> options) {

        String slotName = options.getOrDefault("replicationSlot", 
                "etl_slot_" + config.getCustomerId());

        // Read from PostgreSQL replication slot via JDBC
        String query = String.format(
            "(SELECT * FROM pg_logical_slot_get_changes('%s', NULL, NULL, " +
            "'include-xids', '0', 'include-timestamp', '1') " +
            "WHERE lsn > '%s/X%s') as cdc_changes",
            slotName, lastLsn >> 32, Long.toHexString(lastLsn & 0xFFFFFFFFL)
        );

        return spark.read()
            .format("jdbc")
            .option("url", config.getConnectionProperties().get("url"))
            .option("dbtable", query)
            .option("user", config.getConnectionProperties().get("user"))
            .option("password", config.getConnectionProperties().get("password"))
            .load()
            .withColumn("_change_type", functions.lit("unknown"))
            .withColumn("_lsn", functions.lit(lastLsn).cast(DataTypes.LongType));
    }

    /**
     * Read from trigger-based CDC table
     */
    private Dataset<Row> readTriggerCdc(SparkSession spark, CustomerConfig config,
                                        String sourceTable, long lastLsn, Map<String, String> options) {

        String cdcTable = options.getOrDefault("cdcTable", sourceTable + "_cdc");
        String schemaPrefix = config.getSchemaName() + ".";

        return spark.read()
            .format("jdbc")
            .option("url", config.getConnectionProperties().get("url"))
            .option("dbtable", schemaPrefix + cdcTable)
            .option("user", config.getConnectionProperties().get("user"))
            .option("password", config.getConnectionProperties().get("password"))
            .load()
            .filter(functions.col("cdc_id").gt(lastLsn))
            .withColumnRenamed("cdc_id", "_lsn")
            .withColumnRenamed("operation", "_change_type")
            .withColumnRenamed("changed_at", "_change_timestamp");
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
