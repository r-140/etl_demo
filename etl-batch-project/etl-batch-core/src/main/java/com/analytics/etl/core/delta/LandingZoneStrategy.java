package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.storage.MetadataStore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Landing Zone Strategy.
 * Best for: File-based ingestion (CSV, JSON, Parquet) from landing zones.
 * Processes new files in a directory, tracks processed files to avoid reprocessing.
 * Supports file pattern matching and archive/move after processing.
 */
public class LandingZoneStrategy implements DeltaStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(LandingZoneStrategy.class);
    private static final String STRATEGY_NAME = "LandingZone";

    private final MetadataStore metadataStore;

    public LandingZoneStrategy() {
        this.metadataStore = new MetadataStore.JdbcMetadataStore();
    }

    public LandingZoneStrategy(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public boolean supports(CustomerConfig config) {
        // Supports all sizes when file-based ingestion is used
        return true;
    }

    @Override
    public Dataset<Row> extractDelta(SparkSession spark, CustomerConfig config,
                                     String sourceTable, Map<String, String> sourceOptions,
                                     ETLMetrics metrics) {

        LOG.info("[{}] Extracting delta for source '{}' using landing zone strategy",
                config.getCustomerId(), sourceTable);

        String landingPath = sourceOptions.get("landingPath");
        if (landingPath == null) {
            throw new IllegalArgumentException("landingPath is required for LandingZone strategy");
        }

        String filePattern = sourceOptions.getOrDefault("filePattern", "*");
        String fileFormat = sourceOptions.getOrDefault("fileFormat", "parquet");
        String archivePath = sourceOptions.getOrDefault("archivePath", landingPath + "/archive");
        boolean moveAfterProcess = Boolean.parseBoolean(sourceOptions.getOrDefault("moveAfterProcess", "true"));

        DeltaMetadata lastMeta = getLastExtractionMetadata(config, sourceTable);
        String lastProcessedFile = lastMeta != null ? lastMeta.getWatermarkValue() : "";

        // List files in landing zone
        org.apache.hadoop.conf.Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        org.apache.hadoop.fs.FileSystem fs;
        try {
            fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(landingPath), hadoopConf);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access landing zone: " + landingPath, e);
        }

        java.util.List<String> newFiles = new java.util.ArrayList<>();
        try {
            org.apache.hadoop.fs.FileStatus[] files = fs.globStatus(
                new org.apache.hadoop.fs.Path(landingPath + "/" + filePattern));

            if (files != null) {
                for (org.apache.hadoop.fs.FileStatus file : files) {
                    String fileName = file.getPath().getName();
                    if (!file.isDirectory() && fileName.compareTo(lastProcessedFile) > 0) {
                        newFiles.add(file.getPath().toString());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to list files in landing zone", e);
        }

        if (newFiles.isEmpty()) {
            LOG.info("[{}] No new files to process in landing zone '{}'", 
                    config.getCustomerId(), landingPath);
            return spark.emptyDataFrame();
        }

        LOG.info("[{}] Found {} new files to process", config.getCustomerId(), newFiles.size());
        metrics.recordGauge("delta.landing.new_files", newFiles.size(), config.getCustomerId(), sourceTable);

        // Read all new files
        Dataset<Row> combinedDF = null;
        String latestFile = lastProcessedFile;

        for (String filePath : newFiles) {
            LOG.debug("Reading file: {}", filePath);

            Dataset<Row> fileDF = spark.read()
                .format(fileFormat)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);

            // Add metadata columns
            fileDF = fileDF
                .withColumn("_source_file", org.apache.spark.sql.functions.lit(filePath))
                .withColumn("_ingestion_time", org.apache.spark.sql.functions.current_timestamp());

            if (combinedDF == null) {
                combinedDF = fileDF;
            } else {
                combinedDF = combinedDF.unionByName(fileDF, true);
            }

            // Track latest file
            String fileName = new org.apache.hadoop.fs.Path(filePath).getName();
            if (fileName.compareTo(latestFile) > 0) {
                latestFile = fileName;
            }

            // Move to archive if configured
            if (moveAfterProcess) {
                try {
                    String archiveFilePath = archivePath + "/" + fileName;
                    fs.mkdirs(new org.apache.hadoop.fs.Path(archivePath));
                    fs.rename(new org.apache.hadoop.fs.Path(filePath), 
                             new org.apache.hadoop.fs.Path(archiveFilePath));
                    LOG.debug("Moved file to archive: {}", archiveFilePath);
                } catch (Exception e) {
                    LOG.warn("Failed to archive file {}: {}", filePath, e.getMessage());
                }
            }
        }

        long count = combinedDF != null ? combinedDF.count() : 0;
        LOG.info("[{}] Extracted {} records from {} files", 
                config.getCustomerId(), count, newFiles.size());

        metrics.recordCounter("delta.records.extracted", count, config.getCustomerId(), sourceTable);
        metrics.recordCounter("delta.landing.files.processed", newFiles.size(), config.getCustomerId(), sourceTable);

        // Persist metadata
        if (count > 0) {
            DeltaMetadata newMeta = DeltaMetadata.builder()
                    .customerId(config.getCustomerId())
                    .sourceTable(sourceTable)
                    .strategyName(STRATEGY_NAME)
                    .recordCount(count)
                    .watermarkValue(latestFile)
                    .status(DeltaMetadata.ExtractionStatus.SUCCESS)
                    .build();
            persistExtractionMetadata(config, sourceTable, newMeta);
        }

        return combinedDF != null ? combinedDF : spark.emptyDataFrame();
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
