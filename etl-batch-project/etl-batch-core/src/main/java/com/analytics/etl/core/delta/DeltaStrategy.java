package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Instant;
import java.util.Map;

/**
 * Strategy pattern for different delta extraction approaches.
 * Each strategy defines how to identify and extract changed records.
 */
public interface DeltaStrategy {

    /**
     * Extract delta records from source based on strategy-specific logic
     */
    Dataset<Row> extractDelta(
            SparkSession spark,
            CustomerConfig config,
            String sourceTable,
            Map<String, String> sourceOptions,
            ETLMetrics metrics
    );

    /**
     * Get the strategy name for identification
     */
    String getStrategyName();

    /**
     * Check if this strategy supports the given customer configuration
     */
    boolean supports(CustomerConfig config);

    /**
     * Get metadata about the last successful extraction
     */
    DeltaMetadata getLastExtractionMetadata(CustomerConfig config, String sourceTable);

    /**
     * Persist extraction metadata after successful run
     */
    void persistExtractionMetadata(CustomerConfig config, String sourceTable, DeltaMetadata metadata);
}
