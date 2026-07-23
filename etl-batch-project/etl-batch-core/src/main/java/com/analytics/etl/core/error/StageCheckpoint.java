package com.analytics.etl.core.error;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Stage checkpointing for pipeline resumability.
 * Saves intermediate DataFrames to durable storage, allowing pipeline restart from last checkpoint.
 * Supports exactly-once semantics with idempotent writes.
 */
public class StageCheckpoint {

    private static final Logger LOG = LoggerFactory.getLogger(StageCheckpoint.class);

    private final SparkSession spark;
    private final String checkpointBasePath;
    private final String checkpointFormat;

    public StageCheckpoint(SparkSession spark, String checkpointBasePath) {
        this(spark, checkpointBasePath, "delta");
    }

    public StageCheckpoint(SparkSession spark, String checkpointBasePath, String checkpointFormat) {
        this.spark = spark;
        this.checkpointBasePath = checkpointBasePath;
        this.checkpointFormat = checkpointFormat;
    }

    /**
     * Save checkpoint for a pipeline stage.
     * Uses idempotent write with overwrite for the specific stage.
     */
    public void saveCheckpoint(String pipelineId, String stage, String customerId,
                               Dataset<Row> data, Map<String, String> metadata) {

        String checkpointPath = buildCheckpointPath(pipelineId, stage, customerId);

        LOG.info("[{}] Saving checkpoint for stage '{}' at: {}", customerId, stage, checkpointPath);

        Dataset<Row> checkpointDF = data
            .withColumn("_checkpoint_timestamp", org.apache.spark.sql.functions.current_timestamp())
            .withColumn("_checkpoint_stage", org.apache.spark.sql.functions.lit(stage))
            .withColumn("_checkpoint_pipeline", org.apache.spark.sql.functions.lit(pipelineId));

        checkpointDF.write()
            .format(checkpointFormat)
            .mode("overwrite")
            .save(checkpointPath);

        // Save metadata separately
        saveCheckpointMetadata(pipelineId, stage, customerId, metadata, checkpointPath);

        LOG.info("[{}] Checkpoint saved for stage '{}' with {} records", 
                customerId, stage, data.count());
    }

    /**
     * Load checkpoint if it exists
     */
    public Dataset<Row> loadCheckpoint(String pipelineId, String stage, String customerId) {
        String checkpointPath = buildCheckpointPath(pipelineId, stage, customerId);

        try {
            Dataset<Row> checkpointDF = spark.read()
                .format(checkpointFormat)
                .load(checkpointPath);

            long count = checkpointDF.count();
            LOG.info("[{}] Loaded checkpoint for stage '{}' with {} records from: {}",
                    customerId, stage, count, checkpointPath);

            return checkpointDF.drop("_checkpoint_timestamp", "_checkpoint_stage", "_checkpoint_pipeline");

        } catch (Exception e) {
            LOG.info("[{}] No checkpoint found for stage '{}' at: {}", 
                    customerId, stage, checkpointPath);
            return null;
        }
    }

    /**
     * Check if checkpoint exists for a stage
     */
    public boolean hasCheckpoint(String pipelineId, String stage, String customerId) {
        try {
            String checkpointPath = buildCheckpointPath(pipelineId, stage, customerId);
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(
                new java.net.URI(checkpointPath), spark.sparkContext().hadoopConfiguration());
            return fs.exists(new org.apache.hadoop.fs.Path(checkpointPath));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Clean up checkpoint after successful pipeline completion
     */
    public void cleanupCheckpoint(String pipelineId, String stage, String customerId) {
        String checkpointPath = buildCheckpointPath(pipelineId, stage, customerId);

        try {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(
                new java.net.URI(checkpointPath), spark.sparkContext().hadoopConfiguration());
            fs.delete(new org.apache.hadoop.fs.Path(checkpointPath), true);
            LOG.info("[{}] Cleaned up checkpoint for stage '{}'", customerId, stage);
        } catch (Exception e) {
            LOG.warn("[{}] Failed to cleanup checkpoint for stage '{}': {}", 
                    customerId, stage, e.getMessage());
        }
    }

    private String buildCheckpointPath(String pipelineId, String stage, String customerId) {
        return String.format("%s/pipeline=%s/stage=%s/customer=%s",
                checkpointBasePath, pipelineId, stage, customerId);
    }

    private void saveCheckpointMetadata(String pipelineId, String stage, String customerId,
                                        Map<String, String> metadata, String checkpointPath) {
        // In production, this would write to a metadata store (e.g., PostgreSQL, etcd)
        LOG.debug("Checkpoint metadata saved: pipeline={}, stage={}, path={}", 
                pipelineId, stage, checkpointPath);
    }

    /**
     * Resume pipeline from last successful checkpoint
     */
    public Dataset<Row> resumeFromCheckpoint(String pipelineId, String[] stages, String customerId) {
        for (int i = stages.length - 1; i >= 0; i--) {
            if (hasCheckpoint(pipelineId, stages[i], customerId)) {
                LOG.info("[{}] Resuming pipeline '{}' from stage '{}'", 
                        customerId, pipelineId, stages[i]);
                return loadCheckpoint(pipelineId, stages[i], customerId);
            }
        }
        LOG.info("[{}] No checkpoint found for pipeline '{}', starting from beginning", 
                customerId, pipelineId);
        return null;
    }
}
