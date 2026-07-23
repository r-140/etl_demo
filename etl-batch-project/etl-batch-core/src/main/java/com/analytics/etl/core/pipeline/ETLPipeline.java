package com.analytics.etl.core.pipeline;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.delta.DeltaMetadata;
import com.analytics.etl.core.delta.DeltaStrategy;
import com.analytics.etl.core.delta.DeltaStrategyFactory;
import com.analytics.etl.core.error.*;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.metrics.NoOpMetrics;
import com.analytics.etl.core.storage.JobExecution;
import com.analytics.etl.core.transform.SparkTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Main ETL Pipeline orchestrator.
 * Executes Extract -> Validate -> Transform -> Load with full error handling,
 * checkpointing, metrics, and quarantine support.
 */
public class ETLPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ETLPipeline.class);

    private final SparkSession spark;
    private final ETLMetrics metrics;
    private final StageCheckpoint checkpoint;
    private final QuarantineManager quarantineManager;
    private final String pipelineId;

    public ETLPipeline(SparkSession spark, String pipelineId, ETLMetrics metrics,
                       StageCheckpoint checkpoint, QuarantineManager quarantineManager) {
        this.spark = spark;
        this.pipelineId = pipelineId;
        this.metrics = metrics != null ? metrics : new NoOpMetrics();
        this.checkpoint = checkpoint;
        this.quarantineManager = quarantineManager;
    }

    public PipelineResult run(CustomerConfig config, PipelineDefinition definition) {
        String customerId = config.getCustomerId();
        LOG.info("Starting pipeline '{}' for customer: {}", pipelineId, customerId);

        Instant startTime = Instant.now();
        long totalRecords = 0;
        long quarantinedRecords = 0;

        try {
            // Stage 1: Extract
            Dataset<Row> extracted = runStage("extract", config, definition, () -> {
                DeltaStrategy strategy = DeltaStrategyFactory.getStrategy(config);
                return strategy.extractDelta(spark, config, definition.getSourceTable(),
                        definition.getSourceOptions(), metrics);
            });

            if (extracted == null || extracted.isEmpty()) {
                LOG.info("[{}] No data to process, skipping pipeline", customerId);
                return PipelineResult.skipped(customerId, pipelineId);
            }

            // Stage 2: Validate & Quarantine
            Dataset<Row> validated = runStage("validate", config, definition, () -> {
                if (quarantineManager != null && definition.getValidationRules() != null) {
                    return quarantineManager.quarantineInvalidRecords(extracted,
                            definition.getValidationRules(), customerId,
                            definition.getSourceTable(), pipelineId);
                }
                return extracted;
            });

            quarantinedRecords = extracted.count() - validated.count();

            // Stage 3: Transform
            Dataset<Row> transformed = runStage("transform", config, definition, () -> {
                Dataset<Row> result = validated;
                for (SparkTransform transform : definition.getTransforms()) {
                    LOG.info("[{}] Applying transform: {}", customerId, transform.getName());
                    result = transform.transform(result);
                }
                return result;
            });

            // Stage 4: Load
            runStage("load", config, definition, () -> {
                loadData(transformed, config, definition);
                return null;
            });

            totalRecords = transformed.count();

            // Cleanup checkpoints on success
            if (checkpoint != null) {
                for (String stage : new String[]{"extract", "validate", "transform", "load"}) {
                    checkpoint.cleanupCheckpoint(pipelineId, stage, customerId);
                }
            }

            Instant endTime = Instant.now();
            long durationMs = endTime.toEpochMilli() - startTime.toEpochMilli();

            metrics.recordCounter("pipeline_success", 1, customerId, pipelineId);
            metrics.recordGauge("pipeline_duration_ms", durationMs, customerId, pipelineId);
            metrics.recordGauge("pipeline_records_processed", totalRecords, customerId, pipelineId);

            LOG.info("[{}] Pipeline '{}' completed successfully: {} records in {}ms",
                    customerId, pipelineId, totalRecords, durationMs);

            return PipelineResult.success(customerId, pipelineId, totalRecords, quarantinedRecords, durationMs);

        } catch (Exception e) {
            LOG.error("[{}] Pipeline '{}' failed: {}", customerId, pipelineId, e.getMessage(), e);
            metrics.recordCounter("pipeline_failed", 1, customerId, pipelineId);

            return PipelineResult.failed(customerId, pipelineId, e.getMessage());
        } finally {
            metrics.flush();
        }
    }

    private Dataset<Row> runStage(String stageName, CustomerConfig config, 
                                   PipelineDefinition definition, StageExecutor executor) {
        String customerId = config.getCustomerId();

        // Try to resume from checkpoint
        if (checkpoint != null && checkpoint.hasCheckpoint(pipelineId, stageName, customerId)) {
            LOG.info("[{}] Resuming stage '{}' from checkpoint", customerId, stageName);
            Dataset<Row> checkpointed = checkpoint.loadCheckpoint(pipelineId, stageName, customerId);
            if (checkpointed != null) return checkpointed;
        }

        // Execute stage
        RetryHandler retry = new RetryHandler(config.getRetryPolicy(), metrics, customerId, stageName);
        Dataset<Row> result = retry.executeWithRetry(() -> {
            long stageStart = System.currentTimeMillis();
            Dataset<Row> data = executor.execute();
            long stageDuration = System.currentTimeMillis() - stageStart;

            metrics.recordGauge("stage_duration_ms", stageDuration, customerId, pipelineId, stageName);

            // Save checkpoint
            if (checkpoint != null && config.isCheckpointEnabled()) {
                checkpoint.saveCheckpoint(pipelineId, stageName, customerId, data, Map.of());
            }

            return data;
        });

        return result;
    }

    private void loadData(Dataset<Row> data, CustomerConfig config, PipelineDefinition definition) {
        String targetPath = definition.getTargetPath();
        String format = definition.getTargetFormat();
        String mode = definition.getLoadMode(); // overwrite, append, merge

        LOG.info("[{}] Loading data to: {} (format: {}, mode: {})",
                config.getCustomerId(), targetPath, format, mode);

        if ("merge".equals(mode) && "delta".equals(format)) {
            // Use Delta Lake merge for upserts
            io.delta.tables.DeltaTable deltaTable = io.delta.tables.DeltaTable.forPath(spark, targetPath);
            deltaTable.as("target")
                .merge(data.as("source"), definition.getMergeCondition())
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();
        } else {
            data.write()
                .format(format)
                .mode(mode)
                .save(targetPath);
        }
    }

    @FunctionalInterface
    interface StageExecutor {
        Dataset<Row> execute() throws Exception;
    }
}
