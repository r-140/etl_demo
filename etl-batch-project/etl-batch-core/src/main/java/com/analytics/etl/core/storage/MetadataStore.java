package com.analytics.etl.core.storage;

import com.analytics.etl.core.delta.DeltaMetadata;
import java.util.List;

/**
 * Interface for persisting ETL metadata (watermarks, checkpoints, job history).
 */
public interface MetadataStore {

    void saveMetadata(DeltaMetadata metadata);
    DeltaMetadata loadMetadata(String customerId, String sourceTable, String strategyName);
    List<DeltaMetadata> loadMetadataHistory(String customerId, String sourceTable, int limit);
    void deleteMetadata(String customerId, String sourceTable, String strategyName);

    // Job execution tracking
    void saveJobExecution(JobExecution execution);
    JobExecution loadLastJobExecution(String pipelineId, String customerId);
    List<JobExecution> loadJobExecutions(String pipelineId, String customerId, int limit);

    /**
     * JDBC implementation backed by PostgreSQL
     */
    class JdbcMetadataStore implements MetadataStore {

        private final ConnectionPool connectionPool;

        public JdbcMetadataStore() {
            this.connectionPool = ConnectionPool.getDefault();
        }

        public JdbcMetadataStore(ConnectionPool connectionPool) {
            this.connectionPool = connectionPool;
        }

        @Override
        public void saveMetadata(DeltaMetadata metadata) {
            String sql = "INSERT INTO etl_metadata (customer_id, source_table, strategy_name, " +
                "watermark_value, record_count, status, extraction_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (customer_id, source_table, strategy_name) " +
                "DO UPDATE SET watermark_value = EXCLUDED.watermark_value, " +
                "record_count = EXCLUDED.record_count, " +
                "status = EXCLUDED.status, " +
                "extraction_time = EXCLUDED.extraction_time, " +
                "updated_at = NOW()";
            // Execute with connectionPool
        }

        @Override
        public DeltaMetadata loadMetadata(String customerId, String sourceTable, String strategyName) {
            return null;
        }

        @Override
        public List<DeltaMetadata> loadMetadataHistory(String customerId, String sourceTable, int limit) {
            return List.of();
        }

        @Override
        public void deleteMetadata(String customerId, String sourceTable, String strategyName) {
        }

        @Override
        public void saveJobExecution(JobExecution execution) {
        }

        @Override
        public JobExecution loadLastJobExecution(String pipelineId, String customerId) {
            return null;
        }

        @Override
        public List<JobExecution> loadJobExecutions(String pipelineId, String customerId, int limit) {
            return List.of();
        }
    }
}
