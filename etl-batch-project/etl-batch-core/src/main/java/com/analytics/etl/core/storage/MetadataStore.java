package com.analytics.etl.core.storage;

import com.analytics.etl.core.delta.DeltaMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

        private final DataSource dataSource;
        private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        public JdbcMetadataStore() {
            this.dataSource = ConnectionPool.getDefault();
        }

        public JdbcMetadataStore(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public void saveMetadata(DeltaMetadata metadata) {
            String sql = "INSERT INTO etl_metadata (customer_id, source_table, strategy_name, " +
                "watermark_value, record_count, status, extraction_time, additional_properties) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb)) " +
                "ON CONFLICT (customer_id, source_table, strategy_name) " +
                "DO UPDATE SET watermark_value = EXCLUDED.watermark_value, " +
                "record_count = EXCLUDED.record_count, " +
                "status = EXCLUDED.status, " +
                "extraction_time = EXCLUDED.extraction_time, additional_properties = EXCLUDED.additional_properties, " +
                "updated_at = NOW()";
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, metadata.getCustomerId());
                statement.setString(2, metadata.getSourceTable());
                statement.setString(3, metadata.getStrategyName());
                statement.setString(4, metadata.getWatermarkValue());
                statement.setLong(5, metadata.getRecordCount());
                statement.setString(6, metadata.getStatus().name());
                statement.setTimestamp(7, Timestamp.from(metadata.getExtractionTime()));
                statement.setString(8, toJson(metadata.getAdditionalProperties()));
                statement.executeUpdate();
                try (PreparedStatement history = connection.prepareStatement(
                        "INSERT INTO etl_metadata_history (customer_id,source_table,strategy_name,watermark_value," +
                        "record_count,status,extraction_time,additional_properties) VALUES (?,?,?,?,?,?,?,CAST(? AS jsonb))")) {
                    history.setString(1, metadata.getCustomerId());
                    history.setString(2, metadata.getSourceTable());
                    history.setString(3, metadata.getStrategyName());
                    history.setString(4, metadata.getWatermarkValue());
                    history.setLong(5, metadata.getRecordCount());
                    history.setString(6, metadata.getStatus().name());
                    history.setTimestamp(7, Timestamp.from(metadata.getExtractionTime()));
                    history.setString(8, toJson(metadata.getAdditionalProperties()));
                    history.executeUpdate();
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to save extraction metadata", e);
            }
        }

        @Override
        public DeltaMetadata loadMetadata(String customerId, String sourceTable, String strategyName) {
            String sql = "SELECT * FROM etl_metadata WHERE customer_id=? AND source_table=? AND strategy_name=?";
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, customerId);
                statement.setString(2, sourceTable);
                statement.setString(3, strategyName);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next() ? mapMetadata(result) : null;
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to load extraction metadata", e);
            }
        }

        @Override
        public List<DeltaMetadata> loadMetadataHistory(String customerId, String sourceTable, int limit) {
            String sql = "SELECT * FROM etl_metadata_history WHERE customer_id=? AND source_table=? " +
                    "ORDER BY extraction_time DESC LIMIT ?";
            List<DeltaMetadata> items = new ArrayList<>();
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, customerId);
                statement.setString(2, sourceTable);
                statement.setInt(3, limit);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) items.add(mapMetadata(result));
                }
                return items;
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to load metadata history", e);
            }
            executeUpdate("DELETE FROM etl_metadata WHERE customer_id=? AND source_table=? AND strategy_name=?",
                    customerId, sourceTable, strategyName);
        }

        @Override
        public void deleteMetadata(String customerId, String sourceTable, String strategyName) {
        }

        @Override
        public void saveJobExecution(JobExecution execution) {
            String sql = "INSERT INTO etl_job_executions (execution_id,pipeline_id,customer_id,stage,status," +
                    "start_time,end_time,records_processed,records_quarantined,duration_ms,error_message,metadata) " +
                    "VALUES (CAST(? AS uuid),?,?,?,?,?,?,?,?,?,?,CAST(? AS jsonb))";
            String executionId = execution.getExecutionId() == null ? UUID.randomUUID().toString() : execution.getExecutionId();
            try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, executionId);
                statement.setString(2, execution.getPipelineId());
                statement.setString(3, execution.getCustomerId());
                statement.setString(4, execution.getStage());
                statement.setString(5, execution.getStatus().name());
                setInstant(statement, 6, execution.getStartTime());
                setInstant(statement, 7, execution.getEndTime());
                statement.setLong(8, execution.getRecordsProcessed());
                statement.setLong(9, execution.getRecordsQuarantined());
                long duration = execution.getStartTime() != null && execution.getEndTime() != null
                        ? execution.getEndTime().toEpochMilli() - execution.getStartTime().toEpochMilli() : 0;
                statement.setLong(10, duration);
                statement.setString(11, execution.getErrorMessage());
                statement.setString(12, toJson(execution.getMetadata()));
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to save job execution", e);
            }
        }

        @Override
        public JobExecution loadLastJobExecution(String pipelineId, String customerId) {
            List<JobExecution> executions = loadJobExecutions(pipelineId, customerId, 1);
            return executions.isEmpty() ? null : executions.get(0);
        }

        @Override
        public List<JobExecution> loadJobExecutions(String pipelineId, String customerId, int limit) {
            String sql = "SELECT * FROM etl_job_executions WHERE pipeline_id=? AND customer_id=? " +
                    "ORDER BY start_time DESC LIMIT ?";
            List<JobExecution> items = new ArrayList<>();
            try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, pipelineId);
                statement.setString(2, customerId);
                statement.setInt(3, limit);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) items.add(mapExecution(result));
                }
                return items;
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to load job executions", e);
            }
        }

        private DeltaMetadata mapMetadata(ResultSet result) throws SQLException {
            Timestamp extractionTime = result.getTimestamp("extraction_time");
            return DeltaMetadata.builder()
                    .customerId(result.getString("customer_id"))
                    .sourceTable(result.getString("source_table"))
                    .strategyName(result.getString("strategy_name"))
                    .watermarkValue(result.getString("watermark_value"))
                    .recordCount(result.getLong("record_count"))
                    .status(DeltaMetadata.ExtractionStatus.valueOf(result.getString("status")))
                    .extractionTime(extractionTime == null ? Instant.now() : extractionTime.toInstant())
                    .additionalProperties(fromJson(result.getString("additional_properties"), new TypeReference<Map<String, Object>>() {}))
                    .build();
        }

        private JobExecution mapExecution(ResultSet result) throws SQLException {
            JobExecution item = new JobExecution();
            item.setExecutionId(result.getString("execution_id"));
            item.setPipelineId(result.getString("pipeline_id"));
            item.setCustomerId(result.getString("customer_id"));
            item.setStage(result.getString("stage"));
            item.setStatus(JobExecution.Status.valueOf(result.getString("status")));
            item.setStartTime(toInstant(result.getTimestamp("start_time")));
            item.setEndTime(toInstant(result.getTimestamp("end_time")));
            item.setRecordsProcessed(result.getLong("records_processed"));
            item.setRecordsQuarantined(result.getLong("records_quarantined"));
            item.setErrorMessage(result.getString("error_message"));
            item.setMetadata(fromJson(result.getString("metadata"), new TypeReference<Map<String, String>>() {}));
            return item;
        }

        private void executeUpdate(String sql, String... values) {
            try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < values.length; i++) statement.setString(i + 1, values[i]);
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new IllegalStateException("Metadata update failed", e);
            }
        }

        private String toJson(Object value) {
            try { return mapper.writeValueAsString(value == null ? Map.of() : value); }
            catch (Exception e) { throw new IllegalArgumentException("Unable to serialize metadata", e); }
        }

        private <T> T fromJson(String json, TypeReference<T> type) {
            try { return mapper.readValue(json == null ? "{}" : json, type); }
            catch (Exception e) { throw new IllegalArgumentException("Unable to deserialize metadata", e); }
        }

        private static Instant toInstant(Timestamp value) { return value == null ? null : value.toInstant(); }
        private static void setInstant(PreparedStatement statement, int index, Instant value) throws SQLException {
            if (value == null) statement.setNull(index, Types.TIMESTAMP); else statement.setTimestamp(index, Timestamp.from(value));
        }
    }
}
