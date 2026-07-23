package com.analytics.etl.core.delta;

import java.time.Instant;
import java.util.Map;

/**
 * Metadata about a delta extraction operation.
 * Used for checkpointing and resumability.
 */
public class DeltaMetadata {

    private final String customerId;
    private final String sourceTable;
    private final String strategyName;
    private final Instant extractionTime;
    private final long recordCount;
    private final String watermarkValue;      // e.g., last timestamp, version number
    private final Map<String, Object> additionalProperties;
    private final ExtractionStatus status;

    public enum ExtractionStatus {
        SUCCESS,
        PARTIAL,
        FAILED,
        EMPTY
    }

    private DeltaMetadata(Builder builder) {
        this.customerId = builder.customerId;
        this.sourceTable = builder.sourceTable;
        this.strategyName = builder.strategyName;
        this.extractionTime = builder.extractionTime;
        this.recordCount = builder.recordCount;
        this.watermarkValue = builder.watermarkValue;
        this.additionalProperties = builder.additionalProperties;
        this.status = builder.status;
    }

    // Getters
    public String getCustomerId() { return customerId; }
    public String getSourceTable() { return sourceTable; }
    public String getStrategyName() { return strategyName; }
    public Instant getExtractionTime() { return extractionTime; }
    public long getRecordCount() { return recordCount; }
    public String getWatermarkValue() { return watermarkValue; }
    public Map<String, Object> getAdditionalProperties() { return additionalProperties; }
    public ExtractionStatus getStatus() { return status; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String customerId;
        private String sourceTable;
        private String strategyName;
        private Instant extractionTime = Instant.now();
        private long recordCount;
        private String watermarkValue;
        private Map<String, Object> additionalProperties;
        private ExtractionStatus status = ExtractionStatus.SUCCESS;

        public Builder customerId(String customerId) { this.customerId = customerId; return this; }
        public Builder sourceTable(String sourceTable) { this.sourceTable = sourceTable; return this; }
        public Builder strategyName(String strategyName) { this.strategyName = strategyName; return this; }
        public Builder extractionTime(Instant extractionTime) { this.extractionTime = extractionTime; return this; }
        public Builder recordCount(long recordCount) { this.recordCount = recordCount; return this; }
        public Builder watermarkValue(String watermarkValue) { this.watermarkValue = watermarkValue; return this; }
        public Builder additionalProperties(Map<String, Object> additionalProperties) { this.additionalProperties = additionalProperties; return this; }
        public Builder status(ExtractionStatus status) { this.status = status; return this; }

        public DeltaMetadata build() {
            return new DeltaMetadata(this);
        }
    }
}
