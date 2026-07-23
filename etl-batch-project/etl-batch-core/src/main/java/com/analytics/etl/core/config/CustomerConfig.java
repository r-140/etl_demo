package com.analytics.etl.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Customer-specific configuration for ETL pipelines.
 * Determines storage type, delta strategy, and scaling parameters.
 */
public class CustomerConfig {

    @JsonProperty("customerId")
    private String customerId;

    @JsonProperty("customerName")
    private String customerName;

    @JsonProperty("customerSize")
    private CustomerSize customerSize;

    @JsonProperty("storageType")
    private StorageType storageType;

    @JsonProperty("deltaStrategy")
    private String deltaStrategy;

    @JsonProperty("schemaName")
    private String schemaName;

    @JsonProperty("connectionProperties")
    private Map<String, String> connectionProperties;

    @JsonProperty("batchSize")
    private int batchSize = 10000;

    @JsonProperty("parallelism")
    private int parallelism = 4;

    @JsonProperty("retryPolicy")
    private RetryPolicyConfig retryPolicy;

    @JsonProperty("checkpointEnabled")
    private boolean checkpointEnabled = true;

    @JsonProperty("quarantineEnabled")
    private boolean quarantineEnabled = true;

    public enum CustomerSize {
        SMALL,      // < 1M records -> OLTP (PostgreSQL)
        MEDIUM,     // 1M - 10M records -> OLTP (PostgreSQL)
        LARGE,      // 10M - 100M records -> OLAP (ClickHouse)
        WHALE       // > 100M records -> OLAP (ClickHouse) + Delta Lake
    }

    public enum StorageType {
        OLTP,       // PostgreSQL, 3NF normalized
        OLAP        // ClickHouse, Star Schema
    }

    // Getters and setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }

    public CustomerSize getCustomerSize() { return customerSize; }
    public void setCustomerSize(CustomerSize customerSize) { this.customerSize = customerSize; }

    public StorageType getStorageType() { return storageType; }
    public void setStorageType(StorageType storageType) { this.storageType = storageType; }

    public String getDeltaStrategy() { return deltaStrategy; }
    public void setDeltaStrategy(String deltaStrategy) { this.deltaStrategy = deltaStrategy; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public Map<String, String> getConnectionProperties() { return connectionProperties; }
    public void setConnectionProperties(Map<String, String> connectionProperties) { this.connectionProperties = connectionProperties; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public int getParallelism() { return parallelism; }
    public void setParallelism(int parallelism) { this.parallelism = parallelism; }

    public RetryPolicyConfig getRetryPolicy() { return retryPolicy; }
    public void setRetryPolicy(RetryPolicyConfig retryPolicy) { this.retryPolicy = retryPolicy; }

    public boolean isCheckpointEnabled() { return checkpointEnabled; }
    public void setCheckpointEnabled(boolean checkpointEnabled) { this.checkpointEnabled = checkpointEnabled; }

    public boolean isQuarantineEnabled() { return quarantineEnabled; }
    public void setQuarantineEnabled(boolean quarantineEnabled) { this.quarantineEnabled = quarantineEnabled; }

    /**
     * Auto-determine storage type based on customer size
     */
    public StorageType resolveStorageType() {
        if (storageType != null) return storageType;
        return switch (customerSize) {
            case SMALL, MEDIUM -> StorageType.OLTP;
            case LARGE, WHALE -> StorageType.OLAP;
        };
    }

    /**
     * Auto-determine delta strategy based on customer size and data characteristics
     */
    public String resolveDeltaStrategy() {
        if (deltaStrategy != null && !deltaStrategy.isEmpty()) return deltaStrategy;
        return switch (customerSize) {
            case SMALL -> "TimestampDelta";
            case MEDIUM -> "PartitionDelta";
            case LARGE -> "DeltaLakeMerge";
            case WHALE -> "CdcDelta";
        };
    }

    @Override
    public String toString() {
        return String.format("CustomerConfig{customerId='%s', size=%s, storage=%s, strategy=%s}",
                customerId, customerSize, resolveStorageType(), resolveDeltaStrategy());
    }
}
