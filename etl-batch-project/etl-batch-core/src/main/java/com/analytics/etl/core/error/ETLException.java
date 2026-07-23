package com.analytics.etl.core.error;

/**
 * Base exception for ETL pipeline errors.
 * Categorizes errors for appropriate handling strategies.
 */
public class ETLException extends RuntimeException {

    private final ErrorCategory category;
    private final ErrorSeverity severity;
    private final String pipelineId;
    private final String stage;
    private final String customerId;

    public enum ErrorCategory {
        NETWORK,           // Transient network issues, DB unavailable
        DATA_QUALITY,      // Schema mismatch, validation failures
        RESOURCE,          // OOM, disk full, timeout
        CONFIGURATION,     // Missing config, invalid parameters
        PERMISSION,        // Auth failures, access denied
        LOGIC,             // Business logic errors
        EXTERNAL,          // Third-party service failures
        UNKNOWN            // Unclassified errors
    }

    public enum ErrorSeverity {
        WARNING,      // Non-blocking, can continue
        ERROR,        // Blocking for current task, retry possible
        CRITICAL      // Blocking for entire pipeline, manual intervention needed
    }

    public ETLException(String message, ErrorCategory category, ErrorSeverity severity,
                        String pipelineId, String stage, String customerId) {
        super(message);
        this.category = category;
        this.severity = severity;
        this.pipelineId = pipelineId;
        this.stage = stage;
        this.customerId = customerId;
    }

    public ETLException(String message, Throwable cause, ErrorCategory category, 
                        ErrorSeverity severity, String pipelineId, String stage, String customerId) {
        super(message, cause);
        this.category = category;
        this.severity = severity;
        this.pipelineId = pipelineId;
        this.stage = stage;
        this.customerId = customerId;
    }

    // Getters
    public ErrorCategory getCategory() { return category; }
    public ErrorSeverity getSeverity() { return severity; }
    public String getPipelineId() { return pipelineId; }
    public String getStage() { return stage; }
    public String getCustomerId() { return customerId; }

    /**
     * Check if this error is retryable
     */
    public boolean isRetryable() {
        return category == ErrorCategory.NETWORK ||
               category == ErrorCategory.RESOURCE ||
               category == ErrorCategory.EXTERNAL;
    }

    /**
     * Check if this error should trigger quarantine
     */
    public boolean shouldQuarantine() {
        return category == ErrorCategory.DATA_QUALITY;
    }

    @Override
    public String toString() {
        return String.format("ETLException[category=%s, severity=%s, pipeline=%s, stage=%s, customer=%s]: %s",
                category, severity, pipelineId, stage, customerId, getMessage());
    }
}
