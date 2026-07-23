package com.analytics.etl.core.pipeline;

import java.time.Instant;

/**
 * Result of pipeline execution.
 */
public class PipelineResult {

    private final String customerId;
    private final String pipelineId;
    private final Status status;
    private final long recordsProcessed;
    private final long recordsQuarantined;
    private final long durationMs;
    private final String errorMessage;
    private final Instant completedAt;

    public enum Status {
        SUCCESS, FAILED, SKIPPED, PARTIAL
    }

    private PipelineResult(String customerId, String pipelineId, Status status,
                           long recordsProcessed, long recordsQuarantined,
                           long durationMs, String errorMessage) {
        this.customerId = customerId;
        this.pipelineId = pipelineId;
        this.status = status;
        this.recordsProcessed = recordsProcessed;
        this.recordsQuarantined = recordsQuarantined;
        this.durationMs = durationMs;
        this.errorMessage = errorMessage;
        this.completedAt = Instant.now();
    }

    public static PipelineResult success(String customerId, String pipelineId,
                                          long records, long quarantined, long durationMs) {
        return new PipelineResult(customerId, pipelineId, Status.SUCCESS, records, quarantined, durationMs, null);
    }

    public static PipelineResult failed(String customerId, String pipelineId, String error) {
        return new PipelineResult(customerId, pipelineId, Status.FAILED, 0, 0, 0, error);
    }

    public static PipelineResult skipped(String customerId, String pipelineId) {
        return new PipelineResult(customerId, pipelineId, Status.SKIPPED, 0, 0, 0, null);
    }

    // Getters
    public String getCustomerId() { return customerId; }
    public String getPipelineId() { return pipelineId; }
    public Status getStatus() { return status; }
    public long getRecordsProcessed() { return recordsProcessed; }
    public long getRecordsQuarantined() { return recordsQuarantined; }
    public long getDurationMs() { return durationMs; }
    public String getErrorMessage() { return errorMessage; }
    public Instant getCompletedAt() { return completedAt; }

    public boolean isSuccess() { return status == Status.SUCCESS; }
    public boolean isFailed() { return status == Status.FAILED; }
}
