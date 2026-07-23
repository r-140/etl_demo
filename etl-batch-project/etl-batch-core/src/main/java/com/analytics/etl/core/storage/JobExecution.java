package com.analytics.etl.core.storage;

import java.time.Instant;
import java.util.Map;

/**
 * Tracks execution of a pipeline job.
 */
public class JobExecution {

    private String executionId;
    private String pipelineId;
    private String customerId;
    private String stage;
    private Status status;
    private Instant startTime;
    private Instant endTime;
    private long recordsProcessed;
    private long recordsQuarantined;
    private String errorMessage;
    private Map<String, String> metadata;

    public enum Status {
        RUNNING, SUCCESS, FAILED, PARTIAL, SKIPPED
    }

    public String getExecutionId() { return executionId; }
    public void setExecutionId(String executionId) { this.executionId = executionId; }
    public String getPipelineId() { return pipelineId; }
    public void setPipelineId(String pipelineId) { this.pipelineId = pipelineId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getStage() { return stage; }
    public void setStage(String stage) { this.stage = stage; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }
    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }
    public long getRecordsProcessed() { return recordsProcessed; }
    public void setRecordsProcessed(long recordsProcessed) { this.recordsProcessed = recordsProcessed; }
    public long getRecordsQuarantined() { return recordsQuarantined; }
    public void setRecordsQuarantined(long recordsQuarantined) { this.recordsQuarantined = recordsQuarantined; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public Map<String, String> getMetadata() { return metadata; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
}
