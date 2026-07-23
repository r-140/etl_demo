package com.analytics.etl.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Retry policy configuration for transient failures.
 */
public class RetryPolicyConfig {

    @JsonProperty("maxRetries")
    private int maxRetries = 3;

    @JsonProperty("baseDelayMs")
    private long baseDelayMs = 1000;

    @JsonProperty("maxDelayMs")
    private long maxDelayMs = 30000;

    @JsonProperty("exponentialBackoff")
    private boolean exponentialBackoff = true;

    @JsonProperty("retryableExceptions")
    private String[] retryableExceptions = {
        "java.sql.SQLException",
        "java.net.ConnectException",
        "java.net.SocketTimeoutException",
        "org.apache.spark.SparkException"
    };

    // Getters and setters
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public long getBaseDelayMs() { return baseDelayMs; }
    public void setBaseDelayMs(long baseDelayMs) { this.baseDelayMs = baseDelayMs; }

    public long getMaxDelayMs() { return maxDelayMs; }
    public void setMaxDelayMs(long maxDelayMs) { this.maxDelayMs = maxDelayMs; }

    public boolean isExponentialBackoff() { return exponentialBackoff; }
    public void setExponentialBackoff(boolean exponentialBackoff) { this.exponentialBackoff = exponentialBackoff; }

    public String[] getRetryableExceptions() { return retryableExceptions; }
    public void setRetryableExceptions(String[] retryableExceptions) { this.retryableExceptions = retryableExceptions; }

    /**
     * Calculate delay for a given retry attempt using exponential backoff with jitter
     */
    public long calculateDelay(int attempt) {
        if (!exponentialBackoff) return baseDelayMs;

        long delay = baseDelayMs * (long) Math.pow(2, attempt);
        // Add jitter (±25%)
        double jitter = 0.75 + Math.random() * 0.5;
        delay = (long) (delay * jitter);

        return Math.min(delay, maxDelayMs);
    }
}
