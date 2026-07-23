package com.analytics.etl.core.error;

import com.analytics.etl.core.config.RetryPolicyConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * Handles retry logic for transient failures.
 * Supports exponential backoff with jitter, circuit breaker pattern,
 * and configurable retryable exceptions.
 */
public class RetryHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RetryHandler.class);

    private final RetryPolicyConfig policy;
    private final ETLMetrics metrics;
    private final String customerId;
    private final String operation;

    // Circuit breaker state
    private int consecutiveFailures = 0;
    private static final int CIRCUIT_BREAKER_THRESHOLD = 5;
    private boolean circuitOpen = false;
    private long circuitOpenTime = 0;
    private static final long CIRCUIT_BREAKER_TIMEOUT_MS = 60000; // 1 minute

    public RetryHandler(RetryPolicyConfig policy, ETLMetrics metrics, 
                        String customerId, String operation) {
        this.policy = policy;
        this.metrics = metrics;
        this.customerId = customerId;
        this.operation = operation;
    }

    /**
     * Execute a callable with retry logic
     */
    public <T> T executeWithRetry(Callable<T> callable) throws ETLException {
        // Check circuit breaker
        if (circuitOpen) {
            if (System.currentTimeMillis() - circuitOpenTime > CIRCUIT_BREAKER_TIMEOUT_MS) {
                LOG.info("Circuit breaker half-open for customer {}, operation {}", customerId, operation);
                circuitOpen = false;
                consecutiveFailures = 0;
            } else {
                throw new ETLException(
                    "Circuit breaker is open for operation: " + operation,
                    ETLException.ErrorCategory.NETWORK,
                    ETLException.ErrorSeverity.ERROR,
                    operation, "retry", customerId
                );
            }
        }

        int attempt = 0;
        Exception lastException = null;

        while (attempt <= policy.getMaxRetries()) {
            try {
                T result = callable.call();

                // Success - reset circuit breaker
                if (consecutiveFailures > 0) {
                    consecutiveFailures = 0;
                    LOG.info("Reset circuit breaker after success for customer {}, operation {}", 
                            customerId, operation);
                }

                if (attempt > 0) {
                    metrics.recordCounter("retry.success_after_retry", 1, customerId, operation);
                    LOG.info("Operation '{}' succeeded after {} retries", operation, attempt);
                }

                return result;

            } catch (Exception e) {
                lastException = e;
                attempt++;

                if (!isRetryable(e) || attempt > policy.getMaxRetries()) {
                    break;
                }

                long delay = policy.calculateDelay(attempt - 1);
                metrics.recordCounter("retry.attempt", 1, customerId, operation);
                metrics.recordGauge("retry.delay_ms", delay, customerId, operation);

                LOG.warn("Attempt {}/{} failed for operation '{}' (customer: {}): {}. Retrying in {}ms...",
                        attempt, policy.getMaxRetries() + 1, operation, customerId, 
                        e.getMessage(), delay);

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ETLException("Retry interrupted", ie,
                            ETLException.ErrorCategory.UNKNOWN,
                            ETLException.ErrorSeverity.CRITICAL,
                            operation, "retry", customerId);
                }
            }
        }

        // All retries exhausted
        consecutiveFailures++;
        if (consecutiveFailures >= CIRCUIT_BREAKER_THRESHOLD) {
            circuitOpen = true;
            circuitOpenTime = System.currentTimeMillis();
            LOG.error("Circuit breaker opened for customer {}, operation {} after {} consecutive failures",
                    customerId, operation, consecutiveFailures);
            metrics.recordCounter("circuit_breaker.opened", 1, customerId, operation);
        }

        metrics.recordCounter("retry.exhausted", 1, customerId, operation);

        throw new ETLException(
            "Operation failed after " + policy.getMaxRetries() + " retries: " + lastException.getMessage(),
            lastException,
            classifyError(lastException),
            ETLException.ErrorSeverity.ERROR,
            operation, "retry", customerId
        );
    }

    /**
     * Check if exception type is in retryable list
     */
    private boolean isRetryable(Exception e) {
        String exceptionClass = e.getClass().getName();
        return Arrays.stream(policy.getRetryableExceptions())
                .anyMatch(retryable -> retryable.equals(exceptionClass) || 
                        isAssignableFrom(retryable, e.getClass()));
    }

    private boolean isAssignableFrom(String parentClassName, Class<?> childClass) {
        try {
            Class<?> parentClass = Class.forName(parentClassName);
            return parentClass.isAssignableFrom(childClass);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Classify exception into error category
     */
    private ETLException.ErrorCategory classifyError(Exception e) {
        String className = e.getClass().getName();
        if (className.contains("SQL") || className.contains("Connect") || className.contains("Socket")) {
            return ETLException.ErrorCategory.NETWORK;
        } else if (className.contains("OutOfMemory") || className.contains("Timeout")) {
            return ETLException.ErrorCategory.RESOURCE;
        } else if (className.contains("IllegalArgument") || className.contains("Configuration")) {
            return ETLException.ErrorCategory.CONFIGURATION;
        } else if (className.contains("Security") || className.contains("Auth")) {
            return ETLException.ErrorCategory.PERMISSION;
        }
        return ETLException.ErrorCategory.UNKNOWN;
    }
}
