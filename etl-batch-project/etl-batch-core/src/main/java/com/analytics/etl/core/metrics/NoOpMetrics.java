package com.analytics.etl.core.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No-op metrics implementation for testing and local development.
 */
public class NoOpMetrics implements ETLMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(NoOpMetrics.class);

    @Override
    public void recordCounter(String name, long value, String... labels) {
        LOG.trace("Counter: {} = {} (labels: {})", name, value, String.join(", ", labels));
    }

    @Override
    public void recordGauge(String name, double value, String... labels) {
        LOG.trace("Gauge: {} = {} (labels: {})", name, value, String.join(", ", labels));
    }

    @Override
    public void recordHistogram(String name, double value, String... labels) {
        LOG.trace("Histogram: {} = {} (labels: {})", name, value, String.join(", ", labels));
    }

    @Override
    public void recordTimer(String name, Runnable operation, String... labels) {
        operation.run();
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
