package com.analytics.etl.core.metrics;

/**
 * Abstraction for ETL metrics collection.
 * Implementations can push to Prometheus, CloudWatch, Datadog, etc.
 */
public interface ETLMetrics {

    void recordCounter(String name, long value, String... labels);
    void recordGauge(String name, double value, String... labels);
    void recordHistogram(String name, double value, String... labels);
    void recordTimer(String name, Runnable operation, String... labels);
    void flush();
    void close();
}
