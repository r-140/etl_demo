package com.analytics.etl.core.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Prometheus metrics implementation with Pushgateway support.
 * Designed for batch jobs that complete and push metrics before exit.
 */
public class PrometheusMetrics implements ETLMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetrics.class);
    private static final String METRIC_PREFIX = "etl_batch_";

    private final CollectorRegistry registry;
    private final PushGateway pushGateway;
    private final String jobName;
    private final Map<String, String> defaultLabels;

    // Metric caches
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();
    private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();

    public PrometheusMetrics(String pushGatewayUrl, String jobName, Map<String, String> defaultLabels) {
        this.registry = new CollectorRegistry();
        this.pushGateway = new PushGateway(pushGatewayUrl);
        this.jobName = jobName;
        this.defaultLabels = defaultLabels;

        // Register JVM metrics
        DefaultExports.register(registry);

        LOG.info("Prometheus metrics initialized: job={}, pushgateway={}", jobName, pushGatewayUrl);
    }

    @Override
    public void recordCounter(String name, long value, String... labels) {
        String fullName = METRIC_PREFIX + name;
        Counter counter = counters.computeIfAbsent(fullName, k -> 
            Counter.build()
                .name(k)
                .help("Counter for " + name)
                .labelNames(mergeLabelNames(labels))
                .register(registry)
        );
        counter.labels(mergeLabelValues(labels)).inc(value);
    }

    @Override
    public void recordGauge(String name, double value, String... labels) {
        String fullName = METRIC_PREFIX + name;
        Gauge gauge = gauges.computeIfAbsent(fullName, k ->
            Gauge.build()
                .name(k)
                .help("Gauge for " + name)
                .labelNames(mergeLabelNames(labels))
                .register(registry)
        );
        gauge.labels(mergeLabelValues(labels)).set(value);
    }

    @Override
    public void recordHistogram(String name, double value, String... labels) {
        String fullName = METRIC_PREFIX + name;
        Histogram histogram = histograms.computeIfAbsent(fullName, k ->
            Histogram.build()
                .name(k)
                .help("Histogram for " + name)
                .labelNames(mergeLabelNames(labels))
                .buckets(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)
                .register(registry)
        );
        histogram.labels(mergeLabelValues(labels)).observe(value);
    }

    @Override
    public void recordTimer(String name, Runnable operation, String... labels) {
        long start = System.currentTimeMillis();
        try {
            operation.run();
            recordCounter(name + "_total", 1, labels);
        } catch (Exception e) {
            recordCounter(name + "_errors", 1, labels);
            throw e;
        } finally {
            double duration = (System.currentTimeMillis() - start) / 1000.0;
            recordHistogram(name + "_duration_seconds", duration, labels);
        }
    }

    @Override
    public void flush() {
        try {
            pushGateway.pushAdd(registry, jobName, defaultLabels);
            LOG.debug("Metrics pushed to Pushgateway: job={}", jobName);
        } catch (IOException e) {
            LOG.error("Failed to push metrics: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        flush();
        registry.clear();
    }

    private String[] mergeLabelNames(String... labels) {
        String[] result = new String[defaultLabels.size() + labels.length / 2];
        int i = 0;
        for (String key : defaultLabels.keySet()) result[i++] = key;
        for (int j = 0; j < labels.length; j += 2) result[i++] = labels[j];
        return result;
    }

    private String[] mergeLabelValues(String... labels) {
        String[] result = new String[defaultLabels.size() + labels.length / 2];
        int i = 0;
        for (String value : defaultLabels.values()) result[i++] = value;
        for (int j = 1; j < labels.length; j += 2) result[i++] = labels[j];
        return result;
    }
}
