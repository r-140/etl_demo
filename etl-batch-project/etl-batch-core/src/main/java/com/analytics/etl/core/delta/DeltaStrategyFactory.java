package com.analytics.etl.core.delta;

import com.analytics.etl.core.config.CustomerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory for creating appropriate DeltaStrategy based on configuration.
 * Uses ServiceLoader for extensibility - new strategies can be added as plugins.
 */
public class DeltaStrategyFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaStrategyFactory.class);
    private static final Map<String, DeltaStrategy> strategies = new HashMap<>();

    static {
        // Register built-in strategies
        registerStrategy(new TimestampDeltaStrategy());
        registerStrategy(new PartitionDeltaStrategy());
        registerStrategy(new CdcDeltaStrategy());
        registerStrategy(new DeltaLakeMergeStrategy());
        registerStrategy(new LandingZoneStrategy());
        registerStrategy(new FullReloadStrategy());

        // Load additional strategies via ServiceLoader
        ServiceLoader.load(DeltaStrategy.class).forEach(DeltaStrategyFactory::registerStrategy);
    }

    private static void registerStrategy(DeltaStrategy strategy) {
        strategies.put(strategy.getStrategyName(), strategy);
        LOG.info("Registered delta strategy: {}", strategy.getStrategyName());
    }

    /**
     * Get the best strategy for a given customer configuration.
     * Falls back to auto-resolution if not explicitly configured.
     */
    public static DeltaStrategy getStrategy(CustomerConfig config) {
        String strategyName = config.resolveDeltaStrategy();
        DeltaStrategy strategy = strategies.get(strategyName);

        if (strategy == null) {
            LOG.warn("Strategy '{}' not found, falling back to auto-detection", strategyName);
            strategy = autoDetectStrategy(config);
        }

        if (!strategy.supports(config)) {
            LOG.warn("Strategy '{}' does not support config, using fallback", strategyName);
            strategy = getFallbackStrategy(config);
        }

        return strategy;
    }

    /**
     * Get strategy by explicit name
     */
    public static DeltaStrategy getStrategy(String strategyName) {
        DeltaStrategy strategy = strategies.get(strategyName);
        if (strategy == null) {
            throw new IllegalArgumentException("Unknown delta strategy: " + strategyName);
        }
        return strategy;
    }

    private static DeltaStrategy autoDetectStrategy(CustomerConfig config) {
        for (DeltaStrategy strategy : strategies.values()) {
            if (strategy.supports(config)) {
                LOG.info("Auto-detected strategy '{}' for customer {}", 
                        strategy.getStrategyName(), config.getCustomerId());
                return strategy;
            }
        }
        throw new IllegalStateException("No suitable delta strategy found for customer: " + config.getCustomerId());
    }

    private static DeltaStrategy getFallbackStrategy(CustomerConfig config) {
        // Fallback chain: CDC -> DeltaLake -> Partition -> Timestamp -> FullReload
        String[] fallbackChain = {"CdcDelta", "DeltaLakeMerge", "PartitionDelta", "TimestampDelta", "FullReload"};
        for (String name : fallbackChain) {
            DeltaStrategy strategy = strategies.get(name);
            if (strategy != null && strategy.supports(config)) {
                return strategy;
            }
        }
        throw new IllegalStateException("No fallback strategy available");
    }
}
