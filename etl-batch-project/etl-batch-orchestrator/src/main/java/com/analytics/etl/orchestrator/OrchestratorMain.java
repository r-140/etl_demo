package com.analytics.etl.orchestrator;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.delta.DeltaStrategyFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Small configuration-driven entry point used by schedulers. It validates a
 * customer configuration and prints the resolved extraction strategy. Actual
 * Spark jobs remain separate submit targets, which keeps orchestration from
 * embedding a Spark driver.
 */
public final class OrchestratorMain {
    private OrchestratorMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: OrchestratorMain <customer-config.json>");
            System.exit(2);
        }
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        CustomerConfig config = mapper.readValue(Files.readString(Path.of(args[0])), CustomerConfig.class);
        String selected = DeltaStrategyFactory.getStrategy(config).getStrategyName();
        System.out.printf("customer=%s storage=%s delta_strategy=%s%n",
                config.getCustomerId(), config.resolveStorageType(), selected);
    }
}
