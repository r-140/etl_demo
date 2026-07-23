package com.analytics.datagen;

import com.analytics.datagen.config.GeneratorConfig;
import com.analytics.datagen.generator.*;
import com.analytics.datagen.producer.KafkaEventProducer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.*;

/**
 * Main entry point for the data generator.
 * Simulates multiple microservices producing events to Kafka topics.
 * 
 * Usage:
 *   java -jar data-generator.jar --customers=4 --orders-per-customer=10000 --mode=kafka
 *   java -jar data-generator.jar --customers=4 --orders-per-customer=10000 --mode=direct
 */
@Command(name = "data-generator", 
         description = "Generate test data for ETL pipeline",
         mixinStandardHelpOptions = true)
public class DataGeneratorMain implements Callable<Integer> {

    @Option(names = {"-c", "--customers"}, description = "Number of customers to generate", defaultValue = "4")
    private int customerCount;

    @Option(names = {"--orders-per-customer"}, description = "Orders per customer", defaultValue = "10000")
    private int ordersPerCustomer;

    @Option(names = {"--products-per-customer"}, description = "Products per customer", defaultValue = "1000")
    private int productsPerCustomer;

    @Option(names = {"--mode"}, description = "Output mode: kafka, direct, file", defaultValue = "kafka")
    private String mode;

    @Option(names = {"--kafka-brokers"}, description = "Kafka bootstrap servers", defaultValue = "localhost:9092")
    private String kafkaBrokers;

    @Option(names = {"--threads"}, description = "Number of generator threads", defaultValue = "4")
    private int threads;

    @Option(names = {"--rate"}, description = "Events per second per thread", defaultValue = "100")
    private int ratePerSecond;

    @Option(names = {"--duration"}, description = "Duration in seconds (0 = infinite)", defaultValue = "60")
    private int durationSeconds;

    private final ExecutorService executor;
    private final CountDownLatch completionLatch;

    public DataGeneratorMain() {
        this.executor = Executors.newFixedThreadPool(threads);
        this.completionLatch = new CountDownLatch(1);
    }

    @Override
    public Integer call() throws Exception {
        System.out.println("========================================");
        System.out.println("  ETL Data Generator");
        System.out.println("========================================");
        System.out.println("Customers: " + customerCount);
        System.out.println("Orders per customer: " + ordersPerCustomer);
        System.out.println("Mode: " + mode);
        System.out.println("Threads: " + threads);
        System.out.println("Rate: " + ratePerSecond + " events/sec/thread");
        System.out.println();

        GeneratorConfig config = GeneratorConfig.builder()
                .customerCount(customerCount)
                .ordersPerCustomer(ordersPerCustomer)
                .productsPerCustomer(productsPerCustomer)
                .mode(GeneratorConfig.Mode.valueOf(mode.toUpperCase()))
                .kafkaBrokers(kafkaBrokers)
                .threadCount(threads)
                .ratePerSecond(ratePerSecond)
                .durationSeconds(durationSeconds)
                .build();

        // Initialize output based on mode
        EventSink sink = createSink(config);

        // Create microservice simulators
        MicroserviceSimulator[] services = {
            new OrderServiceSimulator(config, sink),
            new CustomerServiceSimulator(config, sink),
            new ProductServiceSimulator(config, sink),
            new InventoryServiceSimulator(config, sink)
        };

        // Start all services in parallel
        CountDownLatch startLatch = new CountDownLatch(1);
        ExecutorService serviceExecutor = Executors.newFixedThreadPool(services.length);

        for (MicroserviceSimulator service : services) {
            serviceExecutor.submit(() -> {
                try {
                    startLatch.await();
                    service.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Start generation
        System.out.println("Starting data generation...");
        startLatch.countDown();

        // Wait for duration or shutdown
        if (durationSeconds > 0) {
            Thread.sleep(durationSeconds * 1000L);
            System.out.println("Duration reached, shutting down...");
        } else {
            System.out.println("Running indefinitely (Ctrl+C to stop)...");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown signal received...");
                shutdown(services, sink, serviceExecutor);
            }));
            completionLatch.await();
        }

        shutdown(services, sink, serviceExecutor);
        System.out.println("Data generation complete!");
        return 0;
    }

    private EventSink createSink(GeneratorConfig config) {
        return switch (config.getMode()) {
            case KAFKA -> new KafkaEventProducer(config.getKafkaBrokers());
            case DIRECT -> new DirectDatabaseSink();
            case FILE -> new FileEventSink("/data/generator/output");
        };
    }

    private void shutdown(MicroserviceSimulator[] services, EventSink sink, ExecutorService executor) {
        for (MicroserviceSimulator service : services) {
            service.shutdown();
        }
        sink.close();
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DataGeneratorMain()).execute(args);
        System.exit(exitCode);
    }
}
