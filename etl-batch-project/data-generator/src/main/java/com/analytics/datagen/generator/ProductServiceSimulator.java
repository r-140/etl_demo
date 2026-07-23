package com.analytics.datagen.generator;

import com.analytics.datagen.EventSink;
import com.analytics.datagen.MicroserviceSimulator;
import com.analytics.datagen.config.GeneratorConfig;
import com.analytics.datagen.model.ProductCreatedEvent;
import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates Product Service producing product catalog events.
 */
public class ProductServiceSimulator implements MicroserviceSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(ProductServiceSimulator.class);
    private static final Faker FAKER = new Faker();
    private static final Random RANDOM = new Random();

    private final GeneratorConfig config;
    private final EventSink sink;
    private final AtomicLong productIdGenerator;
    private volatile boolean running = true;

    public ProductServiceSimulator(GeneratorConfig config, EventSink sink) {
        this.config = config;
        this.sink = sink;
        this.productIdGenerator = new AtomicLong(1);
    }

    @Override
    public void run() {
        LOG.info("Product Service simulator started");

        for (int i = 0; i < config.getProductsPerCustomer() * config.getCustomerCount() && running; i++) {
            try {
                generateProductEvent();
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LOG.info("Product Service simulator finished");
    }

    private void generateProductEvent() {
        String customerId = String.format("%03d", RANDOM.nextInt(config.getCustomerCount()) + 1);
        Long productId = productIdGenerator.incrementAndGet();

        BigDecimal price = BigDecimal.valueOf(RANDOM.nextDouble() * 1000 + 5).setScale(2, RoundingMode.HALF_UP);
        BigDecimal cost = price.multiply(BigDecimal.valueOf(0.3 + RANDOM.nextDouble() * 0.5))
            .setScale(2, RoundingMode.HALF_UP);

        ProductCreatedEvent event = new ProductCreatedEvent(
            customerId,
            productId,
            FAKER.commerce().productName(),
            "SKU-" + productId,
            (long) (RANDOM.nextInt(20) + 1),
            (long) (RANDOM.nextInt(50) + 1),
            price,
            cost
        );

        sink.send("products." + customerId, event);
    }

    @Override
    public void shutdown() {
        running = false;
    }
}
