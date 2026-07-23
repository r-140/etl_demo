package com.analytics.datagen.generator;

import com.analytics.datagen.EventSink;
import com.analytics.datagen.MicroserviceSimulator;
import com.analytics.datagen.config.GeneratorConfig;
import com.analytics.datagen.model.InventoryChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Simulates Inventory Service producing stock movement events.
 */
public class InventoryServiceSimulator implements MicroserviceSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(InventoryServiceSimulator.class);
    private static final Random RANDOM = new Random();

    private final GeneratorConfig config;
    private final EventSink sink;
    private volatile boolean running = true;

    public InventoryServiceSimulator(GeneratorConfig config, EventSink sink) {
        this.config = config;
        this.sink = sink;
    }

    @Override
    public void run() {
        LOG.info("Inventory Service simulator started");

        int eventCount = config.getOrdersPerCustomer() * config.getCustomerCount() / 2;

        for (int i = 0; i < eventCount && running; i++) {
            try {
                generateInventoryEvent();
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LOG.info("Inventory Service simulator finished");
    }

    private void generateInventoryEvent() {
        String customerId = String.format("%03d", RANDOM.nextInt(config.getCustomerCount()) + 1);
        Long productId = (long) (RANDOM.nextInt(config.getProductsPerCustomer()) + 1);

        int oldQty = RANDOM.nextInt(1000);
        int change = RANDOM.nextInt(100) - 50; // Can be positive (restock) or negative (sale)
        int newQty = Math.max(0, oldQty + change);

        String reason = change > 0 
            ? List.of("purchase_order", "return", "adjustment").get(RANDOM.nextInt(3))
            : List.of("sale", "damage", "adjustment").get(RANDOM.nextInt(3));

        InventoryChangedEvent event = new InventoryChangedEvent(
            customerId,
            productId,
            "WH-" + RANDOM.nextInt(5),
            oldQty,
            newQty,
            reason
        );

        sink.send("inventory." + customerId, event);
    }

    @Override
    public void shutdown() {
        running = false;
    }
}
