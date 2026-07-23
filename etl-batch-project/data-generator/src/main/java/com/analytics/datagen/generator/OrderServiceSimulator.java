package com.analytics.datagen.generator;

import com.analytics.datagen.EventSink;
import com.analytics.datagen.MicroserviceSimulator;
import com.analytics.datagen.config.GeneratorConfig;
import com.analytics.datagen.model.OrderCreatedEvent;
import com.analytics.datagen.model.OrderUpdatedEvent;
import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates Order Service producing order events.
 * Generates CREATE and UPDATE events to Kafka topic: orders.{customerId}
 */
public class OrderServiceSimulator implements MicroserviceSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(OrderServiceSimulator.class);
    private static final Faker FAKER = new Faker();
    private static final Random RANDOM = new Random();

    private final GeneratorConfig config;
    private final EventSink sink;
    private final AtomicLong orderIdGenerator;
    private volatile boolean running = true;

    public OrderServiceSimulator(GeneratorConfig config, EventSink sink) {
        this.config = config;
        this.sink = sink;
        this.orderIdGenerator = new AtomicLong(System.currentTimeMillis());
    }

    @Override
    public void run() {
        LOG.info("Order Service simulator started");

        int eventsPerThread = config.getOrdersPerCustomer() / config.getThreadCount();
        long delayMs = 1000 / config.getRatePerSecond();

        for (int i = 0; i < eventsPerThread && running; i++) {
            try {
                generateOrderEvent();
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LOG.info("Order Service simulator finished");
    }

    private void generateOrderEvent() {
        String customerId = String.format("%03d", ThreadLocalRandom.current().nextInt(1, config.getCustomerCount() + 1));
        Long orderId = orderIdGenerator.incrementAndGet();
        Long customerNaturalId = Long.parseLong(customerId);

        // Generate 1-5 items per order
        int itemCount = RANDOM.nextInt(5) + 1;
        List<OrderCreatedEvent.OrderItem> items = new ArrayList<>();
        BigDecimal totalAmount = BigDecimal.ZERO;

        for (int i = 0; i < itemCount; i++) {
            Long productId = (long) (RANDOM.nextInt(config.getProductsPerCustomer()) + 1);
            int quantity = RANDOM.nextInt(10) + 1;
            BigDecimal unitPrice = BigDecimal.valueOf(RANDOM.nextDouble() * 500 + 10).setScale(2, RoundingMode.HALF_UP);
            BigDecimal discountPercent = BigDecimal.valueOf(RANDOM.nextDouble() * 0.3).setScale(2, RoundingMode.HALF_UP);

            items.add(new OrderCreatedEvent.OrderItem(productId, quantity, unitPrice, discountPercent));

            BigDecimal lineTotal = unitPrice.multiply(BigDecimal.valueOf(quantity))
                .multiply(BigDecimal.ONE.subtract(discountPercent));
            totalAmount = totalAmount.add(lineTotal);
        }

        OrderCreatedEvent event = new OrderCreatedEvent(
            customerId,
            orderId,
            customerNaturalId,
            LocalDate.now().minusDays(RANDOM.nextInt(30)),
            RANDOM.nextDouble() > 0.1 ? "completed" : "pending",
            totalAmount.setScale(2, RoundingMode.HALF_UP),
            items,
            FAKER.address().fullAddress(),
            List.of("credit_card", "debit_card", "paypal", "cash").get(RANDOM.nextInt(4))
        );

        sink.send("orders." + customerId, event);

        // Occasionally generate update event (10% chance)
        if (RANDOM.nextDouble() < 0.1) {
            OrderUpdatedEvent updateEvent = new OrderUpdatedEvent(
                customerId,
                orderId,
                "pending",
                "completed",
                totalAmount
            );
            sink.send("orders." + customerId, updateEvent);
        }
    }

    @Override
    public void shutdown() {
        running = false;
    }
}
