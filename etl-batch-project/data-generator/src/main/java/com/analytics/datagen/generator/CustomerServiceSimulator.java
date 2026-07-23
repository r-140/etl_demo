package com.analytics.datagen.generator;

import com.analytics.datagen.EventSink;
import com.analytics.datagen.MicroserviceSimulator;
import com.analytics.datagen.config.GeneratorConfig;
import com.analytics.datagen.model.CustomerRegisteredEvent;
import com.analytics.datagen.model.CustomerUpdatedEvent;
import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates Customer Service producing customer lifecycle events.
 */
public class CustomerServiceSimulator implements MicroserviceSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerServiceSimulator.class);
    private static final Faker FAKER = new Faker();
    private static final Random RANDOM = new Random();

    private final GeneratorConfig config;
    private final EventSink sink;
    private final AtomicLong customerIdGenerator;
    private volatile boolean running = true;

    public CustomerServiceSimulator(GeneratorConfig config, EventSink sink) {
        this.config = config;
        this.sink = sink;
        this.customerIdGenerator = new AtomicLong(1000);
    }

    @Override
    public void run() {
        LOG.info("Customer Service simulator started");

        // Generate initial customer registrations
        for (int i = 0; i < config.getCustomerCount() * 100 && running; i++) {
            try {
                generateCustomerEvent();
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LOG.info("Customer Service simulator finished");
    }

    private void generateCustomerEvent() {
        String customerId = String.format("%03d", RANDOM.nextInt(config.getCustomerCount()) + 1);
        Long customerNaturalId = customerIdGenerator.incrementAndGet();

        CustomerRegisteredEvent event = new CustomerRegisteredEvent(
            customerId,
            customerNaturalId,
            FAKER.name().firstName(),
            FAKER.name().lastName(),
            FAKER.internet().emailAddress(),
            FAKER.phoneNumber().cellPhone(),
            LocalDate.now().minusDays(RANDOM.nextInt(365))
        );

        sink.send("customers." + customerId, event);

        // Occasionally generate update (5% chance)
        if (RANDOM.nextDouble() < 0.05) {
            CustomerUpdatedEvent updateEvent = new CustomerUpdatedEvent(
                customerId,
                customerNaturalId,
                "tier",
                "bronze",
                List.of("silver", "gold", "vip").get(RANDOM.nextInt(3))
            );
            sink.send("customers." + customerId, updateEvent);
        }
    }

    @Override
    public void shutdown() {
        running = false;
    }
}
