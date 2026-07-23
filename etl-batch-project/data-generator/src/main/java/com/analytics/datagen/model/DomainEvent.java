package com.analytics.datagen.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;
import java.util.UUID;

/**
 * Base class for all domain events produced by microservices.
 * Uses Jackson polymorphic deserialization for Kafka consumers.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "eventType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderCreatedEvent.class, name = "ORDER_CREATED"),
    @JsonSubTypes.Type(value = OrderUpdatedEvent.class, name = "ORDER_UPDATED"),
    @JsonSubTypes.Type(value = CustomerRegisteredEvent.class, name = "CUSTOMER_REGISTERED"),
    @JsonSubTypes.Type(value = CustomerUpdatedEvent.class, name = "CUSTOMER_UPDATED"),
    @JsonSubTypes.Type(value = ProductCreatedEvent.class, name = "PRODUCT_CREATED"),
    @JsonSubTypes.Type(value = InventoryChangedEvent.class, name = "INVENTORY_CHANGED")
})
public abstract class DomainEvent {

    @JsonProperty("eventId")
    private final String eventId;

    @JsonProperty("eventType")
    private final String eventType;

    @JsonProperty("customerId")
    private final String customerId;

    @JsonProperty("timestamp")
    private final Instant timestamp;

    @JsonProperty("schemaVersion")
    private final String schemaVersion;

    @JsonProperty("sourceService")
    private final String sourceService;

    protected DomainEvent(String eventType, String customerId, String sourceService) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.customerId = customerId;
        this.timestamp = Instant.now();
        this.schemaVersion = "1.0";
        this.sourceService = sourceService;
    }

    // Getters
    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public String getCustomerId() { return customerId; }
    public Instant getTimestamp() { return timestamp; }
    public String getSchemaVersion() { return schemaVersion; }
    public String getSourceService() { return sourceService; }
}
