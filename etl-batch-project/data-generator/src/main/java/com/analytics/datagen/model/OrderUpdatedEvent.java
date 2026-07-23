package com.analytics.datagen.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public class OrderUpdatedEvent extends DomainEvent {

    @JsonProperty("orderId")
    private final Long orderId;

    @JsonProperty("oldStatus")
    private final String oldStatus;

    @JsonProperty("newStatus")
    private final String newStatus;

    @JsonProperty("totalAmount")
    private final BigDecimal totalAmount;

    public OrderUpdatedEvent(String customerId, Long orderId, String oldStatus, 
                             String newStatus, BigDecimal totalAmount) {
        super("ORDER_UPDATED", customerId, "order-service");
        this.orderId = orderId;
        this.oldStatus = oldStatus;
        this.newStatus = newStatus;
        this.totalAmount = totalAmount;
    }

    public Long getOrderId() { return orderId; }
    public String getOldStatus() { return oldStatus; }
    public String getNewStatus() { return newStatus; }
    public BigDecimal getTotalAmount() { return totalAmount; }
}
