package com.analytics.datagen.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/**
 * Event produced when a new order is created.
 * Published by Order Service to Kafka topic: orders.{customerId}
 */
public class OrderCreatedEvent extends DomainEvent {

    @JsonProperty("orderId")
    private final Long orderId;

    @JsonProperty("customerId")
    private final Long customerNaturalId;

    @JsonProperty("orderDate")
    private final LocalDate orderDate;

    @JsonProperty("status")
    private final String status;

    @JsonProperty("totalAmount")
    private final BigDecimal totalAmount;

    @JsonProperty("items")
    private final List<OrderItem> items;

    @JsonProperty("shippingAddress")
    private final String shippingAddress;

    @JsonProperty("paymentMethod")
    private final String paymentMethod;

    public OrderCreatedEvent(String customerId, Long orderId, Long customerNaturalId,
                             LocalDate orderDate, String status, BigDecimal totalAmount,
                             List<OrderItem> items, String shippingAddress, String paymentMethod) {
        super("ORDER_CREATED", customerId, "order-service");
        this.orderId = orderId;
        this.customerNaturalId = customerNaturalId;
        this.orderDate = orderDate;
        this.status = status;
        this.totalAmount = totalAmount;
        this.items = items;
        this.shippingAddress = shippingAddress;
        this.paymentMethod = paymentMethod;
    }

    public static class OrderItem {
        @JsonProperty("productId") private Long productId;
        @JsonProperty("quantity") private Integer quantity;
        @JsonProperty("unitPrice") private BigDecimal unitPrice;
        @JsonProperty("discountPercent") private BigDecimal discountPercent;

        public OrderItem(Long productId, Integer quantity, BigDecimal unitPrice, BigDecimal discountPercent) {
            this.productId = productId;
            this.quantity = quantity;
            this.unitPrice = unitPrice;
            this.discountPercent = discountPercent;
        }

        // Getters
        public Long getProductId() { return productId; }
        public Integer getQuantity() { return quantity; }
        public BigDecimal getUnitPrice() { return unitPrice; }
        public BigDecimal getDiscountPercent() { return discountPercent; }
    }

    // Getters
    public Long getOrderId() { return orderId; }
    public Long getCustomerNaturalId() { return customerNaturalId; }
    public LocalDate getOrderDate() { return orderDate; }
    public String getStatus() { return status; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public List<OrderItem> getItems() { return items; }
    public String getShippingAddress() { return shippingAddress; }
    public String getPaymentMethod() { return paymentMethod; }
}
