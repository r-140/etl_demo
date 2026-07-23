package com.analytics.datagen.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public class ProductCreatedEvent extends DomainEvent {

    @JsonProperty("productId")
    private final Long productNaturalId;

    @JsonProperty("name")
    private final String name;

    @JsonProperty("sku")
    private final String sku;

    @JsonProperty("categoryId")
    private final Long categoryId;

    @JsonProperty("vendorId")
    private final Long vendorId;

    @JsonProperty("price")
    private final BigDecimal price;

    @JsonProperty("cost")
    private final BigDecimal cost;

    public ProductCreatedEvent(String customerId, Long productNaturalId, String name,
                               String sku, Long categoryId, Long vendorId,
                               BigDecimal price, BigDecimal cost) {
        super("PRODUCT_CREATED", customerId, "product-service");
        this.productNaturalId = productNaturalId;
        this.name = name;
        this.sku = sku;
        this.categoryId = categoryId;
        this.vendorId = vendorId;
        this.price = price;
        this.cost = cost;
    }

    public Long getProductNaturalId() { return productNaturalId; }
    public String getName() { return name; }
    public String getSku() { return sku; }
    public Long getCategoryId() { return categoryId; }
    public Long getVendorId() { return vendorId; }
    public BigDecimal getPrice() { return price; }
    public BigDecimal getCost() { return cost; }
}
