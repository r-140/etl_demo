package com.analytics.datagen.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class InventoryChangedEvent extends DomainEvent {

    @JsonProperty("productId")
    private final Long productNaturalId;

    @JsonProperty("warehouseId")
    private final String warehouseId;

    @JsonProperty("oldQuantity")
    private final Integer oldQuantity;

    @JsonProperty("newQuantity")
    private final Integer newQuantity;

    @JsonProperty("changeReason")
    private final String changeReason;

    public InventoryChangedEvent(String customerId, Long productNaturalId, String warehouseId,
                                  Integer oldQuantity, Integer newQuantity, String changeReason) {
        super("INVENTORY_CHANGED", customerId, "inventory-service");
        this.productNaturalId = productNaturalId;
        this.warehouseId = warehouseId;
        this.oldQuantity = oldQuantity;
        this.newQuantity = newQuantity;
        this.changeReason = changeReason;
    }

    public Long getProductNaturalId() { return productNaturalId; }
    public String getWarehouseId() { return warehouseId; }
    public Integer getOldQuantity() { return oldQuantity; }
    public Integer getNewQuantity() { return newQuantity; }
    public String getChangeReason() { return changeReason; }
}
