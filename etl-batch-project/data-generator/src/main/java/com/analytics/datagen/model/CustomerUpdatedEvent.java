package com.analytics.datagen.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomerUpdatedEvent extends DomainEvent {

    @JsonProperty("customerId")
    private final Long customerNaturalId;

    @JsonProperty("fieldName")
    private final String fieldName;

    @JsonProperty("oldValue")
    private final String oldValue;

    @JsonProperty("newValue")
    private final String newValue;

    public CustomerUpdatedEvent(String customerId, Long customerNaturalId,
                                 String fieldName, String oldValue, String newValue) {
        super("CUSTOMER_UPDATED", customerId, "customer-service");
        this.customerNaturalId = customerNaturalId;
        this.fieldName = fieldName;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public Long getCustomerNaturalId() { return customerNaturalId; }
    public String getFieldName() { return fieldName; }
    public String getOldValue() { return oldValue; }
    public String getNewValue() { return newValue; }
}
