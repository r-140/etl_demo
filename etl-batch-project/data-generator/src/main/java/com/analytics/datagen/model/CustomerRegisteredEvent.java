package com.analytics.datagen.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;

public class CustomerRegisteredEvent extends DomainEvent {

    @JsonProperty("customerId")
    private final Long customerNaturalId;

    @JsonProperty("firstName")
    private final String firstName;

    @JsonProperty("lastName")
    private final String lastName;

    @JsonProperty("email")
    private final String email;

    @JsonProperty("phone")
    private final String phone;

    @JsonProperty("registrationDate")
    private final LocalDate registrationDate;

    public CustomerRegisteredEvent(String customerId, Long customerNaturalId,
                                    String firstName, String lastName, String email,
                                    String phone, LocalDate registrationDate) {
        super("CUSTOMER_REGISTERED", customerId, "customer-service");
        this.customerNaturalId = customerNaturalId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phone = phone;
        this.registrationDate = registrationDate;
    }

    public Long getCustomerNaturalId() { return customerNaturalId; }
    public String getFirstName() { return firstName; }
    public String getLastName() { return lastName; }
    public String getEmail() { return email; }
    public String getPhone() { return phone; }
    public LocalDate getRegistrationDate() { return registrationDate; }
}
