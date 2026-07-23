package com.analytics.datagen.producer;

import com.analytics.datagen.EventSink;
import com.analytics.datagen.model.DomainEvent;
import com.analytics.datagen.model.OrderCreatedEvent;
import com.analytics.datagen.model.CustomerRegisteredEvent;
import com.analytics.datagen.model.ProductCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

/**
 * Direct database sink for development/testing.
 * Inserts events directly into PostgreSQL OLTP.
 */
public class DirectDatabaseSink implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(DirectDatabaseSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Connection connection;

    public DirectDatabaseSink() {
        try {
            String url = System.getProperty("db.url", "jdbc:postgresql://localhost:5432/oltp");
            Properties props = new Properties();
            props.setProperty("user", System.getProperty("db.user", "etl"));
            props.setProperty("password", System.getProperty("db.password", "etl"));
            this.connection = DriverManager.getConnection(url, props);
            LOG.info("Direct database sink connected: {}", url);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to database", e);
        }
    }

    @Override
    public void send(String topicSuffix, DomainEvent event) {
        try {
            if (event instanceof OrderCreatedEvent) {
                insertOrder((OrderCreatedEvent) event);
            } else if (event instanceof CustomerRegisteredEvent) {
                insertCustomer((CustomerRegisteredEvent) event);
            } else if (event instanceof ProductCreatedEvent) {
                insertProduct((ProductCreatedEvent) event);
            }
        } catch (SQLException e) {
            LOG.error("Failed to insert event: {}", e.getMessage());
            throw new RuntimeException("Database insert failed", e);
        }
    }

    private void insertOrder(OrderCreatedEvent event) throws SQLException {
        String schema = "customer_" + event.getCustomerId();
        String sql = "INSERT INTO " + schema + ".orders (id, customer_id, order_date, status, total_amount, payment_method) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, event.getOrderId());
            ps.setLong(2, event.getCustomerNaturalId());
            ps.setDate(3, Date.valueOf(event.getOrderDate()));
            ps.setString(4, event.getStatus());
            ps.setBigDecimal(5, event.getTotalAmount());
            ps.setString(6, event.getPaymentMethod());
            ps.executeUpdate();
        }

        // Insert order items
        String itemSql = "INSERT INTO " + schema + ".order_items (order_id, product_id, quantity, unit_price, discount_percent) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(itemSql)) {
            for (OrderCreatedEvent.OrderItem item : event.getItems()) {
                ps.setLong(1, event.getOrderId());
                ps.setLong(2, item.getProductId());
                ps.setInt(3, item.getQuantity());
                ps.setBigDecimal(4, item.getUnitPrice());
                ps.setBigDecimal(5, item.getDiscountPercent());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertCustomer(CustomerRegisteredEvent event) throws SQLException {
        String schema = "customer_" + event.getCustomerId();
        String sql = "INSERT INTO " + schema + ".customers (id, first_name, last_name, email, phone, registration_date) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, event.getCustomerNaturalId());
            ps.setString(2, event.getFirstName());
            ps.setString(3, event.getLastName());
            ps.setString(4, event.getEmail());
            ps.setString(5, event.getPhone());
            ps.setDate(6, Date.valueOf(event.getRegistrationDate()));
            ps.executeUpdate();
        }
    }

    private void insertProduct(ProductCreatedEvent event) throws SQLException {
        String schema = "customer_" + event.getCustomerId();
        String sql = "INSERT INTO " + schema + ".products (id, name, sku, category_id, vendor_id, price, cost) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, event.getProductNaturalId());
            ps.setString(2, event.getName());
            ps.setString(3, event.getSku());
            ps.setLong(4, event.getCategoryId());
            ps.setLong(5, event.getVendorId());
            ps.setBigDecimal(6, event.getPrice());
            ps.setBigDecimal(7, event.getCost());
            ps.executeUpdate();
        }
    }

    @Override
    public void flush() {
        // No-op for JDBC
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("Error closing connection: {}", e.getMessage());
        }
    }
}
