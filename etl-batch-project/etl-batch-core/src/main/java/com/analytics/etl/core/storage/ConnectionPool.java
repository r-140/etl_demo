package com.analytics.etl.core.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connection pool manager using HikariCP.
 * Maintains separate pools per database (OLTP metadata, per-customer OLTP, OLAP).
 */
public class ConnectionPool {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPool.class);
    private static final Map<String, HikariDataSource> pools = new ConcurrentHashMap<>();
    private static final String DEFAULT_POOL = "default";

    public static DataSource getDefault() {
        return getOrCreate(DEFAULT_POOL, Map.of(
            "jdbcUrl", System.getProperty("etl.metadata.url", "jdbc:postgresql://localhost:5432/etl_metadata"),
            "username", System.getProperty("etl.metadata.user", "etl"),
            "password", System.getProperty("etl.metadata.password", "etl"),
            "driverClassName", "org.postgresql.Driver",
            "maximumPoolSize", "10",
            "minimumIdle", "2",
            "connectionTimeout", "30000",
            "idleTimeout", "600000",
            "maxLifetime", "1800000"
        ));
    }

    public static DataSource getOrCreate(String poolName, Map<String, String> properties) {
        return pools.computeIfAbsent(poolName, k -> {
            LOG.info("Creating connection pool: {}", poolName);
            HikariConfig config = new HikariConfig();
            properties.forEach(config::addDataSourceProperty);
            config.setJdbcUrl(properties.get("jdbcUrl"));
            config.setUsername(properties.get("username"));
            config.setPassword(properties.get("password"));
            config.setDriverClassName(properties.getOrDefault("driverClassName", "org.postgresql.Driver"));
            config.setMaximumPoolSize(Integer.parseInt(properties.getOrDefault("maximumPoolSize", "10")));
            config.setMinimumIdle(Integer.parseInt(properties.getOrDefault("minimumIdle", "2")));
            config.setConnectionTimeout(Long.parseLong(properties.getOrDefault("connectionTimeout", "30000")));
            config.setPoolName("etl-pool-" + poolName);
            return new HikariDataSource(config);
        });
    }

    public static Connection getConnection(String poolName) throws SQLException {
        return pools.get(poolName).getConnection();
    }

    public static void closeAll() {
        pools.values().forEach(HikariDataSource::close);
        pools.clear();
    }
}
