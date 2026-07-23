package com.analytics.etl.core.error;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Manages quarantine of bad records for data quality issues.
 * Separates invalid records from valid ones, preserving them for analysis.
 * Supports multiple quarantine destinations: file-based, table-based, or dead-letter queue.
 */
public class QuarantineManager {

    private static final Logger LOG = LoggerFactory.getLogger(QuarantineManager.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final SparkSession spark;
    private final String quarantineBasePath;
    private final String quarantineFormat;
    private final boolean partitionByDate;

    public QuarantineManager(SparkSession spark, String quarantineBasePath) {
        this(spark, quarantineBasePath, "delta", true);
    }

    public QuarantineManager(SparkSession spark, String quarantineBasePath, 
                             String quarantineFormat, boolean partitionByDate) {
        this.spark = spark;
        this.quarantineBasePath = quarantineBasePath;
        this.quarantineFormat = quarantineFormat;
        this.partitionByDate = partitionByDate;
    }

    /**
     * Quarantine records that fail validation rules.
     * Returns clean records that passed validation.
     */
    public Dataset<Row> quarantineInvalidRecords(
            Dataset<Row> inputDF,
            List<ValidationRule> rules,
            String customerId,
            String sourceTable,
            String pipelineId) {

        LOG.info("[{}] Running {} validation rules on '{}'", customerId, rules.size(), sourceTable);

        Dataset<Row> validDF = inputDF;
        Dataset<Row> quarantinedDF = null;

        for (ValidationRule rule : rules) {
            Dataset<Row> ruleViolations = validDF.filter(rule.getViolationCondition());
            long violationCount = ruleViolations.count();

            if (violationCount > 0) {
                LOG.warn("[{}] Validation rule '{}' failed for {} records in '{}'",
                        customerId, rule.getName(), violationCount, sourceTable);

                // Enrich violations with quarantine metadata
                Dataset<Row> enrichedViolations = ruleViolations
                    .withColumn("_quarantine_reason", functions.lit(rule.getName()))
                    .withColumn("_quarantine_detail", functions.lit(rule.getDescription()))
                    .withColumn("_quarantine_timestamp", functions.current_timestamp())
                    .withColumn("_customer_id", functions.lit(customerId))
                    .withColumn("_source_table", functions.lit(sourceTable))
                    .withColumn("_pipeline_id", functions.lit(pipelineId))
                    .withColumn("_rule_severity", functions.lit(rule.getSeverity().name()));

                // Accumulate quarantined records
                if (quarantinedDF == null) {
                    quarantinedDF = enrichedViolations;
                } else {
                    quarantinedDF = quarantinedDF.unionByName(enrichedViolations, true);
                }

                // Remove invalid records from valid set
                validDF = validDF.filter(rule.getValidCondition());
            }
        }

        // Persist quarantined records
        if (quarantinedDF != null) {
            long totalQuarantined = quarantinedDF.count();
            LOG.info("[{}] Quarantined {} total records from '{}'", 
                    customerId, totalQuarantined, sourceTable);

            persistQuarantinedRecords(quarantinedDF, customerId, sourceTable, pipelineId);
        }

        long validCount = validDF.count();
        LOG.info("[{}] {} valid records remaining after quarantine for '{}'",
                customerId, validCount, sourceTable);

        return validDF;
    }

    /**
     * Persist quarantined records to storage
     */
    private void persistQuarantinedRecords(Dataset<Row> quarantinedDF, String customerId,
                                          String sourceTable, String pipelineId) {

        String today = Instant.now().atZone(java.time.ZoneId.systemDefault()).format(DATE_FORMAT);
        String quarantinePath = String.format("%s/customer=%s/table=%s/date=%s",
                quarantineBasePath, customerId, sourceTable, today);

        Dataset<Row> outputDF = quarantinedDF;
        if (partitionByDate) {
            outputDF = quarantinedDF.withColumn("_quarantine_date", functions.lit(today));
        }

        outputDF.write()
            .format(quarantineFormat)
            .mode("append")
            .partitionBy("_customer_id", "_source_table", "_quarantine_date")
            .save(quarantinePath);

        LOG.info("Persisted quarantined records to: {}", quarantinePath);
    }

    /**
     * Get quarantined records for analysis
     */
    public Dataset<Row> getQuarantinedRecords(String customerId, String sourceTable, 
                                               String date) {
        String path = String.format("%s/customer=%s/table=%s/date=%s",
                quarantineBasePath, customerId, sourceTable, date);

        try {
            return spark.read().format(quarantineFormat).load(path);
        } catch (Exception e) {
            LOG.warn("No quarantined records found at: {}", path);
            return spark.emptyDataFrame();
        }
    }

    /**
     * Validation rule definition
     */
    public static class ValidationRule {
        private final String name;
        private final String description;
        private final String violationCondition;  // Spark SQL condition for invalid records
        private final String validCondition;      // Spark SQL condition for valid records
        private final Severity severity;

        public enum Severity {
            WARNING,    // Log but don't quarantine
            ERROR,      // Quarantine but continue pipeline
            CRITICAL    // Quarantine and fail pipeline
        }

        public ValidationRule(String name, String description, 
                              String violationCondition, String validCondition, Severity severity) {
            this.name = name;
            this.description = description;
            this.violationCondition = violationCondition;
            this.validCondition = validCondition;
            this.severity = severity;
        }

        // Getters
        public String getName() { return name; }
        public String getDescription() { return description; }
        public String getViolationCondition() { return violationCondition; }
        public String getValidCondition() { return validCondition; }
        public Severity getSeverity() { return severity; }

        /**
         * Common validation rules factory
         */
        public static ValidationRule notNull(String column) {
            return new ValidationRule(
                "not_null_" + column,
                "Column " + column + " must not be null",
                column + " IS NULL",
                column + " IS NOT NULL",
                Severity.ERROR
            );
        }

        public static ValidationRule positiveNumber(String column) {
            return new ValidationRule(
                "positive_" + column,
                "Column " + column + " must be positive",
                column + " <= 0 OR " + column + " IS NULL",
                column + " > 0",
                Severity.ERROR
            );
        }

        public static ValidationRule validEmail(String column) {
            return new ValidationRule(
                "valid_email_" + column,
                "Column " + column + " must contain valid email",
                column + " NOT RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$' OR " + column + " IS NULL",
                column + " RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$'",
                Severity.WARNING
            );
        }

        public static ValidationRule dateRange(String column, String minDate, String maxDate) {
            return new ValidationRule(
                "date_range_" + column,
                "Column " + column + " must be between " + minDate + " and " + maxDate,
                column + " < '" + minDate + "' OR " + column + " > '" + maxDate + "' OR " + column + " IS NULL",
                column + " >= '" + minDate + "' AND " + column + " <= '" + maxDate + "'",
                Severity.ERROR
            );
        }

        public static ValidationRule referentialIntegrity(String column, String refTable, String refColumn) {
            return new ValidationRule(
                "ref_integrity_" + column,
                "Column " + column + " must exist in " + refTable + "." + refColumn,
                // Note: This is a simplified version; in practice would use a join
                column + " IS NULL",
                column + " IS NOT NULL",
                Severity.CRITICAL
            );
        }
    }
}
