package com.analytics.etl.jobs.order;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.delta.DeltaStrategyFactory;
import com.analytics.etl.core.error.QuarantineManager;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.metrics.PrometheusMetrics;
import com.analytics.etl.core.pipeline.ETLPipeline;
import com.analytics.etl.core.pipeline.PipelineDefinition;
import com.analytics.etl.core.pipeline.PipelineResult;
import com.analytics.etl.core.transform.AggregationTransform;
import com.analytics.etl.core.transform.DeduplicationTransform;
import com.analytics.etl.core.transform.EnrichmentTransform;
import com.analytics.etl.core.transform.SCDType2Transform;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Order Fact ETL Job.
 * 
 * Pipeline:
 * 1. Extract: orders + order_items (joined at source or in Spark)
 * 2. Validate: data quality rules (positive amounts, valid dates, referential integrity)
 * 3. Transform: deduplication, enrichment with customer/product dims, SCD Type 2 for customer
 * 4. Load: fact_orders (OLAP) + pre-aggregations (daily_sales_summary)
 */
public class OrderFactJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrderFactJob.class);
    private static final String PIPELINE_ID = "order_fact_pipeline";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: OrderFactJob <customer-config-json>");
            System.exit(1);
        }

        String configJson = args[0];
        CustomerConfig config = parseConfig(configJson);

        SparkSession spark = SparkSession.builder()
            .appName("OrderFactJob-" + config.getCustomerId())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();

        ETLMetrics metrics = new PrometheusMetrics(
            System.getProperty("prometheus.pushgateway", "localhost:9091"),
            "order_fact_job",
            Map.of("customer_id", config.getCustomerId())
        );

        try {
            PipelineResult result = run(spark, config, metrics);
            System.exit(result.isSuccess() ? 0 : 1);
        } finally {
            spark.stop();
            metrics.close();
        }
    }

    public static PipelineResult run(SparkSession spark, CustomerConfig config, ETLMetrics metrics) {
        LOG.info("Running OrderFactJob for customer: {}", config.getCustomerId());

        // Source options based on customer size
        Map<String, String> sourceOptions = Map.of(
            "partitionColumn", "id",
            "lowerBound", "0",
            "upperBound", "100000000",
            "timestampColumn", "updated_at"
        );

        // Validation rules
        List<QuarantineManager.ValidationRule> rules = List.of(
            QuarantineManager.ValidationRule.notNull("order_id"),
            QuarantineManager.ValidationRule.positiveNumber("total_amount"),
            QuarantineManager.ValidationRule.positiveNumber("quantity"),
            QuarantineManager.ValidationRule.dateRange("order_date", "2020-01-01", "2030-12-31")
        );

        // Transforms
        List transforms = List.of(
            new DeduplicationTransform(new String[]{"order_id"}, "updated_at"),
            new EnrichmentTransform(spark, "customer_enrichment", "dim_customer", 
                "customer_id", new String[]{"customer_key", "customer_segment"}, true),
            new EnrichmentTransform(spark, "product_enrichment", "dim_product",
                "product_id", new String[]{"product_key", "category", "vendor_name"}, true),
            new AggregationTransform("daily_summary", 
                List.of("date_key", "product_key", "customer_key"),
                List.of(
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.SUM, "total_amount", "revenue"),
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.COUNT, "order_id", "order_count"),
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.SUM, "quantity", "total_quantity")
                ),
                List.of("1 day")
            )
        );

        PipelineDefinition definition = PipelineDefinition.builder()
            .name("order_fact")
            .sourceTable("orders")
            .sourceOptions(sourceOptions)
            .transforms(transforms)
            .validationRules(rules)
            .targetPath("/data/warehouse/" + config.getCustomerId() + "/fact_orders")
            .targetFormat(config.resolveStorageType() == CustomerConfig.StorageType.OLAP ? "clickhouse" : "delta")
            .loadMode("append")
            .build();

        ETLPipeline pipeline = new ETLPipeline(spark, PIPELINE_ID, metrics, null, null);
        return pipeline.run(config, definition);
    }

    private static CustomerConfig parseConfig(String json) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                .readValue(json, CustomerConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse customer config", e);
        }
    }
}
