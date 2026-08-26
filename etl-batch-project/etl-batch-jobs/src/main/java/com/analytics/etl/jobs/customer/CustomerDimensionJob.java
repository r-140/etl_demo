package com.analytics.etl.jobs.customer;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.pipeline.ETLPipeline;
import com.analytics.etl.core.pipeline.PipelineDefinition;
import com.analytics.etl.core.pipeline.PipelineResult;
import com.analytics.etl.core.transform.SCDType2Transform;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.analytics.etl.core.metrics.PrometheusMetrics;

/**
 * Customer Dimension ETL Job with SCD Type 2.
 * 
 * Tracks historical changes to customer attributes over time.
 * Produces dim_customer with valid_from, valid_to, is_current flags.
 */
public class CustomerDimensionJob {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerDimensionJob.class);
    private static final String PIPELINE_ID = "customer_dimension_pipeline";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: CustomerDimensionJob <customer-config-json>");
            System.exit(1);
        }

        try {
            CustomerConfig config = new ObjectMapper().registerModule(new JavaTimeModule())
                    .readValue(args[0], CustomerConfig.class);
            SparkSession spark = SparkSession.builder()
                    .appName("CustomerDimensionJob-" + config.getCustomerId())
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getOrCreate();
            ETLMetrics metrics = new PrometheusMetrics(
                    System.getProperty("prometheus.pushgateway", "localhost:9091"),
                    "customer_dimension_job", Map.of("customer_id", config.getCustomerId()));
            try {
                PipelineResult result = run(spark, config, metrics);
                if (!result.isSuccess()) System.exit(1);
            } finally {
                metrics.close();
                spark.stop();
            }
        } catch (Exception e) {
            LOG.error("CustomerDimensionJob failed", e);
            System.exit(1);
        }
    }

    public static PipelineResult run(SparkSession spark, CustomerConfig config, ETLMetrics metrics) {
        List transforms = List.of(
            new SCDType2Transform(
                new String[]{"customer_id"},
                new String[]{"first_name", "last_name", "email", "phone", "segment", "tier"}
            )
        );

        PipelineDefinition definition = PipelineDefinition.builder()
            .name("customer_dimension")
            .sourceTable("customers")
            .transforms(transforms)
            .targetPath("/data/warehouse/" + config.getCustomerId() + "/dim_customer")
            .targetFormat("delta")
            .loadMode("merge")
            .mergeCondition("target.customer_id = source.customer_id AND target.is_current = true")
            .build();

        ETLPipeline pipeline = new ETLPipeline(spark, PIPELINE_ID, metrics, null, null);
        return pipeline.run(config, definition);
    }
}
