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

        String configJson = args[0];
        // Parse and run similar to OrderFactJob
        LOG.info("CustomerDimensionJob starting...");
    }

    public static PipelineResult run(SparkSession spark, CustomerConfig config, ETLMetrics metrics) {
        List transforms = List.of(
            new SCDType2Transform(
                new String[]{"customer_id"},
                new String[]{"name", "email", "segment", "tier"}
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
